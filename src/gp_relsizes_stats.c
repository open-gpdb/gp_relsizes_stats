#include "postgres.h"

#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include <sys/stat.h>

#define MAX_QUERY_SIZE PATH_MAX // obviously 150 is enough (150 > 135 + 10)

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(collect_table_sizes);
PG_FUNCTION_INFO_V1(get_file_sizes_for_database);

void _PG_init(void);
void _PG_fini(void);
static List *get_collectable_db_ids(List *ignored_db_names, MemoryContext saved_context);
static int create_truncate_fill_tables();
static bool is_number(char symbol);
static unsigned int fill_relfilenode(char *name);
static void fill_file_sizes(int segment_id, char *data_dir, FunctionCallInfo fcinfo);
static int get_file_sizes_for_databases(List *databases_ids);

Datum get_file_sizes_for_database(PG_FUNCTION_ARGS);
Datum collect_table_sizes(PG_FUNCTION_ARGS);

static List *get_collectable_db_ids(List *ignored_db_names, MemoryContext saved_context) {
    /* default C typed data */
    int retcode;
    char *sql = "SELECT datname, oid \
                 FROM pg_database \
                 WHERE datname NOT IN ('template0', 'template1', 'diskquota', 'gpperfmon')";

    /* PostgreSQL typed data */
    MemoryContext old_context = MemoryContextSwitchTo(saved_context);
    List *collectable_db_ids = NIL;
    MemoryContextSwitchTo(old_context);

    /* connect to SPI */
    retcode = SPI_connect();
    if (retcode < 0) { /* error */
        elog(ERROR, "gp_table_sizes: SPI_connect returned %d", retcode);
        goto finish_SPI;
    }

    /* execute sql query to get table */
    retcode = SPI_execute(sql, true, 0);

    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_SELECT || SPI_processed < 0) {
        elog(ERROR, "get_collectable_db_ids: SPI_execute returned %d, processed %lu rows", retcode, SPI_processed);
        goto finish_SPI;
    }

    Datum *tuple_values = palloc0(SPI_tuptable->tupdesc->natts * sizeof(*tuple_values));
    bool *tuple_nullable = palloc0(SPI_tuptable->tupdesc->natts * sizeof(*tuple_nullable));

    for (int i = 0; i < SPI_processed; ++i) {
        HeapTuple current_tuple = SPI_tuptable->vals[i];
        heap_deform_tuple(current_tuple, SPI_tuptable->tupdesc, tuple_values, tuple_nullable);

        /* check if datname not in ignored_db_names */
        bool ignored = false;
        ListCell *current_cell;

        foreach (current_cell, ignored_db_names) {
            retcode = strcmp((char *)lfirst(current_cell), DatumGetCString(tuple_values[0]));
            if (retcode == 0) {
                ignored = true;
                break;
            }
        }

        if (!ignored) {
            MemoryContext old_context = MemoryContextSwitchTo(saved_context);
            collectable_db_ids = lappend_int(collectable_db_ids, DatumGetInt32(tuple_values[1]));
            MemoryContextSwitchTo(old_context);
        }
    }

    pfree(tuple_values);
    pfree(tuple_nullable);

finish_SPI:
    /* finish SPI */
    SPI_finish();
    return collectable_db_ids;
}

static bool is_number(char symbol) { return '0' <= symbol && symbol <= '9'; }

static unsigned int fill_relfilenode(char *name) {
    char dst[PATH_MAX];
    memset(dst, 0, PATH_MAX);
    int start_pos = 0, pos = 0;
    while (start_pos < strlen(name) && !is_number(name[start_pos])) {
        ++start_pos;
    }
    while (start_pos < strlen(name) && is_number(name[start_pos])) {
        dst[pos++] = name[start_pos++];
    }
    return strtoul(dst, NULL, 10);
}

static void fill_file_sizes(int segment_id, char *data_dir, FunctionCallInfo fcinfo) {
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("set-valued function called in context that cannot "
                                                                "accept a set")));
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("materialize mode required, but it is not allowed "
                                                              "in this context")));
    }

    /* The tupdesc and tuplestore must be created in ecxt_per_query_memory */
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    /* Makes the output TupleDesc */
    TupleDesc tupdesc;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");
    tupdesc = BlessTupleDesc(tupdesc);

    /* Checks if random access is allowed */
    bool randomAccess = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
    /* Starts the tuplestore */
    Tuplestorestate *tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);

    /* Set the output */
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    Datum outputValues[5];
    bool outputNulls[5];
    MemSet(outputNulls, 0, sizeof(outputNulls));

    /* Returns to the old context */
    MemoryContextSwitchTo(oldcontext);

    /* if {path} is NULL => return */
    if (!data_dir) {
        return;
    }

    char new_path[PATH_MAX];
    char relfilenode[PATH_MAX];
    memset(relfilenode, 0, PATH_MAX);

    DIR *current_dir = AllocateDir(data_dir);
    /* if {current_dir} did not opened => return */
    if (!current_dir) {
        return;
    }

    struct dirent *file;
    /* start itterating in {current_dir} */
    while ((file = ReadDir(current_dir, data_dir)) != NULL) {
        char *filename = file->d_name;

        /* if filename is special as "." or ".." => continue */
        if (strcmp(filename, ".") == 0 || strcmp(filename, "..") == 0) {
            continue;
        }

        /* if limit of PATH_MAX reached skip file */
        if (sprintf(new_path, "%s/%s", data_dir, filename) >= sizeof(new_path)) {
            continue;
        }

        struct stat stb;
        /* do lstat if returned error => continue */
        if (lstat(new_path, &stb) < 0) {
            continue;
        }

        if (S_ISREG(stb.st_mode)) {
            // here write to csv data about file
            // need to write ({start_time}, {segment}, {filename_numbers}, {filename},
            // {stb.st_size}, {stb.st_mtimespec}) can store a lot of different stats
            // here (lstat sys call is MVP (P = player))
            outputValues[0] = Int32GetDatum(segment_id);
            outputValues[1] = ObjectIdGetDatum(fill_relfilenode(filename));
            outputValues[2] = CStringGetTextDatum(new_path);
            outputValues[3] = Int64GetDatum(stb.st_size);
            outputValues[4] = Int64GetDatum(stb.st_mtime);

            /* Builds the output tuple (row) */
            /* Puts in the output tuplestore */
            tuplestore_putvalues(tupstore, tupdesc, outputValues, outputNulls);
        } else if (S_ISDIR(stb.st_mode)) {
            fill_file_sizes(segment_id, new_path, fcinfo);
        }
    }
    FreeDir(current_dir);
}

Datum get_file_sizes_for_database(PG_FUNCTION_ARGS) {
    char cwd[PATH_MAX];
    char data_dir[PATH_MAX];

    int segment_id = GpIdentity.segindex;
    int dboid = PG_GETARG_INT32(0);

    getcwd(cwd, sizeof(cwd));
    sprintf(data_dir, "%s/base/%d", cwd, dboid);

    // i need segment_id, dboid, base_dir (where files placed)
    fill_file_sizes(segment_id, data_dir, fcinfo);

    return (Datum)0;
}

static int get_file_sizes_for_databases(List *databases_ids) {
    /* default C typed data */
    int retcode = 0;
    char query[MAX_QUERY_SIZE];

    /* PostreSQL typed data */
    ListCell *current_cell;

    foreach (current_cell, databases_ids) {
        int dbid = lfirst_int(current_cell);
        sprintf(query, "INSERT INTO gp_toolkit.segment_file_sizes (segment, relfilenode, filepath, size, mtime) \
                SELECT * from get_file_sizes_for_database(%d)",
                dbid);

        /* connect to SPI */
        retcode = SPI_connect();
        if (retcode < 0) { /* error */
            elog(ERROR, "get_file_sizes_for_databases: SPI_connect returned %d", retcode);
            retcode = -1;
            goto finish_SPI;
        }

        /* execute sql query to create table (if it not exists) */
        retcode = SPI_execute(query, false, 0);

        /* check errors if they're occured during execution */
        if (retcode != SPI_OK_INSERT) {
            elog(ERROR, "get_file_sizes_for_databases: SPI_execute returned %d", retcode);
            retcode = -1;
            goto finish_SPI;
        }

    finish_SPI:
        SPI_finish();
        if (retcode < 0) {
            break;
        }
    }

    return retcode;
}

Datum collect_table_sizes(PG_FUNCTION_ARGS) {
    /* default C typed data */
    bool elem_type_by_val;
    bool *args_nulls;
    char elem_alignment_code;
    int16 elem_width;
    int args_count;

    /* PostreSQL typed data */
    ArrayType *ignored_db_names_array;
    Datum *args_datums;
    List *ignored_db_names = NIL, *databases_ids;
    Oid elem_type;
    MemoryContext saved_context = CurrentMemoryContext;

    // put all ignored_db names from fisrt array-argument
    ignored_db_names_array = PG_GETARG_ARRAYTYPE_P(0);
    elem_type = ARR_ELEMTYPE(ignored_db_names_array);
    get_typlenbyvalalign(elem_type, &elem_width, &elem_type_by_val, &elem_alignment_code);
    deconstruct_array(ignored_db_names_array, elem_type, elem_width, elem_type_by_val, elem_alignment_code,
                      &args_datums, &args_nulls, &args_count);
    for (int i = 0; i < args_count; ++i) {
        ignored_db_names =
            lappend(ignored_db_names, (void *)DatumGetCString(DirectFunctionCall1(textout, args_datums[i])));
    }

    databases_ids = get_collectable_db_ids(ignored_db_names, saved_context);

    if (get_file_sizes_for_databases(databases_ids) < 0) {
        PG_RETURN_VOID();
    }

    PG_RETURN_VOID();
}

void _PG_init(void) {
    // nothing to do here for this template, but usually we register hooks here,
    // allocate shared memory, start background workers, etc
}

void _PG_fini(void) {
    // nothing to do here for this template
}
