#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "tcop/utility.h"

#include "catalog/namespace.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "funcapi.h"

#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define FILEINFO_ARGS_CNT 5
#define HOUR_TIME 3600000 // milliseconds
#define MINUTE_TIME 60000 // milliseconds
#define FILE_NAPTIME 1    //  milliseconds

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(get_stats_for_database);
PG_FUNCTION_INFO_V1(relsizes_collect_stats_once);
Datum get_stats_for_database(PG_FUNCTION_ARGS);
Datum relsizes_collect_stats_once(PG_FUNCTION_ARGS);

static void worker_sigterm(SIGNAL_ARGS);
static Datum *get_databases_oids(int *databases_cnt, MemoryContext ctx, bool create_transaction);
static int update_segment_file_map_table(void);
static int update_table_sizes_history(void);
static void get_stats_for_databases(Datum *databases_oids, int databases_cnt);
static void run_database_stats_worker(void);
static int plugin_created(void);
static BgwHandleStatus WaitForBackgroundWorkerShutdown(BackgroundWorkerHandle *handle);
static void relsizes_shmem_startup(void);
static int truncate_data_in_history(void);
static int put_data_into_history(void);
void _PG_init(void);
void _PG_fini(void);

void relsizes_collect_stats(Datum main_arg);
void relsizes_database_stats_job(Datum args);

/* flags set by signal handlers */

/* GUC variables */
static int worker_restart_naptime = 0;  /* set up in _PG_init() function */
static int worker_database_naptime = 0; /* set up in _PG_init() function */
static int worker_file_naptime = 0;     /* set up in _PG_init() function */
static bool enabled = false;            /* set up in _PG_init() function */

static volatile sig_atomic_t got_sigterm = false;

struct RelsizesSharedData {
    char dbname[NAMEDATALEN + 1];
};
static struct RelsizesSharedData *shared_data;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void worker_sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sigterm = true;
    if (MyProc) {
        SetLatch(&MyProc->procLatch);
    }
    errno = save_errno;
}

/*
 * The functon is borrowed from more recent versions of PG
 *
 * Wait for a background worker to stop.
 *
 * If the worker hasn't yet started, or is running, we wait for it to stop
 * and then return BGWH_STOPPED.  However, if the postmaster has died, we give
 * up and return BGWH_POSTMASTER_DIED, because it's the postmaster that
 * notifies us when a worker's state changes.
 */
static BgwHandleStatus WaitForBackgroundWorkerShutdown(BackgroundWorkerHandle *handle) {
    BgwHandleStatus status;
    int rc;
    bool save_set_latch_on_sigusr1;

    save_set_latch_on_sigusr1 = set_latch_on_sigusr1;
    set_latch_on_sigusr1 = true;

    PG_TRY();
    {
        for (;;) {
            pid_t pid;

            status = GetBackgroundWorkerPid(handle, &pid);
            if (status == BGWH_STOPPED)
                return status;

            rc = WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);

            ResetLatch(&MyProc->procLatch);

            CHECK_FOR_INTERRUPTS();

            if (rc & WL_POSTMASTER_DEATH) {
                status = BGWH_POSTMASTER_DIED;
                break;
            }
        }
    }
    PG_CATCH();
    {
        set_latch_on_sigusr1 = save_set_latch_on_sigusr1;
        PG_RE_THROW();
    }
    PG_END_TRY();

    set_latch_on_sigusr1 = save_set_latch_on_sigusr1;
    return status;
}

static Datum *get_databases_oids(int *databases_cnt, MemoryContext ctx, bool create_transaction) {
    int retcode = 0;
    char *sql = "SELECT datname, oid FROM pg_database WHERE datname NOT IN ('template0', 'template1', 'diskquota', "
                "'gpperfmon')";
    char *error = NULL;

    Datum *databases_oids = NULL;
    *databases_cnt = 0;

    /* get timestamp and start transaction */
    if (create_transaction) {
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
    }

    /* connect to spi */
    retcode = SPI_connect();
    if (retcode < 0) { /* error */
        error = "get_databases_oids: SPI_connect failed";
        goto finish_transaction;
    }
    if (create_transaction) {
        PushActiveSnapshot(GetTransactionSnapshot());
        pgstat_report_activity(STATE_RUNNING, sql);
    }

    /* execute sql query to get table */
    retcode = SPI_execute(sql, true, 0);

    /* check errors if they're occured during execution */
    if (retcode != SPI_OK_SELECT || SPI_processed < 0) { /* error */
        error = "get_databases_oids: SPI_execute failed (select datname, oid)";
        goto finish_spi;
    }

    /* current store  */
    Datum *tuple_values = palloc0(SPI_tuptable->tupdesc->natts * sizeof(*tuple_values));
    bool *tuple_nullable = palloc0(SPI_tuptable->tupdesc->natts * sizeof(*tuple_nullable));

    /* prepare for coping datum variables */
    bool typByVal;
    int16 typLen;
    char typAlign;

    /* allocate memory for result */
    *databases_cnt = SPI_processed;
    MemoryContext old_context = MemoryContextSwitchTo(ctx);
    databases_oids = palloc0(SPI_tuptable->tupdesc->natts * (*databases_cnt) * sizeof(*databases_oids));
    MemoryContextSwitchTo(old_context);

    for (int i = 0; i < SPI_processed; ++i) {
        /* fetch tuple from tuptable */
        HeapTuple current_tuple = SPI_tuptable->vals[i];
        heap_deform_tuple(current_tuple, SPI_tuptable->tupdesc, tuple_values, tuple_nullable);
        /* store tuple in Datum* */
        old_context = MemoryContextSwitchTo(ctx);
        /* copy datum */
        get_typlenbyvalalign(NAMEOID, &typLen, &typByVal, &typAlign);
        databases_oids[2 * i] = datumCopy(tuple_values[0], typByVal, typLen);
        /* copy datum */
        get_typlenbyvalalign(INT8OID, &typLen, &typByVal, &typAlign);
        databases_oids[2 * i + 1] = datumCopy(tuple_values[1], typByVal, typLen);
        /* sitch back */
        MemoryContextSwitchTo(old_context);
    }

    pfree(tuple_values);
    pfree(tuple_nullable);

finish_spi:
    SPI_finish();
finish_transaction:
    if (create_transaction) {
        PopActiveSnapshot();
        CommitTransactionCommand();
        pgstat_report_stat(false);
        pgstat_report_activity(STATE_IDLE, NULL);
    }

    if (error != NULL) {
        ereport(ERROR, (errmsg("%s: %m", error)));
    }

    return databases_oids;
}

static int update_segment_file_map_table() {
    int retcode = 0;
    char *sql_truncate = "TRUNCATE TABLE relsizes_stats_schema.segment_file_map";
    char *sql_insert = "INSERT INTO relsizes_stats_schema.segment_file_map SELECT gp_segment_id, oid, relfilenode FROM "
                       "gp_dist_random('pg_class')";
    char *error = NULL;
    /* update report activity */
    pgstat_report_activity(STATE_RUNNING, sql_truncate);
    /* truncate table */
    retcode = SPI_execute(sql_truncate, false, 0);
    if (retcode != SPI_OK_UTILITY) {
        error = "update_segment_file_map_table: failed to truncate table";
        goto cleanup;
    }
    /* update report activity */
    pgstat_report_activity(STATE_RUNNING, sql_insert);
    /* insert new rows */
    retcode = SPI_execute(sql_insert, false, 0);
    if (retcode != SPI_OK_INSERT) {
        error = "update_segment_file_map_table: failed to insert new rows into table";
        goto cleanup;
    }

cleanup:
    pgstat_report_activity(STATE_IDLE, NULL);
    if (error != NULL) {
        ereport(WARNING, (errmsg("%s: %m", error)));
    }
    return retcode;
}

static bool is_number(char symbol) { return '0' <= symbol && symbol <= '9'; }

/* fill_relfilenode(char *name) - finds first group of nubers in {name}
 * and returns it numeric value
 */
static unsigned int fill_relfilenode(char *name) {
    unsigned int result = 0, pos = 0;
    while (pos < strlen(name) && !is_number(name[pos])) {
        ++pos;
    }
    while (pos < strlen(name) && is_number(name[pos])) {
        result = (result * 10 + (name[pos] - '0'));
        ++pos;
    }
    return result;
}

void relsizes_database_stats_job(Datum args) {
    int retcode = 0;
    char *sql = NULL;
    char *error = NULL;

    pqsignal(SIGTERM, worker_sigterm);
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnection(shared_data->dbname, NULL);

    /* get timestamp and start transaction */
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();

    /* connect to SPI */
    retcode = SPI_connect();
    if (retcode < 0) { /* error */
        error = "relsizes_database_stats_job: SPI_connect failed";
        goto finish_transaction;
    }
    PushActiveSnapshot(GetTransactionSnapshot());

    /* check if plugin created */
    int created = plugin_created();
    if (created < 0) {
        error = "relsizes_database_stats_job: SPI execute failed while looking for plugin";
        goto finish_spi;
    } else if (created == 0) {
        goto finish_spi;
    }

    retcode = update_segment_file_map_table();
    if (retcode < 0) {
        error = "relsizes_database_stats_job: updating segment_file_map failed";
        goto finish_spi;
    }

    /* update report activity */
    char *sql_truncate = "TRUNCATE TABLE relsizes_stats_schema.segment_file_sizes";
    pgstat_report_activity(STATE_RUNNING, sql_truncate);
    retcode = SPI_execute(sql_truncate, false, 0);
    if (retcode != SPI_OK_UTILITY || SPI_processed < 0) { /* error */
        error = "relsizes_database_stats_job: SPI_execute failed (truncate segment_file_sizes)";
        goto finish_spi;
    }

    sql = psprintf("INSERT INTO relsizes_stats_schema.segment_file_sizes (segment, relfilenode, filepath, size, mtime) "
                   "SELECT * FROM relsizes_stats_schema.get_stats_for_database(%d)",
                   MyDatabaseId);
    pgstat_report_activity(STATE_RUNNING, sql);
    retcode = SPI_execute(sql, false, 0);
    if (retcode != SPI_OK_INSERT) {
        error = "relsizes_database_stats_job: SPI_execute failed (insert into segment_file_sizes)";
        goto finish_spi;
    }

    retcode = update_table_sizes_history();
    if (retcode < 0) {
        error = "relsizes_database_stats_job: updating tables sizes history table failed";
        goto finish_spi;
    }

finish_spi:
    if (sql != NULL) {
        pfree(sql);
    }
    if (error != NULL) {
        ereport(ERROR, (errmsg("%s: %m", error)));
    }
    SPI_finish();
finish_transaction:
    PopActiveSnapshot();
    CommitTransactionCommand();
    pgstat_report_stat(false);
    pgstat_report_activity(STATE_IDLE, NULL);
}

static void run_database_stats_worker() {
    bool ret;
    MemoryContext old_ctx;
    BackgroundWorkerHandle *handle;
    BgwHandleStatus status;
    /* allocate shared memory, start background workers, etc */
    BackgroundWorker database_worker;
    /* set up common data for our worker */
    memset(&database_worker, 0, sizeof(database_worker));
    database_worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    database_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    database_worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf(database_worker.bgw_library_name, "gp_relsizes_stats");
    sprintf(database_worker.bgw_function_name, "relsizes_database_stats_job");
    database_worker.bgw_notify_pid = MyProcPid;
    database_worker.bgw_main_arg = (Datum)0;
    database_worker.bgw_start_rule = NULL;
    /* Fill in worker-specific data, and do the actual registrations. */
    snprintf(database_worker.bgw_name, BGW_MAXLEN, "database_relsizes_collector_worker for %s", shared_data->dbname);
    old_ctx = MemoryContextSwitchTo(TopMemoryContext);
    ret = RegisterDynamicBackgroundWorker(&database_worker, &handle);
    MemoryContextSwitchTo(old_ctx);
    if (!ret) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not register background process"),
                        errhint("You may need to increase max_worker_processes.")));
    }
    pid_t pid;
    status = WaitForBackgroundWorkerStartup(handle, &pid);
    if (status != BGWH_STARTED) {
        ereport(ERROR, (errmsg("Failed to start background worker [%s]", database_worker.bgw_name)));
    }
    status = WaitForBackgroundWorkerShutdown(handle);
    if (status != BGWH_STOPPED) {
        ereport(ERROR, (errmsg("Failure during background worker execution [%s]", database_worker.bgw_name)));
    }
}

Datum get_stats_for_database(PG_FUNCTION_ARGS) {
    int retcode;
    int segment_id = GpIdentity.segindex;
    int dboid = PG_GETARG_INT32(0);

    char cwd[PATH_MAX];
    char *data_dir = NULL;
    char *error = NULL;
    char *file_path = NULL;

    getcwd(cwd, sizeof(cwd));
    data_dir = psprintf("%s/base/%d", cwd, dboid);
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    /* Check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        error = "get_stats_for_databa: set-valued function called in context that cannot accept a set";
        goto finish_data;
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        error = "get_stats_for_database: materialize mode required, but it is not allowed in this context";
        goto finish_data;
    }

    /* Switch to query context */
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    /* Make the output TupleDesc */
    TupleDesc tupdesc;
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        MemoryContextSwitchTo(oldcontext);
        error = "get_stats_for_database: incorrect return type in fcinfo (must be a row type)";
        goto finish_data;
    }
    tupdesc = BlessTupleDesc(tupdesc);

    /* Checks if random access is allowed */
    bool randomAccess = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
    /* Starts the tuplestore */
    Tuplestorestate *tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);
    /* Set the output */
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    Datum outputValues[FILEINFO_ARGS_CNT];
    bool outputNulls[FILEINFO_ARGS_CNT];
    MemSet(outputNulls, 0, sizeof(outputNulls));

    /* Returns to the old context */
    MemoryContextSwitchTo(oldcontext);

    DIR *current_dir = AllocateDir(data_dir);
    /* if {current_dir} did not opened => return */
    if (!current_dir) {
        error = "get_stats_for_database: failed to allocate current directory";
        goto finish_data;
    }

    struct dirent *file;
    /* start itterating in {current_dir} */
    while ((file = ReadDir(current_dir, data_dir)) != NULL) {
        char *filename = file->d_name;
        if (strcmp(filename, ".") == 0 ||
            strcmp(filename, "..") == 0) { /* if filename is special as "." or ".." => continue */
            continue;
        }

        file_path = psprintf("%s/%s", data_dir, filename);
        struct stat stb;
        if (lstat(file_path, &stb) < 0) { /* do lstat if returned error => continue */
            ereport(WARNING,
                    (errmsg("get_stats_for_database: lstat failed with %s file (unexpected behavior)", file_path)));
            pfree(file_path);
            continue;
        }

        if (S_ISREG(stb.st_mode)) {
            /* If file is regular we should count
             * its size and put values into tupstore
             *
             * insert tuple:
             * (segment_is, relfilenode, file_path, stb.st_size, stb.st_mtime)
             */

            outputValues[0] = Int32GetDatum(segment_id);
            outputValues[1] = ObjectIdGetDatum(fill_relfilenode(filename));
            outputValues[2] = CStringGetTextDatum(file_path);
            outputValues[3] = Int64GetDatum(stb.st_size);
            outputValues[4] = Int64GetDatum(stb.st_mtime);

            /* Builds the output tuple (row)
             * and put it in the tuplestore
             */
            tuplestore_putvalues(tupstore, tupdesc, outputValues, outputNulls);

            /* sleep between file proccessing */
            retcode =
                WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, worker_file_naptime);
            ResetLatch(&MyProc->procLatch);

            CHECK_FOR_INTERRUPTS();

            /* emergency bailout if postmaster has died */
            if (retcode & WL_POSTMASTER_DEATH) {
                proc_exit(1);
            }
        }
        pfree(file_path);
    }

    FreeDir(current_dir);
finish_data:
    pfree(data_dir);
    if (error != NULL) {
        ereport(ERROR, (errmsg("%s: %m", error)));
    }

    return (Datum)0;
}

static void get_stats_for_databases(Datum *databases_oids, int databases_cnt) {
    for (int i = 0; i < databases_cnt; ++i) {
        int retcode = 0;
        char *dbname = NameStr(*DatumGetName(databases_oids[2 * i]));
        strncpy(shared_data->dbname, dbname, NAMEDATALEN);
        run_database_stats_worker();
        retcode = WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                            (worker_database_naptime / databases_cnt));
        ResetLatch(&MyProc->procLatch);
        CHECK_FOR_INTERRUPTS();
        /* emergency bailout if postmaster has died */
        if (retcode & WL_POSTMASTER_DEATH) {
            proc_exit(1);
        }
    }
}

static int plugin_created() {
    char *sql = "SELECT * FROM pg_extension WHERE extname = 'gp_relsizes_stats'";
    pgstat_report_activity(STATE_RUNNING, sql);
    int retcode = SPI_execute(sql, true, 0);
    pgstat_report_activity(STATE_IDLE, NULL);
    return (retcode == SPI_OK_SELECT ? SPI_processed : -1);
}

static int truncate_data_in_history() {
    char *sql = "TRUNCATE TABLE relsizes_stats_schema.table_sizes_history";

    /* report to pg_stat_activity about query */
    pgstat_report_activity(STATE_RUNNING, sql);
    return (SPI_execute(sql, false, 0) == SPI_OK_UTILITY ? 0 : -1);
}

static int put_data_into_history() {
    char *sql = "INSERT INTO relsizes_stats_schema.table_sizes_history SELECT CURRENT_DATE, * FROM "
                "relsizes_stats_schema.table_sizes";

    /* report to pg_stat_activity about query */
    pgstat_report_activity(STATE_RUNNING, sql);
    return (SPI_execute(sql, false, 0) == SPI_OK_INSERT && SPI_processed >= 0 ? 0 : -1);
}

static int update_table_sizes_history() {
    int retcode = 0;
    char *error = NULL;

    /* truncate old data in history table */
    retcode = truncate_data_in_history();
    if (retcode < 0) {
        error = "update_table_sizes_history: trucate old data failed";
        goto cleanup;
    }

    /* put collected data into history table */
    retcode = put_data_into_history();
    if (retcode < 0) {
        error = "update_table_sizes_history: put actual data into history failed";
    }

cleanup:
    pgstat_report_activity(STATE_IDLE, NULL);
    if (error != NULL) {
        ereport(WARNING, (errmsg("%s: %m", error)));
    }
    return retcode;
}

void relsizes_collect_stats(Datum main_arg) {
    int retcode = 0;
    int databases_cnt;
    Datum *databases_oids;

    pqsignal(SIGTERM, worker_sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", NULL);

    while (!got_sigterm) {

        /* check if background worker enabled */
        char *enabled_option = GetConfigOptionByName("gp_relsizes_stats.enabled", NULL);
        if (strcmp(enabled_option, "on") != 0) {
            goto bgw_sleep;
        }

        /* get databases oids with database's names */
        databases_oids = get_databases_oids(&databases_cnt, CurrentMemoryContext, true);
        /* start collecting stats for databases */
        get_stats_for_databases(databases_oids, databases_cnt);
        /* free allocated memory for data about databases */
        pfree(databases_oids);
    bgw_sleep:
        /* sleep for restart_naptime time */
        retcode =
            WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, worker_restart_naptime);
        ResetLatch(&MyProc->procLatch);
        CHECK_FOR_INTERRUPTS();
        /* emergency bailout if postmaster has died */
        if (retcode & WL_POSTMASTER_DEATH) {
            proc_exit(1);
        }
    }
}

Datum relsizes_collect_stats_once(PG_FUNCTION_ARGS) {
    int databases_cnt;
    Datum *databases_oids;

    /* get databases oids with database's names */
    databases_oids = get_databases_oids(&databases_cnt, CurrentMemoryContext, false);
    /* start collecting stats for databases */
    get_stats_for_databases(databases_oids, databases_cnt);
    /* free allocated memory for data about databases */
    pfree(databases_oids);

    PG_RETURN_VOID();
}

static void relsizes_shmem_startup() {
    bool found;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    shared_data =
        (struct RelsizesSharedData *)(ShmemInitStruct("relsizes_stats", sizeof(struct RelsizesSharedData), &found));
    if (!found) {
        memset(shared_data->dbname, 0, sizeof(shared_data->dbname));
    }
    LWLockRelease(AddinShmemInitLock);
}

void _PG_init(void) {
    /* define GUC bgw enable flag */
    DefineCustomBoolVariable("gp_relsizes_stats.enabled", "Enable main background worker flag", NULL, &enabled, false,
                             PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);
    /* define GUC naptime variables */
    DefineCustomIntVariable("gp_relsizes_stats.restart_naptime", "Duration between every collect-phases (in ms).", NULL,
                            &worker_restart_naptime,
                            6 * HOUR_TIME, /* set naptime between check-phase (in milliseconds) */
                            0, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("gp_relsizes_stats.database_naptime", "Duration between collect-phase for db (in ms).",
                            NULL, &worker_database_naptime,
                            0, /* set naptime between collecting stats of databases (in milliseconds) */
                            0, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("gp_relsizes_stats.file_naptime", "Duration between each collect-phase for files (in ms).",
                            NULL, &worker_file_naptime,
                            FILE_NAPTIME, /* set naptime between check-phase (in milliseconds) */
                            0, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);

    if (!process_shared_preload_libraries_in_progress) {
        return;
    }

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = relsizes_shmem_startup;

    /* allocate shared memory, start background workers, etc */
    BackgroundWorker worker;
    /* set up common data for our worker */
    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf(worker.bgw_library_name, "gp_relsizes_stats");
    sprintf(worker.bgw_function_name, "relsizes_collect_stats");
    worker.bgw_notify_pid = 0;
    worker.bgw_start_rule = NULL;

    /* Fill in worker-specific data, and do the actual registrations. */
    snprintf(worker.bgw_name, BGW_MAXLEN, "gp_relsizes_stats_worker");
    worker.bgw_main_arg = Int32GetDatum(0);
    RegisterBackgroundWorker(&worker);
}

void _PG_fini(void) { shmem_startup_hook = prev_shmem_startup_hook; }
