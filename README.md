# gp_relsizes_stats: Table sizes monitoring tool for Greenplum

### Features
gp_relsizes_stats is an extension for the Greenplum database that calculates and stores statistics on the size of files and tables, occupied space on the disks of the master and segment hosts.

#### Features include
- BackgroundWorker support for collecting statistics automatically
- the ability to fine-tune the timeout values between actions, for example, between launches for different databases, or during file processing to distribute the load over time 

### Supported versions and platforms
At the moment, the program is being tested only for GP6 and Linux.

### Installation
Install from source:
```
git clone git@github.com:yezzey-gp/gp_relsizes_stats.git
cd gp_relsizes_stats
# Build it. Building would require GP installed nearby and sourcing greenplum_path.sh
source <path_to_gp>/greenplum_path.sh
make && make install
```

### Confguration
gp_relsizes_stats configuration parameters:
| **Parameter** | **Type**     | **Default**  | **Default**  |
| ---------------- | --------------- | ------------ | ------------ |
| `gp_relsizes_stats.enabled`          | bool    | false    | Using `gp_relsizes_stats.enabled` you can enable/disable stats collection for database where extension installed.|
| `gp_relsizes_stats.restart_naptime`  | int     | 21600000 | Using `gp_relsizes_stats.restart_naptime` you can set naptime between each startup of collecting process. Value set time in milliseconds. Default is equal to 6 hours.|
| `gp_relsizes_stats.database_naptime` | int     | 0        | Using `gp_relsizes_stats.database_naptime` you can set naptime between collecting stats for each databases. Value set time in milliseconds. Default is equal to 0 milliseconds.|
| `gp_relsizes_stats.file_naptime`     | int     | 1        | Using `gp_relsizes_stats.file_naptime` you can set naptime between each file stats calculating. Value set time in milliseconds. Default is equal to 1 millisecond.|

### About collected data and tables
All tables store actual info about database, but it will be truncated after each collecting operation restart, so for some historical data, check relsizes_stats_schema.table_sizes_history table for some non-actual info (actual for previous restart).

| Name of tbale | Row description | Description |
| ------------- | --------------- | ----------- |
| relsizes_stats_schema.table_sizes_history | (insert_date, nspname, relname, size, mtime) | insert_date - date where rows was inserted into history table, nspname - namespace, relname - name of relation in namespace, size - size of relation, mtime - last time when relation has been modified |
| relsizes_stats_schema.segment_file_sizes | (segment, relfilenode, filepath, size, mtime) | segment - id of segment, relfilenode - relation oid, filepath - path to file on the segment disk, size - size of file, mtime - last time when file has been modified |
| relsizes_stats_schema.table_sizes | (nspname, relname, relsize) | nspname - namespace, relname - name of relation in namespace, relsize - size of that relation |
| relsizes_stats_schema.namespace_sizes | (nspname, nspsize) | nspname - name of namespace, nspsize - size of that namespace |
