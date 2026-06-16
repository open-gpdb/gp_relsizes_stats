<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

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
git clone git@github.com:open-gpdb/gp_relsizes_stats.git
cd gp_relsizes_stats
# Build it. Building would require GP installed nearby and sourcing greenplum_path.sh
source <path_to_gp>/greenplum_path.sh
make && make install
```

### Confguration
gp_relsizes_stats configuration parameters:
| **Parameter** | **Type**     | **Default**  | **Description**  |
| ---------------- | --------------- | ------------ | ------------ |
| `gp_relsizes_stats.enabled`              | bool    | false    | You can enable/disable background stats collection for database where extension installed (actually enable/disable background worker which collecting stats).|
| `gp_relsizes_stats.save_history`         | bool    | true    | You can disable the collection of statistics records in the history table (table_sizes_history).|
| `gp_relsizes_stats.restart_naptime`      | int     | 21600000 | You can set naptime between each startup of collecting process. Value set time in milliseconds. Default is equal to 6 hours.|
| `gp_relsizes_stats.database_naptime`     | int     | 0        | You can set naptime between collecting stats for each databases. Value set time in milliseconds. Default is equal to 0 milliseconds.|
| `gp_relsizes_stats.file_naptime`         | int     | 1        | You can set naptime between each file stats calculating. Value set time in milliseconds. Default is equal to 1 millisecond.|

### Usage
You can use a background worker to collect statistics, but if you sometimes need to change the format of the settings or if you don't want to collect statistics on a regular basis, you can do so. In these situations, you could set
```
gp_relsizes_stats.enabled = off
```

And use the function
```
relsizes_stats_schema.relsizes_collect_stats_once()
```
which can be called manually using 'select'.
It will launch a single statistics collection procedure.


### About collected data and tables
| Name of table | Row description | Description |
| ------------- | --------------- | ----------- |
| relsizes_stats_schema.segment_file_sizes | (segment, relfilenode, filepath, size, mtime) | Current size and last modify time of each file of specific relation on specific segment |
| relsizes_stats_schema.namespace_sizes | (nspname, nspsize) | Current size of namespace |
| relsizes_stats_schema.table_sizes | (nspname, relname, relsize) | Current size of relation in specific namespace |
| relsizes_stats_schema.table_sizes_history | (insert_date, nspname, relname, size, mtime) | Size and last modify time of relation in specific namespace with date when information was collected |
