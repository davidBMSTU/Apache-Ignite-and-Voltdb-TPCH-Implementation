# Implementation of TPC-H like workload for Apache Ignite

## How to use it:
1. Set ignite_core_lib variable in config.conf file. That must be a path to ignite-core-*.*.*.jar. Usually located in IGNITE_HOME/libs
2. Set IP verible in config.conf file with IP of any ignite cluster node. If you start it locally - keep the value localhost
3. python3 prepare.py - it will generate test data if needed(you can control this with setting gen_data variable value in config.conf file), create test tables and put data into them
4. python3 run.py - it will run queries and put results into the res/ dir

## Also you can set(in config.conf file):
1. distributed_joins variable value(https://apacheignite-sql.readme.io/docs/distributed-joins) 2. Control indexies creating by setting CREATE_INDEXES variable value
3. scale - volume of generating by TPC-H dbgen data. If scale = 1, data volume = 1Gb
4. numruns - how many time each query will run. If numruns > 1 -- result time will be an average time