HDCC
=======

HDCC is a hybrid concurrency control that adaptively employs Calvin and Silo in the same database systems.

HDCC is based on the testbed Deneva, which can be found in the following paper:

    An Evaluation of Distributed Concurrency Control
    Rachael Harding, Dana Van Aken, Andrew Pavlo, Michael Stonebraker
    https://www.vldb.org/pvldb/vol10/p553-harding.pdf

We added full TPC-C (including range query) to the "mixed" branch and the checkpoint mechanism to the "checkpoint" branch for evaluation.

Build & Test
------------

To build the database.

    make deps
    make -j

Configuration
-------------

DBMS configurations can be changed in the config.h file. Here we only list several most important and general ones.

    NODE_CNT          : Number of server nodes in the database
    THREAD_CNT        : Number of worker threads running per server
    WORKLOAD          : Supported workloads include YCSB and TPCC
    CC_ALG            : Concurrency control algorithm. Six algorithms are supported
                        (HDCC, CALVIN, SILO, SNAPPER, ARIA)
    MAX_TXN_IN_FLIGHT  : Maximum number of active transactions at each server at a given time
    DONE_TIMER        : Amount of time to run experiment

Most configs is grouped under a certain category.

Configurations can also be specified as command argument at runtime. Run the following command for a full list of program argument.

    ./rundb -h

Run
---

The DBMS can be run with

    ./rundb -nid[N]
    ./runcl -nid[M]

where N and M are the ID of a server and client, respectively

You can also run HDCC with scripts

    ./run_experiments.py [experiment_name1][experiment_name2]...

The experiments are defined in scripts/experiments.py and where to deploy is defined in scripts/run_config.py.
