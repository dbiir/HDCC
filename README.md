# Dependencies
The implementation of HDCC depends on some dynamic libraries (such as libnanomsg.so.5) and some static libraries (such as boost).

Please use ldd command to check any missing dynamic libraries, as shown below:
```bash
ldd ./rundb
ldd ./runcl
# rundb and runcl is corresponding binary file after compile
```
Compiler will tell information about missing but necessary static libraries, just follow its instructions.

# Build
To build the database. 

The output of compile is two binary files: rundb and runcl, corresponding to server and client.

Note: Once configuration is changed, recompile is needed.
```bash
make deps
make -j4 # you can append appropriate number to boost compile
```

# Configuration
## Compile Configuration
All compile configuration is listed in config.h. Please refer to README for the meaning of them. Here we list several most important ones.
```
NODE_CNT            : number of computation nodes modeled in the system
THREAD_CNT	        : number of worker threads
REM_THREAD_CNT	    : number of message receiver threads
SEND_THREAD_CNT	    : number of message sender threads
DONE_TIMER          : number of nanoseconds to run experiment
WARMUP_TIMER		: number of nanoseconds to run for warmup
WORKLOAD            : Supported workloads: YCSB, TPCC
CC_ALG              : Concurrency control algorithm, for the test of HDCC, please set it to HDCC
MAX_TXN_IN_FLIGHT   : Maximum number of active transactions at each server at a given time
MPR                 : Ratio of distributed transactions
PRORATE_RATIO       : Ratio of transations of unkown access set
TUP_WRITE_PERC      : Ratio of write operation in a transaction 
TXN_WRITE_PERC      : Ratio of write transaction in a experiment
```
These configurations can also be specified as command argument at runtime. Run the following command for a full list of program argument.

    ./rundb -h

## IP Configuration
Create a file called ifconfig.txt with IP addresses for the servers and clients, one per line. These IP addresses will be used at runtime for the communication of servers and clients.
And corresponding parsing will be done **sequentially**, which means you need to put servers before clients.

Say we have 2 servers and 2 clients, then ifconfig.txt will look like this:
```bash
x1.x2.x3.x4 # 1st server, global serial number: 0
y1.y2.y3.y4 # 2nd server, global serial number: 1
z1.z2.z3.z4 # 1st client, global serial number: 2
t1.t2.t3.t4 # 2nd client, global serial number: 3
```

## Running Environment Configuration
Files need to be transferred to each node:
- rundb or runcl
- ifconfig.txt
- benchmarks/TPCC_full_schema.txt
- benchmarks/TPCC_short_schema.txt
- benchmarks/YCSB_schema.txt

For each node, please create benchmarks directory under `~/`, transfer first two ones to `~/` and last three ones to `~/benchmarks/`

# Run
```bash
./rundb -nid[unique server ID]
./runcl -nid[unique client ID]
# Take the last example, the 1st server, x1.x2.x3.x4, its global serial number is 0, then its unique server ID is 0 
```

# Automation
To save us from tedious operations under multiple node situation, automation scripts come, they are:
- scripts/experiments.py
- scripts/run_experiments.py
- scripts/run_config.py

## Explanation
### experiments.py
experiments.py is responsible for transferring configuration value to run_experiments.py.

The dictionary `configs` in experiments.py is default value for each configuration, but maybe not the final value, because functions may overwrite these default values, e.g.
```python
    def ycsb_skew():
    wl = 'YCSB'
    nnodes = [1]    # number of servers
    algos=['HDCC']
    base_table_size=2097152*8
    txn_write_perc = [1]
    tup_write_perc = [0.5, 0.1]
    load = [10000]
    tcnt = [4]
    skew = [0.0,0.6,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

# fmt lists the name of configuration terms that this function wants to overwrite
# exp is a combination of all possibility for given parameter terms (such as wl, nnodes)
# for this function, it will produce 2 * 3 = 6 combinations, that is, 6 round tests
# 2 refers to tup_write_perc, 3 refers to skew, other parameter term only has 1 value
 ```

### run_experiments.py
Receive all configuration combinations from experiments.py, for each one, run a seperate test

Output:
1. Servers running report 

Under directory `results`, report will be named by global serial number and configuration setting, e.g.
```
0_MVCC_CRT-2_CST-2_CT-4_EXTREME_MODE-true_TIF-10000_MPR-0.2_N-2_PRORATE_RATIO-0_RT-2_ST-2_SYNTH_TABLE_SIZE-16777216_T-16_TWR-0.2_WR-1_YCSB_SKEW-0.1_20230413-210347.out
# This is a running report from node 0 (indicated by first number), for configuration setting
# its EXTREME_MODE = true, MPR = 0.2 ...
```

2. Summary

`simple_summary.out` list throughput for each round of test, you can append other information you want to grab by modifying run_experiments.py

### run_config.py
Replace username with `whoami`, replace vcloud_uname with `home/$username`, fill your IP addresses of servers and clients in vcloud_machines, one per line, but number of servers need to be **equal** to the number of clients

## How to use:
Note: **Password-free login is needed among servers and clients**
```bash
cd scripts
./run_experiments.py -c -e vcloud ycsb_skew 
# replace ycsb_skew with the function you want
```