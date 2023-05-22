#!/bin/python

import re, os, sys
import matplotlib.pyplot as plt


dir = '/home/sigmod21-deneva/results/20230512-103757'
os.chdir(dir)
ccSet = set()  #concurrency contorl
ccPernode = []
tputsPernodePercc = dict()
avg = dict()
APPEND = False

for arg in sys.argv[1:]:
    if arg == '-a':
        APPEND = True
    elif arg == '-p':
        PLOT = True
    else:
        sys.exit('wrong args')


for file in os.listdir(dir):
    if file[0] == '0' or file[0] == '1':
        name = file.split('_CRT')[0]
        ccSet.add(name[2:])
        ccPernode.append(name)
        tputs = []
        with open(file, 'r') as f:
            for line in f:
                if 'tputs0' in line:
                    line = line[2:].rstrip('\n')
                    results = re.split(',', line)
                    for i in range(len(results)):
                        if i >= 60 and i < 120:
                            (_, val) = re.split('=', results[i])
                            tputs.append(int(val))
        tputsPernodePercc[name] = tputs

# plotting
for cc in ccSet:
    node0 = '0_' + cc
    node1 = '1_' + cc
    tputsAvg = [(x + y) / 2 for x, y in zip(tputsPernodePercc[node0], tputsPernodePercc[node1])]
    avg[cc] = tputsAvg

x = list(range(60))
for cc in ccSet:
    plt.plot(x, avg[cc], label=cc)

plt.xlabel('seconds')
plt.ylabel('tput')
plt.legend()
plt.savefig('tputsPerSecond')


# append tputs into simple_summary.out
if APPEND:
    with open('simple_summary.out', 'a') as simple_f:
        for name in ccPernode:
            simple_f.write(name + '\n')
            for v in tputsPernodePercc[name]:
                simple_f.write(str(v) + '\n')
