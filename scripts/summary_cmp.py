import ast
import os

filepath = []
# replace below two paths with yours
filepath.append('/home/sigmod21-deneva/results/20230413-114536/0_MIXED_LOCK_CT-4_EXTREME_MODE-false_TIF-10000_MPR-0.1_N-2_NUM_WH-2_PP-0_PRORATE_RATIO-0_T-16_TPCC_20230413-114536.out')
filepath.append('/home/sigmod21-deneva/results/20230413-114536/0_CALVIN_CT-4_EXTREME_MODE-false_TIF-10000_MPR-0.1_N-2_NUM_WH-2_PP-0_PRORATE_RATIO-0_T-16_TPCC_20230413-114536.out')
summary = []
dict = []
minimal = []

for i in range(2):
    cmd = 'cat %s'%(filepath[i]) + '| grep summary | awk \'{print$2}\''
    summary.append(''.join(os.popen(cmd).readlines()))
    summary[i] = summary[i].strip('\n')
    summary[i] = summary[i].replace('=', '": "')
    summary[i] = summary[i].replace(',', '", "')
    summary[i] = '{"' + summary[i] + '"}'
    dict.append(ast.literal_eval(summary[i]))

print('{:<30} {:<25} {:<25} {:<5}'.format('metric', 'value0', 'value1', 'multiple'))
for k,v in dict[0].items():
    if v == dict[1][k]:
        print('{:<30} {:<25} {:<25}'.format(k, v, dict[1][k]))
    else:
        if float(v) > float(dict[1][k]):
            multiple = 'INF' if float(dict[1][k]) == 0 else float(v) / float(dict[1][k])
            if multiple == 'INF' or multiple > 10:
                minimal.append('{:30} \033[91m{:25}\033[00m {:25} {:<5}'.format(k, v, dict[1][k], multiple))
            print('{:30} \033[91m{:25}\033[00m {:25} {:<5}'.format(k, v, dict[1][k], multiple))
        else :
            multiple = 'INF' if float(v) == 0 else float(dict[1][k]) / float(v)
            if multiple == 'INF' or multiple > 10:
                minimal.append('{:30} {:25} \033[91m{:25}\033[00m {:<5}'.format(k, v, dict[1][k], multiple))
            print('{:30} {:25} \033[91m{:25}\033[00m {:<5}'.format(k, v, dict[1][k], multiple))

print('\n\033[91m{:^100}\033[00m\n'.format('MINIMAL OUTPUT'))
print('{:<30} {:<25} {:<25} {:<5}'.format('metric', 'value0', 'value1', 'multiple'))
for s in minimal:
    print(s)


