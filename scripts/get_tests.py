import os

#os.chdir('..')

PATH=os.getcwd()

user=""
machine = ""
project_dir = ""

cmd = "scp {}@{}:{}tests.tgz .".format(user,machine,project_dir)
os.system(cmd)

cmd = "tar -xvf tests.tgz"
os.system(cmd)
