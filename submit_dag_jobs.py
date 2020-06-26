import random
import os
import time

import htcondor
import shutil

schedd = htcondor.Schedd()

home = os.path.expanduser("~")

test_file = os.path.join(home, "condor_watch_q")
t = str(int(time.time()))

for x in range(1, 6):
    log_dir = os.path.join(home, t, "{}".format(x))
    if log_dir is not None:
        try:
            os.makedirs(os.path.dirname(log_dir))
        except OSError:
            pass

        os.chdir(os.path.dirname(log_dir))
        os.mkdir("{}".format(x))
        os.chdir("{}".format(x))
        shutil.copy(os.path.join(test_file, "test.dag"), os.getcwd())
        shutil.copy(os.path.join(test_file, "test.condor"), os.getcwd())

    sub = htcondor.Submit.from_dag("test.dag")

    with schedd.transaction() as txn:
        sub.queue(txn, random.randint(1, 5))

os.system("condor_q")
print()

# os.chdir(os.path.join(home, "deeply", "nested"))
# os.chdir(os.path.expanduser("~"))
print("Running from", os.getcwd())
print("-" * 40)

