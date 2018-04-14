import os
import subprocess

# ============================ parameters =====================================

# ============================= fixtures ======================================

# ============================== tests ========================================

def test_runSim():
    wd = os.getcwd()
    os.chdir("bin/")
    rc = subprocess.call("python runSim.py")
    os.chdir(wd)
    assert rc==0
