import sys
import os
import subprocess
sys.path.append(os.path.join(os.path.dirname(sys.path[0]), 'src'))


subprocess.call('python3 src/main.py'.split(" "))
