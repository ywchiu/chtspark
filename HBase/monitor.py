import os
import psutil
pid = os.getpid()
py = psutil.Process(pid)
print psutil.virtual_memory().percent