import random
import time

with open('/tmp/machine.log', 'wa') as f:
    while True:
        recnum = random.randint(3,100)
        time.sleep(1)
        for i in range(1, recnum+1):
            machine_id  = random.choice('12345')
            log_message = random.choice(['normal', 'warning', 'info', 'error'])
            log_value   = random.randint(1,99)
            f.write('%s,%s,%s\n'%(machine_id, log_message, log_value))
        f.flush()
