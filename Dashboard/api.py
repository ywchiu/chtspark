import random
import time, datetime
import happybase
from datetime import datetime

app = Flask(__name__)

@app.route("/")
def api():
    connection = happybase.Connection('localhost', autoconnect=False)
    connection.open()
    table = connection.table('mytable')

    current_date = datetime.now()
    dt = current_date.timetuple()
    ts = int(time.mktime(dt)) - 30000

    res = []
    for key, data in table.scan(row_start=str(ts)):
        res.append({'time':key, 'data':data})

    connection.close()

    return jsonify(res[-1])

if __name__ == "__main__":
    app.run(host='0.0.0.0')