from flask import Flask, jsonify
import random
import time, datetime
import happybase
from datetime import datetime

connection = happybase.Connection('localhost', autoconnect=False)
app = Flask(__name__)

@app.route("/<ts>")
def api(ts):
    connection.open()
    table = connection.table('mytable')
    res = []
    for rowkey, data in table.scan(row_start = str(int(ts) - 3)):
        res.append({'rowkey':rowkey, 'data':data})
    connection.close()

    return jsonify(res[-1])

if __name__ == "__main__":
    app.run(host='0.0.0.0')
