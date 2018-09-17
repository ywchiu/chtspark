from flask import Flask, jsonify
import random
from datetime import datetime
import happybase, uuid, json
connection = happybase.Connection('localhost', autoconnect=False)



app = Flask(__name__)

@app.route("/system/<cnt>")
def api2(cnt):
    connection.open()
    data   = connection.table('system_memory')
    ary = []
    for k,v in data.scan():
        ary.append({'tm':k, 'val': v})
    print ary[0:3]
    connection.close()
    return  jsonify({'data': ary[0:int(cnt)]})


if __name__ == "__main__":
    app.run(host='0.0.0.0', port = 9999)