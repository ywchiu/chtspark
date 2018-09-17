from flask import Flask, jsonify
import random
from datetime import datetime
app = Flask(__name__)

@app.route("/")
def api():
    dt   = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data = random.randint(50,900)
    return jsonify({'dt':dt, 'data':data})

@app.route("/hello/<name>")
def api2(name):
    return 'hello, ' + name

@app.route("/data2/", methods=['POST'])
def postapi2(name):
    if request.method=='POST':
        area=request.form['area']
        return jsonify({'data':area})

@app.route("/data")
def data():
    return "<html><body> <a href='https://www.largitdata.com'> LargitData </a></body></html>"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port = 9999)
