from flask import Flask, jsonify
import random
import time, datetime
app = Flask(__name__)

@app.route("/")
def api():
    dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    data = random.randint(50,900)
    return jsonify({'dt':dt, 'data':data})

if __name__ == "__main__":
    app.run()

	
	