from flask import Flask
from flask import jsonify
from flask_cors import CORS, cross_origin
app=Flask(__name__)
CORS(app)
@app.route("/",methods=["POST","GET"])
@cross_origin(supports_credentials=True)
def exp():
	return jsonify({'h': 'helloWorld'})

	app.run(host='0.0.0.0', port=5000)