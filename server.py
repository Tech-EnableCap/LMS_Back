from flask import Flask,request
from flask_mysqldb import MySQL;
import json
import base64
import io
import pandas as pd
from flask import jsonify
from flask_cors import CORS, cross_origin

app=Flask(__name__)
CORS(app)

app.config['MYSQL_HOST']='localhost'
app.config['MYSQL_USER']='root'
app.config['MYSQL_PASSWORD']=''
app.config['MYSQL_DB']='lms'
app.config['MYSQL_DATABASE_PORT']=80

mysql=MySQL(app)


@app.route("/",methods=["POST"])
@cross_origin(supports_credentials=True)
def res():
	msg="upload file"
	#if(request.method=="POST"):
	d=request.data
	d=json.loads(d)
	d=d['disbursement']
	d=d.split(';')[1]
	d=d.split(',')[1]
	d=base64.b64decode(d)
	toread=io.BytesIO()
	toread.write(d)
	toread.seek(0)
	data=pd.read_excel(toread,engine='openpyxl')
	data=data.fillna("N/A")
	col_names=data.iloc[0]
	data=data[1:]
	data.columns=col_names
	try:
		dic={}
		cursor=mysql.connection.cursor()
		for length in range(len(data)):
			for i,j in enumerate(data.columns):
				dic[j]=data.iloc[length][i]
			kk=list(dic.values())
			cursor.execute('''INSERT INTO upload_file VALUES(%s,%s,%s,%s,%s
				,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
				,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
				%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',(kk))
			dic={}
			kk=[]
		mysql.connection.commit()
		cursor.close()
		msg="upload done"
	except Exception as e:
		msg=str(e)
		print("database error")
		print(msg)
	return jsonify({"msg":msg})

@app.route("/other",methods=["POST","GET"])
@cross_origin(supports_credentials=True)
def exp():
	return jsonify({"msg":"hello"})


@app.route("/another",methods=["POST"])
@cross_origin(supports_credentials=True)
def expt():
	return jsonify({"msg":"another"})


if __name__=="__main__":
	app.run(host='0.0.0.0', port=5000)