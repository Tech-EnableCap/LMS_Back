from flask import Flask,request
from flask_mysqldb import MySQL;
import json
import base64
import datetime
from helper import *
import io
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
	if(list(d.keys())[0]=='disbursement'):
		print("here")
		d=d['disbursement']
		d=d.split(';')[1]
		d=d.split(',')[1]
		d=base64.b64decode(d)
		toread=io.BytesIO()
		toread.write(d)
		toread.seek(0)
		data=pd.read_excel(toread)
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
	else:
		d=d['efx']
		d=d.split(';')[1]
		d=d.split(',')[1]
		d=base64.b64decode(d)
		toread=io.BytesIO()
		toread.write(d)
		toread.seek(0)
		data=pd.read_excel(toread)
		data=data.drop(['Category','Loan Amount'],axis=1)
		data['Category']=data['Equifax Score'].apply(lambda x:eq(x))
		amount_list=[]
		try:
			cursor=mysql.connection.cursor()
			for i in range(len(data)):
				query="SELECT sanction_amount FROM upload_file WHERE partner_loan_id=%s"
				cursor.execute(query,(data['partner_loan_id'].iloc[i],))
				data_all=cursor.fetchall()
				amount_list.append(data_all[0][0])
			cursor.close()
		except Exception as e:
			msg=str(e)
			print("database error")
		data['Loan Amount']=amount_list
		data=data.fillna("N/A")
		try:
			dic={}
			cursor=mysql.connection.cursor()
			for length in range(len(data)):
				for i,j in enumerate(data.columns):
					dic[j]=data.iloc[length][i]
				kk=list(dic.values())
				select_q="SELECT equifax_score FROM candidate_equifax WHERE partner_loan_id=%s"
				cursor.execute(select_q,(dic['partner_loan_id'],))
				if(cursor.rowcount>0):
					up_query="UPDATE candidate_equifax SET equifax_score=%s,category=%s,loan_amount=%s WHERE partner_loan_id=%s"
					cursor.execute(up_query,(dic['Equifax Score'],dic['Category'],dic['Loan Amount'],dic['partner_loan_id']))
				else:
					in_query="INSERT INTO candidate_equifax VALUES(%s,%s,%s,%s,%s,%s)"
					cursor.execute(in_query,(kk))
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



@app.route("/dmis",methods=["POST"])
@cross_origin(supports_credentials=True)
def exp():
	msg={}
	req=request.data
	pageidx=request.args.get('idx')
	req=json.loads(req)
	lid=req.get("lid",None)
	first_name=req.get("fname",None)
	last_name=req.get("lname",None)
	st_date=req.get("stDate",None)
	end_date=req.get("endDate",None)
	typ=req.get("cat","loan_app_date")
	if(st_date is not None and end_date is not None):
		st_d=datetime.datetime.strptime(st_date,"%Y-%m-%d")
		en_d=datetime.datetime.strptime(end_date,"%Y-%m-%d")
		gap=en_d-st_d
		if(gap.days<0):
			msg["error"]="end date must be bigger"
			return jsonify({"error":msg})

	try:
		perpage=20
		startat=int(pageidx)*perpage
		cursor=mysql.connection.cursor()
		#query="SELECT a1.* a2.equifax_score,a2.category FROM upload_file a1 candidate_equifax a2 WHERE a1.partner_loan_id=a2.partner_loan_id"
		query="SELECT upload_file.*,candidate_equifax.equifax_score,candidate_equifax.category FROM upload_file LEFT JOIN candidate_equifax ON upload_file.partner_loan_id=candidate_equifax.partner_loan_id WHERE (upload_file.partner_loan_id=%s OR upload_file.first_name=%s AND upload_file.last_name=%s OR upload_file."+typ+" BETWEEN %s AND %s) ORDER BY upload_file."+typ+" LIMIT %s,%s;"
		cols_query="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='upload_file'"
		cursor.execute(query,(lid,first_name,last_name,st_date,end_date,startat,perpage,))
		data_all=cursor.fetchall()
		if(len(data_all)<1):
			msg["error"]="no data found based on this search"
			return jsonify({"msg":msg})
		cursor.execute(cols_query,())
		columns=cursor.fetchall()

		cols=[i[0] for i in columns]
		cols=cols+['eq_score','risk_cat']
		data=pd.DataFrame(data_all,columns=cols)
		data.index=range(1,len(data)+1)

		final_data=disbursal_mis_process(data=data)

		######

		body=[list(final_data.iloc[i].values) for i in range(len(final_data))]
		cl_name=list(final_data.columns)
		msg={
			"clName":cl_name,
			"data":body
		}
		cursor.close()
	except Exception as e:
		msg['error']=str(e)

	return jsonify({"msg":msg})



@app.route("/viewupload",methods=["POST"])
@cross_origin(supports_credentials=True)
def view_up():
	pageidx=request.args.get('idx')
	msg={}
	req=request.data
	req=json.loads(req)
	lid=req.get("lid",None)
	first_name=req.get("fname",None)
	last_name=req.get("lname",None)
	st_date=req.get("stDate",None)
	end_date=req.get("endDate",None)
	typ=req.get("cat","loan_app_date")
	if(st_date is not None and end_date is not None):
		st_d=datetime.datetime.strptime(st_date,"%Y-%m-%d")
		en_d=datetime.datetime.strptime(end_date,"%Y-%m-%d")
		gap=en_d-st_d
		if(gap.days<0):
			msg["error"]="end date must be bigger"
			return jsonify({"msg":msg})
	if(pageidx):
		try:
			perpage=20
			startat=int(pageidx)*perpage
			cursor=mysql.connection.cursor()
			query="SELECT * FROM upload_file WHERE (partner_loan_id=%s OR first_name=%s AND last_name=%s OR "+typ+" BETWEEN %s AND %s) ORDER BY "+typ+" LIMIT %s,%s;"
			cols_query="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='upload_file'"
			cursor.execute(query,(lid,first_name,last_name,st_date,end_date,startat,perpage,))
			data_all=cursor.fetchall()
			if(len(data_all)<1):
				msg['error']='no data found'
				return jsonify({"msg":msg})
			cursor.execute(cols_query,())
			columns=cursor.fetchall()
			cols=[i[0] for i in columns]
			data=pd.DataFrame(data_all,columns=cols)
			final_data=process_str(data)
			final_data.index=range(1,len(final_data)+1)
			body=[list(final_data.iloc[i].values) for i in range(len(final_data))]
			cl_name=list(final_data.columns)
			msg={
				"clName":cl_name,
				"data":body
			}
			cursor.close()
		except Exception as e:
			msg['error']=str(e)

		return jsonify({"msg":msg})







@app.route("/bankupload",methods=["POST"])
@cross_origin(supports_credentials=True)
def expt():
	msg={}
	pageidx=request.args.get('idx')
	req=request.data
	req=json.loads(req)
	lid=req.get("lid",None)
	first_name=req.get("fname",None)
	last_name=req.get("lname",None)
	st_date=req.get("stDate",None)
	end_date=req.get("endDate",None)
	typ=req.get("cat","loan_app_date")
	if(st_date is not None and end_date is not None):
		st_d=datetime.datetime.strptime(st_date,"%Y-%m-%d")
		en_d=datetime.datetime.strptime(end_date,"%Y-%m-%d")
		gap=en_d-st_d
		if(gap.days<0):
			msg["error"]="end date must be bigger"
			return jsonify({"msg":msg})
	try:
		perpage=20
		startat=int(pageidx)*perpage
		cursor=mysql.connection.cursor()
		query="SELECT * FROM upload_file WHERE (partner_loan_id=%s OR first_name=%s AND last_name=%s OR "+typ+" BETWEEN %s AND %s) ORDER BY "+typ+" LIMIT %s,%s;"
		cols_query="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='upload_file'"
		cursor.execute(query,(lid,first_name,last_name,st_date,end_date,startat,perpage,))
		data_all=cursor.fetchall()
		if(len(data_all)<1):
			msg["error"]="no data found based on this search"
			return jsonify({"msg":msg})
		cursor.execute(cols_query,())
		columns=cursor.fetchall()
		cols=[i[0] for i in columns]
		data=pd.DataFrame(data_all,columns=cols)
		data.index=range(1,len(data)+1)
		#######
		final_data=bank_upload_process(data=data)
		body=[list(final_data.iloc[i].values) for i in range(len(final_data))]
		cl_name=list(final_data.columns)
		msg={
			"clName":cl_name,
			"data":body
		}
		cursor.close()

	except Exception as e:
		msg['error']=str(e)	

	return jsonify({"msg":msg})


if __name__=="__main__":
	app.run(host='0.0.0.0', port=5000)