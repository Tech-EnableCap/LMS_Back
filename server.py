'''
REST API enablecap lms system
@ tech@enablecap.in
@ tech2@enablecap.in
enablecap loan management system v1 2021

'''

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

#app.config['MYSQL_HOST']='lms1.cp0iwsjv1k3d.ap-south-1.rds.amazonaws.com'
#app.config['MYSQL_USER']='tech'
#app.config['MYSQL_PASSWORD']='tech_enablecap'
app.config['MYSQL_HOST']='localhost'
app.config['MYSQL_USER']='root'
app.config['MYSQL_PASSWORD']=''
app.config['MYSQL_DB']='lms'
app.config['MYSQL_DATABASE_PORT']=3306

mysql=MySQL(app)


@app.route("/",methods=["POST"])
@cross_origin(supports_credentials=True)
def res():
	msg={}
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
		#d1=master_repay_helper(data)
		#print(d1)
		#print("=================")
		#print(d2)
		try:
			cursor=mysql.connection.cursor()

			query="create table master_repay(transaction_id varchar(100) not null primary key,first_name varchar(100),last_name varchar(100),type varchar(100),no_of_emi varchar(100),emi_amt varchar(100),st_date date,end_date date);"

			cursor.execute(query)
			cursor=helper_upload(data=data,cursor=cursor,file_type="upload_file")

			#####
			master_repay=master_repay_helper(data)
			cursor=helper_upload(data=master_repay,cursor=cursor,file_type="master_repay")


			mysql.connection.commit()
			cursor.close()
			msg["msg"]="Success"
			print(msg)
		except Exception as e:
			msg["error"]=str(e)
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
	#pageidx=req.get("idx","0")
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
		cursor=mysql.connection.cursor()
		cols_query="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='upload_file'"
		cursor.execute(cols_query,())
		columns=cursor.fetchall()
		cols=[i[0] for i in columns]
		cols=cols+['eq_score','risk_cat']
		if(pageidx=="0"):
			query="SELECT upload_file.*,candidate_equifax.equifax_score,candidate_equifax.category FROM upload_file LEFT JOIN candidate_equifax ON upload_file.partner_loan_id=candidate_equifax.partner_loan_id WHERE (upload_file.partner_loan_id=%s OR upload_file.first_name=%s AND upload_file.last_name=%s OR upload_file."+typ+" BETWEEN %s AND %s);"
			cursor.execute(query,(lid,first_name,last_name,st_date,end_date,))
			count=cursor.fetchall()
			msg["count"]=len(count)

		if(pageidx=="-2"):
			query="SELECT upload_file.*,candidate_equifax.equifax_score,candidate_equifax.category FROM upload_file LEFT JOIN candidate_equifax ON upload_file.partner_loan_id=candidate_equifax.partner_loan_id WHERE (upload_file.partner_loan_id=%s OR upload_file.first_name=%s AND upload_file.last_name=%s OR upload_file."+typ+" BETWEEN %s AND %s);"
			cursor.execute(query,(lid,first_name,last_name,st_date,end_date,))

		else:
			perpage=20
			startat=int(pageidx)*perpage
			cursor=mysql.connection.cursor()
			#query="SELECT a1.* a2.equifax_score,a2.category FROM upload_file a1 candidate_equifax a2 WHERE a1.partner_loan_id=a2.partner_loan_id"
			query="SELECT upload_file.*,candidate_equifax.equifax_score,candidate_equifax.category FROM upload_file LEFT JOIN candidate_equifax ON upload_file.partner_loan_id=candidate_equifax.partner_loan_id WHERE (upload_file.partner_loan_id=%s OR upload_file.first_name=%s AND upload_file.last_name=%s OR upload_file."+typ+" BETWEEN %s AND %s) ORDER BY upload_file."+typ+" LIMIT %s,%s;"
			
			cursor.execute(query,(lid,first_name,last_name,st_date,end_date,startat,perpage,))

		data_all=cursor.fetchall()
		if(len(data_all)<1):
			msg["error"]="no data found based on this search"
			return jsonify({"msg":msg})

		data=pd.DataFrame(data_all,columns=cols)
		data.index=range(1,len(data)+1)

		final_data=disbursal_mis_process(data=data)

		######

		body=[list(final_data.iloc[i].values) for i in range(len(final_data))]
		cl_name=list(final_data.columns)
		msg["clName"]=cl_name
		msg["data"]=body
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
	print(pageidx)
	#pageidx=req.get("idx","0")
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

		cursor=mysql.connection.cursor()
		cols_query="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='upload_file';"
		cursor.execute(cols_query,())
		columns=cursor.fetchall()
		cols=[i[0] for i in columns]

		if(pageidx=="0"):
			query="SELECT COUNT(*) FROM upload_file WHERE (transaction_id=%s OR first_name=%s AND last_name=%s OR "+typ+" BETWEEN %s AND %s);"
			cursor.execute(query,(lid,first_name,last_name,st_date,end_date,))
			count=cursor.fetchall()
			msg["count"]=count[0][0]

		if(pageidx=="-2"):
			query="SELECT * FROM upload_file WHERE (transaction_id=%s OR first_name=%s AND last_name=%s OR "+typ+" BETWEEN %s AND %s);"
			cursor.execute(query,(lid,first_name,last_name,st_date,end_date,))

		else:

			perpage=20
			startat=int(pageidx)*perpage
			cursor=mysql.connection.cursor()
			query="SELECT * FROM upload_file WHERE (transaction_id=%s OR first_name=%s AND last_name=%s OR "+typ+" BETWEEN %s AND %s) ORDER BY "+typ+" LIMIT %s,%s;"
			cursor.execute(query,(lid,first_name,last_name,st_date,end_date,startat,perpage,))

		data_all=cursor.fetchall()
		if(len(data_all)<1):
			msg['error']='no data found'
			return jsonify({"msg":msg})
		
		data=pd.DataFrame(data_all,columns=cols)
		final_data=process_str(data)
		final_data.index=range(1,len(final_data)+1)
		body=[list(final_data.iloc[i].values) for i in range(len(final_data))]
		cl_name=list(final_data.columns)
		msg["clName"]=cl_name
		msg["data"]=body
		cursor.close()

	except Exception as e:
		msg['error']=str(e)

	return jsonify({"msg":msg})


@app.route("/viewequifax",methods=["POST"])
@cross_origin(supports_credentials=True)
def view_eq():
	msg={}
	pageidx=request.args.get("idx")
	req=request.data
	req=json.loads(req)
	#pageidx=req.get("idx","0")
	lid=req.get("lid",None)
	first_name=req.get("fname",None)
	last_name=req.get("lname",None)
	try:
		cursor=mysql.connection.cursor()
		cols_query="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='candidate_equifax';"
		cursor.execute(cols_query,())
		columns=cursor.fetchall()
		cols=[i[0] for i in columns]

		if(pageidx=="0"):
			query="SELECT COUNT(*) FROM candidate_equifax WHERE (partner_loan_id=%s OR first_name=%s AND last_name=%s);"
			cursor.execute(query,(lid,first_name,last_name,))
			count=cursor.fetchall()
			msg["count"]=count[0][0]

		if(pageidx=="-2"):
			query="SELECT * FROM candidate_equifax WHERE (partner_loan_id=%s OR first_name=%s AND last_name=%s);"
			cursor.execute(query,(lid,first_name,last_name,))

		else:

			perpage=20
			startat=int(pageidx)*perpage
			query="SELECT * FROM candidate_equifax WHERE (partner_loan_id=%s OR first_name=%s AND last_name=%s) LIMIT %s,%s;"
		
			cursor.execute(query,(lid,first_name,last_name,startat,perpage,))

		data_all=cursor.fetchall()
		if(len(data_all)<1):
			msg["error"]="no data found based on this search"
			return jsonify({"msg":msg})
	
		data=pd.DataFrame(data_all,columns=cols)
		data.index=range(1,len(data)+1)
		body=[list(data.iloc[i].values) for i in range(len(data))]
		cl_name=list(data.columns)
		msg["clName"]=cl_name
		msg["data"]=body
		cursor.close()

	except Exception as e:
		msg['error']=str(e)	

	return jsonify({"msg":msg})


@app.route("/bankupload",methods=["POST"])
@cross_origin(supports_credentials=True)
def expt():
	msg={}
	pageidx=request.args.get("idx")
	req=request.data
	req=json.loads(req)
	#pageidx=req.get("idx","0")
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
		cursor=mysql.connection.cursor()
		cols_query="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='upload_file';"
		cursor.execute(cols_query,())
		columns=cursor.fetchall()
		cols=[i[0] for i in columns]

		if(pageidx=="0"):
			query="SELECT COUNT(*) FROM upload_file WHERE (transaction_id=%s OR first_name=%s AND last_name=%s OR "+typ+" BETWEEN %s AND %s);"
			cursor.execute(query,(lid,first_name,last_name,st_date,end_date,))
			count=cursor.fetchall()
			msg["count"]=count[0][0]

		if(pageidx=="-2"):
			query="SELECT * FROM upload_file WHERE (transaction_id=%s OR first_name=%s AND last_name=%s OR "+typ+" BETWEEN %s AND %s);"
			cursor.execute(query,(lid,first_name,last_name,st_date,end_date,))

		else:

			perpage=20
			startat=int(pageidx)*perpage
			query="SELECT * FROM upload_file WHERE (transaction_id=%s OR first_name=%s AND last_name=%s OR "+typ+" BETWEEN %s AND %s) ORDER BY "+typ+" LIMIT %s,%s;"
		
			cursor.execute(query,(lid,first_name,last_name,st_date,end_date,startat,perpage,))

		data_all=cursor.fetchall()
		if(len(data_all)<1):
			msg["error"]="no data found based on this search"
			return jsonify({"msg":msg})
		
		data=pd.DataFrame(data_all,columns=cols)
		data.index=range(1,len(data)+1)
		#######
		final_data=bank_upload_process(data=data)
		body=[list(final_data.iloc[i].values) for i in range(len(final_data))]
		cl_name=list(final_data.columns)
		msg["clName"]=cl_name
		msg["data"]=body
	
		cursor.close()

	except Exception as e:
		msg['error']=str(e)	

	return jsonify({"msg":msg})


@app.route("/analysis",methods=["POST"])
@cross_origin(supports_credentials=True)
def analysis():
	msg={}
	risk={}
	req=request.data
	if(req):
		req=json.loads(req)
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
	else:
		try:
			cursor=mysql.connection.cursor()
			query="SELECT disburse_date FROM upload_file ORDER BY disburse_date;"
			cursor.execute(query,())
			dates=cursor.fetchall()
			end_date=str(dates[-1][0])
			st_date=str(dates[-1][0]-relativedelta(months=+1))
			typ="disburse_date"
			cursor.close()
		except Exception as e:
			msg["error"]=str(e)
			print(e)
	try:
		cursor=mysql.connection.cursor()
		query="SELECT * FROM upload_file WHERE "+typ+" BETWEEN %s AND %s;"
		cols_query="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='upload_file';"
		cursor.execute(query,(st_date,end_date,))
		data_all=cursor.fetchall()

		query_eq="SELECT * FROM candidate_equifax;"
		cols_eq="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='candidate_equifax';"
		cursor.execute(query_eq,())
		eq_data=cursor.fetchall()

		if(len(data_all)<1 or len(eq_data)<1):
			msg["error"]="no data found on this search"
			return jsonify({"msg":msg})

		cursor.execute(cols_query,())
		columns=cursor.fetchall()
		cursor.execute(cols_eq,())
		columns_eq=cursor.fetchall()

		cols=[i[0] for i in columns]
		data=pd.DataFrame(data_all,columns=cols)
		data.index=range(1,len(data)+1)

		cols_equifax=[i[0] for i in columns_eq]
		data_equifax=pd.DataFrame(eq_data,columns=cols_equifax)
		data_equifax.index=range(1,len(data_equifax)+1)

		analyzed_weekly=monthly_weekly_analysis(data,typ="Weekly")
		analyzed_monthly=monthly_weekly_analysis(data,typ="Monthly")
		analyzed_total=analysis_total(data)

		number_of_loans=risk_params(data_equifax,typ="Number")
		volume_of_loans=risk_params(data_equifax,typ="Volume")

		msg["weekly"]=analyzed_weekly
		msg["monthly"]=analyzed_monthly
		msg["total"]=analyzed_total

		risk["nloans"]=number_of_loans
		risk["vloans"]=volume_of_loans

		msg["risk"]=risk

		#msg["risk"]["nloans"]=number_of_loans
		#msg["risk"]["vloans"]=volume_of_loans

		print(analyzed_monthly)
		print("-----------")
		print(analyzed_weekly)
		print("-----------")
		print(analyzed_total)
		print("-----------")
		print(number_of_loans)
		print("-----------")
		print(volume_of_loans)

		cursor.close()

	except Exception as e:
		msg["error"]=str(e)
		print(e)

	return jsonify({"msg":msg})

####### master repay

@app.route("/search_repay",methods=["POST"])
@cross_origin(supports_credentials=True)
def search_repay_data():
	msg={}
	req=request.data
	pageidx=request.args.get("idx")
	req=json.loads(req)
	lid=req.get("lid",None)
	f_name=req.get("fname",None)
	l_name=req.get("lname",None)
	st_date=req.get("stDate",None)
	end_date=req.get("endDate",None)

	if(st_date is not None and end_date is not None):
		st_d=datetime.datetime.strptime(st_date,"%Y-%m-%d")
		en_d=datetime.datetime.strptime(end_date,"%Y-%m-%d")
		gap=en_d-st_d
		if(gap.days<0):
			msg["error"]="end date must be bigger"
			return jsonify({"msg":msg})

	try:
		cursor=mysql.connection.cursor()
		cols_query="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='master_repay';"
		cursor.execute(cols_query,())
		columns=cursor.fetchall()
		cols=[i[0] for i in columns]
		if(st_date and end_date):

			if(pageidx=="0"):
				query="SELECT COUNT(*) FROM master_repay WHERE (%s<=st_date AND %s>=st_date OR %s>=end_date AND %s<=end_date OR %s<=st_date AND %s>=end_date);"
				cursor.execute(query,(st_date,end_date,end_date,st_date,st_date,end_date,))
				count=cursor.fetchall()
				msg["count"]=count[0][0]

			if(pageidx=="-2"):
				query="SELECT * FROM master_repay WHERE (%s<=st_date AND %s>=st_date OR %s>=end_date AND %s<=end_date OR %s<=st_date AND %s>=end_date);"
				cursor.execute(query,(st_date,end_date,end_date,st_date,st_date,end_date,))

			else:	
				perpage=20
				startat=int(pageidx)*perpage
				#query="SELECT DISTINCT transaction_id FROM master_repay WHERE emi_date BETWEEN %s AND %s;"
				query="SELECT * FROM master_repay WHERE (%s<=st_date AND %s>=st_date OR %s>=end_date AND %s<=end_date OR %s<=st_date AND %s>=end_date) LIMIT %s,%s;"
				cursor.execute(query,(st_date,end_date,end_date,st_date,st_date,end_date,startat,perpage,))
			
			data_all=cursor.fetchall()

			if(len(data_all)<1):
				msg["error"]="no data found based on this search"
				return jsonify({"msg":msg})

			data=pd.DataFrame(data_all,columns=cols)
			data['st_date']=data['st_date'].apply(lambda x:str(x).split(" ")[0])
			data['end_date']=data['end_date'].apply(lambda x:str(x).split(" ")[0])
			
			data.index=range(1,len(data)+1)

			data=handle_date(data,st_date,end_date)

			body=[list(data.iloc[i].values) for i in range(len(data))]
			cl_name=list(data.columns)
			msg["clName"]=cl_name
			msg["data"]=body

			
		if(lid):
			query="SELECT * FROM master_repay WHERE transaction_id=%s"
			cursor.execute(query,(lid,))
			data_all=cursor.fetchall()
			if(len(data_all)<1):
				msg["error"]="no data found based on this search"
				return jsonify({"msg":msg})
			data=pd.DataFrame(data_all,columns=cols)
			data.index=range(1,len(data)+1)
			m_r=handle_single_tid_data(data)
			body=[list(m_r.iloc[i].values) for i in range(len(m_r))]
			cl_name=list(m_r.columns)
			msg["clName"]=cl_name
			msg["data"]=body
			msg["count"]=1

		if(f_name and l_name):
			query="SELECT * FROM master_repay WHERE (first_name=%s AND last_name=%s)"
			cursor.execute(query,(f_name,l_name))
			data_all=cursor.fetchall()
			if(len(data_all)<1):
				msg["error"]="no data found based on this search"
				return jsonify({"msg":msg})
			data=pd.DataFrame(data_all,columns=cols)
			#print(data)
			data.index=range(1,len(data)+1)
			m_r=handle_single_tid_data(data)
			body=[list(m_r.iloc[i].values) for i in range(len(m_r))]
			cl_name=list(m_r.columns)
			msg["clName"]=cl_name
			msg["data"]=body
			msg["count"]=1

		cursor.close()
	except Exception as e:
		print(e)
		msg["error"]=str(e)

	return jsonify({"msg":msg})


	#test=pd.DataFrame([['a',['2021-05-05','2021-05-12','2021-05-19']],
                   #['b',['2021-05-05','2021-06-12','2021-07-19']]],columns=["c","d"])
	#s=test.apply(lambda x: pd.Series(x['d']),axis=1).stack().reset_index(level=1,drop=True)
	#s.name="f"
	#test2=test.drop('d',axis=1).join(s)
	#print(test2)
	#try:
		#cursor=mysql.connection.cursor()
		#dic={}
		#for length in range(len(test2)):
			#for i,j in enumerate(test2.columns):
				#dic[j]=test2.iloc[length][i]
			#kk=list(dic.values())
			#cursor.execute('''INSERT INTO test2 VALUES(%s,%s)''',(kk))
			#dic={}
			#kk=[]
		#mysql.connection.commit()
		#cursor.close()
		#msg="upload done"
	#except Exception as e:
		#msg=str(e)
		#print("database error")
		#print(msg)
	#master_repay=master_repay_helper(d)
	#print(master_repay)

	#return jsonify({"msg":msg})


@app.route("/findtest",methods=["POST"])
@cross_origin(supports_credentials=True)
def find_test():
	req=request.data
	req=json.loads(req)
	st_date=req.get("stDate",None)
	end_date=req.get("endDate",None)
	try:
		cursor=mysql.connection.cursor()
		query="SELECT * FROM test2 WHERE f BETWEEN %s AND %s;"
		cursor.execute(query,(st_date,end_date,))
		data_all=cursor.fetchall()
		print(data_all)
		cursor.close()
	except Exception as e:
		print(e)
	return jsonify({"msg":"ok"})


@app.route("/testq",methods=["GET"])
@cross_origin(supports_credentials=True)
def testq():
	return jsonify({"msg":"hello world"})





if __name__=="__main__":
	app.run(host='0.0.0.0', port=5000)