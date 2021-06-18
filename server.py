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
import jwt
from flask import jsonify
from flask_cors import CORS, cross_origin
from functools import wraps

app=Flask(__name__)
CORS(app)

app.config['SECRET_KEY']='secretkey'
app.config['MYSQL_HOST']='lms1.cp0iwsjv1k3d.ap-south-1.rds.amazonaws.com'
app.config['MYSQL_USER']='tech'
app.config['MYSQL_PASSWORD']='tech_enablecap'
#app.config['MYSQL_USER']='root'
#app.config['MYSQL_PASSWORD']=''
app.config['MYSQL_DB']='lms'
app.config['MYSQL_DATABASE_PORT']=3306

mysql=MySQL(app)


def login_required(f):
	@wraps(f)
	def check(*args,**kwargs):
		msg={}
		token=None
		print(request.headers)
		if 'Authorization' in request.headers:
			token=request.headers['Authorization']
			token=token.split(" ")[1]
		if not token:
			msg["error"]="token is missing"
			return jsonify({"msg":msg})
		#data=jwt.decode(token,app.config['SECRET_KEY'])
		try:
			data=jwt.decode(token,app.config['SECRET_KEY'],algorithms="HS256")
		except:
			msg["error"]="invalid token"
			return jsonify({"msg":msg})
		return f(*args,**kwargs)
	return check

@app.route("/login",methods=["POST"])
@cross_origin(supports_credentials=True)
def login():
	msg={}
	req=request.data
	req=json.loads(req)
	uname=req.get("email",None)
	password=req.get("pass",None)
	try:
		cursor=mysql.connection.cursor()
		query="SELECT email,pass FROM users WHERE email=%s AND pass=%s";
		cursor.execute(query,(uname,password,))
		data=cursor.fetchall()
		if(len(data)==0):
			msg["error"]="invalid credential"
			return jsonify({"msg":msg})
		token=jwt.encode({'user':uname,'exp':datetime.datetime.utcnow()+datetime.timedelta(minutes=1440)},app.config['SECRET_KEY'],algorithm="HS256")
		msg["token"]=token
		cursor.close()
	except Exception as e:
		msg["error"]=str(e)
	
	return jsonify({"msg":msg})


@app.route("/",methods=["POST"])
@cross_origin(supports_credentials=True)
@login_required
def res():
	msg={}
	d=request.data
	d=json.loads(d)
	f_type=list(d.keys())[0]
	if(f_type=='efx'):
		d=d[f_type]
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
			msg["error"]=str(e)
			return jsonify({'msg':msg})

	else:
		print("here")
		d=d[f_type]
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

			db_type=f_type.split("_")[1]

			cursor=helper_upload(data=data,cursor=cursor,db_type=db_type,file_type="upload_file")

			#####
			master_repay=master_repay_helper(data)
			cursor=helper_upload(data=master_repay,cursor=cursor,db_type=db_type,file_type="master_repay")


			mysql.connection.commit()
			cursor.close()
			msg["msg"]="Success"
			print(msg)
		except Exception as e:
			msg["error"]=str(e)
			return jsonify({'msg':msg})
		

	return jsonify({"msg":msg})



@app.route("/dmis",methods=["POST"])
@cross_origin(supports_credentials=True)
@login_required
def exp():
	msg={}
	req=request.data
	pageidx=request.args.get('idx')
	req=json.loads(req)
	#pageidx=req.get("idx","0")
	lid=req.get("lid",None)
	if(lid):
		lid=lid.split(" ")
	first_name=req.get("fname",None)
	last_name=req.get("lname",None)
	st_date=req.get("stDate",None)
	end_date=req.get("endDate",None)
	typ=req.get("cat","loan_app_date")
	comp=req.get("comp",None)
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
			if(lid):
				cursor.execute("SELECT partner_loan_id FROM upload_file WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s)",{"tid":lid,"comp":comp})
				partner_lids=cursor.fetchall()
				cursor.execute("SELECT upload_file.*,candidate_equifax.equifax_score,candidate_equifax.category FROM upload_file LEFT JOIN candidate_equifax ON upload_file.partner_loan_id=candidate_equifax.partner_loan_id WHERE upload_file.partner_loan_id IN %(tid)s",{"tid":partner_lids})
				count=cursor.fetchall()
			else:
				query="SELECT upload_file.*,candidate_equifax.equifax_score,candidate_equifax.category FROM upload_file LEFT JOIN candidate_equifax ON upload_file.partner_loan_id=candidate_equifax.partner_loan_id WHERE (upload_file.first_name=%s AND upload_file.last_name=%s AND upload_file.comp_name=%s OR upload_file."+typ+" BETWEEN %s AND %s);"
				cursor.execute(query,(first_name,last_name,comp,st_date,end_date,))
				count=cursor.fetchall()
			msg["count"]=len(count)

		if(pageidx=="-2"):
			if(lid):
				cursor.execute("SELECT partner_loan_id FROM upload_file WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s)",{"tid":lid,"comp":comp})
				partner_lids=cursor.fetchall()
				cursor.execute("SELECT upload_file.*,candidate_equifax.equifax_score,candidate_equifax.category FROM upload_file LEFT JOIN candidate_equifax ON upload_file.partner_loan_id=candidate_equifax.partner_loan_id WHERE upload_file.partner_loan_id IN %(tid)s",{"tid":partner_lids})
			else:
				query="SELECT upload_file.*,candidate_equifax.equifax_score,candidate_equifax.category FROM upload_file LEFT JOIN candidate_equifax ON upload_file.partner_loan_id=candidate_equifax.partner_loan_id WHERE (upload_file.first_name=%s AND upload_file.last_name=%s AND upload_file.comp_name=%s OR upload_file."+typ+" BETWEEN %s AND %s);"
				cursor.execute(query,(first_name,last_name,comp,st_date,end_date,))

		else:
			perpage=20
			startat=int(pageidx)*perpage
			if(lid):
				cursor.execute("SELECT partner_loan_id FROM upload_file WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s)",{"tid":lid,"comp":comp})
				partner_lids=cursor.fetchall()
				cursor.execute("SELECT upload_file.*,candidate_equifax.equifax_score,candidate_equifax.category FROM upload_file LEFT JOIN candidate_equifax ON upload_file.partner_loan_id=candidate_equifax.partner_loan_id WHERE upload_file.partner_loan_id IN %(tid)s LIMIT %(st)s,%(end)s",{'tid':partner_lids,'st':startat,'end':perpage})
			else:
				#query="SELECT a1.* a2.equifax_score,a2.category FROM upload_file a1 candidate_equifax a2 WHERE a1.partner_loan_id=a2.partner_loan_id"
				query="SELECT upload_file.*,candidate_equifax.equifax_score,candidate_equifax.category FROM upload_file LEFT JOIN candidate_equifax ON upload_file.partner_loan_id=candidate_equifax.partner_loan_id WHERE (upload_file.first_name=%s AND upload_file.last_name=%s AND upload_file.comp_name=%s OR upload_file."+typ+" BETWEEN %s AND %s) ORDER BY upload_file."+typ+" LIMIT %s,%s;"
			
				cursor.execute(query,(first_name,last_name,comp,st_date,end_date,startat,perpage,))

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
@login_required
def view_up():
	pageidx=request.args.get('idx')
	msg={}
	req=request.data
	req=json.loads(req)
	print(req)
	#pageidx=req.get("idx","0")
	lid=req.get("lid",None)
	if(lid):
		lid=lid.split(" ")
	first_name=req.get("fname",None)
	last_name=req.get("lname",None)
	st_date=req.get("stDate",None)
	end_date=req.get("endDate",None)
	typ=req.get("cat","loan_app_date")
	comp=req.get("comp",None)
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
			if(lid):
				cursor.execute("SELECT COUNT(*) FROM upload_file WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s)",{"tid":lid,"comp":comp})
				count=cursor.fetchall()
			else:
				query="SELECT COUNT(*) FROM upload_file WHERE (first_name=%s AND last_name=%s AND comp_name=%s OR "+typ+" BETWEEN %s AND %s);"
				cursor.execute(query,(first_name,last_name,comp,st_date,end_date,))
				count=cursor.fetchall()
			msg["count"]=count[0][0]

		if(pageidx=="-2"):
			if(lid):
				cursor.execute("SELECT * FROM upload_file WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s)",{"tid":lid,"comp":comp})
			else:
				query="SELECT * FROM upload_file WHERE (first_name=%s AND last_name=%s AND comp_name=%s OR "+typ+" BETWEEN %s AND %s);"
				cursor.execute(query,(first_name,last_name,comp,st_date,end_date,))

		else:

			perpage=20
			startat=int(pageidx)*perpage
			if(lid):
				cursor.execute("SELECT * FROM upload_file WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s) LIMIT %(st)s,%(end)s;",{"tid":lid,"comp":comp,"st":startat,"end":perpage})
			else:
				query="SELECT * FROM upload_file WHERE (first_name=%s AND last_name=%s AND comp_name=%s OR "+typ+" BETWEEN %s AND %s) ORDER BY "+typ+" LIMIT %s,%s;"
				cursor.execute(query,(first_name,last_name,comp,st_date,end_date,startat,perpage,))

		data_all=cursor.fetchall()
		print(data_all)
		if(len(data_all)<1):
			msg['error']='no data found'
			return jsonify({"msg":msg})
		
		data=pd.DataFrame(data_all,columns=cols)
		final_data=process_str(data)
		final_data.index=range(1,len(final_data)+1)
		final_data['emi_amount_received']=final_data['emi_amount_received'].apply(lambda x:str(x))
		final_data['carry_f']=final_data['carry_f'].apply(lambda x:str(x))
		final_data['emi_number']=final_data['emi_number'].apply(lambda x:str(x))
		final_data['emi_date_flag']=final_data['emi_date_flag'].apply(lambda x:str(x).split(" ")[0])
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
@login_required
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
@login_required
def expt():
	msg={}
	pageidx=request.args.get("idx")
	req=request.data
	req=json.loads(req)
	#pageidx=req.get("idx","0")
	lid=req.get("lid",None)
	if(lid):
		lid=lid.split(" ")
	first_name=req.get("fname",None)
	last_name=req.get("lname",None)
	st_date=req.get("stDate",None)
	end_date=req.get("endDate",None)
	typ=req.get("cat","loan_app_date")
	comp=req.get("comp",None)
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
			if(lid):
				cursor.execute("SELECT COUNT(*) FROM upload_file WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s)",{"tid":lid,"comp":comp})
				count=cursor.fetchall()
			else:
				query="SELECT COUNT(*) FROM upload_file WHERE (first_name=%s AND last_name=%s AND comp_name=%s OR "+typ+" BETWEEN %s AND %s);"
				cursor.execute(query,(first_name,last_name,comp,st_date,end_date,))
				count=cursor.fetchall()
			msg["count"]=count[0][0]

		if(pageidx=="-2"):
			if(lid):
				cursor.execute("SELECT * FROM upload_file WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s)",{"tid":lid,"comp":comp})
			else:
				query="SELECT * FROM upload_file WHERE (first_name=%s AND last_name=%s AND comp_name=%s OR "+typ+" BETWEEN %s AND %s);"
				cursor.execute(query,(first_name,last_name,comp,st_date,end_date,))

		else:

			perpage=20
			startat=int(pageidx)*perpage
			if(lid):
				cursor.execute("SELECT * FROM upload_file WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s) LIMIT %(st)s,%(end)s",{"tid":lid,"comp":comp,"st":startat,"end":perpage})
			else:
				query="SELECT * FROM upload_file WHERE (first_name=%s AND last_name=%s AND comp_name=%s OR "+typ+" BETWEEN %s AND %s) ORDER BY "+typ+" LIMIT %s,%s;"
		
				cursor.execute(query,(first_name,last_name,comp,st_date,end_date,startat,perpage,))

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
@login_required
def analysis():
	msg={}
	risk={}
	req=request.data
	#else:
		#try:
			#cursor=mysql.connection.cursor()
			#query="SELECT disburse_date FROM upload_file ORDER BY disburse_date;"
			#cursor.execute(query,())
			#dates=cursor.fetchall()
			#end_date=str(dates[-1][0])
			#st_date=str(dates[-1][0]-relativedelta(months=+1))
			#typ="disburse_date"
			#cursor.close()
		#except Exception as e:
			#msg["error"]=str(e)
	try:
		cursor=mysql.connection.cursor()
		
		cols_query="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='upload_file';"
		cursor.execute(cols_query,())
		columns=cursor.fetchall()

		query_eq="SELECT * FROM candidate_equifax;"
		cursor.execute(query_eq,())
		eq_data=cursor.fetchall()

		cols_eq="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='candidate_equifax';"
		cursor.execute(cols_eq,())
		columns_eq=cursor.fetchall()

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

			query="SELECT * FROM upload_file WHERE "+typ+" BETWEEN %s AND %s;"
			cursor.execute(query,(st_date,end_date,))
		else:
			query="SELECT * FROM upload_file;"
			cursor.execute(query,())

		data_all=cursor.fetchall()

		if(len(data_all)<1 or len(eq_data)<1):
			msg["error"]="no data found on this search"
			return jsonify({"msg":msg})

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

		#print(analyzed_monthly)
		#print("-----------")
		#print(analyzed_weekly)
		#print("-----------")
		#print(analyzed_total)
		#print("-----------")
		#print(number_of_loans)
		#print("-----------")
		#print(volume_of_loans)

		cursor.close()

	except Exception as e:
		msg["error"]=str(e)
		print(e)

	return jsonify({"msg":msg})

####### master repay

@app.route("/search_repay",methods=["POST"])
@cross_origin(supports_credentials=True)
@login_required
def search_repay_data():
	msg={}
	req=request.data
	pageidx=request.args.get("idx")
	req=json.loads(req)
	lid=req.get("lid",None)
	if(lid):
		lid=lid.split(" ")
	f_name=req.get("fname",None)
	l_name=req.get("lname",None)
	st_date=req.get("stDate",None)
	end_date=req.get("endDate",None)
	comp=req.get("comp",None)

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
				query="SELECT COUNT(*) FROM master_repay WHERE (comp_name=%s AND %s<=st_date AND %s>=st_date OR %s>=end_date AND %s<=end_date OR %s<=st_date AND %s>=end_date);"
				cursor.execute(query,(comp,st_date,end_date,end_date,st_date,st_date,end_date,))
				count=cursor.fetchall()
				msg["count"]=count[0][0]

			if(pageidx=="-2"):
				query="SELECT * FROM master_repay WHERE (comp_name=%s AND %s<=st_date AND %s>=st_date OR %s>=end_date AND %s<=end_date OR %s<=st_date AND %s>=end_date);"
				cursor.execute(query,(comp,st_date,end_date,end_date,st_date,st_date,end_date,))

			else:	
				perpage=20
				startat=int(pageidx)*perpage
				#query="SELECT DISTINCT transaction_id FROM master_repay WHERE emi_date BETWEEN %s AND %s;"
				query="SELECT * FROM master_repay WHERE (comp_name=%s AND %s<=st_date AND %s>=st_date OR %s>=end_date AND %s<=end_date OR %s<=st_date AND %s>=end_date) LIMIT %s,%s;"
				cursor.execute(query,(comp,st_date,end_date,end_date,st_date,st_date,end_date,startat,perpage,))
	
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
			if(pageidx=="0"):
				cursor.execute("SELECT COUNT(*) FROM master_repay WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s)",{"tid":lid,"comp":comp})
				count=cursor.fetchall()
				msg["count"]=count[0][0]
				print(msg)
			if(pageidx=="-2"):
				cursor.execute("SELECT COUNT * FROM master_repay WHERE (transaction_id IN %(tid)s AND comp_name IN %(comp)s)",{"tid":lid,"comp":comp})
			else:
				perpage=20
				startat=int(pageidx)*perpage
				cursor.execute("SELECT * FROM master_repay WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s) LIMIT %(st)s,%(end)s",{"tid":lid,"comp":comp,"st":startat,"end":perpage})
			#query="SELECT * FROM master_repay WHERE transaction_id=%s"
			#cursor.execute(query,(lid,))
			data_all=cursor.fetchall()
			print(data_all)
			if(len(data_all)<1):
				msg["error"]="no data found based on this search"
				return jsonify({"msg":msg})
			data=pd.DataFrame(data_all,columns=cols)
			data.index=range(1,len(data)+1)
			data['st_date']=data['st_date'].apply(lambda x:str(x).split(" ")[0])
			data['end_date']=data['end_date'].apply(lambda x:str(x).split(" ")[0])
			body=[list(data.iloc[i].values) for i in range(len(data))]
			cl_name=list(data.columns)
			msg["clName"]=cl_name
			msg["data"]=body
			#msg["count"]=count

		if(f_name and l_name):
			query="SELECT * FROM master_repay WHERE (first_name=%s AND last_name=%s AND comp_name=%s)"
			cursor.execute(query,(f_name,l_name,comp,))
			data_all=cursor.fetchall()
			if(len(data_all)<1):
				msg["error"]="no data found based on this search"
				return jsonify({"msg":msg})
			data=pd.DataFrame(data_all,columns=cols)
			data.index=range(1,len(data)+1)
			data['st_date']=data['st_date'].apply(lambda x:str(x).split(" ")[0])
			data['end_date']=data['end_date'].apply(lambda x:str(x).split(" ")[0])
			body=[list(data.iloc[i].values) for i in range(len(data))]
			cl_name=list(data.columns)
			msg["clName"]=cl_name
			msg["data"]=body

		cursor.close()
	except Exception as e:

		msg["error"]=str(e)

	return jsonify({"msg":msg})


@app.route("/repay_track",methods=["POST"])
@cross_origin(supports_credentials=True)
@login_required
def add_repay_tracker():
	msg={}
	req=request.data
	req=json.loads(req)
	lid=req.get("lid",None)
	p_date=req.get("date",None)
	amt=req.get("pmt",None)
	remark=req.get("rem",None)
	if(lid is None or p_date is None or amt is None):
		msg["error"]="no valid data"
	else:
		try:
			cursor=mysql.connection.cursor()
			#query="INSERT INTO repay_tracker(transaction_id,payment_date,payment_amount) VALUES(%s,%s,%s);"
			#cursor.execute(query,(lid,p_date,amt))
			#mysql.connection.commit()
			query="SELECT loan_tenure,emi_amt,repayment_type,first_inst_date,emi_amount_received,carry_f,emi_number,emi_date_flag,partner_loan_id,first_name,last_name,last_date_flag FROM upload_file WHERE transaction_id=%s;"
			cursor.execute(query,(lid,))
			data_all=cursor.fetchall()

			out=repay_generator(data_all,p_date,amt)
			if(len(out)==1):
				msg["error"]="emi amount is more than total due"
				return jsonify({"msg":msg})
			if(len(out)==2):
				msg["error"]="invalid date"
				return jsonify({"msg":msg})
			query="UPDATE upload_file SET emi_amount_received=%s,carry_f=%s,emi_number=%s,emi_date_flag=%s,receipt_status=%s,last_date_flag=%s WHERE transaction_id=%s;"
			cursor.execute(query,(out[0],out[1],out[2],out[3],out[5],out[10],lid))
			
			#query="SELECT emi_amount_received,carry_f,emi_number,emi_date_flag FROM upload_file WHERE transaction_id=%s;"
			#cursor.execute(query,(lid,))
			#data_all=cursor.fetchall()
			#print(data_all)

			query="INSERT INTO repay_tracker(transaction_id,payment_date,payment_amount,due,carry_f,remark) VALUES(%s,%s,%s,%s,%s,%s);"
			cursor.execute(query,(lid,p_date,amt,out[4],out[1],remark))
			mysql.connection.commit()

			msg["success"]="data added"
			cursor.close()
		except Exception as e:
			msg["error"]=str(e)
	return jsonify({"msg":msg})


@app.route("/prfdt",methods=["POST"])
@cross_origin(supports_credentials=True)
@login_required
def prf():
	msg={}
	req=request.data
	req=json.loads(req)
	lid=req.get("lid",None)
	date=req.get("date",None)
	if(lid and date):
		try:
			cursor=mysql.connection.cursor()
			query="SELECT loan_tenure,emi_amt,repayment_type,first_inst_date,emi_amount_received,carry_f,emi_number,emi_date_flag,partner_loan_id,first_name,last_name,last_date_flag FROM upload_file WHERE transaction_id=%s;"
			cursor.execute(query,(lid,))
			data_all=cursor.fetchall()
			out=repay_generator(data_all,date,"0",mode="prfdt")
			due=out[4]
			partner_id=out[6]
			f_name=out[7]
			l_name=out[8]
			emi=data_all[0][1]
			outstanding=out[9]
			msg["fn"]=f_name
			msg["ln"]=l_name
			msg["emi"]=emi
			msg["pid"]=out[6]
			msg["out"]=outstanding
			msg["due"]=due
			msg["status"]=out[5]
		except Exception as e:
			msg["error"]=str(e)
	else:
		msg["error"]="invalid data"
	return jsonify({"msg":msg})


@app.route("/track_history",methods=["POST"])
@cross_origin(supports_credentials=True)
@login_required
def repay_history():
	msg={}
	req=request.data
	req=json.loads(req)
	lid=req.get("lid",None)
	if(lid):
		try:
			cursor=mysql.connection.cursor()
			query="SELECT repayment_type,loan_tenure,first_inst_date,emi_amt FROM upload_file WHERE transaction_id=%s;"
			cursor.execute(query,(lid,))
			fetch_data=cursor.fetchall()
			if(len(fetch_data)<1):
				msg["error"]="no data found based on this search"
				return jsonify({"msg":msg})
			loan_type=fetch_data[0][0]
			loan_tenure=int(fetch_data[0][1])
			first_emi_date=fetch_data[0][2]
			emi_amt=fetch_data[0][3]
			emi_dates=generate_emi_dates(loan_type,loan_tenure,first_emi_date)
			print(emi_dates)
			query="SELECT payment_date,payment_amount,due,carry_f,remark FROM repay_tracker WHERE transaction_id=%s ORDER BY payment_date;"
			cursor.execute(query,(lid,))
			data_all=cursor.fetchall()
			print(data_all)
			print("==================")
			all_history=generate_payment_report(data_all,emi_dates,emi_amt)
			print(all_history)
			all_history=[all_history[i] for i in all_history.keys()]
			msg["data"]=all_history
			#cursor.execute(query,(lid,))
			#data_all=cursor.fetchall()
			#cols_query="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='repay_tracker';"
			#cursor.execute(cols_query,())
			#columns=cursor.fetchall()
			#cols=[i[0] for i in columns]
			#data=pd.DataFrame(data_all,columns=cols)
			#data=data.drop(["id","transaction_id"],axis=1)
			#data['payment_amount']=data['payment_amount'].apply(lambda x:str(x))
			#data['carry_f']=data['carry_f'].apply(lambda x:str(x))
			#data['due']=data['due'].apply(lambda x:str(x))
			#data['payment_date']=data['payment_date'].apply(lambda x:str(x))
			#data['supposed_date']=data['supposed_date'].apply(lambda x:str(x))
			#body=[list(data.iloc[i].values) for i in range(len(data))]
			#msg["data"]=body

		except Exception as e:
			msg["error"]=str(e)
	else:
		msg["error"]="invalid data"
	return jsonify({"msg":msg})






if __name__=="__main__":
	app.run(host='0.0.0.0', port=5000)