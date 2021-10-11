'''
REST API enablecap lms system
@ tech@enablecap.in
@ tech2@enablecap.in
enablecap loan management system v1 2021

'''

from flask import Flask,request
import json
import base64
import datetime
from helper import *
import io
import jwt
from flask import jsonify
from flask_cors import CORS, cross_origin
from functools import wraps
from flask_mysqldb import MySQL
import time
from threading_p import Workers
from queue import Queue
from time import time
import boto3
import json


app=Flask(__name__)
CORS(app)






def login_required(f):
	@wraps(f)
	def check(*args,**kwargs):
		msg={}
		token=None
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

def update_log(eid,job_type):
	for file in my_bucket.objects.all():
		file_name=file.key
		if(file_name=="logger.json"):
			data=json.load(file.get()["Body"])
			date=str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")).split(" ")[0]
			time=str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")).split(" ")[1]
			if (date not in data["log"][job_type]):
				data["log"][job_type][date]=[]
			data["log"][job_type][date].append([eid,time])
			print(data)
			s3object=s3.Object(s3_bucket_name,file_name)
			s3object.put(Body=(bytes(json.dumps(data).encode('UTF-8'))))


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
		update_log(uname,"login")
		cursor.close()
	except Exception as e:
		msg["error"]=str(e)
	
	return jsonify({"msg":msg})


############## api endpint for upload ##############

@app.route("/",methods=["POST"])
@cross_origin(supports_credentials=True)
@login_required
def res():
	msg={}
	d=request.data
	d=json.loads(d)
	f_type=list(d.keys())[0]
	email=d["email"]
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

	elif('emi' in f_type):
		d=d[f_type]
		d=d.split(';')[1]
		d=d.split(',')[1]
		d=base64.b64decode(d)
		toread=io.BytesIO()
		toread.write(d)
		toread.seek(0)
		data=pd.read_csv(toread)
		db_type=f_type.split("_")[-1]
		data=upload_repay_once(data)
		#print(data)
		try:
			cursor=mysql.connection.cursor()
			for i in range(len(data.iloc[:])):
				lid=data.iloc[i]['Tid']
				p_date=data.iloc[i]['Repayment date']
				amt=data.iloc[i]['Actual EMI deducted']
				'''
				amt=str(amt)
				#amt=int(amt)
				if ',' in amt[:]:
					am=amt.split(",")
					amt="".join(am)
					try:
						amt=int(amt)
					except:
						amt=int(float(amt))
				'''
				
				remark=None
				query="SELECT loan_tenure,emi_amt,repayment_type,first_inst_date,emi_amount_received,carry_f,emi_number,emi_date_flag,partner_loan_id,first_name,last_name,last_date_flag FROM upload_file WHERE transaction_id=%s;"
				cursor.execute(query,(lid,))
				data_all=cursor.fetchall()
				
				out=repay_generator(data_all,p_date,amt)

				#print(lid)
				#print("//////////")
				#print(out)
				 
				#if(lid=='LOAN0237344316'):
					#print(out)
				today=str(datetime.datetime.now()).split(" ")[0]

				query="UPDATE upload_file SET emi_amount_received=%s,carry_f=%s,emi_number=%s,emi_date_flag=%s,receipt_status=%s,last_date_flag=%s WHERE transaction_id=%s;"
				cursor.execute(query,(out[0],out[1],out[2],out[3],out[5],out[10],lid))
				query="INSERT INTO repay_tracker(transaction_id,payment_date,payment_amount,due,carry_f,remark) VALUES(%s,%s,%s,%s,%s,%s);"
				cursor.execute(query,(lid,p_date,amt,out[4],out[1],remark))
				query="UPDATE emi_upload_track SET "+db_type+"=%s;"
				cursor.execute(query,(today,))

			mysql.connection.commit()
			msg["success"]="data added"
			cursor.close()
			update_log(email,"upload_reco")
		except Exception as e:
			msg["error"]=str(e)
			print(msg)

		return jsonify({"msg":msg})
		
		'''
		data=upload_repay_once(data)
		try:
			cursor=mysql.connection.cursor()
			for i in range(len(data.iloc[:])):
				lid=data.iloc[i]['Tid']
				p_date=data.iloc[i]['Repayment Date']
				amt=data.iloc[i]['Actual EMI deducted']
				
				amt=str(amt)
				#amt=int(amt)
				if ',' in amt[:]:
					am=amt.split(",")
					amt="".join(am)
					try:
						amt=int(amt)
					except:
						amt=int(float(amt))
				
				
				remark=None
				query="SELECT loan_tenure,emi_amt,repayment_type,first_inst_date,emi_amount_received,carry_f,emi_number,emi_date_flag,partner_loan_id,first_name,last_name,last_date_flag FROM upload_file WHERE transaction_id=%s;"
				cursor.execute(query,(lid,))
				data_all=cursor.fetchall()
				
				out=repay_generator(data_all,p_date,amt)
				

				query="UPDATE upload_file SET emi_amount_received=%s,carry_f=%s,emi_number=%s,emi_date_flag=%s,receipt_status=%s,last_date_flag=%s WHERE transaction_id=%s;"
				cursor.execute(query,(out[0],out[1],out[2],out[3],out[5],out[10],lid))
				query="INSERT INTO repay_tracker(transaction_id,payment_date,payment_amount,due,carry_f,remark) VALUES(%s,%s,%s,%s,%s,%s);"
				cursor.execute(query,(lid,p_date,amt,out[4],out[1],remark))
			mysql.connection.commit()
			msg["success"]="data added"
			cursor.close()
		except Exception as e:
			msg["error"]=str(e)
			return jsonify({"msg":msg})
		'''

	else:
		d=d[f_type]
		d=d.split(';')[1]
		d=d.split(',')[1]
		d=base64.b64decode(d)
		toread=io.BytesIO()
		toread.write(d)
		toread.seek(0)
		data=pd.read_excel(toread)
		col_names=data.iloc[0]
		data=data[1:]
		data.columns=col_names
		data=data.dropna(axis=0,subset=['transactionid'])
		data=data.fillna("N/A")
		print(data)
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
			cursor.close()
			update_log(email,"upload")
		except Exception as e:
			msg["error"]=str(e)
			#cursor.close()
			print(e)
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
	email=req.get("email",None)
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
				if(len(partner_lids)==0):
					msg["error"]="no data found based on this search"
					return jsonify({"msg":msg})
				cursor.execute("SELECT upload_file.*,candidate_equifax.equifax_score,candidate_equifax.category FROM upload_file LEFT JOIN candidate_equifax ON upload_file.partner_loan_id=candidate_equifax.partner_loan_id WHERE upload_file.partner_loan_id IN %(tid)s",{"tid":partner_lids})
				count=cursor.fetchall()
			else:
				query="SELECT upload_file.*,candidate_equifax.equifax_score,candidate_equifax.category FROM upload_file LEFT JOIN candidate_equifax ON upload_file.partner_loan_id=candidate_equifax.partner_loan_id WHERE (upload_file.comp_name=%s AND (upload_file.first_name=%s AND upload_file.last_name=%s OR upload_file."+typ+" BETWEEN %s AND %s));"
				cursor.execute(query,(comp,first_name,last_name,st_date,end_date,))
				count=cursor.fetchall()
			msg["count"]=len(count)

		if(pageidx=="-2"):
			if(lid):
				cursor.execute("SELECT partner_loan_id FROM upload_file WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s)",{"tid":lid,"comp":comp})
				partner_lids=cursor.fetchall()
				cursor.execute("SELECT upload_file.*,candidate_equifax.equifax_score,candidate_equifax.category FROM upload_file LEFT JOIN candidate_equifax ON upload_file.partner_loan_id=candidate_equifax.partner_loan_id WHERE upload_file.partner_loan_id IN %(tid)s",{"tid":partner_lids})
			else:
				query="SELECT upload_file.*,candidate_equifax.equifax_score,candidate_equifax.category FROM upload_file LEFT JOIN candidate_equifax ON upload_file.partner_loan_id=candidate_equifax.partner_loan_id WHERE (upload_file.comp_name=%s AND (upload_file.first_name=%s AND upload_file.last_name=%s OR upload_file."+typ+" BETWEEN %s AND %s));"
				cursor.execute(query,(comp,first_name,last_name,st_date,end_date,))

		else:
			perpage=20
			startat=int(pageidx)*perpage
			if(lid):
				cursor.execute("SELECT partner_loan_id FROM upload_file WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s)",{"tid":lid,"comp":comp})
				partner_lids=cursor.fetchall()
				cursor.execute("SELECT upload_file.*,candidate_equifax.equifax_score,candidate_equifax.category FROM upload_file LEFT JOIN candidate_equifax ON upload_file.partner_loan_id=candidate_equifax.partner_loan_id WHERE upload_file.partner_loan_id IN %(tid)s LIMIT %(st)s,%(end)s",{'tid':partner_lids,'st':startat,'end':perpage})
			else:
				#query="SELECT a1.* a2.equifax_score,a2.category FROM upload_file a1 candidate_equifax a2 WHERE a1.partner_loan_id=a2.partner_loan_id"
				query="SELECT upload_file.*,candidate_equifax.equifax_score,candidate_equifax.category FROM upload_file LEFT JOIN candidate_equifax ON upload_file.partner_loan_id=candidate_equifax.partner_loan_id WHERE (upload_file.comp_name=%s AND (upload_file.first_name=%s AND upload_file.last_name=%s OR upload_file."+typ+" BETWEEN %s AND %s)) ORDER BY upload_file."+typ+" LIMIT %s,%s;"
			
				cursor.execute(query,(comp,first_name,last_name,st_date,end_date,startat,perpage,))

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
		if(pageidx=="0"):
			update_log(email,"disbursal_mis")
		elif(pageidx=="-2"):
			update_log(email,"download_dmis")
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
	#pageidx=req.get("idx","0")
	lid=req.get("lid",None)
	if(lid):
		lid=lid.split(" ")
	first_name=req.get("fname",None)
	last_name=req.get("lname",None)
	st_date=req.get("stDate",None)
	end_date=req.get("endDate",None)
	email=req.get("email",None)
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
				query="SELECT * FROM upload_file WHERE (comp_name=%s AND (first_name=%s AND last_name=%s OR "+typ+" BETWEEN %s AND %s));"
				cursor.execute(query,(comp,first_name,last_name,st_date,end_date,))

		else:

			perpage=20
			startat=int(pageidx)*perpage
			if(lid):
				cursor.execute("SELECT * FROM upload_file WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s) LIMIT %(st)s,%(end)s;",{"tid":lid,"comp":comp,"st":startat,"end":perpage})
			else:
				query="SELECT * FROM upload_file WHERE (comp_name=%s AND (first_name=%s AND last_name=%s OR "+typ+" BETWEEN %s AND %s)) ORDER BY "+typ+" LIMIT %s,%s;"
				cursor.execute(query,(comp,first_name,last_name,st_date,end_date,startat,perpage,))

		data_all=cursor.fetchall()
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
		if(pageidx=="0"):
			update_log(email,"view_upload")
		elif(pageidx=="-2"):
			update_log(email,"download_upload")
		cursor.close()
	except Exception as e:
		msg['error']=str(e)

	return jsonify({"msg":msg})


####### generate equifax file 

@app.route("/genefx",methods=["POST"])
@cross_origin(supports_credentials=True)
@login_required
def generate_efx_report():
	msg={}
	req=request.data
	pageidx=request.args.get("idx")
	req=json.loads(req)
	lid=req.get("lid",None)
	if(lid):
		lid=lid.split(" ")
	first_name=req.get("fname",None)
	last_name=req.get("lname",None)
	st_date=req.get("stDate",None)
	end_date=req.get("endDate",None)
	email=req.get("email",None)
	comp=req.get("comp",None)
	typ=req.get("cat","loan_app_date")
	r_status=req.get("status","ongoing")
	if(st_date is not None and end_date is not None):
		st_d=datetime.datetime.strptime(st_date,"%Y-%m-%d")
		en_d=datetime.datetime.strptime(end_date,"%Y-%m-%d")
		gap=en_d-st_d
		if(gap.days<0):
			msg["error"]="end date must be bigger"
			return jsonify({"msg":msg})

	else:
		en_d=datetime.datetime.now()
		end_date=str(en_d).split(" ")[0]

	try:
		cursor=mysql.connection.cursor()
		cols_query="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='upload_file';"
		cursor.execute(cols_query,())
		columns=cursor.fetchall()
		cols=[i[0] for i in columns]

		if(pageidx=="0"):
			if(lid):
				cursor.execute("SELECT COUNT(*) FROM upload_file WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s AND receipt_status=%(ong)s)",{"tid":lid,"comp":comp,"ong":r_status})
				count=cursor.fetchall()
			else:
				query="SELECT COUNT(*) FROM upload_file WHERE (comp_name=%s AND (receipt_status=%s AND (first_name=%s AND last_name=%s OR "+typ+">=%s AND "+typ+"<=%s)));"
				cursor.execute(query,(comp,r_status,first_name,last_name,st_date,end_date,))
				count=cursor.fetchall()
			msg["count"]=count[0][0]

		if(pageidx=="-2"):
			if(lid):
				cursor.execute("SELECT * FROM upload_file WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s)",{"tid":lid,"comp":comp})
			else:
				query="SELECT * FROM upload_file WHERE (comp_name=%s AND (first_name=%s AND last_name=%s OR "+typ+">=%s AND "+typ+"<=%s));"
				cursor.execute(query,(comp,first_name,last_name,st_date,end_date,))

		else:

			perpage=20
			startat=int(pageidx)*perpage
			if(lid):
				cursor.execute("SELECT * FROM upload_file WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s AND receipt_status=%(ong)s) LIMIT %(st)s,%(end)s;",{"tid":lid,"comp":comp,"ong":r_status,"st":startat,"end":perpage})
			else:
				query="SELECT * FROM upload_file WHERE (comp_name=%s AND (receipt_status=%s AND (first_name=%s AND last_name=%s OR "+typ+">=%s AND "+typ+"<=%s))) ORDER BY "+typ+" LIMIT %s,%s;"
				cursor.execute(query,(comp,r_status,first_name,last_name,st_date,end_date,startat,perpage,))

		data_all=cursor.fetchall()
		if(len(data_all)<1):
			msg['error']='no data found'
			return jsonify({"msg":msg})
		
		data=pd.DataFrame(data_all,columns=cols)
		received_amount=[]
		due_list=[]
		no_emi_due=[]
		for i in range(len(data.iloc[:])):
			r_amt=0
			due_data=0
			query_t="SELECT * FROM repay_tracker WHERE transaction_id=%s ORDER BY payment_date"
			cursor.execute(query_t,(data.iloc[i]["transaction_id"],))
			data_all=cursor.fetchall()
			if(len(data_all)<1):

				query_data=((data.iloc[i]["loan_tenure"],data.iloc[i]["emi_amt"],data.iloc[i]["repayment_type"],data.iloc[i]["first_inst_date"],
					data.iloc[i]["emi_amount_received"],data.iloc[i]["carry_f"],data.iloc[i]["emi_number"],data.iloc[i]["emi_date_flag"],
					data.iloc[i]["partner_loan_id"],data.iloc[i]["first_name"],data.iloc[i]["last_name"],data.iloc[i]["last_date_flag"]),)

				pp=repay_generator(query_data,end_date,"0",mode="prfdt")
				due_data=int(pp[4])
				if(due_data<0):
					due_data=0
				r_amt=int(data.iloc[i]["emi_amount_received"])

				due_list.append(due_data)
				received_amount.append(r_amt)


			if(len(data_all)>0):

				if(en_d<=datetime.datetime.strptime(str(data_all[-1][3]),"%Y-%m-%d")):
					
					for p_data in reversed(data_all):
						if(en_d>=datetime.datetime.strptime(str(p_data[3]),"%Y-%m-%d")):
							due_data=int(p_data[4])-int(p_data[2])
							if(due_data<0):
								due_data=0
							break

					for p_data in data_all:
						if(en_d<datetime.datetime.strptime(str(p_data[3]),"%Y-%m-%d")):
							break
						r_amt+=int(p_data[2])

				elif(en_d>datetime.datetime.strptime(str(data_all[-1][3]),"%Y-%m-%d")):

					query_data=((data.iloc[i]["loan_tenure"],data.iloc[i]["emi_amt"],data.iloc[i]["repayment_type"],data.iloc[i]["first_inst_date"],
						data.iloc[i]["emi_amount_received"],data.iloc[i]["carry_f"],data.iloc[i]["emi_number"],data.iloc[i]["emi_date_flag"],
						data.iloc[i]["partner_loan_id"],data.iloc[i]["first_name"],data.iloc[i]["last_name"],data.iloc[i]["last_date_flag"]),)

					pp=repay_generator(query_data,end_date,"0",mode="prfdt")
					due_data=int(pp[4])
					if(due_data<0):
						due_data=0
					r_amt=int(data.iloc[i]["emi_amount_received"])

				due_list.append(due_data)
				received_amount.append(r_amt)


		#print(due_list)

			####### equifax ######
			emi_dates=generate_emi_dates(data.iloc[i]['repayment_type'],int(data.iloc[i]['loan_tenure']),data.iloc[i]['first_inst_date'])
			emi_d=[j for j in emi_dates if j<=datetime.datetime.strptime(end_date,"%Y-%m-%d")]
			tot_amt=len(emi_d)*int(data.iloc[i]["emi_amt"])
			rec_amt=data.iloc[i]["emi_amount_received"]
			tot_due=tot_amt-int(rec_amt)
			due_till=int(float(int(tot_due)/int(data.iloc[i]["emi_amt"])))
			if(due_till<=0):
				no_emi_due.append(0)
			else:
				no_emi_due.append(due_till)
			######################
			

		data=equifax_generator(data,end_date,due_list,received_amount,data['last_date_flag'],no_emi_due)
		data.index=range(1,len(data)+1)
		body=[list(data.iloc[i].values) for i in range(len(data))]
		cl_name=list(data.columns)
		msg["clName"]=cl_name
		msg["data"]=body
		if(pageidx=="0"):
			update_log(email,"eqfx")
		elif(pageidx=="-2"):
			update_log(email,"download_eqfx")
		cursor.close()
		
	except Exception as e:
		msg['error']=str(e)
		print(msg)

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
	email=req.get("email",None)
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
				query="SELECT * FROM upload_file WHERE (comp_name=%s AND (first_name=%s AND last_name=%s OR "+typ+" BETWEEN %s AND %s));"
				cursor.execute(query,(comp,first_name,last_name,st_date,end_date,))

		else:

			perpage=20
			startat=int(pageidx)*perpage
			if(lid):
				cursor.execute("SELECT * FROM upload_file WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s) LIMIT %(st)s,%(end)s",{"tid":lid,"comp":comp,"st":startat,"end":perpage})
			else:
				query="SELECT * FROM upload_file WHERE (comp_name=%s AND (first_name=%s AND last_name=%s OR "+typ+" BETWEEN %s AND %s)) ORDER BY "+typ+" LIMIT %s,%s;"
		
				cursor.execute(query,(comp,first_name,last_name,st_date,end_date,startat,perpage,))

		data_all=cursor.fetchall()
		if(len(data_all)<1):
			msg["error"]="no data found based on this search"
			return jsonify({"msg":msg})
		
		data=pd.DataFrame(data_all,columns=cols)
		data.index=range(1,len(data)+1)
		#######
		final_data=bank_upload_process(data=data,type_comp=comp)
		body=[list(final_data.iloc[i].values) for i in range(len(final_data))]
		cl_name=list(final_data.columns)
		msg["clName"]=cl_name
		msg["data"]=body
		if(pageidx=="0"):
			update_log(email,"bank_upload")
		elif(pageidx=="-2"):
			update_log(email,"download_bank_up")
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
	req=json.loads(req)
	email=req.get("email",None)
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

		print(msg)

		update_log(email,"view_analysis")

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
	email=req.get("email",None)
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
				query="SELECT COUNT(*) FROM master_repay WHERE (comp_name=%s AND (%s<=st_date AND %s>=st_date OR %s>=end_date AND %s<=end_date OR %s<=st_date AND %s>=end_date));"
				cursor.execute(query,(comp,st_date,end_date,end_date,st_date,st_date,end_date,))
				count=cursor.fetchall()
				msg["count"]=count[0][0]

			if(pageidx=="-2"):
				query="SELECT * FROM master_repay WHERE (comp_name=%s AND (%s<=st_date AND %s>=st_date OR %s>=end_date AND %s<=end_date OR %s<=st_date AND %s>=end_date));"
				cursor.execute(query,(comp,st_date,end_date,end_date,st_date,st_date,end_date,))

			else:	
				perpage=20
				startat=int(pageidx)*perpage
				#query="SELECT DISTINCT transaction_id FROM master_repay WHERE emi_date BETWEEN %s AND %s;"
				query="SELECT * FROM master_repay WHERE (comp_name=%s AND (%s<=st_date AND %s>=st_date OR %s>=end_date AND %s<=end_date OR %s<=st_date AND %s>=end_date)) LIMIT %s,%s;"
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
			if(pageidx=="-2"):
				cursor.execute("SELECT COUNT * FROM master_repay WHERE (transaction_id IN %(tid)s AND comp_name IN %(comp)s)",{"tid":lid,"comp":comp})
			else:
				perpage=20
				startat=int(pageidx)*perpage
				cursor.execute("SELECT * FROM master_repay WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s) LIMIT %(st)s,%(end)s",{"tid":lid,"comp":comp,"st":startat,"end":perpage})
			#query="SELECT * FROM master_repay WHERE transaction_id=%s"
			#cursor.execute(query,(lid,))
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
			#msg["count"]=count

		if(f_name and l_name):
			if(pageidx=="0"):
				query="SELECT COUNT(*) FROM master_repay WHERE (first_name=%s AND last_name=%s AND comp_name=%s)"
				cursor.execute(query,(f_name,l_name,comp,))
				count=cursor.fetchall()
				msg["count"]=count[0][0]
			if(pageidx=="-2"):
				query="SELECT * FROM master_repay WHERE (first_name=%s AND last_name=%s AND comp_name=%s)"
				cursor.execute(query,(f_name,l_name,comp,))
			else:
				perpage=20
				startat=int(pageidx)*perpage
				query="SELECT * FROM master_repay WHERE (first_name=%s AND last_name=%s AND comp_name=%s) LIMIT %s,%s"
				cursor.execute(query,(f_name,l_name,comp,startat,perpage,))

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

		if(pageidx=="0"):
			update_log(email,"master_repay_search")
		elif(pageidx=="-2"):
			update_log(email,"download_mr")

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
	email=req.get("email",None);
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
			update_log(email,"payment")
			cursor.close()
		except Exception as e:
			msg["error"]=str(e)
	return jsonify({"msg":msg})


@app.route("/upload_repay",methods=["GET"])
@cross_origin(supports_credentials=True)
def up_repay():
	msg={}
	data=upload_repay_once()
	try:
		cursor=mysql.connection.cursor()
		for i in range(len(data.iloc[:])):
			lid=data.iloc[i]['Tid']
			p_date=data.iloc[i]['Repayment Date']
			amt=data.iloc[i]['Actual EMI deducted']
			'''
			amt=str(amt)
			#amt=int(amt)
			if ',' in amt[:]:
				am=amt.split(",")
				amt="".join(am)
				try:
					amt=int(amt)
				except:
					amt=int(float(amt))
			'''
			
			remark=None
			query="SELECT loan_tenure,emi_amt,repayment_type,first_inst_date,emi_amount_received,carry_f,emi_number,emi_date_flag,partner_loan_id,first_name,last_name,last_date_flag FROM upload_file WHERE transaction_id=%s;"
			cursor.execute(query,(lid,))
			data_all=cursor.fetchall()
			
			out=repay_generator(data_all,p_date,amt)
			

			query="UPDATE upload_file SET emi_amount_received=%s,carry_f=%s,emi_number=%s,emi_date_flag=%s,receipt_status=%s,last_date_flag=%s WHERE transaction_id=%s;"
			cursor.execute(query,(out[0],out[1],out[2],out[3],out[5],out[10],lid))
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
	email=req.get("email",None);
	if(lid and date):
		try:
			cursor=mysql.connection.cursor()
			query="SELECT loan_tenure,emi_amt,repayment_type,first_inst_date,emi_amount_received,carry_f,emi_number,emi_date_flag,partner_loan_id,first_name,last_name,last_date_flag FROM upload_file WHERE transaction_id=%s;"
			cursor.execute(query,(lid,))
			data_all=cursor.fetchall()
			out=repay_generator(data_all,date,"0",mode="prfdt")

			query="SELECT payment_status FROM upload_file WHERE transaction_id=%s;"
			cursor.execute(query,(lid,))
			data_status=cursor.fetchall()

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
			msg["status"]=data_status[0]
			update_log(email,"rt_details")
			cursor.close()
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
			query="SELECT payment_date,payment_amount,due,carry_f,remark FROM repay_tracker WHERE transaction_id=%s ORDER BY payment_date;"
			cursor.execute(query,(lid,))
			data_all=cursor.fetchall()
			all_history=generate_payment_report(data_all,emi_dates,emi_amt)
			all_history=[all_history[i] for i in all_history.keys()]
			print(all_history)
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
			cursor.close()
		except Exception as e:
			msg["error"]=str(e)
	else:
		msg["error"]="invalid data"
	return jsonify({"msg":msg})



###################
'''
class CronJob(object):
    def __init__(self,event):
        self.event=event
        
    def run(self):
        t=datetime.datetime.strptime("2021-06-26 18:38:00","%Y-%m-%d %H:%M:%S")
        while 1:
            while datetime.datetime.now() < t:
                time.sleep(600)

            print(t)
            t += datetime.timedelta(minutes=10)
            print(self.event())

'''

#print(datetime.datetime.now())

#cr=CronJob(update_status_cron)
#cr.run()


@app.route("/view_report",methods=["POST"])
@cross_origin(supports_credentials=True)
def view_report_status():
	#print("here kk");
	msg={}
	req=request.data
	pageidx=request.args.get("idx")
	req=json.loads(req)
	lid=req.get("lid",None)
	if(lid):
		lid=lid.split(" ")
	first_name=req.get("fname",None)
	last_name=req.get("lname",None)
	email=req.get("email",None)
	comp=req.get("comp",None)
	l_status=req.get("loan_status",None)
	#if(l_status):
		#l_status=l_status.split(" ")

	try:
		cursor=mysql.connection.cursor()
		cols_query="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='chk_status';"
		cursor.execute(cols_query,())
		columns=cursor.fetchall()
		cols=[i[0] for i in columns]

		if(pageidx=="0"):
			if(lid):
				cursor.execute("SELECT COUNT(*) FROM chk_status WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s)",{"tid":lid,"comp":comp})
				count=cursor.fetchall()
			if(first_name and last_name):
				query="SELECT COUNT(*) FROM chk_status WHERE (comp_name=%s AND (first_name=%s AND last_name=%s));"
				cursor.execute(query,(comp,first_name,last_name,))
				count=cursor.fetchall()
			if(l_status):
				#query="SELECT COUNT(*) FROM chk_status WHERE (comp_name=%s AND status_up=%s);"
				if("all" in l_status or "ALL" in l_status or "All" in l_status):
					cursor.execute("SELECT COUNT(*) FROM chk_status WHERE comp_name=%(comp)s",{"comp":comp})
				else:
					cursor.execute("SELECT COUNT(*) FROM chk_status WHERE (comp_name=%(comp)s AND status_up IN %(l_st)s)",{"comp":comp,"l_st":l_status})
				count=cursor.fetchall()

			msg["count"]=count[0][0]

		if(pageidx=="-2"):
			if(lid):
				cursor.execute("SELECT * FROM chk_status WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s)",{"tid":lid,"comp":comp})
			if(first_name and last_name):
				query="SELECT * FROM chk_status WHERE (comp_name=%s AND (first_name=%s AND last_name=%s));"
				cursor.execute(query,(comp,first_name,last_name,))
			if(l_status):
				#query="SELECT * FROM chk_status WHERE (comp_name=%s AND status_up=%s);"
				if("all" in l_status or "ALL" in l_status or "All" in l_status):
					print("damn")
					cursor.execute("SELECT * FROM chk_status WHERE comp_name=%(comp)s",{"comp":comp})
				else:
					cursor.execute("SELECT * FROM chk_status WHERE (comp_name=%(comp)s AND status_up IN %(l_st)s)",{"comp":comp,"l_st":l_status})
				#cursor.execute(query,(comp,l_status,))

		else:

			perpage=20
			startat=int(pageidx)*perpage
			if(lid):
				cursor.execute("SELECT * FROM chk_status WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s) LIMIT %(st)s,%(end)s",{"tid":lid,"comp":comp,"st":startat,"end":perpage})
			if(first_name and last_name):
				query="SELECT * FROM chk_status WHERE (comp_name=%s AND (first_name=%s AND last_name=%s)) LIMIT %s,%s;"
				cursor.execute(query,(comp,first_name,last_name,startat,perpage,))
			if(l_status):
				#query="SELECT * FROM chk_status WHERE (comp_name=%s AND status_up=%s) LIMIT %s,%s;"
				#cursor.execute(query,(comp,l_status,startat,perpage,))
				if("all" in l_status or "ALL" in l_status or "All" in l_status):
					cursor.execute("SELECT * FROM chk_status WHERE comp_name=%(comp)s LIMIT %(st)s,%(end)s",{"comp":comp,"st":startat,"end":perpage})
				else:
					cursor.execute("SELECT * FROM chk_status WHERE (comp_name=%(comp)s AND status_up IN %(l_st)s) LIMIT %(st)s,%(end)s",{"comp":comp,"l_st":l_status,"st":startat,"end":perpage})

		data_all=cursor.fetchall()
		print(data_all)
		if(len(data_all)<1):
			msg['error']='no data found'
			return jsonify({"msg":msg})
		
		data=pd.DataFrame(data_all,columns=cols)
		data['amt_paid_last']=data['amt_paid_last'].apply(lambda x:str(x))
		data['overdue_amount']=data['overdue_amount'].apply(lambda x:str(x))
		data['total_outstanding']=data['total_outstanding'].apply(lambda x:str(x))
		data['no_of_payment_period_missed']=data['no_of_payment_period_missed'].apply(lambda x:str(x))
		data['total_number_of_installment']=data['total_number_of_installment'].apply(lambda x:str(x))
		data['number_of_installments_paid']=data['number_of_installments_paid'].apply(lambda x:str(x))
		body=[list(data.iloc[i].values) for i in range(len(data))]
		cl_name=list(data.columns)
		msg["clName"]=cl_name
		msg["data"]=body
		if(pageidx=="0"):
			update_log(email,"loan_report_search")
		elif(pageidx=="-2"):
			update_log(email,"download_lp")
		cursor.close()
		
	except Exception as e:
		msg['error']=str(e)

	return jsonify({"msg":msg})


@app.route("/getemitrack",methods=["POST"])
@cross_origin(supports_credentials=True)
def get_emi_track():
	msg={}
	try:
		cursor=mysql.connection.cursor()
		cursor.execute("SELECT * FROM emi_upload_track")
		data=cursor.fetchall()
		if(len(data)<1):
			msg['info']='no data found'
			return jsonify({"msg":msg})
		msg["success"]=data[0]
		
		cursor.close()
	except Exception as e:
		msg['error']=str(e)

	return jsonify({"msg":msg})


@app.route("/view_report_out",methods=["POST"])
@cross_origin(supports_credentials=True)
def view_report_st():
	msg={}
	req=request.data
	req=json.loads(req)
	comp=req.get("comp",None)
	try:
		cursor=mysql.connection.cursor()
		query="SELECT total_outstanding FROM chk_status WHERE comp_name=%s"
		cursor.execute(query,(comp,))
		data_all=cursor.fetchall()
		if(len(data_all)<1):
			msg['error']='no data found'
			return jsonify({"msg":msg})

		sum_all=0
		for i in data_all:
			sum_all+=int(i[0])
		
		msg["data"]=sum_all
		update_log(email,"view_os")
		cursor.close()
	except Exception as e:
		msg['error']=str(e)

	return jsonify({"msg":msg})



@app.route("/view_due",methods=["POST"])
@cross_origin(supports_credentials=True)
def view_due():
	msg={}
	req=request.data
	req=json.loads(req)
	comp=req.get("comp",None)
	st_date=req.get("stDate",None)
	end_date=req.get("endDate",None)
	try:
		tot_sum=0
		cursor=mysql.connection.cursor()

		if(st_date is None and end_date is None):
			end_date=datetime.datetime.now()
			query="SELECT loan_tenure,emi_amt,repayment_type,first_inst_date,emi_amount_received,carry_f,emi_number,emi_date_flag,partner_loan_id,first_name,last_name,last_date_flag,comp_name,sanction_amount,transaction_id FROM upload_file WHERE comp_name=%s AND first_inst_date<=%s"
			cursor.execute(query,(comp,end_date,))
			fetch_data=cursor.fetchall()
			if(len(fetch_data)<1):
				msg["error"]="no data found"
				return jsonify({"msg":msg})

			for i in fetch_data:
				loan_type=i[2]
				loan_tenure=int(i[0])
				first_emi_date=i[3]
				emi_amt=i[1]
				f_name=i[9]
				l_name=i[10]
				comp_name=i[12]
				sanction_amount=i[13]
				emi_amount_received=i[4]
				
				emi_dates=generate_emi_dates(loan_type,loan_tenure,first_emi_date)
				query="SELECT payment_date,payment_amount,due,carry_f,remark FROM repay_tracker WHERE transaction_id=%s ORDER BY payment_date;"
				cursor.execute(query,(i[14],))
				data_all=cursor.fetchall()
				all_history=generate_payment_report(data_all,emi_dates,emi_amt)
				for j in all_history.keys():
					if(j<=end_date):
						tot_sum+=int(all_history[j][2])
					else:
						continue

			'''
			end_date=datetime.datetime.now()
			#query="SELECT * FROM repay_tracker WHERE comp_name=%s and payment_date<=%s"
			query="SELECT repay_tracker.payment_date,repay_tracker.due FROM repay_tracker LEFT JOIN upload_file ON repay_tracker.transaction_id=upload_file.transaction_id WHERE upload_file.comp_name=%s AND repay_tracker.payment_date<=%s"
			cursor.execute(query,(comp,end_date,))
			data_all=cursor.fetchall()
			print(len(data_all))
			for i in data_all:
				tot_sum+=int(i[1])
			'''



		elif(st_date is not None and end_date is not None):
			'''
			st_date=datetime.datetime.strptime(st_date,"%Y-%m-%d")
			end_date=datetime.datetime.strptime(end_date,"%Y-%m-%d")
			query="SELECT repay_tracker.payment_date,repay_tracker.due FROM repay_tracker LEFT JOIN upload_file ON repay_tracker.transaction_id=upload_file.transaction_id WHERE upload_file.comp_name=%s AND (repay_tracker.payment_date>=%s AND repay_tracker.payment_date<=%s)"
			cursor.execute(query,(comp,st_date,end_date,))
			data_all=cursor.fetchall()
			print(len(data_all))
			for i in data_all:
				tot_sum+=int(i[1])
			'''
			st_date=datetime.datetime.strptime(st_date,"%Y-%m-%d")
			end_date=datetime.datetime.strptime(end_date,"%Y-%m-%d")
			query="SELECT loan_tenure,emi_amt,repayment_type,first_inst_date,emi_amount_received,carry_f,emi_number,emi_date_flag,partner_loan_id,first_name,last_name,last_date_flag,comp_name,sanction_amount,transaction_id FROM upload_file WHERE comp_name=%s AND first_inst_date<=%s"
			cursor.execute(query,(comp,end_date,))
			fetch_data=cursor.fetchall()
			if(len(fetch_data)<1):
				msg["error"]="no data found"
				return jsonify({"msg":msg})

			for i in fetch_data:
				loan_type=i[2]
				loan_tenure=int(i[0])
				first_emi_date=i[3]
				emi_amt=i[1]
				f_name=i[9]
				l_name=i[10]
				comp_name=i[12]
				sanction_amount=i[13]
				emi_amount_received=i[4]
				
				emi_dates=generate_emi_dates(loan_type,loan_tenure,first_emi_date)
				query="SELECT payment_date,payment_amount,due,carry_f,remark FROM repay_tracker WHERE transaction_id=%s ORDER BY payment_date;"
				cursor.execute(query,(i[14],))
				data_all=cursor.fetchall()
				all_history=generate_payment_report(data_all,emi_dates,emi_amt)
				for j in all_history.keys():
					if(j>=st_date and j<=end_date):
						tot_sum+=int(all_history[j][2])
					else:
						continue
					

		elif(st_date is None and end_date is not None):
			for i in data_all:
				emi_dates=generate_emi_dates_due(i[0],int(i[1]),i[2],"2021-01-01",end_date)
				tot_sum+=len(emi_dates)*int(i[3])+int(i[4])

		msg["data"]=tot_sum

		cursor.close()
			
	except Exception as e:
		msg['error']=str(e)

	return jsonify({"msg":msg})




@app.route("/repayment_tracker",methods=["POST"])
@cross_origin(supports_credentials=True)
@login_required
def repaypemt_tracker():
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
	email=req.get("email",None)
	search_cat=req.get("repay_type",None)
	comp=req.get("comp",None)

	if(st_date is not None and end_date is not None):
		st_d=datetime.datetime.strptime(st_date,"%Y-%m-%d")
		en_d=datetime.datetime.strptime(end_date,"%Y-%m-%d")
		gap=en_d-st_d
		if(gap.days<0):
			msg["error"]="end date must be bigger"
			return jsonify({"msg":msg})
	elif(st_date is None or end_date is None):
		if(search_cat=="analysis"):
			msg["error"]="You have to specify both start date and date for analysis"
			return jsonify({"msg":msg})

	try:
		cursor=mysql.connection.cursor()
		tracker_data=[]
		if(st_date and end_date):

			if(pageidx=="0"):
				if(search_cat=="repay_tracker"):
					query="SELECT COUNT(*) FROM master_repay WHERE (comp_name=%s AND (end_date>=%s AND st_date<=%s));"
					cursor.execute(query,(comp,st_date,end_date,))
					count=cursor.fetchall()
					msg["count"]=count[0][0]
				else:
					query="SELECT COUNT(*) FROM master_repay WHERE (comp_name=%s AND (end_date>=%s AND st_date<=%s));"
					cursor.execute(query,(comp,st_date,end_date,))
					count=cursor.fetchall()


			if(pageidx=="-2"):
				if(search_cat=="repay_tracker"):
					query="SELECT * FROM master_repay WHERE (comp_name=%s AND (end_date>=%s AND st_date<=%s));"
					cursor.execute(query,(comp,st_date,end_date,))
				else:
					query="SELECT * FROM master_repay WHERE (comp_name=%s AND (end_date>=%s AND st_date<=%s));"
					cursor.execute(query,(comp,st_date,end_date,))

			else:
				if(search_cat=="repay_tracker"):	
					perpage=20
					startat=int(pageidx)*perpage
					#query="SELECT DISTINCT transaction_id FROM master_repay WHERE emi_date BETWEEN %s AND %s;"
				
					query="SELECT * FROM master_repay WHERE (comp_name=%s AND (end_date>=%s AND st_date<=%s)) LIMIT %s,%s;"
					cursor.execute(query,(comp,st_date,end_date,startat,perpage,))
				else:
					query="SELECT * FROM master_repay WHERE (comp_name=%s AND (end_date>=%s AND st_date<=%s));"
					cursor.execute(query,(comp,st_date,end_date,))
	
			data_all=cursor.fetchall()

			if(len(data_all)<1):
				msg["error"]="no data found based on this search"
				return jsonify({"msg":msg})

			#msg["data"]=all_history

			
		elif(lid):
			if(pageidx=="0"):
				cursor.execute("SELECT COUNT(*) FROM master_repay WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s)",{"tid":lid,"comp":comp})
				count=cursor.fetchall()
				msg["count"]=count[0][0]
			if(pageidx=="-2"):
				cursor.execute("SELECT * FROM master_repay WHERE (transaction_id IN %(tid)s AND comp_name=%(comp)s)",{"tid":lid,"comp":comp})
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
		

		elif(f_name and l_name):
			if(pageidx=="0"):
				query="SELECT COUNT(*) FROM master_repay WHERE (first_name=%s AND last_name=%s AND comp_name=%s)"
				cursor.execute(query,(f_name,l_name,comp,))
				count=cursor.fetchall()
				msg["count"]=count[0][0]
			if(pageidx=="-2"):
				query="SELECT * FROM master_repay WHERE (first_name=%s AND last_name=%s AND comp_name=%s)"
				cursor.execute(query,(f_name,l_name,comp,))
			else:
				perpage=20
				startat=int(pageidx)*perpage
				query="SELECT * FROM master_repay WHERE (first_name=%s AND last_name=%s AND comp_name=%s) LIMIT %s,%s"
				cursor.execute(query,(f_name,l_name,comp,startat,perpage,))

			data_all=cursor.fetchall()
			if(len(data_all)<1):
				msg["error"]="no data found based on this search"
				return jsonify({"msg":msg})
		
		#total=0
		'''
		queue=Queue()
		for _ in range(8):
			worker=Workers(queue,tracker_data,total,st_date,end_date,st_d,en_d)
			worker.daemon=True
			worker.start()

		for i in data_all:
			query="SELECT payment_date,payment_amount,due,carry_f,remark FROM repay_tracker WHERE transaction_id=%s ORDER BY payment_date;"
			cursor.execute(query,(i[0],))
			fetch_all=cursor.fetchall()
			queue.put((i,fetch_all))
		queue.join()

		'''
		tracker_dict={}
		emi_all={}
		for i in data_all:
			loan_type=i[3]
			loan_tenure=int(i[4])
			first_emi_date=i[6]
			emi_amt=i[5]
			emi_dates=generate_emi_dates(loan_type,loan_tenure,first_emi_date)
			query="SELECT payment_date,payment_amount,due,carry_f,remark FROM repay_tracker WHERE transaction_id=%s ORDER BY payment_date;"
			cursor.execute(query,(i[0],))
			fetch_all=cursor.fetchall()
			all_history=generate_payment_report(fetch_all,emi_dates,emi_amt)
			for hist in all_history.keys():
				if(st_date is not None and end_date is not None):
					'''
					if (st_d==en_d):
						if(repay_tracker_date(all_history[hist][0])==st_d):
							tracker_data.append([i[0],i[1],i[2],loan_type,emi_amt,all_history[hist][0],all_history[hist][1],all_history[hist][2],all_history[hist][3]])
					'''

					if(repay_tracker_date(all_history[hist][0])>=st_d and repay_tracker_date(all_history[hist][0])<=en_d):
						if(search_cat=="repay_tracker"):
							tracker_data.append([i[0],i[1],i[2],loan_type,emi_amt,all_history[hist][0],all_history[hist][1],all_history[hist][2],all_history[hist][3],str(int(all_history[hist][2])-int(emi_amt))])
							#total+=int(all_history[hist][2])
						else:
							tracker_dict[repay_tracker_date(all_history[hist][0])]=tracker_dict.get(repay_tracker_date(all_history[hist][0]),0)+int(all_history[hist][2])
							emi_all[repay_tracker_date(all_history[hist][0])]=emi_all.get(repay_tracker_date(all_history[hist][0]),0)+int(emi_amt)
							#total+=int(all_history[hist][2])
							#print(total)
							#total=0
				else:
					tracker_data.append([i[0],i[1],i[2],loan_type,emi_amt,all_history[hist][0],all_history[hist][1],all_history[hist][2],all_history[hist][3],str(int(all_history[hist][2])-int(emi_amt))])
			


		columns=['TransactionId','First_Name','Last_Name','Loan_Type','EMI_Amount','Payment Date','Amount_Paid','Amount_Due','Amount_to_be_carry_forward','Carry_Forward']

		if(search_cat=="repay_tracker"):	
			msg['clName']=columns
			msg['data']=tracker_data
		elif(search_cat=="analysis"):
			msg['clName']=['Date','Due','Emi Amount']
			msg['data']=[[str(key).split(" ")[0],val,emi_all[key]] for key,val in sorted(tracker_dict.items())]
			msg["count"]=len(tracker_dict)
			if(pageidx=="0"):
				update_log(email,"dbd_due_search")
			elif(pageidx=="-2"):
				update_log(email,"download_dbd_due")
		else:
			msg['clName']=columns
			msg['data']=tracker_data
		#print(total)
		#print(tracker_dict)
		#print(msg)
		if(search_cat is None or search_cat!="analysis"):
			if(pageidx=="0"):
				update_log(email,"rt_search")
			elif(pageidx=="-2"):
				update_log(email,"download_rt")

		cursor.close()
	except Exception as e:

		msg["error"]=str(e)

	return jsonify({"msg":msg})


@app.route("/cronjob",methods=["POST"])
@cross_origin(supports_credentials=True)
def cronJob():
	msg={}
	req=request.data
	req=json.loads(req)
	email=req.get("email",None)
	date=datetime.datetime.now()
	check_time=3
	all_status=[]
	try:
		cursor=mysql.connection.cursor()
		query="SELECT transaction_id FROM upload_file"
		cursor.execute(query,)
		ti=cursor.fetchall()

		if(len(ti)<1):
			msg["error"]="no data found"
			return jsonify({"msg":msg})

		for ti_val in ti:       
			query="SELECT loan_tenure,emi_amt,repayment_type,first_inst_date,emi_amount_received,carry_f,emi_number,emi_date_flag,partner_loan_id,first_name,last_name,last_date_flag,comp_name,sanction_amount FROM upload_file WHERE transaction_id=%s;"
			cursor.execute(query,(ti_val[0],))
			fetch_data=cursor.fetchall()
			if(len(fetch_data)<1):
				msg["error"]="no data found"
				return jsonify({"msg":msg})

			loan_type=fetch_data[0][2]
			loan_tenure=int(fetch_data[0][0])
			first_emi_date=fetch_data[0][3]
			emi_amt=fetch_data[0][1]
			f_name=fetch_data[0][9]
			l_name=fetch_data[0][10]
			comp_name=fetch_data[0][12]
			sanction_amount=fetch_data[0][13]
			emi_amount_received=fetch_data[0][4]
			total_installments_paid=int(float(int(emi_amount_received)/int(emi_amt)))
			emi_dates=generate_emi_dates(loan_type,loan_tenure,first_emi_date)
			query="SELECT payment_date,payment_amount,due,carry_f,remark FROM repay_tracker WHERE transaction_id=%s ORDER BY payment_date;"
			cursor.execute(query,(ti_val[0],))
			data_all=cursor.fetchall()
			all_history=generate_payment_report_V1(data_all,emi_dates,emi_amt)


			due=0
			counter=0
			amount_paid=""
			status=""
			last_date_amt_paid=""
			last_date_payed="no emi received"
			red_flag=''
			date_counter=0

			check_dates=[i for i in all_history.keys() if i<=date]

			if(len(check_dates)==0):
				total_outstanding=loan_tenure*int(emi_amt)
				final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,0,sanction_amount,0,emi_amt,total_outstanding,comp_name,date_counter,"notstarted",red_flag,0,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],0]

			else:

				counter_r=0
				for i in check_dates[::-1]:
					amount_paid=int(all_history[i][1])
					if(amount_paid>0 or int(all_history[i][3])<=0):
						break
					counter_r+=1

				emi_dates_prev=[i for i in all_history.keys() if all_history[i][6]=="ed" and i<=date]

				#due=int(all_history[check_dates[-1]][2])
				due=repay_generator(fetch_data,str(date).split(" ")[0],"0",mode="prfdt")[4]
				due=int(due)

				if(len(emi_dates_prev)<check_time):
					for i in check_dates[::-1]:
						amount_paid=int(all_history[i][1])
						if(amount_paid>0):
							last_date_payed=str(i).split(" ")[0]
							break

		

					total_outstanding=(loan_tenure*int(emi_amt))-int(emi_amount_received)

				

					if(date>emi_dates[-1]):

						if(total_outstanding==0):
							status="emi_closed,payment_received"
						elif(total_outstanding>0):
							status="emi_closed,not_paid"
							red_flag="red"
							if(due<int(emi_amt)):
								date_counter=1
							else:
								date_counter=int(float(int(due)/int(emi_amt)))
						elif(total_outstanding<0):
							status="emi_closed_with_advance"

						final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag,counter_r,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],len(emi_dates_prev)*int(emi_amt)]

					else:

						if(due==0 and int(amount_paid)>0):

							status="ontime,ongoing"
							final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag,counter_r,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],len(emi_dates_prev)*int(emi_amt)]
							
						if(due>0):

							status="overdue,ongoing"
							if(due<int(emi_amt)):
								date_counter=1
							else:
								date_counter=int(float(int(due)/int(emi_amt)))
							final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag,counter_r,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],len(emi_dates_prev)*int(emi_amt)]

						if(due<0):

							status="advance,ongoing"
							final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag,counter_r,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],len(emi_dates_prev)*int(emi_amt)]

				else:
					counter_r=0
					for i in check_dates[::-1]:
						amount_paid=int(all_history[i][1])
						if(amount_paid>0 or int(all_history[i][3])<=0):
							break
						counter_r+=1

					
					counter=0
					for i in check_dates[::-1]:
						flag_date_ct=0
						amount_paid=int(all_history[i][1])
						counter+=1
						if(counter==check_time or amount_paid>0):
							if(amount_paid>0):
								last_date_payed=str(i).split(" ")[0]
							else:
								for lt_date in check_dates[::-1]:
									last_date_amt_paid=int(all_history[lt_date][1])
									if(last_date_amt_paid)>0:
										last_date_payed=str(lt_date).split(" ")[0]
										break
									else:
										flag_date_ct+=1
									
							break

					if(flag_date_ct==len(emi_dates_prev)):
						#last_date_payed="emi started but not paid"
						red_flag="red"

					total_outstanding=(loan_tenure*int(emi_amt))-int(emi_amount_received)



					#if(ti_val[0]=="LOAN0228644306"):
						#print(due)

					if(date>emi_dates[-1]):

						if(total_outstanding==0):

							status="emi_closed,payment_received"

							if(int(amount_paid)==0):
								final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,last_date_amt_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag,counter_r,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],len(emi_dates_prev)*int(emi_amt)]
							else:
								final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag,counter_r,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],len(emi_dates_prev)*int(emi_amt)]

						elif(total_outstanding>0):

							status="emi_closed,not_paid"
							red_flag="red"
							if(due<int(emi_amt)):
								date_counter=1
							else:
								date_counter=int(float(int(due)/int(emi_amt)))

							if(int(amount_paid)==0):
								final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,last_date_amt_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag,counter_r,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],len(emi_dates_prev)*int(emi_amt)]
							else:
								final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag,counter_r,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],len(emi_dates_prev)*int(emi_amt)]

						elif(total_outstanding<0):

							status="emi_closed_with_advance"
							if(int(amount_paid)==0):
								final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,last_date_amt_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag,counter_r,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],len(emi_dates_prev)*int(emi_amt)]
							else:
								final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag,counter_r,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],len(emi_dates_prev)*int(emi_amt)]

					else:

						if(due==0 and int(amount_paid)>0):

							status="ontime,ongoing"
							final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag,counter_r,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],len(emi_dates_prev)*int(emi_amt)]

						if(due==0 and int(amount_paid)==0):

							status="ontime,ongoing"
							final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,last_date_amt_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag,counter_r,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],len(emi_dates_prev)*int(emi_amt)]

						if(due>0 and int(amount_paid)>0):

							status="overdue,ongoing"
							if(due<int(emi_amt)):
								date_counter=1
							else:
								date_counter=int(float(int(due)/int(emi_amt)))
							final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag,counter_r,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],len(emi_dates_prev)*int(emi_amt)]

						if(due>0 and int(amount_paid)==0):

							status="overdue,halted"
							if(due<int(emi_amt)):
								date_counter=1
							else:
								date_counter=int(float(int(due)/int(emi_amt)))
							if(date_counter<3):
								status="overdue,ongoing"
							final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,last_date_amt_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag,counter_r,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],len(emi_dates_prev)*int(emi_amt)]

						if(due<0):
							status="advance,ongoing"

							if(int(amount_paid)==0):
								final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,last_date_amt_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag,counter_r,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],len(emi_dates_prev)*int(emi_amt)]
							else:
								final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag,counter_r,emi_amount_received,loan_tenure,total_installments_paid,loan_type,first_emi_date,emi_dates[-1],len(emi_dates_prev)*int(emi_amt)]

						
			
			query_check="SELECT * FROM chk_status WHERE transaction_id=%s"
			cursor.execute(query_check,(ti_val[0],))
			get_data=cursor.fetchall()
			if(len(get_data)<1):
				cursor.execute('''INSERT INTO chk_status VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',(final_out_per_person))
				upload_file_update_query="UPDATE upload_file SET payment_status=%s where transaction_id=%s"
				cursor.execute(upload_file_update_query,(final_out_per_person[11],final_out_per_person[0]))
			else:
				up_query="UPDATE chk_status SET last_date_paid=%s,amt_paid_last=%s,overdue_amount=%s,total_outstanding=%s,no_of_payment_period_missed=%s,status_up=%s,flag=%s,consecutive_period_missed=%s,amount_received=%s,total_number_of_installment=%s,number_of_installments_paid=%s,repayment_type=%s,first_installment_date=%s,last_installment_date=%s,amount_due_till_date=%s WHERE transaction_id=%s"
				cursor.execute(up_query,(final_out_per_person[3],final_out_per_person[4],final_out_per_person[6],final_out_per_person[8],final_out_per_person[10],final_out_per_person[11],final_out_per_person[12],final_out_per_person[13],final_out_per_person[14],final_out_per_person[15],final_out_per_person[16],final_out_per_person[17],final_out_per_person[18],str(final_out_per_person[19]).split(" ")[0],final_out_per_person[20],final_out_per_person[0]))
				upload_file_update_query="UPDATE upload_file SET payment_status=%s where transaction_id=%s"
				cursor.execute(upload_file_update_query,(final_out_per_person[11],final_out_per_person[0]))

		mysql.connection.commit()
		cursor.close()
		msg["data"]="Succesfully updated loan status"
		update_log(email,"cron")

	except Exception as e:
		msg["error"]=str(e)
		print(msg)
	return jsonify({"msg":msg})


@app.route("/user_log",methods=["POST"])
@cross_origin(supports_credentials=True)
def UserLog():
	msg={}
	req=request.data
	req=json.loads(req)
	time=req.get("time",None)
	st_date=req.get("stDate",None)
	end_date=req.get("endDate",None)
	email=req.get("lid",None)
	job=req.get("job",None)
	out=[]
	try:
		if(email):
			for file in my_bucket.objects.all():
				file_name=file.key
				if(file_name=="logger.json"):
					data=json.load(file.get()["Body"])
					for i in data["log"][job].keys():
						if(datetime.datetime.strptime(st_date,"%Y-%m-%d")<=datetime.datetime.strptime(i,"%Y-%m-%d") and datetime.datetime.strptime(end_date,"%Y-%m-%d")>=datetime.datetime.strptime(i,"%Y-%m-%d")):
							for user in data["log"][job][i]:
								if(user[0]==email):
									user.extend([i,job])
									out.append(user)
			if(len(out)>0):
				msg['clName']=["Email","Time","Date","Job Des"]
				msg["data"]=out[::-1]
				msg["count"]=len(out)
			else:
				msg["error"]="No data found"

		elif(st_date is not None and end_date is not None):
				st_d=datetime.datetime.strptime(st_date,"%Y-%m-%d")
				en_d=datetime.datetime.strptime(end_date,"%Y-%m-%d")
				gap=en_d-st_d
				if(gap.days<0):
					msg["error"]="end date must be bigger"
					return jsonify({"msg":msg})

				for file in my_bucket.objects.all():
					file_name=file.key
					if(file_name=="logger.json"):
						data=json.load(file.get()["Body"])
						for i in data["log"][job].keys():
							if(datetime.datetime.strptime(st_date,"%Y-%m-%d")<=datetime.datetime.strptime(i,"%Y-%m-%d") and datetime.datetime.strptime(end_date,"%Y-%m-%d")>=datetime.datetime.strptime(i,"%Y-%m-%d")):
								for user in data["log"][job][i]:
									print(user)
									user.extend([i,job])
									out.append(user)
				if(len(out)>0):
					msg['clName']=["Email","Time","Date","Job Description"]
					msg["data"]=out[::-1]
					msg["count"]=len(out)
				else:
					msg["error"]="No data found"


	except Exception as e:
		msg["error"]=str(e)

	return jsonify({"msg":msg})


	


if __name__=="__main__":
	app.run(host='0.0.0.0')