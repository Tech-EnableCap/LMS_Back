'''
collecton of helper modules for server.py
@ tech@enablecap.in
@ tech2@enablecap.in
enablecap loan management system v1 2021

'''

import datetime
import numpy_financial as np
import numpy as npx
import pandas as pd
from dateutil.relativedelta import *

def eq(param):
    if(param>=700):
        return 'good'
    if(param>=600 and param<700):
        return 'risky'
    if(param>=10 and param<600):
        return 'very risky'
    if(param>-10 and param<10):
        return 'no info'


def ena_mw_int_rate(param):
    delta=0
    if(param=='Monthly'):
        delta=12
    else:
        delta=52
    return (.24/delta)


def emw_interest_rate(param1,param2):
    delta=0
    if(param1=='Monthly'):
        delta=12
    else:
        delta=52
    int_rate=float(param2)
    return (int_rate/100)/delta


def pre_emi_days(param1,param2):
    d1=param1.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d'))
    d1=d1.apply(lambda x:x.date())
    #param2=param2.apply(lambda x:datetime(x).strftime('%Y-%m-%d'))
    #print(param2)
    #d2=param2.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d'))
    d3=d1-param2
    #d3=d1-param2
    d3=d3.apply(lambda x:int(x.days))
    return d3



def supposed_dis_date(param1,param2):
    delta=0
    if(param1=='Monthly'):
        delta=31
    else:
        delta=7
    #d=datetime.datetime.strptime(param2,'%Y-%m-%d')
    #d_final=d-datetime.timedelta(delta)
    d_final=param2-datetime.timedelta(delta)
    month=d_final.month
    year=d_final.year
    day=d_final.day
    if(month<10):
        month='0'+str(month)
    if(day<10):
        day='0'+str(day)
    return f'{year}-{month}-{day}'


def ifsc(param):
    p=param[:4]
    if(p=="ICIC"):
        return "FT"
    else:
        return "NEFT"


def disbursal_mis_process(data):
	transaction_id_dmis=pd.DataFrame(data['transaction_id'])
	partnet_loan_id_dmis=pd.DataFrame(data['partner_loan_id'])
	disburse_date_dmis=pd.DataFrame(data['disburse_date'].apply(lambda x:str(x)))
	first_name_dmis=pd.DataFrame(data['first_name'])
	last_name_dmis=pd.DataFrame(data['last_name'])
	applied_amount_dmis=data['applied_amount']
	net_disbur_amt_dmis=pd.DataFrame(data['net_disbur_amt'])
	loan_tenure_dmis=data['loan_tenure']
	repayment_type_dmis=pd.DataFrame(data['repayment_type'])
	emi_amt_dmis=pd.DataFrame(data['emi_amt'])
	pre_emi_int_dmis=pd.DataFrame(data['pre_emi_interest'])
	emi_starting_date_dmis=pd.DataFrame(data['first_inst_date'].apply(lambda x:str(x)))
	supposed_disb_date_dmis=data.apply(lambda x:supposed_dis_date(x['repayment_type'],
                                                      x['first_inst_date']),axis=1)
	
	approval_date_dmis=data['final_approve_date']
	pre_emi_days_dmis=pd.DataFrame(pre_emi_days(supposed_disb_date_dmis,approval_date_dmis),
                       columns=['pre_emi_days'])

	pre_emi_as_per_ena_dims=pd.DataFrame((pre_emi_days_dmis['pre_emi_days']/365)*(data['int_rate_reducing_perc'].apply(lambda x:float(x))/100)*(applied_amount_dmis.apply(lambda x:float(x)))
                             ,columns=['pre_emi_as_per_enablecap_calculation'])

	pre_emi_ec_share_dims=pd.DataFrame((pre_emi_days_dmis['pre_emi_days'].apply(lambda x:int(x))/365)*0.24*applied_amount_dmis.apply(lambda x:int(x)),
                           columns=['pre_emi_as_per_enablecap_share'])


	pre_emi_entitled_share_dmis=pd.DataFrame(pre_emi_as_per_ena_dims.values-pre_emi_ec_share_dims.values,
                                 columns=['pre_emi_entitled_share'])
	pre_emi_entitled_share_dmis.index=range(1,len(pre_emi_entitled_share_dmis)+1)
	pre_emi_entitled_share_dmis=pre_emi_entitled_share_dmis['pre_emi_entitled_share'].apply(lambda x:str(x))

	processing_fees_dims=pd.DataFrame(data['pro_fee_amt'].apply(lambda x:float(x)*1.18).values,
                         columns=['precessing_fees_(inc_gst)'])
	processing_fees_dims.index=range(1,len(processing_fees_dims)+1)
	processing_fees_dims=processing_fees_dims['precessing_fees_(inc_gst)'].apply(lambda x:str(x))
	entt_interest_rate_dmis=pd.DataFrame(data.apply(lambda x:emw_interest_rate(x['repayment_type'],
                                                    x['int_rate_reducing_perc']),axis=1),
                           columns=['entitled_monthly/weekly_interest_rate'])
	emw_interest_rate_dmis=data.apply(lambda x:ena_mw_int_rate(x['repayment_type']),axis=1)
	ena_share_per_emi_dims=pd.DataFrame(pd.Series(-np.pmt(emw_interest_rate_dmis,
                                   loan_tenure_dmis.apply(lambda x:int(x)),
                                   applied_amount_dmis.apply(lambda x:float(x)))),
                            columns=['enablecap_share_per_emi'])

	ena_share_per_emi_dims.index=range(1,len(ena_share_per_emi_dims)+1)

	entt_shared_per_emi_dims=emi_amt_dmis['emi_amt'].apply(lambda x:float(x)).values-ena_share_per_emi_dims['enablecap_share_per_emi']
	entt_shared_per_emi_dims=pd.DataFrame(entt_shared_per_emi_dims.values,columns=['entitled_share_per_emi'])
	entt_shared_per_emi_dims.index=range(1,len(entt_shared_per_emi_dims)+1)

	applied_amount_dmis=pd.DataFrame(applied_amount_dmis.values,columns=['applied_loan_amount'])
	applied_amount_dmis.index=range(1,len(applied_amount_dmis)+1)
	loan_tenure_dmis=pd.DataFrame(loan_tenure_dmis.values,columns=['loan_tenure'])
	loan_tenure_dmis.index=range(1,len(loan_tenure_dmis)+1)
	supposed_disb_date_dmis=pd.DataFrame(supposed_disb_date_dmis.values,
                             columns=['supposed_disbursement_date'])
	supposed_disb_date_dmis.index=range(1,len(supposed_disb_date_dmis)+1)
	emw_interest_rate_dmis=pd.DataFrame(emw_interest_rate_dmis.values,
                            columns=['enablecap_monthly/weekly_interest_rate'])
	emw_interest_rate_dmis.index=range(1,len(emw_interest_rate_dmis)+1)
	interest_rate_dmis=pd.DataFrame(data['int_rate_reducing_perc'].values,columns=['interest_rate'])
	disc=pd.DataFrame(pre_emi_int_dmis['pre_emi_interest'].apply(lambda x:float(x))-pre_emi_as_per_ena_dims['pre_emi_as_per_enablecap_calculation'],
      						columns=["discrepancy"])
	disc=disc['discrepancy'].apply(lambda x:str(x))
	interest_rate_dmis.index=range(1,len(interest_rate_dmis)+1)
	eq_score=pd.DataFrame(data['eq_score'])
	risk_cat=pd.DataFrame(data['risk_cat'])
	pre_emi_ec_share_dims=pre_emi_ec_share_dims['pre_emi_as_per_enablecap_share'].apply(lambda x:str(x))
	#pre_emi_ec_share_dims=pre_emi_ec_share_dims['pre_emi_as_per_enablecap_share'].apply(lambda x:str(x))
	pre_emi_as_per_ena_dims=pre_emi_as_per_ena_dims['pre_emi_as_per_enablecap_calculation'].apply(lambda x:str(x))

	interest_rate_dmis=interest_rate_dmis['interest_rate'].apply(lambda x:str(x))
	emw_interest_rate_dmis=emw_interest_rate_dmis['enablecap_monthly/weekly_interest_rate'].apply(lambda x:str(x))
	entt_interest_rate_dmis=entt_interest_rate_dmis['entitled_monthly/weekly_interest_rate'].apply(lambda x:str(x))
	ena_share_per_emi_dims=ena_share_per_emi_dims['enablecap_share_per_emi'].apply(lambda x:str(x))
	entt_shared_per_emi_dims=entt_shared_per_emi_dims['entitled_share_per_emi'].apply(lambda x:str(x))
	pre_emi_days_dmis=pre_emi_days_dmis['pre_emi_days'].apply(lambda x:str(x))
	approval_date_dmis=approval_date_dmis.apply(lambda x:str(x))

	final_data=pd.concat([transaction_id_dmis,partnet_loan_id_dmis,disburse_date_dmis,
              first_name_dmis,last_name_dmis,applied_amount_dmis,net_disbur_amt_dmis,
             loan_tenure_dmis,repayment_type_dmis,emi_amt_dmis,pre_emi_int_dmis,
             pre_emi_as_per_ena_dims,pre_emi_ec_share_dims,pre_emi_entitled_share_dmis,pre_emi_days_dmis,disc,approval_date_dmis,supposed_disb_date_dmis,emi_starting_date_dmis,
             processing_fees_dims,interest_rate_dmis,entt_interest_rate_dmis,emw_interest_rate_dmis,
             ena_share_per_emi_dims,entt_shared_per_emi_dims,eq_score,risk_cat],axis=1)

	final_data=final_data.fillna("N/A")
	return final_data



def bank_upload_process(data):
	t_id=data['transaction_id']
	partner_l_id=data['partner_loan_id']
	pymt_prod_type_code=pd.DataFrame(['pab_vendor']*len(data),columns=['pymt_prod_type_code'])
	pymt_prod_type_code.index=range(1,len(t_id)+1)
	debt_acc_num=pd.DataFrame(['694705602523']*len(data),columns=['debt_acc_num'])
	debt_acc_num.index=range(1,len(t_id)+1)
	bnf_name=data['first_name']+" "+data["last_name"]
	bnf_name=pd.DataFrame(bnf_name,columns=['bnf_name'])
	bene_acc_num=data['borro_bank_acc_num']
	bene_ifsc=data['borro_bank_ifsc']
	pymt_mode=bene_ifsc.apply(lambda x:ifsc(x))
	amt=data['net_disbur_amt']
	debit_narr=data['appl_pan']
	credit=pd.DataFrame(['loan from enablecap']*len(data),columns=['credit'])
	credit.index=range(1,len(t_id)+1)
	mob=data['appl_phone']
	email=pd.DataFrame(['finance@enablecap.in']*len(data),columns=['email'])
	email.index=range(1,len(t_id)+1)
	remark=bnf_name.copy()
	remark.columns=['remark']
	pymt_date=data['disburse_date'].apply(lambda x:str(x))
	bank_up_f=pd.concat([t_id,partner_l_id,pymt_prod_type_code,pymt_mode,debt_acc_num,bnf_name,
             bene_acc_num,bene_ifsc,amt,debit_narr,credit,mob,email,remark,pymt_date],axis=1)
	final_data=bank_up_f.fillna("N/A")
	return final_data


def process_str(data):
	data['dob']=data['dob'].apply(lambda x:str(x))
	data['sacntion_date']=data['sacntion_date'].apply(lambda x:str(x))
	data['loan_app_date']=data['loan_app_date'].apply(lambda x:str(x))
	data['first_inst_date']=data['first_inst_date'].apply(lambda x:str(x))
	data['disburse_date']=data['disburse_date'].apply(lambda x:str(x))
	data['final_approve_date']=data['final_approve_date'].apply(lambda x:str(x))
	data['joining_date']=data['joining_date'].apply(lambda x:str(x))
	return data


def helper_upload(data,cursor,file_type="upload_file"):
	dic={}
	if(file_type=="upload_file"):
		for length in range(len(data)):
			for i,j in enumerate(data.columns):
				dic[j]=data.iloc[length][i]
			kk=list(dic.values())
			check="SELECT * FROM upload_file WHERE transaction_id=%s";
			cursor.execute(check,(kk[0],))
			if(cursor.rowcount<1):
				cursor.execute('''INSERT INTO upload_file VALUES(%s,%s,%s,%s,%s
					,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
					,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
					%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',(kk))
			dic={}
			kk=[]
	elif(file_type=="master_repay"):
		for length in range(len(data)):
			for i,j in enumerate(data.columns):
				dic[j]=data.iloc[length][i]
			kk=list(dic.values())
			check="SELECT * FROM master_repay WHERE transaction_id=%s";
			cursor.execute(check,(kk[0],))
			if(cursor.rowcount<1):
				cursor.execute('''
					INSERT INTO master_repay VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
					''',(kk))
			dic={}
			kk=[]
	return cursor



def master_repay_helper(data):
	d_1=[]
	d_all=[]
	for i in range(len(data)):
	    if(data.iloc[i]['repayment_type']=='Weekly'):
	        d=data.iloc[i]['first_inst_date']
	        d_1.append(str(d).split(" ")[0])
	        for _ in range(int(data.iloc[i]['loan_tenure'])-1):
	            d+=datetime.timedelta(7)
	            d_1.append(str(d).split(" ")[0])
	        #d_all.append(d_1)
	        d_all.append(d_1[::len(d_1)-1])
	        d_1=[]
	    else:
	        d=data.iloc[i]['first_inst_date']
	        d_1.append(str(d).split(" ")[0])
	        for _ in range(int(data.iloc[i]['loan_tenure'])-1):
	            d+=relativedelta(months=+1)
	            d_1.append(str(d).split(" ")[0])
	        #d_all.append(d_1)
	        d_all.append(d_1[::len(d_1)-1])
	        d_1=[]

	#df=pd.DataFrame({"emi_date":d_all})
	df=pd.DataFrame(d_all,columns=["st","end"])
	df.index=range(1,len(df)+1)
	lid=data['transactionid']
	first_name=data['first_name']
	last_name=data['last_name']
	amt=data['emi_amt']
	loan_type=data['repayment_type']
	n_emi=data['loan_tenure']
	data_master=pd.concat([lid,first_name,last_name,loan_type,n_emi,amt,df],axis=1)
	#data_master=data_master.fillna("N/A")
	#s=data_master.apply(lambda x: pd.Series(x['emi_date']),axis=1).stack().reset_index(level=1,drop=True)
	#s.name="emi_date"


	#data_master1=data_master.drop('emi_date',axis=1).join(s)
	#data_master2=data_master1.groupby('emi_date').agg(lambda x:list(x)).reset_index() #['transactionid'].apply(lambda x: [data_master2[data_master2['transactionid']==i]['emi_amt'].values  for i in x]).reset_index()

	return data_master#,data_master2.iloc[10]



###### analysis part #####

def monthly_weekly_analysis(data,typ="Weekly"):
	find_mw_analysis={}
	if(typ=="Weekly" or typ=="Monthly"):
	    d=data[data['repayment_type']==typ]
	    if(len(d)<1):
	    	return "no data found on type "+typ
	    total_number_of_loan=len(d['partner_loan_id'].unique())
	    total_loan=sum(d['sanction_amount'].apply(lambda x:float(x)))
	    avg_ticket_size=total_loan/total_number_of_loan
	    avg_tenure_weeks=sum(d['loan_tenure'].apply(lambda x:
	                                                          float(x)))/(len(d['loan_tenure']))
	    avg_int_rate=sum(d['int_rate_reducing_perc'].apply(lambda x:
	                                                       float(x)))/(len(d['int_rate_reducing_perc']))
	    #df=pd.DataFrame(npx.array([total_number_of_loan,total_loan,
	                        #avg_ticket_size,avg_tenure_weeks,avg_int_rate]).reshape(1,5),columns=[
	    #'number_of_loans','total_loans','avg_ticket_size','avg_tenure','avg_interest_rate'
	    #])
	    #return df
	    find_mw_analysis['number_of_loans']=total_number_of_loan
	    find_mw_analysis['total_amounts_of_loans']=total_loan
	    find_mw_analysis['avg_ticket_size']=avg_ticket_size
	    #if(typ=="Weekly"):
	    	#find_mw_analysis['avg_tenure']=avg_tenure_weeks
	    #else:
	    find_mw_analysis['avg_tenure']=avg_tenure_weeks
	    find_mw_analysis['avg_interest_rate']=avg_int_rate

	    return find_mw_analysis

	else:
	    print("invalid type")
	    return


def analysis_total(data):
	find_total={}
	total_loans=len(data)
	total_amounts_of_loans=sum(data['sanction_amount'].apply(lambda x:float(x)))
	avg_ticket_size=sum(data['sanction_amount'].apply(lambda x:float(x)))/len(data['partner_loan_id'].unique())
	avg_tenure=(sum(data[data['repayment_type']=="Monthly"]['loan_tenure'].apply(lambda x:
		float(x)))+(sum(data[data['repayment_type']=="Weekly"]['loan_tenure'].apply(lambda x:
			float(x)))/4))/total_loans
	avg_interest_rate=(sum(data[data['repayment_type']=="Monthly"]['int_rate_reducing_perc'].apply(lambda x:
                                                           float(x)))+sum(data[data['repayment_type']=="Weekly"]['int_rate_reducing_perc'].apply(lambda x:
                                                           float(x))))/len(data)
	find_total['number_of_loans']=total_loans
	find_total['total_amounts_of_loans']=total_amounts_of_loans
	find_total['avg_ticket_size']=avg_ticket_size
	find_total['avg_tenure']=avg_tenure
	find_total['avg_interest_rate']=avg_interest_rate

	return find_total



def risk_params(data,typ="Number"):
	data_out={}
	if(typ=="Number" or typ=="Volume"):
	    good=data[data['category']=="good"]
	    risky=data[data['category']=="risky"]
	    v_risky=data[data['category']=="very risky"]
	    no_info=data[data['category']=="no info"]
	    if(typ=="Number"):
	        g=len(good)
	        r=len(risky)
	        v_r=len(v_risky)
	        n_f=len(no_info)
	        total=g+r+v_r+n_f
	        #df=pd.DataFrame(npx.array([g,r,v_r,n_f,total]).reshape(1,5),columns=['Good',
	                                                                             #'Risky',
	                                                                            #'Very Risky',
	                                                                            #'No Info',
	                                                                            #'Total sample Size'])
	        #return df

	        data_out['good']=g
	        data_out['risky']=r
	        data_out['very_risky']=v_r
	        data_out['no_info']=n_f
	        data_out['total_sample_size']=total

	        return data_out

	    elif(typ=="Volume"):
	        g=sum(good['loan_amount'].apply(lambda x:float(x)))
	        r=sum(risky['loan_amount'].apply(lambda x:float(x)))
	        v_r=sum(v_risky['loan_amount'].apply(lambda x:float(x)))
	        n_f=sum(no_info['loan_amount'].apply(lambda x:float(x)))
	        total=g+r+v_r+n_f
	        #df=pd.DataFrame(npx.array([g,r,v_r,n_f,total]).reshape(1,5),columns=['Good',
	                                                                             #'Risky',
	                                                                            #'Very Risky',
	                                                                            #'No Info',
	                                                                            #'Total sample Size'])
	        #return df

	        data_out['good']=g
	        data_out['risky']=r
	        data_out['very_risky']=v_r
	        data_out['no_info']=n_f
	        data_out['total_sample_size']=total

	        return data_out

	else:
	    print("invalid type")
	    return

def handle_single_tid_data(data):
	d_1=[]
	d_all=[]
	for i in range(len(data)):
		if(data.iloc[i]['type']=='Weekly'):
			d=data.iloc[i]['st_date']
			d_1.append(str(d).split(" ")[0])
			for _ in range(int(data.iloc[i]['no_of_emi'])-1):
				d+=datetime.timedelta(7)
				d_1.append(str(d).split(" ")[0])
			d_all.append(d_1)
			d_1=[]
		else:
			d=data.iloc[i]['st_date']
			d_1.append(str(d).split(" ")[0])
			for _ in range(int(data.iloc[i]['no_of_emi'])-1):
				d+=relativedelta(months=+1)
				d_1.append(str(d).split(" ")[0])
			d_all.append(d_1)
			d_1=[]

	if(len(d_all)>1):
		dfs=[]
		for i in range(len(d_all)):
			emi_amt=pd.DataFrame(d_all[i],columns=["emi_amt"])
			lid=data.iloc[i]['transaction_id']
			first_name=data.iloc[i]['first_name']
			last_name=data.iloc[i]['last_name']
			amt=data.iloc[i]['emi_amt']
			loan_type=data.iloc[i]['type']
			n_emi=data.iloc[i]['no_of_emi']
			one=pd.DataFrame(npx.array([lid,first_name,last_name,amt,loan_type,n_emi]).reshape(1,6),columns=["transaction_id",
				"first_name","last_name","emi_amt","type","no_of_emi"])
			total=pd.concat([one,emi_amt],axis=1)
			#print(df)
			dfs.append(total)

		df=pd.concat(dfs,axis=0)
		df.reset_index(inplace=True,drop=True)
		
		data_master=df.fillna(" ")
		return data_master

	else:
		#print(d_all)
		df=pd.DataFrame(d_all[0],columns=["emi_date"])
		df.index=range(1,len(df)+1)
		lid=data['transaction_id']
		first_name=data['first_name']
		last_name=data['last_name']
		amt=data['emi_amt']
		loan_type=data['type']
		n_emi=data['no_of_emi']
		data_master=pd.concat([lid,first_name,last_name,loan_type,n_emi,amt,df],axis=1)
		data_master=data_master.fillna(" ")
		return data_master


def handle_date(data,date1,date2):
	d_1=[]
	d_all=[]
	for i in range(len(data)):
		if(data.iloc[i]['type']=="Weekly"):
			d=data.iloc[i]['st_date']
			d=datetime.datetime.strptime(d,"%Y-%m-%d")
			for _ in range(int(data.iloc[i]['no_of_emi'])-1):
				#print(datetime.datetime.strptime(d,"%Y-%m-%d"))
				d+=datetime.timedelta(7)
				if(d>datetime.datetime.strptime(date2,"%Y-%m-%d")):
					break
				
				d_1.append(str(d).split(" ")[0])
			d_all.append(d_1)
			d_1=[]
		else:
			d=data.iloc[i]['st_date']
			d=datetime.datetime.strptime(d,"%Y-%m-%d")
			for _ in range(int(data.iloc[i]['no_of_emi'])-1):
				#print(datetime.datetime.strptime(d,"%Y-%m-%d"))
				d+=relativedelta(months=+1)
				if(d>datetime.datetime.strptime(date2,"%Y-%m-%d")):
					break
				
				d_1.append(str(d).split(" ")[0])
			d_all.append(d_1)
			d_1=[]
	data=data.drop(["end_date"],axis=1)
	data=data.rename({'st_date':'1 payment'},axis=1)
	df=pd.DataFrame(d_all)
	df.index=range(1,len(df)+1)
	#columns=[str(i)+"g" for i in range(2,len(d_all)+2)])
	dic={}
	for i in range(len(list(df.columns))):
		dic[list(df.columns)[i]]=str(i+2)+" payment"
		
	df=df.rename(dic,axis=1)
	#df.index=range(1,len(df)+1)
	#df.columns=[str(i)+"g" for i in range(2,len(d_all)+2)]
	df=df.fillna("Not in the range")
	df=pd.concat([data,df],axis=1)

	return df

