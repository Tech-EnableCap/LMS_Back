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


gender={
	"Female":"1",
	"Male":"2",
	"Transgender":"3"
}

paymt_freq={
	"Weekly":"'01",
	"Fortnightly":"'02",
	"Monthly":"'03",
	"Quarterly":"'04"
}

state_code={
	"Jammu & Kashmir" :"'01",
	"Himachal Pradesh" : "'02",
	"Punjab" : "'03",
	"Chandigarh" : "'04",
	"Uttaranchal" : "'05",
	"Haryana" : "'06",
	"Delhi" : "'07",
	"Rajasthan" : "'08",
	"Uttar Pradesh" : "'09",
	"Bihar" : "10",
	"Sikkim" : "11",
	"Arunachal Pradesh" : "12",
	"Nagaland" : "13",
	"Manipur" : "14",
	"Mizoram" : "15",
	"Tripura" : "16",
	"Meghalaya" : "17",
	"Assam" : "18",
	"West Bengal" : "19",
	"Jharkhand" : "20",
	"Orissa" : "21",
	"Chhattisgarh" : "22",
	"Madhya Pradesh" : "23",
	"Gujarat" : "24",
	"Daman & Diu" : "25",
	"Dadra & Nagar Haveli" : "26",
	"Maharashtra" : "27",
	"Andhra Pradesh" : "28",
	"Karnataka" : "29",
	"Goa" : "30",
	"Lakshadweep" : "31",
	"Kerala" : "32",
	"Tamil Nadu" : "33",
	"Pondicherry" : "34",
	"Andaman & Nicobar Islands" : "35",
	"Telangana" : "36",
	"APO Address" : "99"
}

address_cat={
	"Permanent Address":"'01",
	"Residence Address":"'02",
	"Office Address":"'03",
	"Not Categorized":"'04"
}

residense_code={
	"Owned":"'01",
	"Rented":"'02"
}


acc_type={
	"Auto Loan" : "'01",
	"Housing Loan" : "'02",
	"Property Loan" : "'03",
	"Loan against Shares/Securities" : "'04",
	"Personal Loan" : "'05",
	"Consumer Loan" : "'06",
	"Gold Loan" : "'07",
	"Education Loan" : "'08",
	"Loan to Professional" : "'09",
	"Credit Card" : "10",
	"Lease" : "11",
	"Overdraft" : "12",
	"Two-wheeler Loan" : "13",
	"Non-Funded Credit Facility" : "14",
	"Loan Against Bank Deposits" : "15",
	"Fleet Card" : "16",
	"Commercial Vehicle Loan" : "17",
	"Telco – Wireless (For Future Use)" : "18",
	"Telco – Broadband (For Future Use)" : "19",
	"Telco – Landline (For Future Use)" : "20",
	"Seller Financing" : "21",
	"GECL Loan Secured" : "23",
	"GECL Loan Unsecured" : "24",
	"Secured Credit Card" : "31",
	"Used Car Loan" : "32",
	"Construction Equipment Loan" : "33",
	"Tractor Loan" : "34",
	"Corporate Credit Card" : "35",
	"Kisan Credit Card" : "36",
	"Loan on Credit Card" : "37",
	"Prime Minister Jaan Dhan Yojana – Overdraft" : "38",
	"Mudra Loans – Shishu / Kishor / Tarun" : "39",
	"MicroFinance – Business Loan" : "40",
	"MicroFinance – Personal Loan" : "41",
	"MicroFinance – Housing Loan" : "42",
	"MicroFinance – Others" : "43",
	"Pradhan Mantri Awas Yojana – Credit Link Subsidy Scheme MAY CLSS" : "44",
	"P2P Personal Loan" : "45",
	"P2P Auto Loan" : "46",
	"P2P Education Loan" : "47",
	"Business Loan – Secured" : "50",
	"Business Loan – General" : "51",
	"Business Loan – Priority Sector – Small Business" : "52",
	"Business Loan – Priority Sector – Agriculture" : "53",
	"Business Loan – Priority Sector – Others" : "54",
	"Business Non-Funded Credit Facility – General" : "55",
	"Business Non-Funded Credit Facility – Priority Sector – Small Business" : "56",
	"Business Non-Funded Credit Facility – Priority Sector – Agriculture" : "57",
	"Business Non-Funded Credit Facility – Priority Sector – Others" : "58",
	"Business Loan Against Bank Deposits" : "59",
	"Business Loan – Unsecured" : "61",
	"Other" : "00"
}

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

def gender_mapper(gen):
	return gender[gen]

def pymt_mapper(pymt):
	return paymt_freq[pymt]

def state_code_mapper(state):
	if state in state_code:
		return state_code[state]
	else:
		return state_code["APO Address"]

def convert_acc_num_efx(acc):
	return "'"+str(acc)

def equifax_generator(data,end_date,due_list,received_amount,last_date):

	consumer_name=data['first_name']+" "+data["last_name"]
	consumer_name=pd.DataFrame(consumer_name,columns=["Consumer name"])
	dob=data["dob"].apply(lambda x:str(x).split(" ")[0])
	dob=dob.apply(lambda x:date_convert_efx(x))

	gender=data["gender"].apply(lambda x:gender_mapper(x))
	incom_tax_id=data["appl_pan"]
	passport_num=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Passport Number"])
	passport_issue_date=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Passport Issue date"])
	passport_expiry_date=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Passport Expiry Date"])
	voter_id_num=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Voter ID Num"])
	dv_li_num=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Driving License Number"])
	dv_li_issue_date=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Driving License Issue Date"])
	dv_li_exp_date=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Driving License Expiry Date"])
	ration_num=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Ration Card Number"])
	universal_id=data["aadhar_card_num"]
	add_id_1=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Additional ID #1"])
	add_id_2=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Additional ID #2"])
	mob=data["appl_phone"]
	tel_res=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Telephone No.Residence"])
	tel_office=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Telephone No.Office"])
	ext_office=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Extension Office"])
	tel_no_other=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Telephone No.Other"])
	ext_other=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Extension Other"])
	email_id_1=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Email ID 1"])
	email_id_2=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Email ID 2"])
	address1=data["resi_addr_ln1"]
	st_code1=data["state"].apply(lambda x:state_code_mapper(x))
	pincode1=data["pincode"]
	add_category1=pd.DataFrame(npx.array(["'02"]*len(data)).reshape(len(data)),columns=["Address Category 1"])
	res_code1=pd.DataFrame(npx.array(["'02"]*len(data)).reshape(len(data)),columns=["Residence Code 1"])

	address2=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Address 2"])
	st_code2=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["State Code 2"])
	pincode2=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["PIN Code2"])
	add_category2=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Address Category 2"])
	res_code2=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Residence Code 2"])

	cur_member_code=pd.DataFrame(npx.array(["019FP14598"]*len(data)).reshape(len(data)),columns=["Current/New Member Code"])
	cur_member_short_name=pd.DataFrame(npx.array(["GVRK"]*len(data)).reshape(len(data)),columns=["Current/New Member Short Name"])

	#curr_new_acc_num=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Curr/New Account No"])
	curr_new_acc_num=data['borro_bank_acc_num'].apply(lambda x:convert_acc_num_efx(x))
	
	acc_type=pd.DataFrame(npx.array(["'00"]*len(data)).reshape(len(data)),columns=["Account type"])
	own_ind=pd.DataFrame(npx.array(["'01"]*len(data)).reshape(len(data)),columns=["Ownership Indicator"])

	date_op=data["first_inst_date"].apply(lambda x:str(x).split(" ")[0])
	date_op=date_op.apply(lambda x:date_convert_efx(x))
	date_clsd=master_repay_helper(data,dtype="other")
	date_clsd=date_clsd.apply(lambda x:date_convert_efx(x))
	#date_last_pay=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Date of Last Payment"])

	date_last_pay=last_date.apply(lambda x:date_convert_efx(x))

	high_cred=data["applied_amount"]
	actual_emi=data["loan_tenure"].apply(lambda x:int(x))*data["emi_amt"].apply(lambda x:int(x))

	received_amt=pd.Series(npx.array(received_amount).reshape(len(received_amount),))

	current_bal=actual_emi-received_amt

	'''

	data_for_due=data[["loan_tenure","emi_amt","repayment_type","first_inst_date","emi_amount_received","carry_f","emi_number","emi_date_flag","partner_loan_id","first_name","last_name","last_date_flag"]]
	
	#print(end_date)

	due_list=[]
	for i in range(len(data_for_due.iloc[:])):
		query_data=((data_for_due.iloc[i]["loan_tenure"],data_for_due.iloc[i]["emi_amt"],
			data_for_due.iloc[i]["repayment_type"],data_for_due.iloc[i]["first_inst_date"],data_for_due.iloc[i]["emi_amount_received"],
			data_for_due.iloc[i]["carry_f"],data_for_due.iloc[i]["emi_number"],data_for_due.iloc[i]["emi_date_flag"],
			data_for_due.iloc[i]["partner_loan_id"],data_for_due.iloc[i]["first_name"],data_for_due.iloc[i]["last_name"],data_for_due.iloc[i]["last_date_flag"]),)
		#print(query_data)
		pp=repay_generator(query_data,end_date,"0",mode="prfdt")
		due_list.append(int(pp[4]))
		#print(due_list)

	'''

	amt_arr=npx.array(due_list)
	amt_arr=amt_arr.reshape(len(due_list),1)

	amt_overdue=pd.DataFrame(amt_arr,columns=["Amt Overdue"])
	
	dpd=[]

	tenure_missed=amt_overdue['Amt Overdue']/data['emi_amt'].apply(lambda x:int(x))
	for i in range(len(tenure_missed)):
		if(data.iloc[i]['repayment_type']=='Weekly'):
			if(i>0):
				dpd.append(int(float(tenure_missed[i]))*7)
			else:
				dpd.append(0)
		else:
			if(i>0):
				dpd.append(int(float(tenure_missed[i]))*30)
			else:
				dpd.append(0)

	num_days_past_due=pd.DataFrame(npx.array(dpd).reshape(len(data)),columns=["No of Days Past Due"])
	num_days_past_due=num_days_past_due["No of Days Past Due"].apply(lambda x:str(x))

	#amt_overdue.index=range(1,len(amt_overdue)+1)

	#amt_overdue=data["applied_amount"].apply(lambda x:int(x))-data["emi_amount_received"].apply(lambda x:int(x))
	current_bal=current_bal.apply(lambda x:str(x))
	amt_overdue=amt_overdue["Amt Overdue"].apply(lambda x:str(x))




	
	old_mbr=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Old Mbr Code"])
	old_mbr_st_name=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Old Mbr Short Name"])
	old_acc_num=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Old Acc No"])
	old_acc_type=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Old Acc Type"])
	old_ownership_ind=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Old Ownership Indicator"])
	suit_filed=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Suit Filed / Wilful Default"])
	wt_off_status=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Written-off and Settled Status"])

	asset_classification=pd.DataFrame(npx.array(["'01"]*len(data)).reshape(len(data)),columns=["Asset Classification"])
	val_coll=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Value of Collateral"])
	types_col=pd.DataFrame(npx.array(["'00"]*len(data)).reshape(len(data)),columns=["Type of Collateral"])

	credit_lim=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Credit Limit"])
	cash_lim=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Cash Limit"])
	rate_of_int=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Rate of Interest"])
	repay_ten=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["RepaymentTenure"])
	emi_amt=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["EMI Amount"])
	total_wt_amt=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Written- off Amount (Total)"])
	wt_principal=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Written- off Principal Amount"])
	settle_amt=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Settlement Amt"])
	payment_frequency=data["repayment_type"].apply(lambda x:pymt_mapper(x))
	frequency=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Frequency"])
	act_payment_amt=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Actual Payment Amt"])
	occupation=pd.DataFrame(npx.array(["'01"]*len(data)).reshape(len(data)),columns=["Occupation Code"])
	income=data['annual_income']
	net_gr_income_ind=pd.DataFrame(npx.array(["N"]*len(data)).reshape(len(data)),columns=["Net/Gross Income Indicator"])
	ann_income_ind=pd.DataFrame(npx.array(["A"]*len(data)).reshape(len(data)),columns=["Monthly/Annual Income Indicator"])

	date_reported=pd.DataFrame(npx.array([" "]*len(data)).reshape(len(data)),columns=["Date Reported"])


	df=pd.concat([consumer_name,dob,gender,incom_tax_id,passport_num,
		passport_issue_date,passport_expiry_date,voter_id_num,
		dv_li_num,dv_li_issue_date,dv_li_exp_date,ration_num,universal_id,
		add_id_1,add_id_2,mob,tel_res,tel_office,ext_office,tel_no_other,
		ext_other,email_id_1,email_id_2,address1,st_code1,pincode1,add_category1,res_code1,
		address2,st_code2,pincode2,add_category2,res_code2,cur_member_code,cur_member_short_name,
		curr_new_acc_num,acc_type,own_ind,date_op,date_last_pay,date_clsd,date_reported,high_cred,current_bal,amt_overdue,
		num_days_past_due,old_mbr,old_mbr_st_name,old_acc_num,old_acc_type,old_ownership_ind,suit_filed,wt_off_status,
		asset_classification,val_coll,types_col,credit_lim,cash_lim,rate_of_int,repay_ten,emi_amt,total_wt_amt,
		wt_principal,settle_amt,payment_frequency,act_payment_amt,occupation,income,net_gr_income_ind,
		ann_income_ind],axis=1)

	df=df.rename({
		'dob':'Date of Birth',
		'gender':'Gender',
		'appl_pan':'Income Tax ID Number',
		'aadhar_card_num':'Universal ID Number',
		'appl_phone':'Telephone No.Mobile',
		'resi_addr_ln1':'Address 1',
		'state':'State Code 1',
		'pincode':'PIN Code1',
		'borro_bank_acc_num':'Curr/New Account No',
		'first_inst_date':'Date Opened',
		'end':'Date Closed',
		0:'Current balance',
		'last_date_flag':'Date of Last Payment',
		'applied_amount':'High Credit/Sanctioned Amt',
		"repayment_type":"Payment Frequency",
		"annual_income":"income"
		},axis=1)
	return df


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
	bene_ifsc=pd.DataFrame(bene_ifsc.values,columns=['bene_ifsc'])
	pymt_mode=pd.DataFrame(pymt_mode.values,columns=['pymt_mode'])
	bene_ifsc.index=range(1,len(bene_ifsc)+1)
	pymt_mode.index=range(1,len(pymt_mode)+1)
	amt=data['net_disbur_amt']
	debit_narr=data['appl_pan']
	credit=pd.DataFrame(['loan from enablecap']*len(data),columns=['credit'])
	credit.index=range(1,len(t_id)+1)
	mob=data['appl_phone']
	email=pd.DataFrame(['finance@enablecap.in']*len(data),columns=['email'])
	email.index=range(1,len(t_id)+1)
	remark=bnf_name.copy()
	remark.columns=['remark']
	pymt_date=data['disburse_date'].apply(lambda x:handle_bank_upload_date_structure(x))
	bank_up_f=pd.concat([t_id,partner_l_id,pymt_prod_type_code,pymt_mode,debt_acc_num,bnf_name,
             bene_acc_num,bene_ifsc,amt,debit_narr,credit,mob,email,remark,pymt_date],axis=1)
	bank_up_f=bank_up_f.rename({'borro_bank_acc_num':'bene_acc_no','net_disbur_amt':'amount','appl_pan':'debit_narr',
		'credit':'credit_narr','appl_phone':'mobile_num','disburse_date':'pymt_date','debt_acc_num':'debit_acc_no','email':'email_id'},axis=1)
	final_data=bank_up_f.fillna("N/A")
	return final_data


def handle_bank_upload_date_structure(date):
	return datetime.datetime.strftime(date,"%d-%m-%Y")


def process_str(data):
	data['dob']=data['dob'].apply(lambda x:str(x).split(" ")[0])
	data['sacntion_date']=data['sacntion_date'].apply(lambda x:str(x))
	data['loan_app_date']=data['loan_app_date'].apply(lambda x:str(x))
	data['first_inst_date']=data['first_inst_date'].apply(lambda x:str(x))
	data['disburse_date']=data['disburse_date'].apply(lambda x:str(x))
	data['final_approve_date']=data['final_approve_date'].apply(lambda x:str(x))
	data['joining_date']=data['joining_date'].apply(lambda x:str(x))
	return data


def helper_upload(data,cursor,db_type,file_type="upload_file"):
	c=0
	dic={}
	if(file_type=="upload_file"):
		if(db_type=="Entitle"):
			for length in range(len(data)):
				for i,j in enumerate(data.columns):
					dic[j]=data.iloc[length][i]
				kk=list(dic.values())
				check="SELECT * FROM upload_file WHERE transaction_id=%s";
				cursor.execute(check,(kk[0],))

				if(cursor.rowcount==0):
					cursor.execute('''INSERT INTO upload_file VALUES(%s,%s,%s,%s,%s
					,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
					,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
					%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',(kk+[0,0,0,kk[40],'ongoing','2021-01-01',db_type,'','','','','','','']))
				
				dic={}
				kk=[]
		else:
			kk=[]
			for length in range(len(data)):
				
				for i,j in enumerate(data.columns[:54]):
					kk.append(data.iloc[length][i])
				
				kk.extend([0,0,0,kk[40],'ongoing','2021-01-01',db_type,''])

				for i,j in enumerate(data.columns[54:]):
					kk.append(data.iloc[length][j])

				check="SELECT * FROM upload_file WHERE transaction_id=%s";
				cursor.execute(check,(kk[0],))


				if(cursor.rowcount==0):
					cursor.execute('''INSERT INTO upload_file VALUES(%s,%s,%s,%s,%s
					,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
					,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
					%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',(kk))

				kk=[]


	elif(file_type=="master_repay"):
		for length in range(len(data)):
			for i,j in enumerate(data.columns):
				dic[j]=data.iloc[length][i]
			kk=list(dic.values())
			check="SELECT * FROM master_repay WHERE transaction_id=%s";
			cursor.execute(check,(kk[0],))
			if(cursor.rowcount==0):
				cursor.execute('''
					INSERT INTO master_repay VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
					''',(kk+[db_type]))
			dic={}
			kk=[]
	return cursor



def master_repay_helper(data,dtype="master_repay"):
	#d_1=[]
	d_all=[]
	for i in range(len(data)):
	    if(data.iloc[i]['repayment_type']=='Weekly'):
	        d1=data.iloc[i]['first_inst_date']
	        #d_1.append(str(d).split(" ")[0])
	        #for _ in range(int(data.iloc[i]['loan_tenure'])-1):
	            #d+=datetime.timedelta(7)
	            #d_1.append(str(d).split(" ")[0])
	        #d_all.append(d_1[::len(d_1)-1])
	        d2=d1+datetime.timedelta(7*(int(data.iloc[i]['loan_tenure'])-1))
	        d_all.append([str(d1).split(" ")[0],str(d2).split(" ")[0]])
	        #d_1=[]
	    else:
	        d1=data.iloc[i]['first_inst_date']
	        #d_1.append(str(d).split(" ")[0])
	        #for _ in range(int(data.iloc[i]['loan_tenure'])-1):
	            #d+=relativedelta(months=+1)
	            #d_1.append(str(d).split(" ")[0])
	        d2=d1+relativedelta(months=+(int(data.iloc[i]['loan_tenure'])-1))
	        d_all.append([str(d1).split(" ")[0],str(d2).split(" ")[0]])
	        #d_all.append(d_1[::len(d_1)-1])
	        #d_1=[]

	#df=pd.DataFrame({"emi_date":d_all})
	df=pd.DataFrame(d_all,columns=["st","end"])
	if(dtype=="master_repay"):
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
	else:
		return df["end"]



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
'''
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
			emi_date=pd.DataFrame(d_all[i],columns=["emi_date"])
			lid=data.iloc[i]['transaction_id']
			first_name=data.iloc[i]['first_name']
			last_name=data.iloc[i]['last_name']
			amt=data.iloc[i]['emi_amt']
			loan_type=data.iloc[i]['type']
			n_emi=data.iloc[i]['no_of_emi']
			one=pd.DataFrame(npx.array([lid,first_name,last_name,amt,loan_type,n_emi]).reshape(1,6),columns=["transaction_id",
				"first_name","last_name","emi_amt","type","no_of_emi"])
			total=pd.concat([one,emi_date],axis=1)
			#print(df)
			dfs.append(total)

		df=pd.concat(dfs,axis=0)
		df.reset_index(inplace=True,drop=True)
		
		data_master=df.fillna(" ")
		return data_master,len(d_all)

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
		return data_master,len(d_all)

'''
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
	data=data.rename({'st_date':'payment_date'},axis=1)
	df=pd.DataFrame(d_all)
	df.index=range(1,len(df)+1)
	#columns=[str(i)+"g" for i in range(2,len(d_all)+2)])
	dic={}
	for i in range(len(list(df.columns))):
		dic[list(df.columns)[i]]="payment_date"

	df=df.rename(dic,axis=1)
	#df.index=range(1,len(df)+1)
	#df.columns=[str(i)+"g" for i in range(2,len(d_all)+2)]
	df=df.fillna("Not in the range")
	df=pd.concat([data,df],axis=1)

	return df


def repay_generator(data,p_date,amt,mode="upload"):
	n_emi=0
	loan_type=data[0][2]
	first_emi=data[0][3]
	loan_tenure=int(data[0][0])
	emi=int(data[0][1])
	payed_total=int(data[0][4])
	carry_forward=int(data[0][5])
	emi_number=int(data[0][6])
	emi_date_flag=datetime.datetime.strptime(data[0][7].split(" ")[0],"%Y-%m-%d")
	receipt_status=" "
	flag_date="2200-12-31"

	last_emi_date=datetime.datetime.strptime(data[0][11].split(" ")[0],"%Y-%m-%d")
	p_date=datetime.datetime.strptime(p_date,"%Y-%m-%d")
	if(mode=="upload"):
		if(p_date<last_emi_date):
			return [0,0]
	total_loan_given=emi*loan_tenure
	residual=total_loan_given-payed_total
	#if(int(amt)>residual):
		#return [0]

	d_1=generate_emi_dates(loan_type,loan_tenure,first_emi)
	d_1.append(datetime.datetime.strptime(flag_date,"%Y-%m-%d"))

	
	if(p_date>=emi_date_flag):
		flag=0
		for i in range(len(d_1)-1):
			if(p_date>=d_1[i] and p_date<d_1[i+1]):
				flag=i
		no_of_emi_expected=flag
		counter=no_of_emi_expected
		if(no_of_emi_expected<emi_number):
			no_of_emi_expected=emi_number
		due=abs(no_of_emi_expected-emi_number)*emi+carry_forward+emi
		received=payed_total+int(amt)
		carry_f=due-int(amt)
		num_emi=no_of_emi_expected+1
		emi_dt_flg=[i for i in d_1 if p_date<i][0]

	if(p_date<emi_date_flag):
		due=carry_forward
		carry_f=due-int(amt)
		num_emi=emi_number
		received=payed_total+int(amt)
		emi_dt_flg=emi_date_flag


	total_to_be_received=loan_tenure*emi
	if(total_to_be_received==received):
		receipt_status="complete"
	else:
		receipt_status="ongoing"

	p_date=str(p_date).split(" ")[0]


	return [received,carry_f,num_emi,emi_dt_flg,due,receipt_status,data[0][8],data[0][9],data[0][10],residual,p_date]


def generate_emi_dates(loan_type,loan_tenure,first_emi):
	d_1=[]
	
	if(loan_type=="Weekly"):
		d=first_emi
		d_1.append(d)
		for _ in range(loan_tenure-1):
			d+=datetime.timedelta(7)
			d_1.append(d)
	else:
		d=first_emi
		d_1.append(d)
		for _ in range(loan_tenure-1):
			d+=relativedelta(months=+1)			
			d_1.append(d)
	d_1=[datetime.datetime.strptime(str(i).split(" ")[0],"%Y-%m-%d") for i in d_1]
	
	return d_1



def generate_payment_report(data_all,emi_dates,emi_amt):
	all_history={}
	due=0
	carry_f=0
	p=0
	st=" "
	if(len(data_all)>0):
		paid_dates=[datetime.datetime.strptime(str(i[0]),"%Y-%m-%d") for i in data_all]
		emi_dates_var=emi_dates.copy()
		for i in emi_dates:
			if i in paid_dates:
				emi_dates.remove(i)
		extracted_dates=emi_dates+paid_dates
		all_dates=sorted(extracted_dates)
		#print(all_dates)
		for i in all_dates:
			if i not in paid_dates:
				if(all_dates.index(i)==0):
					payment_amount=0
					due=int(emi_amt)
					carry_f=int(emi_amt)
					date_ff=date_convert(i)
					all_history[i]=(date_ff,str(payment_amount),str(due),str(carry_f),"not paid"," ","ed")
				else:
					vals=all_history[list(all_history.keys())[-1]]
					payment_amount=0
					due=int(emi_amt)+int(vals[-4])
					carry_f=due
					date_ff=date_convert(i)
					all_history[i]=(date_ff,str(payment_amount),str(due),str(carry_f),"not paid"," ","ed")
			else:
				for data in data_all:
					if(datetime.datetime.strptime(str(data[0]),"%Y-%m-%d")==i):
						due=data[2]
						p+=data[1]
						carry_f=data[3]
						st=data[4]
				if i in emi_dates_var:
					date_ff=date_convert(i)
					all_history[i]=(date_ff,str(p),str(due),str(carry_f),"not paid",st,"ed")
					#print(all_history)
				else:
					#print(i)
					date_ff=date_convert(i)
					all_history[i]=(date_ff,str(p),str(due),str(carry_f),"not paid",st,"pd")
			due=0
			carry_f=0
			p=0
			st=" "

	else:
		emi_dates=sorted(emi_dates)
		for i in range(len(emi_dates)):
			if(i==0):
				payment_amount=0
				due=int(emi_amt)
				carry_f=int(emi_amt)
				all_history[emi_dates[i]]=(str(emi_dates[i]).split(" ")[0],str(payment_amount),str(due),str(carry_f),"not paid"," ","ed")
			else:
				vals=all_history[list(all_history.keys())[-1]]
				payment_amount=0
				due=int(emi_amt)+int(vals[-4])
				carry_f=due
				all_history[emi_dates[i]]=(str(emi_dates[i]).split(" ")[0],str(payment_amount),str(due),str(carry_f),"not paid"," ","ed")
	return all_history


def date_convert(date):
	if(type(date)==datetime.datetime):
		date=str(date).split(" ")[0]
	date_ff=date.split("-")
	date_ff="-".join([d for d in reversed(date_ff)])
	return date_ff

def date_convert_efx(date):
	date_ff=date.split("-")
	list_date=date_ff[::-1]
	final_date=[]
	for i in range(len(list_date)):
		if i==0:
			d="'"+list_date[i]
		else:
			d=list_date[i]
		final_date.append(d)
	out_date="".join([d for d in final_date])
	return out_date

def prepare_data(x):
	x=str(x)
	if(x.strip()=="-"):
		return 0
	else:
		x=x.strip()
		if ',' in x[:]:
			am=x.split(",")
			amt="".join(am)
			try:
				amt=int(amt)
			except:
				amt=int(float(amt))
			if(amt<0):
				return 0
			return amt
		return int(x)


def date_convert_v2(date):
	date_sp=date.split("-")[0]
	if(len(date_sp)<4):
		date=date_convert(date)
		return date
	return date


def upload_repay_once(data):
	#data=pd.read_csv('WEEKLY EMI RECO_30062021 .xlsx - Entitled.csv')
	#data=data[['Transactionid','Date','AMOUNT RECEIVED']]
	print(data)
	data=data[['Tid','Repayment date','Actual EMI deducted']]
	data["Actual EMI deducted"]=data["Actual EMI deducted"].apply(lambda x:prepare_data(x))


	data=data[data["Actual EMI deducted"]!=0]
	try:
		data["Repayment date"]=data["Repayment date"].apply(lambda x:date_convert_v2(x))
	except Exception as e:
		data["Repayment date"]=data["Repayment date"]
	data.index=range(0,len(data))

	#data=data.iloc[:1053]
	return data


#def repay_tracker_equifax()

