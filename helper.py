import datetime
import numpy_financial as np
import numpy as npx
import pandas as pd

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