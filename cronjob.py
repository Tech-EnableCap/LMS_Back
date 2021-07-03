from server import *
from helper import *
from flask import jsonify

def update_status_cron():
	with app.app_context():
		msg={}
		#lid="LOAN0228644306"
		date=datetime.datetime.now()
		check_time=3
		all_status=[]
		try:
			cursor=mysql.connection.cursor()
			query="SELECT transaction_id FROM upload_file"
			cursor.execute(query,)
			ti=cursor.fetchall()

			#print(transaction_id_list)

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
				#fullname=f_name+" "+l_name
				emi_dates=generate_emi_dates(loan_type,loan_tenure,first_emi_date)
				query="SELECT payment_date,payment_amount,due,carry_f,remark FROM repay_tracker WHERE transaction_id=%s ORDER BY payment_date;"
				cursor.execute(query,(ti_val[0],))
				data_all=cursor.fetchall()
				all_history=generate_payment_report(data_all,emi_dates,emi_amt)


				due=0
				counter=0
				amount_paid=""
				status=""
				last_date_amt_paid=""
				last_date_payed="no emi received"
				red_flag=''
				date_counter=0
				#due_amt=0

				check_dates=[i for i in all_history.keys() if i<=date]

				
				if(len(check_dates)==0):
					total_outstanding=loan_tenure*int(emi_amt)
					final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,0,sanction_amount,0,emi_amt,total_outstanding,comp_name,date_counter,"notstarted",red_flag]

				else:

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

								#last_date_payed="not paid "+str(date_counter)+" emi's"

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

							final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]

						else:

							if(due==0 and int(amount_paid)>0):

								status="ontime,ongoing"
								final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]
								
							if(due>0):

								status="overdue,ongoing"
								if(due<int(emi_amt)):
									date_counter=1
								else:
									date_counter=int(float(int(due)/int(emi_amt)))
								final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]

							if(due<0):

								status="advance,ongoing"
								final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,0,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]

					else:
		
						for i in check_dates[::-1]:
							flag_date_ct=0
							amount_paid=int(all_history[i][1])
							counter+=1
							if(counter==check_time or amount_paid>0):
								if(amount_paid>0):
									last_date_payed=str(i).split(" ")[0]
								else:
									for lt_date in emi_dates_prev[::-1]:
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
									final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,last_date_amt_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]
								else:
									final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]

							elif(total_outstanding>0):
								status="emi_closed,not_paid"
								red_flag="red"
								if(due<int(emi_amt)):
									date_counter=1
								else:
									date_counter=int(float(int(due)/int(emi_amt)))

								if(int(amount_paid)==0):
									final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,last_date_amt_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]
								else:
									final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]

							elif(total_outstanding<0):

								status="emi_closed_with_advance"
								if(int(amount_paid)==0):
									final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,last_date_amt_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]
								else:
									final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]
	
						else:

							if(due==0 and int(amount_paid)>0):

								status="ontime,ongoing"
								final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]

							if(due==0 and int(amount_paid)==0):

								status="ontime,ongoing"
								final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,last_date_amt_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]

							if(due>0 and int(amount_paid)>0):

								status="overdue,ongoing"
								if(due<int(emi_amt)):
									date_counter=1
								else:
									date_counter=int(float(int(due)/int(emi_amt)))
								final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]

							if(due>0 and int(amount_paid)==0):

								status="overdue,halted"
								if(due<int(emi_amt)):
									date_counter=1
								else:
									date_counter=int(float(int(due)/int(emi_amt)))
								final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,last_date_amt_paid,sanction_amount,due,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]

							if(due<0):
								status="advance,ongoing"

								if(int(amount_paid)==0):
									final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,last_date_amt_paid,sanction_amount,0,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]
								else:
									final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,amount_paid,sanction_amount,0,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]

							
							#if(due<0 and int(amount_paid)==0):
								#status="advance,halted"
								#final_out_per_person=[ti_val[0],f_name,l_name,last_date_payed,last_date_amt_paid,sanction_amount,0,emi_amt,total_outstanding,comp_name,date_counter,status,red_flag]


				query_check="SELECT * FROM chk_status WHERE transaction_id=%s"
				cursor.execute(query_check,(ti_val[0],))
				get_data=cursor.fetchall()
				if(len(get_data)<1):
					cursor.execute('''INSERT INTO chk_status VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',(final_out_per_person))
					upload_file_update_query="UPDATE upload_file SET payment_status=%s where transaction_id=%s"
					cursor.execute(upload_file_update_query,(final_out_per_person[11],final_out_per_person[0]))
				else:
					up_query="UPDATE chk_status SET last_date_paid=%s,amt_paid_last=%s,overdue_amount=%s,total_outstanding=%s,no_of_payment_period_missed=%s,status_up=%s,flag=%s WHERE transaction_id=%s"
					cursor.execute(up_query,(final_out_per_person[3],final_out_per_person[4],final_out_per_person[6],final_out_per_person[8],final_out_per_person[10],final_out_per_person[11],final_out_per_person[12],final_out_per_person[0]))
					upload_file_update_query="UPDATE upload_file SET payment_status=%s where transaction_id=%s"
					cursor.execute(upload_file_update_query,(final_out_per_person[11],final_out_per_person[0]))

			mysql.connection.commit()
			cursor.close()
			msg["data"]="success"
		except Exception as e:
			msg["error"]=str(e)
		return jsonify({"msg":msg})



update_status_cron()