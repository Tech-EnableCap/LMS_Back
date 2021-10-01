from threading import Thread
from helper import *

class Workers(Thread):
    def __init__(self,queue,tracker_data,total,st_date,end_date,st_d,en_d):
        Thread.__init__(self)
        self.queue=queue
        self.tracker_data=tracker_data
        self.total=total
        self.st_date=st_date
        self.end_date=end_date
        self.st_d=st_d
        self.en_d=en_d

    def run(self):
        while True:
        	i,fetch_all=self.queue.get()
        	try:
        		loan_type=i[3]
        		loan_tenure=int(i[4])
        		first_emi_date=i[6]
        		emi_amt=i[5]
        		emi_dates=generate_emi_dates(loan_type,loan_tenure,first_emi_date)
        		all_history=generate_payment_report(fetch_all,emi_dates,emi_amt)
        		for hist in all_history.keys():
        			if(self.st_date is not None and self.end_date is not None):
        				if(repay_tracker_date(all_history[hist][0])>=self.st_d and repay_tracker_date(all_history[hist][0])<=self.en_d):
        					self.tracker_data.append([i[0],i[1],i[2],loan_type,emi_amt,all_history[hist][0],all_history[hist][1],all_history[hist][2],all_history[hist][3],str(int(all_history[hist][2])-int(emi_amt))])
        					self.total+=int(all_history[hist][2])
        			else:
        				self.tracker_data.append([i[0],i[1],i[2],loan_type,emi_amt,all_history[hist][0],all_history[hist][1],all_history[hist][2],all_history[hist][3],str(int(all_history[hist][2])-int(emi_amt))])
        	finally:
        		self.queue.task_done()