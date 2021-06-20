# Importing the necessary libraries in to the environment
import sys
import flask
from flask import request, jsonify,make_response
import uuid
from flask import abort
import mysql.connector

# Setting the environment values for Flask
app = flask.Flask(__name__)
app.config["DEBUG"] = True
app.config['RESTFUL_JSON'] = {"ensure_ascii": False}

# Application to route if the url satisfies the below condition ; method to capture the values and insert into MySQL table
@app.route('/ad/user/<request_id>/serve', methods=['GET'])
def serve(request_id):
    # Initializing MySQL database connection with input from CLI using connector
    db = mysql.connector.connect(
            host=app.config.get('database_host'),
            user=app.config.get('database_username'),
            password=app.config.get('database_password'),
            database=app.config.get('database_name')
        )
    
    # We are initializing three cursors in order to do three distinct operations simultaneously
    db_cursor_ad = db.cursor(buffered=True)
    db_cursor_users = db.cursor(buffered=True)
    db_cursor_served_ads = db.cursor()
    text1=''
    
    # Checking if id is not from masked users
    if request_id != '1111-1111-1111-1111':
        # Gathering the arguments from the request
        device_t = request.args['device_type']
        city_t = request.args['city']
        state_t = request.args['state']
        user_id = request_id
        
        # DB select query to capture the users from the request id
        db_cursor_users.execute("Select * from users where id = %s",(user_id,))
        
        # Fetching all the values for the cursors
        users = db_cursor_users.fetchall()
        
        #Looping over user to get the details
        for user in users:
            target_gen = user[2]
            target_age = user[1]
            target_income = user[4]
            
        # DB select query to capture the available ads for the specified condition
        select_query_ads = """Select distinct text,campaign_id, target_device,cpa,cpc,cpm,target_age_start,target_age_end,target_city,target_state,target_country,target_gender,
        target_income_bucket,Date_range_start,Date_range_end ,Time_range_start ,Time_range_end  from ads where  (target_device='All' or target_device = %s) 
        and ( (target_city='All' or target_city = %s) and (target_state='All' or target_state = %s)) and  (target_country='All' or target_country = %s) and
        (Target_gender = %s or Target_gender= 'All') and ((target_age_start >= %s and target_age_end <= %s) or (target_age_start=0 and target_age_end=0))
        and (target_income_bucket = %s or target_income_bucket='All') and status = 'ACTIVE'  order by cpm desc limit 2
        """
        
        # DB execute query to capture the available ads for the specified condition
        db_cursor_ad.execute(select_query_ads,(device_t,city_t,state_t,target_gen,target_age,target_age,target_income,))
        
        # Fetching all the values for the cursors
        ads = db_cursor_ad.fetchall()
        
        # Handling condition to see if there is only one ad available for the condition specified
        if len(ads)!=2:
            for ad in ads:
                text1=ad[0]
                campaign_id = ad[1]
                auc_cpm = ad[5] # Paying the same auction as it is the single winner 
                auc_cpc = ad[4]
                auc_cpa = ad[3]
                targ_age_range = {"start": ad[6],"end": ad[7]} # Clubbing the values in the json format
                targ_loc = {"city": ad[8],"state": ad[9],"country": ad[10]} # Clubbing the values in the json format
                targ_gen = ad[11]
                targ_income = ad[12]
                targ_dev = ad[2]
                camp_start = ad[13]+" "+ad[15] # Clubbing the values in the string format
                camp_end = ad[14]+" "+ad[16] # Clubbing the values in the string format
        # Handling condition to see if there are multiple ads available for the condition specified
        else:
            text1=ads[0][0]
            campaign_id = ads[0][1]
            auc_cpm = ads[1][5] # Paying the auction to the second ad winner 
            auc_cpc = ads[0][4]
            auc_cpa = ads[0][3]
            targ_age_range = {"start": ads[0][6],"end": ads[0][7]} # Clubbing the values in the json format
            targ_loc = {"city": ads[0][8],"state": ads[0][9],"country": ads[0][10]} # Clubbing the values in the json format
            targ_gen = ads[0][11]
            targ_income = ads[0][12]
            targ_dev = ads[0][2]
            camp_start = ads[0][13]+" "+ads[0][15] # Clubbing the values in the string format
            camp_end = ads[0][14]+" "+ads[0][16] # Clubbing the values in the string format
  
    # Checking if id is a masked users      
    else:
        # Gathering the arguments from the request
        device_t = request.args['device_type']
        city_t = request.args['city']
        state_t = request.args['state']
        user_id = request_id
        
        select_query_ads = """Select distinct text,campaign_id, target_device,cpa,cpc,cpm,target_age_start,target_age_end,target_city,target_state,target_country,
        target_gender,target_income_bucket,Date_range_start,Date_range_end ,Time_range_start ,Time_range_end  
        from ads where (target_device='All' or target_device = %s) and (target_city='All' or target_city = %s) 
        and (target_state='All' or target_state = %s)and (target_country='All' or target_country = %s) and status = 'ACTIVE'  order by cpm desc limit 2
        """
        
        # DB select query to capture the available ads for the specified condition
        db_cursor_ad.execute(select_query_ads,(device_t,city_t,state_t,))
        
        # Fetching all the values for the cursors
        ads = db_cursor_ad.fetchall()
        
        # Handling condition to see if there is only one ad available for the condition specified
        if len(ads)!=2:
            for ad in ads:
                text1=ad[0]
                campaign_id = ad[1] # Paying the same auction as it is the single winner 
                auc_cpm = ad[5]
                auc_cpc = ad[4]
                auc_cpa = ad[3]
                targ_age_range = {"start": ad[6],"end": ad[7]} # Clubbing the values in the json format
                targ_loc = {"city": ad[8],"state": ad[9],"country": ad[10]} # Clubbing the values in the json format
                targ_gen = ad[11]
                targ_income = ad[12]
                targ_dev = ad[2]
                camp_start = ad[13]+" "+ad[15] # Clubbing the values in the string format
                camp_end = ad[14]+" "+ad[16] # Clubbing the values in the string format
        else:
            text1=ads[0][0]
            campaign_id = ads[0][1]
            auc_cpm = ads[1][5] # Paying the auction to the second ad winner 
            auc_cpc = ads[0][4]
            auc_cpa = ads[0][3]
            targ_age_range = {"start": ads[0][6],"end": ads[0][7]} # Clubbing the values in the json format
            targ_loc = {"city": ads[0][8],"state": ads[0][9],"country": ads[0][10]} # Clubbing the values in the json format
            targ_gen = ads[0][11] 
            targ_income = ads[0][12]
            targ_dev = ads[0][2]
            camp_start = ads[0][13]+" "+ads[0][15] # Clubbing the values in the string format
            camp_end = ads[0][14]+" "+ads[0][16] # Clubbing the values in the string format
           
    # Handling condition to see if there is an ad shown to the user
    if len(ads)!=0: 
        # Deriving the unique id from uuid package
        request_id_derived = str(uuid.uuid1())
        
        # DB insert query for the captured ads for the user
        insert_sql = """INSERT IGNORE INTO capstone.served_ads(Request_id, Campaign_id,User_id,Auction_cpm,Auction_cpc,Auction_cpa,Target_age_range,Target_location,Target_gender,Target_income_bucket,Target_device_type,Campaign_start_time,Campaign_end_time,Time_stamp) \
        VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s)"""
        
        # Insert Query value to pass in to insert query for served_ads table
        insert_val = (request_id_derived, campaign_id,user_id,auc_cpm,auc_cpc,auc_cpa,str(targ_age_range),str(targ_loc),targ_gen,targ_income,targ_dev,camp_start,camp_end,"")
        
        # Executing the query to insert in to served_ads table
        db_cursor_served_ads.execute(insert_sql, insert_val)
        
        # Commiting the DB in order to values get reflected instantly in the table
        db.commit()
        
        # Printing the values captured in the console
        print(str(request_id_derived)+" | "+str(campaign_id) +" | "+str(user_id) +" | "+'{:.12f}'.format(auc_cpm) +" | "+str(auc_cpc) +" | "+str(auc_cpa) +" | "+str(targ_age_range) +" | "+str(targ_loc) +" | "+str(targ_gen) +" | "+str(targ_income) +" | "+str(targ_dev) +" | "+str(camp_start) +" | "+str(camp_end) +" | 0000-00-00 00:00:00")
    # Handling condition to see if no ad shown to the user    
    else:
        print("Sorry!!! No Ads Available to show")
    
    # Closing the DB Connection
    db.close()
    
    # Returning the values with text and request_id
    return jsonify({"text": text1,"request_id": request_id}) 

# Defining the main method
if __name__ == "__main__":
# Validate Command line arguments
    if len(sys.argv) != 6:
        print("Usage: ad_server.py  <database_host> <database_username> <database_password> <database_name>  <flask_app_port>")
        exit(-1)
        
    # Captured the values from CLI in to the variables to pass it to initiate method
    app.config['database_host'] = sys.argv[1]
    app.config['database_username'] = sys.argv[2]
    app.config['database_password'] = sys.argv[3]
    app.config['database_name'] = sys.argv[4]
    
    # Processing using exception handling in order to throw an error if it occurs
    try:
        app.run(host=sys.argv[1], port=sys.argv[5])
    # Will exit the program if we hit ctrl+c     
    except KeyboardInterrupt:
        print('Keyboard Interrupt detected!!! (ctrl+c) hitted, exiting the program...')
        
    