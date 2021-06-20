# Importing the necessary libraries in to the environment
import sys
import mysql.connector
import datetime
from decimal import Decimal

class BudgetManager:
    def __init__(self, database_host,database_username, database_password,database_name):
            
        # Initializing MySQL database connection with input from CLI using connector
        self.db = mysql.connector.connect(
         host=database_host,
         user=database_username,
         password=database_password,
         database=database_name
        )
        
    def process_row_data(self):
        # We are initializing two cursors in order to do two distinct operations simultaneously
        db_cursor_ads = self.db.cursor(buffered=True)
        db_cursor_update = self.db.cursor()
        
        # DB select query to capture the available all ads data to update current_slot_budget
        db_cursor_ads.execute("Select campaign_id,budget,Target_city,Target_device,Date_range_start,Date_range_end ,Time_range_start ,Time_range_end from ads")
        
        # Fetching all the values for the cursors
        ads = db_cursor_ads.fetchall()
        #print(len(ads))
        # Looping over in all the elements fetched
        for ad in ads:
            camp_id = ad[0]
            budget = ad[1]
            target_city = ad[2]
            target_dev = ad[3]
            dt_range_end = ad[5]
            tm_range_end = ad[7]
            dt_range_start = ad[4]
            tm_range_start = ad[6]
            slot1 = datetime.datetime.strptime(ad[5]+" "+ad[7],"%Y-%m-%d %H:%M:%S") # Concatenating date and time for end 
            slot2 = datetime.datetime.strptime(ad[4]+" "+ad[6],"%Y-%m-%d %H:%M:%S") # Concatenating date and time for start 
            slots_number = ((slot1-slot2) .total_seconds() / 3600)*6 # As the number of slots given ,slot duration will be 10 minutes in upgrad portal. The same is followed here. 
            if slots_number!=0 or slots_number!=0.0 or budget!=0.0:
                current_slot_budget = float(budget)/float(slots_number)# The formula given in upgrad portal is followed here
            else:
                current_slot_budget = 0.0
                budget = 0.0
            budget_derived = budget - current_slot_budget # Updating budget value after slot budget is updated
            if slots_number !=0 or slots_number!=0.0:
                dt_time_difference = slot2+datetime.timedelta(minutes = 10) # Moving the window time by ten minutes 
                dt_range_start = dt_time_difference.date() # Updating the new start date
                tm_range_start = dt_time_difference.time() # Updating the new time date
            else:
                dt_range_start = dt_range_start # Updating the new start date
                tm_range_start = tm_range_start # Updating the new time date
            
            
            # DB query for supporting UPSERT operation
            sql_update = """UPDATE capstone.ads SET budget = %s,Current_slot_budget = %s ,Date_range_start = %s,Date_range_end = %s, Time_range_start = %s, Time_range_end = %s 
            where  campaign_id = %s and target_city = %s and target_device = %s """
            
            # The values which we have to pass in the update query
            update_val = (budget_derived,current_slot_budget,dt_range_start,dt_range_end,tm_range_start,tm_range_end,camp_id,target_city,target_dev )
            
            # Execute command to update the sql in to the ads table
            db_cursor_update.execute(sql_update, update_val)
        
            # Commit the operation, so that it reflects globally
            self.db.commit()
            
            # Printing the values in console
            print(str(camp_id)+" | "+'{:.4f}'.format(budget_derived)+" | "+'{:.12f}'.format(current_slot_budget)+" | "+str(target_city)+" | "+str(target_dev)+" | "+str(dt_range_start)+" | "+str(tm_range_start)+" | "+str(dt_range_end)+" | "+str(tm_range_end))
            
    
    # Stopping database connection before the program terminates    
    def __del__(self):
        self.db.close()
        
# Defining the main method       
if __name__ == "__main__":
# Validate Command line arguments
    if len(sys.argv) != 5:
        print("Usage: slot_budget_updater.py <database_host> <database_username> <database_password> <database_name>")
        exit(-1)
     
    # Captured the values from CLI in to the variables to pass it to initiate method 
    database_host = sys.argv[1]
    database_username = sys.argv[2]
    database_password = sys.argv[3]
    database_name = sys.argv[4]
    
    # Processing using exception handling in order to throw an error if it occurs
    try:
        budget_manager = BudgetManager(database_host, database_username,database_password, database_name)
        budget_manager.process_row_data()
    # Will exit the program if we hit ctrl+c 
    except KeyboardInterrupt:
        print('KeyboardInterrupt (ctrl+c) da SATHYA, exiting...')
    # As a best practice, we are terminating the instance and pass the same to stop consumer and DB connection using finally    
    finally:
        if budget_manager is not None:
            del budget_manager