# Importing the necessary libraries in to the environment
import sys
import mysql.connector
import json
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import SocketDisconnectedError, LeaderNotAvailable
from datetime import datetime
from decimal import *
import datetime


#Defining the class which contains the initiate methods which carries the connection parameters
class KafkaMySQLSinkConnect:
    def __init__(self, kafka_bootstrap_server, kafka_topic_name, database_host,database_username, database_password,database_name):
    # Initializing Kafka Consumer with input from CLI
        kafka_client = KafkaClient(kafka_bootstrap_server)
        
        self.consumer = kafka_client \
         .topics[kafka_topic_name] \
         .get_simple_consumer(consumer_group="groupid",
         auto_offset_reset=OffsetType.LATEST)
        
        # Initializing MySQL database connection with input from CLI using connector
        
        self.db = mysql.connector.connect(
         host=database_host,
         user=database_username,
         password=database_password,
         database=database_name
        )
        # Printing the values in console to have a header                   
        print("campaign_id | Action | Status")
        
    # Method to process single entire row
    def process_row_data(self, text):
        # Getting the db cursors to read/process the entire db
        db_cursor = self.db.cursor()
        db_cursor_select = self.db.cursor()
        db_cursor_replace = self.db.cursor()
        
        # parsing the json'ed value text into loads function 
        text_value = json.loads(text)
        
        # Assigning a value from Kafka to another variable
        text_derived = text_value['text']
        
        # Initializing various variables to use further with default values
        status = ''
        target_age_start= 0
        target_age_end= 0
        cpm = 0.0000
        current_slot_budget = 0.0000
        date_range_start = ''
        date_range_end = ''
        time_range_start = ''
        time_range_end = ''
                
        # Deriving the status depends on the action received 
        if (text_value['action'].lower() == 'new campaign') | (text_value['action'].lower()  == 'update campaign'):
            status = 'ACTIVE'
        else:
            status = 'INACTIVE'
            
        # Deriving the target_age_start and end from the target_age_range received in the form of json 
        target_age_start = int(text_value['target_age_range']['start'])
        target_age_end= int(text_value['target_age_range']["end"])

        # Deriving the cpm depends on the cpa and cpc received 
        cpc_weight = 0.0075 # This value is taken from upgrad portal as asked to 
        cpa_weight = 0.0005 # This value is taken from upgrad portal as asked to 
        cpm = (cpc_weight*float(text_value['cpc']))+(cpa_weight*float(text_value['cpa']))
        
        # Deriving the slots and number of slots using the date time values provided
        slot1 = datetime.datetime.strptime(text_value['date_range']["end"]+" "+text_value['time_range']["end"],"%Y-%m-%d %H:%M:%S")
        slot2 = datetime.datetime.strptime(text_value['date_range']["start"]+" "+text_value['time_range']["start"],"%Y-%m-%d %H:%M:%S")
        
        # As the number of slots given ,slot duration will be 10 minutes in upgrad portal. The same is followed here.
        slots = ((slot1-slot2).total_seconds() / 3600)*6
            
        # Deriving the current_slot_budget using the budget and the number of slots derived
        current_slot_budget = float(text_value['budget']/int(slots))
        
        # Deriving the date_range_start , time_range_start and end from the date_range,time_range received in the form of json 
        date_range_start = text_value['date_range']["start"]
        date_range_end = text_value['date_range']["end"]
        time_range_start = text_value['time_range']["start"]
        time_range_end = text_value['time_range']["end"]      
        
        # DB query for supporting SELECT operation
        db_cursor_select.execute("select * from capstone.ads where campaign_id = %s", text_value['campaign_id'])
        
        # Retrieves the values from ads table if available
        ads_retrieve = db_cursor_select.fetchall()
        
        # DB query for supporting UPSERT operation        
        replace_sql_query = """REPLACE INTO capstone.ads(text, category,keywords,campaign_id,status,target_gender,target_age_start,target_age_end,target_city,target_state,target_country,target_income_bucket,target_device,cpc,cpa,cpm,budget,current_slot_budget,date_range_start,date_range_end, time_range_start,time_range_end
        VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s)"""
        
        # The values which we have to pass in the replace query
        replace_sql_values = (text_derived, text_value['category'],text_value['keywords'],text_value['campaign_id'],status,text_value['target_gender'],target_age_start,target_age_end,text_value['target_city'],text_value['target_state'],text_value['target_country'],text_value['target_income_bucket'],text_value['target_device'],text_value['cpc'],text_value['cpa'],cpm,text_value['budget'],current_slot_budget,date_range_start,date_range_end,time_range_start,time_range_end)
        
        # DB query for supporting INSERT operation
        insert_sql_query = """INSERT IGNORE INTO capstone.ads(text, category,keywords,campaign_id,status,target_gender,target_age_start,target_age_end,target_city,target_state,target_country,target_income_bucket,target_device,cpc,cpa,cpm,budget,current_slot_budget,date_range_start,date_range_end, time_range_start,time_range_end) \
        VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s)"""
        
        # The values which we have to pass in the insert query
        insert_sql_values = (text_derived, text_value['category'],text_value['keywords'],text_value['campaign_id'],status,text_value['target_gender'],target_age_start,target_age_end,text_value['target_city'],text_value['target_state'],text_value['target_country'],text_value['target_income_bucket'],text_value['target_device'],text_value['cpc'],text_value['cpa'],cpm,text_value['budget'],current_slot_budget,date_range_start,date_range_end,time_range_start,time_range_end)
        
        if len(ads_retrieve)!= 0:
            db_cursor_replace.execute(replace_sql_query,replace_sql_values)
        else:
            # Execute command to insert the sql in to the ads table
            db_cursor.execute(insert_sql_query, insert_sql_values)
           
        # Commit the operation, so that it reflects in the db globally instantly
        self.db.commit()
        
        # Printing the values in console
        print(text_value['campaign_id']+" | "+text_value['action']+" | "+status)
  
    # Method to pocess kafka queue messages subscribed
    def process_kafka_events(self):
        # Processing using exception handling in order to throw an error if it occurs
        # For every message received, we need to pass the same to process_row_data 
        try:
            for queue_message in self.consumer: 
                if queue_message is not None:
                    message = queue_message.value  
                    self.process_row_data(message)
        # Restart consumer and start processing, in any kafka connect error
        except (SocketDisconnectedError, LeaderNotAvailable) as e:
            self.consumer.stop()
            self.consumer.start()
            self.process_kafka_events()
    
    # Stopping consumer and database connection before the program terminates    
    def __del__(self):
        self.consumer.stop()
        self.db.close()

# Defining the main method
if __name__ == "__main__":
    # Validating the Command line arguments
    if len(sys.argv) != 7:
        print("Usage: ad_manager.py <kafka_bootstrap_server> <kafka_topic> <database_host> <database_username> <database_password> <database_name>")
        exit(-1)
    
    # Captured the values from CLI in to the variables to pass it to initiate method
    kafka_bootstrap_server = sys.argv[1]
    kafka_topic = sys.argv[2]
    database_host = sys.argv[3]
    database_username = sys.argv[4]
    database_password = sys.argv[5]
    database_name = sys.argv[6]
    
    # Processing using exception handling in order to throw an error if it occurs
    try:
        kafka_mysql_sink_connect = KafkaMySQLSinkConnect(kafka_bootstrap_server, kafka_topic,database_host, database_username,database_password, database_name)
        kafka_mysql_sink_connect.process_kafka_events()
    # Will exit the program if we hit ctrl+c 
    except KeyboardInterrupt:
        print('Keyboard Interrupt detected!!! (ctrl+c) hitted, exiting the program...')
        
    # As a best practice, we are terminating the instance and pass the same to stop consumer and DB connection using finally
    finally:
        if kafka_mysql_sink_connect is not None:
            del kafka_mysql_sink_connect