"""
extracts trade data from the Luno API
"""

import luno_python.client as luno
import pandas as pd
import csv
from datetime import datetime
from time import sleep
import os

# Logs into API
User = luno.Client(api_key_id=os.getenv('LUNO_API_KEY'), api_key_secret=os.getenv('LUNO_API_KEY_SECRET'))

# orders the data displayed in the csv file
column_order = ['timestamp', 'price', 'volume', 'is_buy', 'sequence']


def add_data(pair):
    """
    appends data to the relative csv file
    parameter:
        pair = currency, LUNO-specific parameter
        \type = string
    """
    
    five_minutes_ago = int((datetime.now().timestamp()-300)*1000)
    one_day_ago = int((datetime.now().timestamp()-86350)*1000)

    with open(pair+'_data.csv', 'a', newline='') as data_file:
        
        try:
            # last timestamp given in the csv file so data will take off from where it ended last time
            last_timestamp = list(pd.read_csv(pair+'_data.csv')['timestamp'])[-1]
            
            # checks if the last_timestamp was within 24 hours
            if last_timestamp - one_day_ago > 0:
                pass
            else:
                last_timestamp = one_day_ago
                
        except:
            # writes headers into new csv file and sets the incoming data to 5 minutes before now
            write_header = csv.writer(data_file)
            write_header.writerow(column_order)
            last_timestamp = five_minutes_ago
        
        try:
            # incoming data from Luno API
            incoming_data = User.list_trades(pair, since=last_timestamp)['trades']
        except NewConnectionError:
            print ('Connection Error')
            return

#         except:
#             print(pair, ': Market not Availble')
#             return
        
        try:
            # Structures the data and writes it into the csv file
            incoming_data.reverse()
            writer_object = csv.DictWriter(data_file, fieldnames=column_order)
            writer_object.writerows(incoming_data)
            print(pair, ': Data Added')
        except:
            print(pair, ': No New Data')
            
        data_file.close()


while True:
    # adds new data every 5 minutes
    print(datetime.now())
    add_data('XBTZAR')
    add_data('ETHZAR')
    add_data('LTCZAR')
    add_data('XRPZAR')

    sleep(300)
