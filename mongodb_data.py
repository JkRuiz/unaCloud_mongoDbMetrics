import pandas as pd
from pymongo import MongoClient
import datetime
import pprint

columnsDelete = ['_id', 'ram', 'swap', 'disk', 'net_io_counters', 'vms', 'virtualbox_status', 'vbox_process_count', 'unacloud_status', 'rtt']
# Dates with format Year,Month,Day,Hour,Minuts
startDate = [2019, 3, 1, 0, 0]
limitDate = [2019, 4, 1, 0, 0]

# Connect to the mongo DB
client = MongoClient('mongodb://157.253.205.96', 27017)
# Get the specific database
db = client.performance_collector_unacloud
# Get the specific collection
collection = db.MetricsCollection
# Set the times in wich the metrics will be obtain
startTotalDate = datetime.datetime(startDate[0], startDate[1], startDate[2], startDate[3], startDate[4]).strftime("%Y-%m-%dT%H:%M:%S")
limitTotalDate = datetime.datetime(limitDate[0], limitDate[1], limitDate[2], limitDate[3], limitDate[4]).strftime("%Y-%m-%dT%H:%M:%S")
# Get the metrics from the DB
metrics = collection.find({"$and": [{"timestamp": {"$gt": startTotalDate}}, {"timestamp": {"$lt": limitTotalDate}}]})
# Create a dataframe of pandas with those metrics
df = pd.DataFrame(list(metrics))
# Delete the columns that won't be use
for column in columnsDelete:
    del df[str(column)]
# Export the metrics to a .csv file
df.to_csv('metrics_unaCloud.csv', decimal=',', sep=';', index=False, encoding='utf-8', float_format='%.3f')
