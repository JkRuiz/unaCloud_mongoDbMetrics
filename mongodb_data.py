import pandas as pd
from pymongo import MongoClient
import datetime
import pprint
import sys
import time

columnsDelete = ['_id', 'swap', 'net_io_counters', 'vms', 'virtualbox_status', 'vbox_process_count', 'unacloud_status', 'rtt']
# Dates with format Year,Month,Day,Hour,Minuts
startDate = []
limitDate = []

initialDate = input("Por favor ingrese la fecha inicial para las consultas en la DB, debe ser bajo el formato Y/m/dTH:M (ej. 2019/03/17T0:0)")
try:
    dateStart = initialDate.split("/")
    startDate.append(dateStart[0])
    startDate.append(dateStart[1])
    restDate = dateStart[2].split("T")
    startDate.append(restDate[0])
    startDate.append(restDate[1].split(":")[0])
    startDate.append(restDate[1].split(":")[1])

    print ("Ingresaste la fecha incial: \n Año " + str(startDate[0]) + " \n Mes " + str(startDate[1]) + " \n Dia " + str(startDate[2]) + " \n Hora " + str(startDate[3]) + " \n Minutos " + str(startDate[4]))
except:
    print("La fecha ingresada no sigue el formato indicado, por favor revisar formato")
    time.sleep(5)
    sys.exit("La fecha ingresada no sigue el formato indicado, por favor revisar formato")

finalDate = input("Por favor ingrese la fecha limite/final para las consultas en la DB, debe ser bajo el formato Y/m/dTH:M (ej. 2019/04/17T0:0)")

try:
    dateLimit = finalDate.split("/")
    limitDate.append(dateLimit[0])
    limitDate.append(dateLimit[1])
    restLimitDate = dateLimit[2].split("T")
    limitDate.append(restLimitDate[0])
    limitDate.append(restLimitDate[1].split(":")[0])
    limitDate.append(restLimitDate[1].split(":")[1])

    print ("Ingresaste la fecha incial: \n Año: " + str(limitDate[0]) + " \n Mes " + str(limitDate[1]) + " \n Dia " + str(limitDate[2]) + " \n Hora " + str(limitDate[3]) + " \n Minutos " + str(limitDate[4]))

except:
    print("La fecha ingresada no sigue el formato indicado, por favor revisar formato")
    time.sleep(5)
    sys.exit("La fecha ingresada no sigue el formato indicado, por favor revisar formato")

# Connect to the mongo DB
client = MongoClient('mongodb://157.253.205.96', 27017)
# Get the specific database
db = client.performance_collector_unacloud
# Get the specific collection
collection = db.MetricsCollection
try:
    # Set the times in wich the metrics will be obtain
    startTotalDate = datetime.datetime(int(startDate[0]), int(startDate[1]), int(startDate[2]), int(startDate[3]), int(startDate[4])).strftime("%Y-%m-%dT%H:%M:%S")
    limitTotalDate = datetime.datetime(int(limitDate[0]), int(limitDate[1]), int(limitDate[2]), int(limitDate[3]), int(limitDate[4])).strftime("%Y-%m-%dT%H:%M:%S")
except:
    print("Ocurrio un error con las fechas ingresadas, revise los valores ingresados y el formato con el que se deben ingresar")
    time.sleep(5)
    sys.exit("Ocurrio un error con las fechas ingresadas, revise los valores ingresados y el formato con el que se deben ingresar")

if startTotalDate > limitTotalDate:
    print("La fecha de inicio ingresada es mayor que la fecha limte ingresada, por favor revise los valores ingresados")
    time.sleep(5)
    sys.exit("La fecha de inicio ingresada es mayor que la fecha limte ingresada, por favor revise los valores ingresados")

print("Obteniendo las metricas de mongoDB...")
# Get the metrics from the DB
metrics = collection.find({"$and": [{"timestamp": {"$gt": startTotalDate}}, {"timestamp": {"$lt": limitTotalDate}}]})
# Create a dataframe of pandas with those metrics
df = pd.DataFrame(list(metrics))
# Delete the columns that won't be use
for column in columnsDelete:
    del df[str(column)]

print("Creando el .csv...")
# Export the metrics to a .csv file
df.to_csv('metrics_unaCloud.csv', decimal=',', sep=';', index=False, encoding='utf-8', float_format='%.3f')

print(".csv creado exitosamente")
