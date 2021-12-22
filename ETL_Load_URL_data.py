

import os
import sys
import petl
import pymssql
import configparser
import requests
import datetime
import json
import decimal



# get data from source file
config = configparser.ConfigParser()
try:
    config.read('ETLDemo.ini')
except Exception as e:
    print('could not read Source file:' + str(e))
    sys.exit()


# read settings from source file
startDate = config['SOURCE']['startDate']
url = config['SOURCE']['url']
destServer = config['SOURCE']['server']
destDatabase = config['SOURCE']['database']

# request data from URL
try:
    BOCResponse = requests.get(url+startDate)
except Exception as e:
    print('could not make request:' + str(e))
    sys.exit()

# initialize list of lists for data storage
BOCDates = []
BOCRates = []

# check response status and process BOC JSON object
if (BOCResponse.status_code == 200):
    BOCRaw = json.loads(BOCResponse.text)

    # extract observation data into column arrays
    for row in BOCRaw['observations']:
        BOCDates.append(datetime.datetime.strptime(row['d'],'%Y-%m-%d'))
        BOCRates.append(decimal.Decimal(row['FXUSDCAD']['v']))

    # create petl table from column arrays and rename the columns
    discountRates = petl.fromcolumns([BOCDates,BOCRates],header=['date','rate'])

    # load sales document
    try:
        sales = petl.io.xlsx.fromxlsx('sales.xlsx',sheet='Github')
    except Exception as e:
        print('could not open sales.xlsx:' + str(e))
        sys.exit()

    # join tables
    sales = petl.outerjoin(exchangeRates,sales,key='date')

    # fill down missing values
    sales = petl.filldown(sales,'rate')

    # remove dates with no sales
    sales = petl.select(sales,lambda rec: rec.USD != None)

    # add default column
    sales = petl.addfield(sales,'def', lambda rec: decimal.Decimal(rec.USD) * rec.rate)
    
    # intialize database connection
    try:
        dbConnection = pymssql.connect(server=destServer,database=destDatabase)
    except Exception as e:
        print('could not connect to database:' + str(e))
        sys.exit()

    # populate sales database table
    try:
        petl.io.todb (sales,dbConnection,'sales')
    except Exception as e:
        print('could not write to database:' + str(e))
    print (sales)
