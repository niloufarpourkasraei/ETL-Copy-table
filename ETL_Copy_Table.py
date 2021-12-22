#import needed libraries
import os
import sys
import petl
import pymssql
import configparser
import requests
import datetime
import json
import decimal


# none ascii charactor
reload(sys)
sys.setdefaultencoding('utf8')

# declare connection 
dbCnxns = {'Log_data':'dbname=Log_data user=etl host=127.0.0.1',
'sales':'dbname=sales user=etl host=127.0.0.1',
'Sales_data':'dbname=Sales_data user=etl host=127.0.0.1'}

# set cursors, connections 
sourceConn = pg.connect(dbCnxns['Log_data']) #grab value by referencing key dictionary
targetConn = pg.connect(dbCnxns['sales']) #grab value by referencing key dictionary
sourceCursor = sourceConn.cursor()
targetCursor = targetConn.cursor()

# names of the source tables to be copied
sourceCursor.execute("""select table_name from information_schema.columns where table_name in ('Customers','Managers') group by 1""")
sourceTables = sourceCursor.fetchall()

# iterate through table names to copy over
for t in sourceTables:
    targetCursor.execute("drop table if exists %s" % (t[0]))
    sourceDs = etl.fromdb(sourceConn, 'select * from %s' % (t[0]))
    etl.todb(sourceDs, targetConn, t[0], create=True, sample=10000)



