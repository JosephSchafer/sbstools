#!/usr/bin/python
# Applying reduction filter to flights
# i.e. reducing the amount of data
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
import time
import MySQLdb
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

class FlightdataFilter:
    ''' filter data to reduce amount '''
    
    # __FIXME__: should be stored in a separate configuration file
    host = 'localhost'
    database = 'flightdb'
    user = 'flight'
    password = 'flyaway'
    
    def __init__(self):
        self.db = MySQLdb.connect(host = self.host, db = self.database, user = self.user, passwd = self.password)
        
    def runFilter(self, flightid):
        ''' reduce amount of data '''
        # 1. get ids for data to be removed from table flightdata
        # 2. remove records
        # 1b. get ids for data to be removed from table airbornevelocitymessage
        # 2b. remove records
        # 3. update status of flight record in flights table 
        flightdataids = self.runFilter(flightid, 'flightdata')
        airbornevelocityids = self.runFilter(flightid, 'airbornevelocitymessage')
        # begin transaction
        "DELETE FROM flightdata WHERE id IN (flightdataids)"
        "DELETE FROM airbornevelocitymessage WHERE id in (airbornevolocitymessages)"
        "UPDATE flights SET status=1 WHERE id = %i" %flightid
        # commit transaction
    
    def runFilter(self, flightid, table='flightdata'):
        
        ids = []
        cursor = self.db.cursor()
        sql = "SELECT id FROM %s WHERE flightid=%i" %(table, flightid)
        logging.info(sql)
        cursor.execute(sql)
        rs = cursor.fetchall()
        for record in rs:
            id = record[0]
            ids.append(id)
        
        # loop over all ids and select those to delete
        step = 10
        removeableids = []
        for id in ids:
            if id % step != 0:
                removeableids.append(ids[i])
        # ensure that the most recent flightdataentry for the particular flight is not going to be removed
        if ids[-1] in removeableids:
            removeableids.remove(ids[-1])
        cursor.close()
        return removeableids
        
    def tagFlight(self, flightid, status=0):
        ''' set flag for flight in db '''
        
        cursor = self.db.cursor()
        sql = "UPDATE flights SET status=%i WHERE id=%i" %(status, flightid)
        logging.info(sql)
        cursor.execute(sql)
        self.db.commit()
        cursor.close()
    
def main():
    logging.info("### FILTER started")
    filter = FlightdataFilter()
    
    cursor = filter.db.cursor()
    # grab all flights, which where marked as _not_ crossing Vorarlberg
    sql = "SELECT id FROM flights WHERE status IS NULL AND overVlbg=0"
    cursor.execute(sql)
    rs = cursor.fetchall()
    
    # loop over all flights and check'em 
    for record in rs:
        flightid = record[0]
        filter.runFilter(flightid)

    cursor.close() 
    logging.info("### FILTER finished")
 
if __name__ == '__main__':
    main()
