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
        
        cursor = self.db.cursor()
        cursor.execute("SET AUTOCOMMIT=0")
        # 1. get ids for data to be removed from table flightdata
        # 2. remove records
        # 1b. get ids for data to be removed from table airbornevelocitymessage
        # 2b. remove records
        # 3. update status of flight record in flights table 
        flightdataids = self.getRemovableIds(flightid, 'flightdata')
        airbornevelocityids = self.getRemovableIds(flightid, 'airbornevelocitymessage')
        # state:
        # 0: not reduced
        # 1: reduced
        state = 1
        
        # if there are less than 50 records, do not reduce!
        if len(flightdataids) < 5 or len(airbornevelocityids) < 5:
            logging.info("length removable flightdata records: %i" %len(flightdataids))
            logging.info("length removable airbornevelocitymessage records: %i" %len(airbornevelocityids))
            logging.info("not reducing flight# %i" %flightid)
           
            flightdataids = airbornevelocityids = []
            state = 0 # not reduced
        
        try:
            # begin transaction
            # don't care about performance for now: execute individual sql-statements
            # __FIXME__: DELETE FROM flightdata WHERE id IN (x1, x2, x3, ...xn)
            for id in flightdataids:
                sql = "DELETE FROM flightdata WHERE id=%i" %id
                logging.info(sql)
                cursor.execute(sql)
            for id in airbornevelocityids:
                sql = "DELETE FROM airbornevelocitymessage WHERE id=%i" %id
                logging.info(sql)
                cursor.execute(sql)
            
            sql = "UPDATE flights SET state=%i WHERE id=%i" %(state, flightid)
            logging.info(sql)
            cursor.execute(sql)
            
        except:
            # on error rollback the complete transaction
            self.db.rollback()
            logging.error("rollback")
            
        # commit transaction
        self.db.commit()
        cursor.close()
    
    def getRemovableIds(self, flightid, table='flightdata'):
        ''' return ids to be deleted '''
        
        ids = []
        cursor = self.db.cursor()
        sql = "SELECT id FROM %s WHERE flightid=%i" %(table, flightid)
        logging.info(sql)
        cursor.execute(sql)
        rs = cursor.fetchall()
        for record in rs:
            id = record[0]
            ids.append(id)
        
        # __FIXME__: a better way of reducing the dataflood would be to
        # consider the timestamp of the entries 
        
        # loop over all ids and select those to delete
        step = 10 # keep ~10% of the data
        removeableids = []
        for id in ids:
            if id % step != 0:
                removeableids.append(id)
        # ensure that the most recent flightdataentry for the particular flight is not going to be removed
        if len(ids) and ids[-1] in removeableids:
            removeableids.remove(ids[-1])
        cursor.close()
        return removeableids
    
def main():
    logging.info("### FILTER started")
    filter = FlightdataFilter()
    
    cursor = filter.db.cursor()
    # grab all flights, which:
    # 1. did _not_ cross Vorarlberg
    # 2. did _not_ have the callsign flickering problem (represented by the subselect)
    # 3. are not yet classified/reduced (state)
    sql = "SELECT id FROM flights WHERE state IS NULL AND overVlbg=0 AND id NOT IN (SELECT DISTINCT a.id FROM flights AS a, flights as b WHERE a.aircraftid=b.aircraftid AND a.id != b.id AND timestampdiff(MINUTE, a.ts, b.ts) BETWEEN 0 AND 45)"
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
