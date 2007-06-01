# !/usr/bin/python
# Reduction filter: reduce flight data
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
import time
import MySQLdb
import logging
import sys, os
from ConfigParser import SafeConfigParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

class FlightdataReducer:
    ''' reduce flight data (tables: flightdata&airbornevelocitymessage) '''
    
    def __init__(self, host, database, user, password):
        self.db = MySQLdb.connect(host = host, db = database, user = user, passwd = password)
    
    def setPercentage(self, percentage):
        ''' percentage of data to be kept '''
        self.percentage = percentage
    
    def reduceFlight(self, flightid):
        ''' reduce flight data by deleting selected flightdata&airbornevelocitymessage-entries '''
        
        cursor = self.db.cursor()
        cursor.execute("SET AUTOCOMMIT=0")
        # 1. get ids for data to be removed from table flightdata
        # 2. remove records
        # 1b. get ids for data to be removed from table airbornevelocitymessage
        # 2b. remove records
        # 3. update status of flight record in flights table 
        flightdataids  = self.getRemovableIds(flightid, 'flightdata')
        airbornevelocityids = self.getRemovableIds(flightid, 'airbornevelocitymessage')
        
        # state 1 => reduced
        state = 1
        try:
            # begin transaction
            # don't care about performance for now: execute individual sql-statements
            # __FIXME__: DELETE FROM flightdata WHERE id IN (x1, x2, x3, ...xn)
            for id in flightdataids:
                sql = "DELETE FROM flightdata WHERE id=%i" %id
                logging.debug(sql)
                cursor.execute(sql)
            for id in airbornevelocityids:
                sql = "DELETE FROM airbornevelocitymessage WHERE id=%i" %id
                logging.debug(sql)
                cursor.execute(sql)
            
            sql = "UPDATE flights SET state=%i WHERE id=%i" %(state, flightid)
            logging.debug(sql)
            cursor.execute(sql)
            
        except:
            # on error rollback the complete transaction
            self.db.rollback()
            logging.error("rollback")
        else:
            self.db.commit()
        cursor.close()
    
    def getRemovableIds(self, flightid, table='flightdata'):
        ''' return ids to be deleted '''
        
        ids = []
        cursor = self.db.cursor()
        sql = "SELECT id FROM %s WHERE flightid=%i" %(table, flightid)
        logging.debug(sql)
        cursor.execute(sql)
        rs = cursor.fetchall()
        for record in rs:
            id = record[0]
            ids.append(id)
        
        # __IDEA__: a better way of reducing the dataflood would be to
        # consider the timestamp of the entries 
        # loop over all ids and select those to delete
        step = 100 / self.percentage
        dispensableids = []
        
        for i in range( len(ids) ):
            if i % step != 0:
                dispensableids.append( ids[i] )
        # ensure that the most recent entry of the current flight is kept
        if len(ids) and ids[-1] in dispensableids:
            dispensableids.remove( ids[-1] )
        cursor.close()
      
        # nice text output
        logging.info("flight #%i (%s)" %(flightid, table) )
        for id in ids:
            if id in dispensableids:
                logging.info("\t|-%i REMOVE!" %id)
            else:
                logging.info("\t|-%i KEEP!" %id)
        logging.info("\t-------------------------")
        logging.info("%i entries of %s kept" %(len(ids) - len(dispensableids), table))
        logging.info("%i entries of %s removed" %(len(dispensableids), table))
        logging.info("\t=========================")
        return dispensableids
    
def main():
    logging.info("### REDUCER started")
    cfg = SafeConfigParser()
    cfg.read(sys.path[0] + os.sep + 'sbstools.cfg')
    
    reducer = FlightdataReducer( cfg.get('db', 'host'), cfg.get('db', 'database'), cfg.get('db', 'user'), cfg.get('db', 'password') )
    percentage = cfg.getint('flightdatareducer', 'percentage')
    reducer.setPercentage(percentage)
    logging.info("%i percent of flightdata will be reduced" %percentage)
    
    cursor = reducer.db.cursor()
    # grab all flights, which:
    # - are not yet reduced (state IS NULL)
    # - did _not_ cross Vorarlberg (overVlbg=0)
    # - where the callsign flickering problem was solved (mergestate IS NOT NULL)
    sql = "SELECT id FROM flights WHERE state IS NULL AND overVlbg=0 AND mergestate IS NOT NULL"
    cursor.execute(sql)
    rs = cursor.fetchall()
    
    # loop over all flights and reduce'em
    for record in rs:
        flightid = record[0]
        reducer.reduceFlight(flightid)

    cursor.close() 
    logging.info("### REDUCER finished")
 
if __name__ == '__main__':
    main()
