#!/usr/bin/python
# callsign stats: compare callsigns of two months
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
import MySQLdb
import logging
from logging import handlers
import ogr, osr
import sys, os
import math
from ConfigParser import SafeConfigParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def setupLogging():
    ''' set up the Python logging facility '''

    # define a Handler which writes INFO messages or higher to a file which is rotated when it reaches 5MB
    handler = handlers.RotatingFileHandler(LOGFILE, maxBytes = 5 * 1024 * 1024, backupCount=7)
    # set a nice format
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    # tell the handler to use this format
    handler.setFormatter(formatter)
    # add the handler to the root logger
    logger = logging.getLogger('')
    logger.setLevel(logging.INFO)
    logging.getLogger('').addHandler(handler)
   
class Flightstbl:
    ''' table containing flight infos '''
    
    def __init__(self): 
        self.tbl = {}
    
    def addFlight(self, name, callsign, options={}):
        ''' add flight to tbl '''
        
        flights = self.tbl.get( callsign )
        if flights == None:
            flights = [ (name, options) ]
        else:
            flights.append( (name, options) )
        self.tbl[callsign] = flights
        
    def printStats(self):
        ''' print statistics '''
        
        for key in self.tbl.keys():
            flights = self.tbl.get( key )
            names = set( [name for name, options in flights] )
            logging.info("->callsign %s" %key)
            for name in names:
                count = len( [name for name, options in flights if name==name] )
                logging.info("\t-%s %i" %(name, count) )
        
class Flightstats:
    ''' statistics about flights  '''
    
    def __init__(self, host, db, user, password):
        self.db = MySQLdb.connect(host = host, db = db, user = user, passwd = password)
        self.month1 = self.year1 = self.month2 = self.year2 = None
    
    def setComparisonMonths(self, month1, year1, month2, year2):
        ''' these two periods shall be compared '''
        
        self.month1 = month1
        self.year1 = year1
        self.month2 = month2
        self.year2 = year2
        
    def run(self):
        ''' start engine '''
        
        tbl = Flightstbl()
        cursor = self.db.cursor()
        sql = "SELECT DISTINCT callsign FROM flights WHERE gpsaccuracy>=8  AND overvlbg=1 AND MONTH(ts)=%i AND YEAR(ts)=%i" %(self.month1, self.year1)
        logging.debug(sql)
        cursor.execute(sql)
        rs = cursor.fetchall()
        for record in rs:
            callsign = record[0]
            tbl.addFlight( "%i-%i"%(month1, year1), callsign ) 
        
        sql = "SELECT DISTINCT callsign FROM flights WHERE gpsaccuracy>=8 AND overvlbg=1 AND MONTH(ts)=%i AND YEAR(ts)=%i" %(self.month2, self.year2)
        logging.debug(sql)
        cursor.execute(sql)
        rs = cursor.fetchall()
        for record in rs:
            callsign = record[0]
            tbl.addFlight( "%i-%i"%(month2, year2), callsign )
        cursor.close()
        
        tbl.printStats()
      
def main():
    ''' month comparison tool '''
    
    logging.info("### flightstats started")
    logging.info("reading configuration ...")
    cfg = SafeConfigParser()
    cfg.read(sys.path[0] + os.sep + '../sbstools.cfg')
    dbhost = cfg.get('db', 'host')
    dbname = cfg.get('db', 'database')
    dbuser = cfg.get('db', 'user')
    dbpassword = cfg.get('db', 'password') 
    
    fs = Flightstats(dbhost, dbname, dbuser, dbpassword)
    fs.setComparisonMonths(4, 2007, 7, 2007)
    fs.run()
    logging.info("### flightstats finished")
 
if __name__ == '__main__':
    #setupLogging()
    main()
