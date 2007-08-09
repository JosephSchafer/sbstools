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
        self.names = []
    
    def addFlight(self, name, callsign, options={}):
        ''' add flight to tbl '''
        
        flights = self.tbl.get( callsign )
        if flights == None:
            flights = [ (name, options) ]
        else:
            flights.append( (name, options) )
        self.tbl[callsign] = flights

        if name not in self.names:
            self.names.append( name )
        
    def printStats(self):
        ''' print statistics '''
        
        print self.tbl 
        names = self.names 
        logging.info( names )
        keys = self.tbl.keys()
        keys.sort()

        # loop over all callsigns and count occurance
        for key in keys:
            flights = self.tbl.get( key )
            logging.info("%s" %key)
            line = ""
            for n in names:
                count = [ name for name, stuff in flights].count(n)
                line += "\t %i" %count
            logging.info( line )
        
class Flightstats:
    ''' statistics about flights  '''
    
    def __init__(self, host, db, user, password):
        self.db = MySQLdb.connect(host = host, db = db, user = user, passwd = password)
        self.setComparisonMonths(None, None, None)   
 
    def setComparisonMonths(self, month1, month2, year):
        ''' these two periods shall be compared '''
        
        self.month1 = month1
        self.month2 = month2
        self.year = year
        
    def run(self):
        ''' start engine '''
        
        tbl = Flightstbl()
        for month in range(self.month1, self.month2 + 1):
            cursor = self.db.cursor()
            sql = "SELECT callsign, DATE(ts) FROM flights WHERE gpsaccuracy>=8 AND overvlbg=1 AND MONTH(ts)=%i AND YEAR(ts)=%i" %(month, self.year)
            logging.debug(sql)
            cursor.execute(sql)
            rs = cursor.fetchall()
            for record in rs:
                callsign = record[0]
                ts = record[1]
                tbl.addFlight( "%i-%i"%(month, self.year), callsign, {'ts':ts} ) 
        
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
    fs.setComparisonMonths(4, 7, 2007)
    fs.run()
    logging.info("### flightstats finished")
 
if __name__ == '__main__':
    #setupLogging()
    main()
