#!/usr/bin/python
# callsign stats: which flights appeared
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
   
class Flightstats:
    ''' statistics about flights  '''
    
    def __init__(self, host, db, user, password):
        self.db = MySQLdb.connect(host = host, db = db, user = user, passwd = password)
      
    def run(self):
        ''' start engine '''
        
        callsigns = []
        callsigns2 = []
        newbies = []
	cursor = self.db.cursor()
        sql = "SELECT DISTINCT callsign FROM flights WHERE gpsaccuracy>=8  AND ts BETWEEN '2007-06-01 00:00' AND '2007-07-01 00:00'"
        cursor.execute(sql)
        rs = cursor.fetchall()
        for record in rs:
            callsign = record[0]
            callsigns.append(callsign)
        
        sql = "SELECT DISTINCT callsign FROM flights WHERE gpsaccuracy>=8 AND overvlbg=1 AND ts BETWEEN '2007-07-01 00:00' AND '2007-08-01 00:00'"
        cursor.execute(sql)
        rs = cursor.fetchall()
        for record in rs:
            callsign = record[0]
            callsigns2.append(callsign)
        cursor.close()
       
        print len(callsigns)
	print len(callsigns2)
	# show difference
	counter = 0
        for callsign in callsigns2:
            if callsign not in callsigns:
		newbies.append(callsign)

	newbies.sort()
	for newbie in newbies:
		print newbie
	print len(newbies)

def main():
    ''' distance checker '''
    
    logging.info("### flightstats started")
    logging.info("reading configuration ...")
    cfg = SafeConfigParser()
    cfg.read(sys.path[0] + os.sep + '../sbstools.cfg')
    dbhost = cfg.get('db', 'host')
    dbname = cfg.get('db', 'database')
    dbuser = cfg.get('db', 'user')
    dbpassword = cfg.get('db', 'password') 
    
    fs = Flightstats(dbhost, dbname, dbuser, dbpassword)
    fs.run()
    logging.info("### flightstats finished")
 
if __name__ == '__main__':
    #setupLogging()
    main()
