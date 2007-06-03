#!/usr/bin/python
# calculate distance between GPS-coordinates of flightdata infos 
# what a hack *bg*
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
import MySQLdb
import logging
import ogr
import sys, os
from ConfigParser import SafeConfigParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

class DistanceChecker:
    ''' hacky thing to calculate distance between two points '''
    
    def __init__(self, host, db, user, password):
        self.db = MySQLdb.connect(host = host, db = db, user = user, passwd = password)
      
    def checkAllFlights(self):
        ''' check all flights '''
        
        cursor = self.db.cursor()
        sql = "SELECT id FROM flights WHERE overvlbg=1"
        cursor.execute(sql)
        rs = cursor.fetchall()
        for record in rs:
            id = record[0]
            self.checkFlight(id)
        cursor.close()
        
    def checkFlight(self, flightid):
        ''' check GPS data of flight '''
        
        logging.info("checking flight #%i" %flightid)
        points = []
        cursor = self.db.cursor()
        sql = "SELECT latitude, longitude FROM flightdata WHERE flightid=%i" %flightid
        cursor.execute(sql)
        rs = cursor.fetchall()
        # add all relevant flightdata to list!
        for record in rs:
            x = record[0]
            y = record[1]
            points.append( (x, y) )
        cursor.close()    
    
        p = 0
        
        for x, y in points:
            p2 = ogr.Geometry(ogr.wkbPoint)
            p2.SetPoint_2D(0, x, y)
            if p == 0:
                p = p2
            distance = p.Distance(p2)
       
            # gotta convert distance into a readable format, e.g. km
            if distance > 0.1:
                logging.info( "\t%f between (%f, %f) and (%f, %f)" %(distance, p.GetX(), p.GetY(), p2.GetX(), p2.GetY()) )
            p = p2
            
def main():
    ''' distance checker '''
    
    logging.info("### distance checker started")
    logging.info("reading configuration ...")
    cfg = SafeConfigParser()
    cfg.read(sys.path[0] + os.sep + '../sbstools.cfg')
    dbhost = cfg.get('db', 'host')
    dbname = cfg.get('db', 'database')
    dbuser = cfg.get('db', 'user')
    dbpassword = cfg.get('db', 'password') 
    
    distancechecker = DistanceChecker(dbhost, dbname, dbuser, dbpassword)
    distancechecker.checkAllFlights()
    #distancechecker.checkFlight(127230)
    
    logging.info("### distance checker finished")
 
if __name__ == '__main__':
    main()
