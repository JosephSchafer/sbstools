#!/usr/bin/python
# create intersecting geom object 
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
# sponsored by Land Vorarlberg (http://www.vorarlberg.at)

import ogr
import time, datetime
import MySQLdb
import logging
from logging import handlers
import sys, os
from ConfigParser import SafeConfigParser
import gpschecker

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
 
class GeomIntersector:
    ''' intersection geom '''
    
    def __init__(self, host, database, user, password):
        self.db = MySQLdb.connect(host = host, db = database, user = user, passwd = password)
    
    def setup(self, shapefile):
        ''' check if flight crossed a certain region '''
        
        self.src = ogr.Open(shapefile)
        self.layer = self.src.GetLayer()
        self.feature = self.layer.GetNextFeature()
        self.geometry = self.feature.GetGeometryRef()
    
    def checkFlight(self, flightid):
        ''' check if flight crosses area of Vorarlberg '''
        
        start = time.time()
        # create linestring for flight route and check if it intersects
        # bugfix 2007/07/09: some linestrings make the Intersect-method hang (at least with python 2.4.x)
        # method 'Distance' solves the problem =>drawback: consumes more CPU-power
        linestring = self.createFlightLine(flightid)
        # linestrings with zero or only one point do not have to be considered
        geom = linestring.Intersection(self.geometry)
        logging.info("\tgeom: %s" % geom)
        
    def createFlightLine(self, flightid):
        """ access geographical database info and create linestring """
        
        cursor = self.db.cursor()
        sql = "SELECT latitude,longitude FROM flightdata WHERE flightid=%i" %flightid
        cursor.execute(sql)
        rs = cursor.fetchall()
       
        # put relevant points in a list
        points = []
        for longitude, latitude in rs:
            # pretty important: skip unnecessary (0, 0) coordinates! 
            # (0, 0) is obviously sent when a new flight is picked up by the Basestation
            if longitude != 0 or latitude != 0:
                lon = float(longitude)
                lat = float(latitude)
                
                # prevent duplicate points in our linestring 
                if (lat, lon) in points:
                     pass
                else:
                    points.append( (lat, lon) )
        cursor.close()
       
        # create empty 2-dimensional linestring-object representing a flight route tracked by the SBS-1
        linestring = ogr.Geometry(ogr.wkbLineString)
        linestring.SetCoordinateDimension(2)
        for lat, lon in points:
             linestring.AddPoint_2D(lat, lon)
        
        logging.info("\tpointcount: %i" % linestring.GetPointCount())
        linestring.FlattenTo2D()
        return linestring

def main():
    ''' geom intersecting '''
    
    logging.info("### geom intersector started")
    logging.info("reading configuration ...")
    cfg = SafeConfigParser()
    cfg.read(sys.path[0] + os.sep + '../sbstools.cfg')
    dbhost = cfg.get('db', 'host')
    dbname = cfg.get('db', 'database')
    dbuser = cfg.get('db', 'user')
    dbpassword = cfg.get('db', 'password') 
    
    
    intersector = GeomIntersector(dbhost, dbname, dbuser, dbpassword)
    intersector.setup( cfg.get('flightprocessor', 'shapefile') )
    #distancechecker.checkAllFlights()
    # check THA971
    intersector.checkFlight(367923)
    logging.info("### distance checker finished")
 
if __name__ == '__main__':
    main()
