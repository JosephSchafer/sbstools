#!/usr/bin/python
# Applying geo-operations to flights
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
import ogr
import time
import MySQLdb
import logging

# found this very nice online tool http://www.mapshaper.org to simplify the map
# the simpliefied map is made public 
SHAPEFILE = 'data/vlbg_wgs84_douglas_14.shp'

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

class FlightAnalyzer:
    ''' geographical analyzer of flights '''
    
    host = 'localhost'
    database = 'flightdb'
    user = 'flight'
    password = 'flyaway'
    
    def __init__(self):
        self.db = MySQLdb.connect(host = self.host, db = self.database, user = self.user, passwd = self.password)
        
        self.src = ogr.Open(SHAPEFILE)
        self.layer = self.src.GetLayer()
        self.feature = self.layer.GetNextFeature()
        self.geometry = self.feature.GetGeometryRef()
        
    def processFlight(self, flightid):
        ''' 1. check flight + 2. tag flight'''
        
        isIntersecting = self.checkFlight(flightid)
        self.tagFlight(flightid, isIntersecting)
    
    def checkFlight(self, flightid):
        ''' check if flight went over Vorarlberg '''

        # benchmark
        start = time.time()
    
        # create linestring from flight route and check if it intersects
        linestring = self.createFlightLine(flightid)
        isIntersecting = self.geometry.Intersect(linestring)
        
        logging.info("flight #%i intersecting?: %i" %(flightid, isIntersecting))
        logging.info("benchmark: %f seconds" % (time.time() - start))
        return isIntersecting
    
    def createFlightLine(self, flightid):
        """ access geographical database info and create linestring """
        
        cursor = self.db.cursor()
        sql = "SELECT latitude,longitude FROM flightdata WHERE flightid=%i" %flightid
        cursor.execute(sql)
        rs = cursor.fetchall()
        
        # create empty 2-dimensional linestring-object representing a flight route tracked by the SBS-1
        linestring = ogr.Geometry(ogr.wkbLineString)
        linestring.SetCoordinateDimension(2)
        for longitude, latitude in rs:
            #pretty important: skip unnecessary (0, 0) coordinates!
            if longitude > 0 and latitude > 0:
                linestring.AddPoint_2D(float(latitude), float(longitude))
        cursor.close()
        
        logging.info("pointcount: %i" % linestring.GetPointCount())
        return linestring
    
    def tagFlight(self, flightid, overVlbg=0):
        ''' set flag for flight in db '''
        
        cursor = self.db.cursor()
        sql = "UPDATE flights SET overvlbg=%i WHERE id=%i" %(overVlbg, flightid)
        logging.info(sql)
        cursor.execute(sql)
	self.db.commit()
        cursor.close()
    
def main():
    logging.info("### GEOFILTER started")
    analyzer = FlightAnalyzer()
    cursor = analyzer.db.cursor()
    # grab all flights not yet classified geographically
    # and not currently in progress
    # NOTE: new flights appear in realtime, other messages are 5 minutes delayed
    # -> 1. only check flights which were entered more than 6 minutes ago
    # -> 2. only check flights where the most recent flightdata is older than 6 minutes
    sql = "SELECT id FROM flights WHERE overVlbg IS NULL AND ts < NOW()-INTERVAL 6 MINUTE AND id NOT IN (SELECT DISTINCT flightid FROM flightdata WHERE time  > NOW() - INTERVAL 6 MINUTE)"
    cursor.execute(sql)
    rs = cursor.fetchall()
 
    # loop over all flights and check'em 
    for record in rs:
        flightid = record[0]
        analyzer.processFlight(flightid)

    cursor.close() 
    logging.info("### GEOFILTER finished")
 
if __name__ == '__main__':
    main()
