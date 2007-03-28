#!/usr/bin/python
# Geo filter experimenting :)
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
# $Id$
import ogr
import time
import MySQLdb
import logging
# found this very nice online tool http://www.mapshaper.org to simplify the map
SHAPEFILE = 'vlbg_wgs84_geogr.shp'
SHAPEFILE = 'vlbg_wgs84_simplified.shp'
SHAPEFILE = 'vlbg_wgs84_visvalingam_75.shp'
#SHAPEFILE = 'vlbg_wgs84_douglas_14.shp'

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
 
        # SHAPELIB approach (unused atm)
        # shapefile = shapelib.open(SHAPEFILE, 'r')
        # numshapes, shapetype, mins, maxs = shapefile.info()
        # print "number of shapes: %i\nshapetype: %i\nmins: %s\nmaxs: %s" % (numshapes, shapetype, mins, maxs)
        
        # there is only one shape in the file, so we access it via index 0
        # shape = shapefile.read_object(0)
        
      
    
        # benchmark
        start = time.time()
        
        # test runs: manually tracked some flights and 
        # checked visually whether they crossed vorarlberger air space
        # 240733 yes ~470 seconds
        # 240742 no
        # 243494 dunno
        # 243663 yes
        # 243666 no
        # 243689 yes
        # 243824 yes
        # 244673 no 
        # 244676 no
        #print "distance?: " + str(geo.Distance(linestring)) #benchmark: 466.047887087
        #print "overlaps?: " + str(geo.Overlaps(linestring)) #benchmark: 466.047887087
        #print "touches?: " + str(geo.Touches(linestring)) #benchmark: 468.383606911
        #print "intersects?:" + str(geo.Intersect(polygon))
        #print "within?: " + str(geo.Contains(point))
        
        linestring = self.createFlightLine(flightid)
        isIntersecting = self.geometry.Intersect(linestring)
        
        logging.info("flight #%i over vlbg?: %i" %(flightid, isIntersecting))
        logging.info("benchmark: %d seconds" % (time.time() - start))
        return isIntersecting
    
    def createFlightLine(self, flightid, step=1):
        """ access geographical database info and create linestring """
        cursor = self.db.cursor()
        sql = "select latitude,longitude from flightdata where flightid=%i" % (flightid,)
        cursor.execute(sql)
        rs = cursor.fetchall()
        
        linestring = ogr.Geometry(ogr.wkbLineString)
        linestring.SetCoordinateDimension(2)
        
        counter = 0
        for longitude, latitude in rs:
            if counter % step == 0:
                #damn important performancewise! skip (0, 0) coordinates!
                if longitude >0 and latitude > 0:
                    linestring.AddPoint_2D(float(latitude), float(longitude))
            counter+=1 
        #logging.debug(linestring)
        cursor.close()
        logging.debug("pointcount: %i" % linestring.GetPointCount())
        return linestring
    
    def tagFlight(self, flightid, overVlbg=0):
        ''' set flag for flight '''
        
        cursor = self.db.cursor()
        sql = "UPDATE flights SET overvlbg=%i WHERE id=%i" %(overVlbg, flightid)
        logging.info(sql)
        cursor.execute(sql)
        cursor.close()
        
    
    
def main():
    analyzer = FlightAnalyzer()
    cursor = analyzer.db.cursor()
    sql = "SELECT DISTINCT flightid FROM flightdata" #WHERE time BETWEEN '2007-03-28 00:00' AND '2007-03-29 23:59'"
    cursor.execute(sql)
    rs = cursor.fetchall()
    #analyzer.processFlight(247785) #THA931 very close touching?
    for record in rs:
        flightid= record[0]
        analyzer.processFlight(flightid) #245680
    
if __name__ == '__main__':
    main()
    # search for flights which are already out of sight 
    # select count(*) from flightdata where flightid=244678 and time>NOW()-1000;

    # sql-statement for finding all flights not yet geographically classified
    # SELECT callsign, hexident FROM flights INNER JOIN aircrafts ON flights.aircraftid=aircrafts.id WHERE flights.overVlbg IS NULL AND flights.id in (SELECT DISTINCT flightid FROM flightdata WHERE time BETWEEN '2007-03-24 14:00' AND '2007-03-24 18:00');
