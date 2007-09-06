#!/usr/bin/python
# create shapefile
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
import MySQLdb
import logging
import ogr, osr
import sys, os
import math
from decimal import *
sys.path.append('/tmp/sbstools')
from gpschecker import DistanceCalc
from ConfigParser import SafeConfigParser


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
verbose = 1

class FlightReader:
    ''' responsible for getting data for selected flights from database '''
    
    def __init__(self, host, db, user, passwd):
        ''' establish database connection '''
        self.db = MySQLdb.connect(host=host, db=db, user=user, passwd=passwd)
        self.basesql = "SELECT DISTINCT flights.id, callsign, aircrafts.hexident, flights.ts FROM flights LEFT JOIN flightdata ON flights.id=flightdata.flightid LEFT JOIN aircrafts ON flights.aircraftid=aircrafts.id WHERE gpsaccuracy>=8"
        self.date = None
    
    def setDate(self, date):
        ''' set date limit '''
        self.date = date
        
    def reducePoints(self, points):
        ''' reduce amount of points '''
       
        distcalc = DistanceCalc() 
        THRESHOLD = 1000 # in metres
        # reduce amount of points
        # threshold: 1km; start- + endpoint shall be kept
        cumulateddist = 0
        # reducedpoints
        rpoints = []
        point = points[0]
        rpoints.append( point )
        lat,long,alt = point
        for longitude, latitude, altitude in points:
            dist = distcalc.distance( lat, long, latitude, longitude )
            cumulateddist += dist
            logging.info( "distance: %i" %dist )
            logging.info( "cumulated distance: %i" %cumulateddist )
            if cumulateddist > THRESHOLD:
                if (longitude, latitude, altitude) not in rpoints:
                    rpoints.append( (longitude, latitude, altitude) )
                    logging.info( "appending point (%f, %f, %d)" %(longitude, latitude, altitude) )
                    cumulateddist = 0
            lat, long, alt = latitude, longitude, altitude
        # make sure that very last point in reduced list
        if points[-1] not in rpoints:
            rpoints.append( points[-1] )
        
        return rpoints
        
    def grabFlights(self):
            ''' read flights from database '''
            if self.date == None:
                self.setDate( time.strftime("%Y-%m-%d") )
            
            flights = []
            sql = self.basesql + " AND overvlbg=1 AND DATE(ts) = '%s' LIMIT 1" %self.date
            logging.info( sql )
            cursor = self.db.cursor()
            cursor.execute( sql )
            rs = cursor.fetchall()
        
            for record in rs:
                flightid = record[0]
                callsign = record[1]
                hexident = record[2]
                ts = record[3]
                
                sql2 = "SELECT longitude, latitude, altitude, time FROM flightdata WHERE flightid=%i AND longitude != 0 AND latitude != 0" %flightid
                logging.info(sql2)
                cursor2 = self.db.cursor()
                cursor2.execute(sql2)
                rs2 = cursor2.fetchall()
                points = []
                
                time = None
                times = []
                for longitude, latitude, altitude, ts in rs2:
                    # remember start datetime
                    if time == None:
                        date, time = ts.date(), ts.time()
                        times.append( (date, time) )
                        
                    # convert altitude from ft to m
                    altitude = altitude * 0.3048
                    logging.info( (longitude, latitude, altitude) )
                    points.append( (longitude, latitude, altitude) )
                cursor2.close()
                # remember last datetime
                date, time = ts.date(), ts.time()
                times.append( (date, time) )
                
                points = self.reducePoints( points )
                flights.append( (callsign, hexident, times, points) )
            
            return flights
            
class ShapefileCreator:
    SHAPEFILEDRV = 'ESRI Shapefile'
    
    def __init__(self):
        self.flights = []
        self.srs = osr.SpatialReference()
        self.srs.SetWellKnownGeogCS('WGS84')
        self.dst = osr.SpatialReference()
        # Austria GK M28
        # http://freegis.org/pipermail/mapserver-de/2004-October/000654.html
        self.dst.ImportFromEPSG(31281)
 
    def addFlight(self, callsign, hexident, times, points):
        ''' add linestring '''
        
        self.flights.append( (callsign, hexident, times, points) )
    
    def createFile(self):
        ''' start the engine '''
        
        filename = 'export_2007-09-05.shp'
        driver = ogr.GetDriverByName( self.SHAPEFILEDRV )
        if os.path.exists( filename ):
            driver.DeleteDataSource( filename )
        src = driver.CreateDataSource( filename )
        layer = src.CreateLayer( 'dub', srs=self.dst, geom_type=ogr.wkbLineString25D) #, options=['SHPT', 'ARCZ'])
        featuredefn = ogr.FeatureDefn()
        featuredefn.SetGeomType( ogr.wkbLineString25D )
        
        # create fields
        field1 = ogr.FieldDefn( 'callsign', ogr.OFTString )
        layer.CreateField( field1 )
        field2 = ogr.FieldDefn( 'hexident', ogr.OFTString )
        layer.CreateField ( field2 )
        field3 = ogr.FieldDefn( 'startdate', ogr.OFTString )
        layer.CreateField ( field3 ) 
        field4 = ogr.FieldDefn( 'starttime', ogr.OFTString )
        layer.CreateField ( field4 ) 
        field5 = ogr.FieldDefn( 'enddate', ogr.OFTString )
        layer.CreateField ( field5 ) 
        field6 = ogr.FieldDefn( 'endtime', ogr.OFTString )
        layer.CreateField ( field6 ) 

        # add all flights
        for callsign, hexident, times, points in self.flights:
            feature = ogr.Feature( layer.GetLayerDefn() )
            feature.SetField( 'callsign', callsign )
            feature.SetField( 'hexident', hexident )
            # date setting stuff
            startdate, starttime = times[0]
            enddate, endtime = times[1]
            feature.SetField( 'startdate', startdate )
            feature.SetField( 'starttime', starttime )
            feature.SetField( 'enddate', enddate )
            feature.SetField( 'endtime', endtime )
            
            linestring = ogr.Geometry( ogr.wkbLineString25D )
            linestring.SetCoordinateDimension( 3 )
            linestring.AssignSpatialReference( self.dst )
            trans = osr.CoordinateTransformation( self.srs, self.dst )
            for x, y, z in points:
                transx, transy, transz = trans.TransformPoint( x, y, z ) 
                print (transx, transy, z) 
                linestring.AddPoint(transx, transy, z)
            feature.SetGeometryDirectly(linestring)
            layer.CreateFeature(feature)
            feature = None
        
        src.Destroy()

def main():
    ''' distance checker '''
    
    logging.info("### shapefile creator started")
    logging.info("reading configuration ...")
    cfg = SafeConfigParser()
    cfg.read(sys.path[0] + os.sep + '../sbstools.cfg')
    dbhost = cfg.get('db', 'host')
    dbname = cfg.get('db', 'database')
    dbuser = cfg.get('db', 'user')
    dbpassword = cfg.get('db', 'password') 
   
    print dbhost, dbname, dbuser, dbpassword 
    flightreader = FlightReader(dbhost, dbname, dbuser, dbpassword)
    flightreader.setDate('2007-09-05')
    flights = flightreader.grabFlights()
    creator = ShapefileCreator()
    for callsign, hexident, times, points in flights:
        creator.addFlight(callsign, hexident, times, points)
    creator.createFile()
    
    logging.info("### shapefile creator finished")
 
if __name__ == '__main__':
    main()
