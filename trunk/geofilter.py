#!/usr/bin/python
# Geo filter experimenting :)
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
# $Id$
#import shapelib
import ogr
import time
import MySQLdb

def createLineForFlight(flightid, step=1):
    """ access geographical database info and create linestring """
    database = 'flightdb'
    user = 'flight'
    password = 'flyaway'
    
    db = MySQLdb.connect(host = 'localhost', db = database, user = user, passwd = password)
    cursor = db.cursor()
    sql = "select latitude,longitude from flightdata where flightid=%i" % (flightid,)
    cursor.execute(sql)
    rs = cursor.fetchall()
    
    linestring = ogr.Geometry(ogr.wkbLineString)
    linestring.SetCoordinateDimension(2)
    #s = "POLYGON((";
    #linestring.AddPoint_2D(23, 24)
    
    counter = 0
    for longitude, latitude in rs:
        if counter % step == 0:
            #damn important performancewise! skip (0, 0) coordinates!
            if longitude >0 and latitude > 0:
                linestring.AddPoint_2D(float(latitude), float(longitude))
        counter+=1 
    print linestring
    cursor.close()
    print linestring.GetPointCount()
    return linestring
    
    
def main():
    SHAPEFILE = 'vlbg_wgs84_geogr.shp'
   
    # shapefile = shapelib.open(SHAPEFILE, 'r')
    # numshapes, shapetype, mins, maxs = shapefile.info()
    # print "number of shapes: %i\nshapetype: %i\nmins: %s\nmaxs: %s" % (numshapes, shapetype, mins, maxs)
    
    # there is only one shape in the file, so we access it via index 0
    # shape = shapefile.read_object(0)
    
    src = ogr.Open(SHAPEFILE)
    layer = src.GetLayer()
    feature = layer.GetNextFeature()
    geo = feature.GetGeometryRef()

 
    #wkt = 'POINT(9.96015 47.45753)'
    #wkt = 'POINT(9.743 47.711)'
    
    #should have same ref

    start = time.time()
    # 240733 yes ~470 seconds
    # 240742 no
    # 243494 dunno
    # 243663 yes
    # 243666 no
    # 243689 yes
    linestring = createLineForFlight(243666)
    #print "distance?: " + str(geo.Distance(linestring)) #benchmark: 466.047887087
    #print "intersects?: " + str(geo.Intersect(linestring)) #benchmark: 474.821190119 seconds
    #print "overlaps?: " + str(geo.Overlaps(linestring)) #benchmark: 466.047887087
    print "touches?: " + str(geo.Touches(linestring)) #benchmark: 468.383606911
    #print "intersects?:" + str(geo.Intersect(polygon))
    #print "within?: " + str(geo.Contains(point))
    print "duration: " + str(time.time() - start)
    
if __name__ == '__main__':
    main()
