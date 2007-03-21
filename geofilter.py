#!/usr/bin/python
# Geo filter experimenting :)
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
# $Id$
#import shapelib
import ogr
import time

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

 
    wkt = 'POINT(9.96015 47.45753)'
    #wkt = 'POINT(9.743 47.711)'
    point = ogr.CreateGeometryFromWkt(wkt)
    #should have same ref

    start = time.time()
    print "within?: " + str(geo.Contains(point))
    print "duration: " + str(time.time() - start)
    
if __name__ == '__main__':
    main()
