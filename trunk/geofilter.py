#!/usr/bin/python
# Geo filter experimenting :)
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
# $Id$
import shapelib


def main():
    SHAPEFILE = 'vlbg_wgs84_geogr.shp'
    shapefile = shapelib.open(SHAPEFILE, 'r')
    numshapes, shapetype, mins, maxs = shapefile.info()
    
if __name__ == '__main__':
    main()
