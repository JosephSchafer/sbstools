# Overview #

  * pyshapelib: http://ftp.intevation.de/users/bh/pyshapelib/

```
    apt-get install shapelib
    apt-get build-dep shapelib
```

  * ogr/gdal: http://www.gdal.org/ogr/

```
    apt-get install python-gdal
```
> performance issues:
> takes about 500 seconds to calculate if GPS-coordinate (Point) is within polygon
> takes the same time to calculate if linestring (flight line consists of ~1000 coordinates) is within polygon (map of Vorarlberg contains 79754 coordinates!)

  * mysql opengis-support: http://dev.mysql.com/doc/refman/5.0/en/opengis-geometry-model.html

## Solution ##

We stick with Python GDAL/OGR. A simplified map was created using the Douglas-Peucker algorithm. Mapshaper:http://www.mapshaper.org does the job!