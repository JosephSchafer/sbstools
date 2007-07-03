#!/uslr/bin/python
# calculate distance between GPS-coordinates of flightdata infos 
# what a hack *bg*
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
import MySQLdb
import logging
import ogr, osr
import sys, os
import math
from ConfigParser import SafeConfigParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

class DistanceCalc:
    # based on code from by Aapo Rista (http://positio.rista.net/en/pys60gps/)
    # http://mathforum.org/library/drmath/view/55417.html
    # tc1=mod(atan2(sin(lon2-lon1)*cos(lat2),
    #         cos(lat1)*sin(lat2)-sin(lat1)*cos(lat2)*cos(lon2-lon1)),
    #         2*pi)
    def rad2deg(self, rad):
        """Convert radians to degrees.
        Return float radians."""
        return rad * 180 / math.pi
    
    def deg2rad(self, deg):
        """Convert degrees to radians.
        Return float degrees"""
        return deg * math.pi / 180

    def distance(self, lat1, lon1, lat2, lon2):
        """Calculate dinstance between two lat/lon pairs.
        Return float distance in meters."""
        lat1 = self.deg2rad(lat1)
        lon1 = self.deg2rad(lon1)
        lat2 = self.deg2rad(lat2)
        lon2 = self.deg2rad(lon2)
        theta = lon1 - lon2
        dist = math.sin(lat1) * math.sin(lat2) \
             + math.cos(lat1) * math.cos(lat2) * math.cos(theta)
        if dist >= 1:
            dist = 1
        dist = math.acos(dist)
        dist = self.rad2deg(dist)
        meters = dist * 60 * 1852
        return meters

class DistanceChecker:
    ''' hacky thing to calculate distance between two points '''
    
    def __init__(self, host, db, user, password):
        self.db = MySQLdb.connect(host = host, db = db, user = user, passwd = password)
      
    def checkAllFlights(self):
        ''' check all flights '''
        
        cursor = self.db.cursor()
        sql = "SELECT id FROM flights WHERE overvlbg=1 and ts > '2007-04-01 00:00'"
        cursor.execute(sql)
        rs = cursor.fetchall()
        for record in rs:
            id = record[0]
            self.checkFlight(id)
        cursor.close()
        
    def checkFlight(self, flightid):
        ''' check GPS data of flight '''
        
        spatref = osr.SpatialReference()
        spatref.SetWellKnownGeogCS("WGS84")
        distcalc = DistanceCalc()
        
        logging.info("checking flight #%i" %flightid)
        points = []
        cursor = self.db.cursor()
        sql = "SELECT latitude, longitude, UNIX_TIMESTAMP(time)*1000 + time_ms AS timestamp_ms FROM flightdata WHERE flightid=%i" %flightid
        cursor.execute(sql)
        rs = cursor.fetchall()
        # add all relevant flightdata to list!
        for record in rs:
            x = record[0]
            y = record[1]
            timestamp_ms = long(record[2])
	    # ignore all (0, 0)-infos 
	    if x != 0 and y != 0:
            	points.append( (x, y, timestamp_ms) )
        cursor.close()    
    
        cumulateddistance = 0
        velocities = []
        p = 0
        time = 0

	# accumulated distance between adjacent points	
	stepdistance = 0
	THRESHOLD = 5*1000   #5 kilometres 
        for x, y, ms in points:
            if stepdistance == 0:
		starttime = ms
	    endtime = ms
 
            p2 = ogr.Geometry(ogr.wkbPoint)
            p2.AssignSpatialReference(spatref)
            p2.SetPoint_2D(0, x, y)
            time2 = ms 
            if p == 0:
                p = p2
                time = time2
            distance = distcalc.distance( p.GetX(), p.GetY(), p2.GetX(), p2.GetY() )
            cumulateddistance += distance
            stepdistance += distance

            # make sure that velocity is also calculated if the threshold cannot be reached
            # due to end of list
            if stepdistance > THRESHOLD or points.index( (x, y, ms) ) == len(points) - 1:
            	timediff = endtime - starttime
            	try:
                	velocity = (3600 * 1000 / timediff) * stepdistance / 1000
            	except:
                	velocity = -1
            	# gotta convert distance into a readable format, e.g. km
            	if distance > 0:
                	logging.info( "%f km between (%f, %f) and (%f, %f)" %(stepdistance/1000, p.GetX(), p.GetY(), p2.GetX(), p2.GetY()) )
			logging.info("\t%d %d" %(starttime, endtime) )
                	logging.info( "\t%d ms between these = %f kmph" % (timediff, velocity))
                	velocities.append( velocity )
		stepdistance = 0

            p = p2
            time = time2
            #logging.info( p.GetSpatialReference() )
        timediff = points[-1][2] - points[0][2]
        velocities.sort()
        velocity = (3600 * 1000 / timediff) * cumulateddistance / 1000
        logging.info("\taverage velocity: %f kmph" % velocity)
        logging.info("\tmaximum velocity: %f kmph" % velocities[-1])
        
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
    #distancechecker.checkAllFlights()
    distancechecker.checkFlight(101545)
    distancechecker.checkFlight(7409)
    distancechecker.checkFlight(4919) 
    logging.info("### distance checker finished")
 
if __name__ == '__main__':
    main()
