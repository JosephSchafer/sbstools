#!/usr/bin/python
# Applying operations to flights (the order is important!) 
#   1 merge flights (keyword "callsign flickering")
#   2 determine accuracy of gps-info
#   3 geo-analysis
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

LOGFILE = "/home/db/sbstools/flightprocessor.log"

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
    
class FlightMerger:
    ''' class holding info about which flights shall be merged '''
    
    def __init__(self):
        self.hexidents = {} # hexident-to-flightid mapping, e.g. {'AE3463' : [23234, 34223, 34234]} 
        self.flightmappings = {} # mainflightid-to-mergedflightid mapping, e.g. {3234: [(342342, 'SWR343'), (234234, 'SWR5')]} 
        self.flighttimestamps = {} # flightid-to-timestamp mapping, e.g. {3234: '2007-04-23 23:23'} 
        
    def determineCallsign(self, flightid):
        ''' choose the callsign which is most likely the "correct" one for this flight '''
        
        mappings = self.flightmappings.get(flightid, None)
        if mappings == None:
            return None
        callsigns = [callsign for flightid, callsign in mappings]
        freq = [(a, callsigns.count(a)) for a in set(callsigns)]
        
        # sort callsigns so that 'None' is last
        freq.sort(lambda a, b: cmp(b[0], a[0]))
        freq.sort(lambda a, b: cmp(b[1], a[1]))
        callsign = freq[0][0]
        logging.debug("callsign: %s" %callsign)
        return callsign

    def getMergers(self):
        ''' list of flights to be merged '''
        
        # format: list of (flightid, callsign, [flightids to be merged])-entries
        mergeinfo = []
        for flightid, connectedflightids in self.flightmappings.items():
            callsign = self.determineCallsign(flightid)
            # skip 1st entry as it is mergeflight itself 
            mergeinfo.append( (flightid, callsign, [id for id, callsign in connectedflightids[1:]]) )
        return mergeinfo
    
    def addFlight(self, flightid, callsign, hexident, timestamp):
        ''' adding flight to table '''
    
        # UNIX timestamp of current flightid
        ts = time.mktime( timestamp.timetuple() )
        
        # check if aircraft (hexident) of flight already exists in hexidents-table
        if self.hexidents.has_key(hexident):
            ids = self.hexidents.get(hexident)
            
            mergeflight = False
            for id in ids[:]:
                # calculate time difference in seconds between current flight and flights in hextable 
                ts_id = self.flighttimestamps.get(id)
                timediff = abs(ts_id - ts)
                
                # same aircraft and within time span of 30 minutes => current flight will be merged!
                if timediff <= self.mergetimespan*60:
                    mergeflight = True
                    mappings = self.flightmappings.get( id )
                    if (flightid, callsign) not in mappings:
                        mappings.append( (flightid, callsign) )
                        self.flightmappings[id] = mappings
                        self.flighttimestamps[flightid] = ts
                    break
                
            # same aircraft, but appeared later => this is another flight!
            if mergeflight == False:
                ids.append( flightid )
                self.hexidents[hexident] = ids
                self.flightmappings[flightid] = [ (flightid, callsign) ]
                self.flighttimestamps[flightid] = ts
                
        # aircraft not yet known at this stage => completely new flight
        else:
            self.hexidents[hexident] = [flightid, ]
            self.flightmappings[flightid] = [ (flightid, callsign) ]
            self.flighttimestamps[flightid] = ts
        
    def logMergerStats(self, logger):
        ''' return ready table '''
        
        logger.info("merger statistics:")
        diffmax = 0
        cf_count = 0
        
        mergers = self.getMergers()
        # sort table by timestamp to make output more clear 
        mergers.sort(lambda a, b: cmp(self.flighttimestamps.get(a[0]), self.flighttimestamps.get(b[0])))
        
        for flightid, callsign, connectedflights in mergers:
            unixtime = self.flighttimestamps.get(flightid)
            ts = datetime.datetime.fromtimestamp(unixtime).isoformat()
            logger.info( "%i (%s)\t %s" %(flightid, callsign, ts) )
            for connectedflight in connectedflights:
                cf_unixtime = self.flighttimestamps.get(connectedflight)
                cf_ts = datetime.datetime.fromtimestamp(cf_unixtime).isoformat()
                diff = (cf_unixtime-unixtime) / 60
                # measure biggest difference
                if diff > diffmax:
                    diffmax = diff
                # count number of all connected flights
                cf_count += 1
                
                logger.info("\t|=%i\t%s\t%i" %(connectedflight, cf_ts, diff))
            logger.info("\t----------------------------")
        logger.info("%i\tmainflights" %len(mergers))
        logger.info("%i\tconnected flights" %cf_count)
        try:
            logger.info("%i\taverage connected flights" % (cf_count/len(mergers)))
        except ZeroDivisionError:
            pass
        logger.info("%i\tmaximum time difference" %diffmax)
        
class FlightAnalyzer:
    ''' analyzer of flights '''
    
    def __init__(self, host, database, user, password):
        self.db = MySQLdb.connect(host = host, db = database, user = user, passwd = password)
    
    def setMergetimespan(self, timespan):
        ''' minutes within which a flight with same aircraft is considered to be only one'''
        self.mergetimespan = timespan
    
    def geoclassifyFlights(self, shapefile):
        ''' check if flight crossed a certain region '''
        
        self.src = ogr.Open(shapefile)
        self.layer = self.src.GetLayer()
        self.feature = self.layer.GetNextFeature()
        self.geometry = self.feature.GetGeometryRef()
    
        cursor = self.db.cursor()
        # grab all flights not yet classified geographically
        # and not currently in progress
        # NOTE: new flights appear in realtime, other messages are 5 minutes delayed
        # -> 1. only check flights which were added more than 6 minutes ago
        # -> 2. only check flights where the most recent flightdata is older than 6 minutes
        # -> 3. only check flights which have already been merged, i.e. where mergestate NOT NULL
        sql = "SELECT id FROM flights WHERE overVlbg IS NULL AND ts < NOW()-INTERVAL 6 MINUTE AND id NOT IN (SELECT DISTINCT flightid FROM flightdata WHERE time  > NOW()-INTERVAL 6 MINUTE) AND flights.aircraftid NOT in (SELECT DISTINCT aircraftid FROM flights INNER JOIN flightdata ON flights.id = flightdata.flightid AND ts > NOW()-INTERVAL 6 MINUTE) AND flights.mergestate IS NOT NULL"
        cursor.execute(sql)
        rs = cursor.fetchall()
        # loop over all flights and check'em 
        for record in rs:
            flightid = record[0]
            self.geoclassifyFlight(flightid)
        cursor.close() 
    
    def mergeFlights(self):
        ''' fix callsign flickering troubles '''
        
        # see forum posting: http://www.kinetic-avionics.co.uk/forums/viewtopic.php?t=3782
        # mergestates:
        #   NULL ... unprocessed
        #   0 ... not merged
        #   1 ... merged
        cursor = self.db.cursor()
        sql = "SELECT DISTINCT a.ts, aircrafts.hexident, a.callsign, a.id, b.id FROM flights AS a, flights as b INNER JOIN aircrafts ON aircraftid = aircrafts.id WHERE a.aircraftid=b.aircraftid AND a.id != b.id AND b.overVlbg IS NULL AND a.overVlbg IS NULL AND a.mergestate IS NULL AND b.mergestate IS NULL AND ABS(timestampdiff(MINUTE, a.ts, b.ts)) BETWEEN 0 AND %i AND b.ts <= NOW() - INTERVAL %i MINUTE AND aircrafts.hexident IS NOT NULL AND a.ts >= '2007-04-01 00:00' AND b.ts >= '2007-04-01 00:00' ORDER BY a.ts" % (self.mergetimespan, self.mergetimespan)
        # clock befor sql statement is executed
        timeinfo = time.strftime('%Y-%m-%d %H:%M')
        cursor.execute(sql)
        rs = cursor.fetchall()
        
        tbl = FlightMerger()
        tbl.mergetimespan = self.mergetimespan
        
        for record in rs:
            ts, hexident, callsign, flightid, flightid2 = record
            tbl.addFlight(flightid, callsign, hexident, ts)
           
        tbl.logMergerStats(logging)
        cursor.close()
        
        mergeinfo = tbl.getMergers()
        cursor = self.db.cursor()
        cursor.execute("SET AUTOCOMMIT=0")
        for flightid, callsign, mergingids in mergeinfo:
            # start transaction
            # __FIXME__: improve performance by issuing combined sql-statements: UPDATE flightdata SET flightid=.. WHERE flightid=x OR flightid=y ...
            try:
                sql = "UPDATE flights SET callsign='%s', mergestate=1 WHERE id=%i" %(callsign, flightid)
                logging.debug(sql)
                logging.info("set flight %i to callsign %s" %(flightid, callsign))
                cursor.execute(sql)
                for id in mergingids:
                    logging.info("merging flight %i with mainflight %i" %(id, flightid))
                    sql = "UPDATE flightdata SET flightid=%i WHERE flightid=%i" %(flightid, id)
                    logging.debug(sql)
                    cursor.execute(sql)
                    sql = "UPDATE airbornevelocitymessage SET flightid=%i WHERE flightid=%i" %(flightid, id)
                    logging.debug(sql)
                    cursor.execute(sql)
                    sql = "DELETE FROM flights WHERE id=%i" %id
                    logging.debug(sql)
                    cursor.execute(sql)
            except Exception, e:
                logging.warn(str(e))
                self.db.rollback()
            self.db.commit()
        
        # set mergestate to zero for all flights not touched by the merge function above
        sql = "UPDATE flights SET mergestate=0 WHERE mergestate IS NULL AND ts < '%s' - INTERVAL %i MINUTE" % (timeinfo, self.mergetimespan)
        cursor.execute(sql)
        logging.info(sql)
        self.db.commit()
        cursor.close()
    
    def geoclassifyFlight(self, flightid):
        ''' check&tag flight '''
        isIntersecting = self.checkFlight(flightid)
        self.tagFlight(flightid, isIntersecting)
    
    def checkFlight(self, flightid):
        ''' check if flight crosses area of Vorarlberg '''

        isIntersecting = 0
        start = time.time()
        # create linestring for flight route and check if it intersects
        # bugfix 2007/07/09: some linestrings make the Intersect-method hang (at least with python 2.4.x)
        # method 'Distance' solves the problem =>drawback: consumes more CPU-power
        linestring = self.createFlightLine(flightid)
        # linestrings with zero or only one point do not have to be considered
        if linestring.GetPointCount() not in (0, 1):
            distance = self.geometry.Distance(linestring)
            logging.info("\tdistance: %f" %distance)
            if distance == 0:
                # zero distance between geometries means that they are intersecting
                isIntersecting = 1
        
        logging.info("\tintersecting?: %i" % isIntersecting)
        logging.info("\tbenchmark: %f seconds" % (time.time() - start))
        return isIntersecting
        
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
    
    def tagFlight(self, flightid, overVlbg=0):
        ''' set flag for flight in db '''
        
        cursor = self.db.cursor()
        sql = "UPDATE flights SET overvlbg=%i WHERE id=%i" %(overVlbg, flightid)
        logging.info(sql)
        cursor.execute(sql)
        self.db.commit()
        cursor.close()
    
def main():
    ''' flightprocessor main '''
    
    setupLogging()
    cfg = SafeConfigParser()
    cfg.read(sys.path[0] + os.sep + 'sbstools.cfg')
    logging.info("### FLIGHTPROCESSOR started")
  
    analyzer = FlightAnalyzer( cfg.get('db', 'host'), cfg.get('db', 'database'), cfg.get('db', 'user'), cfg.get('db', 'password') )
    # set times interval for flights to be merged, i.e. flights with identical aircrafts less than x minutes apart are considered to be equal 
    mergetimespan = cfg.getint('flightprocessor', 'mergetimespan')
    analyzer.setMergetimespan(mergetimespan)
    
    # 1 merge flights to overcome "callsign flickering" issue
    analyzer.mergeFlights()
    # 2 determine gps accuracy
    velocitychecker = gpschecker.VelocityChecker( cfg.get('db', 'host'), cfg.get('db', 'database'), cfg.get('db', 'user'), cfg.get('db', 'password') )
    velocitychecker.checkAllFlights()
    # 3 check if flights crossed area specified in shapefile
    analyzer.geoclassifyFlights( cfg.get('flightprocessor', 'shapefile') )
    
    logging.info("### FLIGHTPROCESSOR finished")
 
if __name__ == '__main__':
    main()
