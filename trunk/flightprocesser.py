#!/usr/bin/python
# Applying operations to flights: 
# - merge flights (keyword "callsign flickering")
# - geo-analysis
# intended usage: run periodically
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
import ogr
import time, datetime
import MySQLdb
import logging

# found this very nice online tool http://www.mapshaper.org to simplify the map
# the simpliefied map is made public 
SHAPEFILE = 'data/vlbg_wgs84_douglas_14.shp'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

class FlightMerger:
    ''' object holding info about how to merge table '''
    
    def __init__(self):
        self.hexidents = {} # {'AE3463' : [23234, 34223, 34234]}
        self.flightmappings = {} # {3234: [(342342, 'SWR343'), (234234, 'SWR5')]}
        self.flighttimestamps = {} # {3234: '2007-04-23 23:23'}
    
    def determineCallsign(self, flightid):
        ''' choose the callsign which occurs most often '''
        
        mappings = self.flightmappings.get(flightid, None)
        if mappings == None:
            return None
        callsigns = [callsign for flightid, callsign in mappings]
        freq = [(a, callsigns.count(a)) for a in set(callsigns)]
        
        # sort callsign so that None is last
        freq.sort(lambda a, b: cmp(b[0], a[0]))
        freq.sort(lambda a, b: cmp(b[1], a[1]))
        logging.info(freq)
        callsign = freq[0][0]
        logging.info("callsign: %s" %callsign)
        return callsign

    def getMergeInfo(self):
        ''' list of merging data '''
        
        mergeinfo = []
        for flightid, connectedflightids in self.flightmappings.items():
            callsign = self.determineCallsign(flightid)
            # remove 1st entry as it is flightid itself 
            del connectedflightids[0]
            mergeinfo.append( (flightid, callsign, [id for id, callsign in connectedflightids]) )
        return mergeinfo
    
    def addFlight(self, flightid, callsign, hexident, timestamp):
        ''' adding flight to table '''
        
        ts = time.mktime( timestamp.timetuple() )
        
        # do we already have the aircraft with hexident in our hexidents-table?
        if self.hexidents.has_key(hexident):
            ids = self.hexidents.get(hexident)
            
            found = 0
            for id in ids[:]:
                # calculate time difference in seconds between these two flights
                ts_id = self.flighttimestamps.get(id)
                timediff = abs(ts_id - ts)
                
                # HIT! flights shall be merged!
                if (timediff / 60) <= 30:
                    found = 1
                    mappings = self.flightmappings.get( id )
                    if (flightid, callsign) not in mappings:
                        mappings.append( (flightid, callsign) )
                        self.flightmappings[id] = mappings
                    break
                
            # this is a NEW flight!
            if found == 0:
                ids.append( flightid )
                self.hexidents[hexident] = ids
                self.flightmappings[flightid] = [ (flightid, callsign) ]
                self.flighttimestamps[flightid] = ts
                
        # totally new flight
        else:
            self.hexidents[hexident] = [flightid, ]
            self.flightmappings[flightid] = [ (flightid, callsign) ]
            self.flighttimestamps[flightid] = ts
        
    def getFlighttable(self):
        ''' returning ready table '''
        
        return self.hexidents#self.flightmappings #, self.flighttimestamps)

class FlightAnalyzer:
    ''' analyzer of flights '''
    
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
    
    def mergeFlights(self):
        ''' fix callsign flickering troubles '''
        # see forum posting: http://www.kinetic-avionics.co.uk/forums/viewtopic.php?t=3782
        
        # mergestate:
        # NULL ... undefined
        # 0 ... not merged
        # 1 ... merged
        cursor = self.db.cursor()
        sql = "SELECT DISTINCT a.ts, aircrafts.hexident, a.callsign, a.id, b.id FROM flights AS a, flights as b INNER JOIN aircrafts ON aircraftid = aircrafts.id WHERE a.aircraftid=b.aircraftid AND a.id != b.id AND b.overVlbg IS NOT NULL AND a.overVlbg IS NOT NULL AND ABS(timestampdiff(MINUTE, a.ts, b.ts)) BETWEEN 0 AND 30 AND a.ts <= NOW() - INTERVAL 30 MINUTE AND aircrafts.hexident IS NOT NULL AND a.ts >= '2007-04-01 00:00' AND b.ts >= '2007-04-01 00:00' ORDER BY a.ts"
        cursor.execute(sql)
        rs = cursor.fetchall()
        
        tbl = FlightMerger()
        for record in rs:
            ts, hexident, callsign, flightid, flightid2 = record
            tbl.addFlight(flightid, callsign, hexident, ts)
            
        logging.info( tbl.getFlighttable() )
        cursor.close()
        
        mergeinfo = tbl.getMergeInfo()
        cursor = self.db.cursor()
        cursor.execute("SET AUTOCOMMIT=0")
        for flightid, callsign, mergingids in mergeinfo:
            
            # start transaction
            # __FIXME__: improve performance by issuing combined sql-statements: UPDATE flightdata SET flightid=.. WHERE flightid=x OR flightid=y ...
            try:
                sql = "UPDATE flights SET callsign='%s', mergestate=1 WHERE id=%i" %(callsign, flightid)
                logging.debug(sql)
                #cursor.execute(sql)
                for id in mergingids:
                    sql = "UPDATE flightdata SET flightid=%i WHERE flightid=%i" %(flightid, id)
                    logging.debug(sql)
                    #cursor.execute(sql)
                    sql = "UPDATE airbornevelocitymessage SET flightid=%i WHERE flightid=%i" %(flightid, id)
                    logging.debug(sql)
                    #cursor.execute(sql)
                    sql = "DELETE FROM flights WHERE id=%i" %id
                    logging.debug(sql)
                    #cursor.execute(sql)
            except:
                self.db.rollback()
            self.db.commit()
        cursor.close()
    
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
    # -> 1. only check flights which were added more than 6 minutes ago
    # -> 2. only check flights where the most recent flightdata is older than 6 minutes
    # -> 3. only check flights where all identical flights with different ids (callsign flickering) are older than 6 minutes
    analyzer.mergeFlights()
    sql = "SELECT id FROM flights WHERE overVlbg IS NULL AND ts < NOW()-INTERVAL 6 MINUTE AND id NOT IN (SELECT DISTINCT flightid FROM flightdata WHERE time  > NOW()-INTERVAL 6 MINUTE) AND flights.aircraftid NOT in (SELECT DISTINCT aircraftid FROM flights INNER JOIN flightdata ON flights.id = flightdata.flightid AND ts > NOW()-INTERVAL 6 MINUTE)"
    cursor.execute(sql)
    rs = cursor.fetchall()
 
    # loop over all flights and check'em 
   # for record in rs:
    #    flightid = record[0]
     #   analyzer.processFlight(flightid)

    cursor.close() 
    logging.info("### GEOFILTER finished")
 
if __name__ == '__main__':
    main()
