#!/usr/bin/python
# Applying operations to flights: 
# - merge flights (keyword "callsign flickering")
# - geo-analysis
# intended usage: run periodically
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
        sql = "SELECT DISTINCT a.ts, aircrafts.hexident, a.callsign, a.id, b.id FROM flights AS a, flights as b INNER JOIN aircrafts ON aircraftid = aircrafts.id WHERE a.aircraftid=b.aircraftid AND a.id != b.id AND b.overVlbg IS NOT NULL AND a.overVlbg IS NOT NULL AND timestampdiff(MINUTE, a.ts, b.ts) BETWEEN 0 AND 30 AND a.ts <= NOW() - INTERVAL 30 MINUTE AND aircrafts.hexident IS NOT NULL ORDER BY a.ts"
        cursor.execute(sql)
        rs = cursor.fetchall()
        
        # collect flights which belong togeter
        # format: FLIGHTID: [FLIGHTID2, FLIGHTID3, ...], FLIGHTID
        # flights belong together when:
        # - they have the same aircraft
        # - flight timestamps differ 30' max
        # __FIXME__: gotta check the time difference between adjacent records:
        # dt = datetime.datetime(*time.strptime('2007-03-16 18:50:24', "%Y-%m-%d %H:%M:%S")[0:6]) 
        # dt2 - dt1: http://docs.python.org/lib/datetime-timedelta.html
        flightsdict = {} # {1784: [2343, 2342, 4234], 1834: [432, 4342, 342] ...} 
        callsigndict = {} # {1784: ['ELY208', 'ELY 2', ...}
        ts_previous = None
        
        hextable = {}
        for record in rs:
            ts = record[0]
            hexident = record[1]
            callsign = record[2]
            flightid = record[3]
            
            # if no previous record exists
            # set previous ts to current ts
            if ts_previous == None:
                ts_previous = ts
            
            pairs = hextable.get(hexident, [])
            pairs.append((flightid, callsign, ts))
            hextable[hexident] = pairs
        logging.info(hextable)
        cursor.close()
        
        cursor = self.db.cursor()
        for key in hextable.keys():
            pairs = hextable.get(key)
            callsigns = [cs for flightid, cs, ts in pairs]
            logging.info(callsigns)
            freq = [(a, callsigns.count(a)) for a in set(callsigns)]
            # sort callsign so that None is last
            freq.sort(lambda a, b: cmp(b[0], a[0]))
            freq.sort(lambda a, b: cmp(b[1], a[1]))
            logging.info(freq)
            # __FIXME__: give priority to callsigns where isalnum() is True
            callsign = freq[0][0]
            logging.info("callsign: %s" %callsign)
            
            mergedflightids = [flightid for flightid, cs, ts in pairs]
            mainflightid = mergedflightids[0]
            mergedflightids.remove(mainflightid)
            
            logging.info("main flightid: %s" %mainflightid)
            logging.info("merged flightids: %s" %mergedflightids)
            
            # start transaction
            # __FIXME__: improve performance by issuing combined sql-statements: UPDATE flightdata SET flightid=.. WHERE flightid=x OR flightid=y ...
            cursor.execute("SET AUTOCOMMIT=0")
            try:
                sql = "UPDATE flights SET callsign='%s' WHERE id=%i" %(callsign, mainflightid)
                #logging.info(sql)
                #cursor.execute(sql)
                for flightid in mergedflightids:
                    sql = "UPDATE flightdata SET flightid=%i WHERE flightid=%i" %(mainflightid, flightid)
                    #logging.info(sql)
                    #cursor.execute(sql)
                    sql = "UPDATE airbornevelocitymessage SET flightid=%i WHERE flightid=%i" %(mainflightid, flightid)
                    #logging.info(sql)
                    #cursor.execute(sql)
                    sql = "DELETE FROM flights WHERE id=%i" %flightid
                    #logging.info(sql)
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
