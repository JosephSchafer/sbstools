#!/usr/bin/python
# Create kml-file from flightdata 
# what a hack *bg*
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
import MySQLdb
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
overVlbg = (1,0)

class HTMLCreator:
    ''' hacky thing to create html with google maps, flights and infos '''
    def __init__(self):
        self.flighttable = []
    
    def addFlight(self, callsign, hexident, date):
        """ add flight to table """
        self.flighttable.append( (callsign, hexident, date) )
    
    def createHTML(self, name):
        # create HTML code for list entries
        listfragment = ""
        for callsign, hexident, date in self.flighttable:
            airline = callsign[0:3]
            number = callsign[3:]
            date = str(date)[0:10]
            listfragment += '<li><a href="http://www.flightstats.com/go/FlightStatus/flightStatusByFlight.do?airline=%s&flightNumber=%s&departureDate=%s">stats for %s </a></li>\n' % (airline, number, date, callsign)
            
        info = """<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <meta http-equiv="content-type" content="text/html; charset=utf-8"/>
    <title>Google Maps Flightradar</title>
    <script src="http://maps.google.com/maps?file=api&amp;v=2.x&amp;key=ABQIAAAA8E-yr4f8IpI_aBX8PhYOVxRuNWC2ipkI6dR5VCD3IuAM1IlVdBQcfokuIrZBiebzigmQ9wXbvXHGjQ"
      type="text/javascript"></script>
    <script type="text/javascript">

var map;
var geoXml = new GGeoXml("http://www.wahuu.at/~db/flights/kml/%s.kml");

function load() {
  if (GBrowserIsCompatible()) {
    map = new GMap2(document.getElementById("map"));
    map.addControl(new GLargeMapControl());
    map.setCenter(new GLatLng(47.45753,9.96015), 3);
    map.setZoom(10);

    map.addOverlay(geoXml);
  }
}
</script>
</head>
  <body onload="load()" onunload="GUnload()">
    <div id="map" style="width: 1000px; height: 700px"></div>
  </body>
</html>
""" % name
        return info

class KMLCreator:
    ''' creator of kml files '''
    
    host = 'localhost'
    database = 'flightdb'
    user = 'flight'
    password = 'flyaway'
 
    def __init__(self, name):
        self.db = MySQLdb.connect(host = self.host, db = self.database, user = self.user, passwd = self.password)
        self.startdate = self.enddate = None
        self.isPlacemarks = True
        self.htmlcreator = HTMLCreator()
        self.name = name
        
    def setLimits(self, startdate, enddate):
        ''' start- + enddate '''
        self.startdate = startdate
        self.enddate = enddate
    
    def setIsZero(self, isZero):
        ''' altitude to zero?'''
        self.isZero = int(isZero)
        
    def setIsPlacemarks(self, state):
        ''' turn on/off placemarks for flights '''
        self.isPlacemarks = state
    
    def createFile(self):
        ''' create kml file! '''
        basesql = "SELECT distinct flights.id, callsign, aircrafts.hexident, flights.ts FROM flights LEFT JOIN flightdata ON flights.id=flightdata.flightid LEFT JOIN aircrafts ON flights.aircraftid=aircrafts.id WHERE ts BETWEEN '%s' AND '%s'" % (self.startdate, self.enddate)
        logging.info(basesql)
        
        # flights crossing Vorarlberg
        if 1 in overVlbg:
            sql = basesql + " AND flights.overVlbg=1" 
            cursor = self.db.cursor()
            cursor.execute(sql)
            rs = cursor.fetchall()
            # loop over all flights and check'em 
            placemark = ""
            for record in rs:
                flightid = record[0]
                callsign = record[1]
                hexident = record[2]
                ts = record[3]
          
                logging.log(logging.INFO, flightid)
                sql = "SELECT longitude, latitude, altitude, time FROM flightdata WHERE flightid=%i" %flightid
                logging.info(sql)
                cursor2 = self.db.cursor()
                cursor2.execute(sql)
                rs2 = cursor2.fetchall()
                
                # only use 10% of data!
                c = 0
                #SKIP = 30
                #length = len(rs2)
                #SKIP = length / 25
                SKIP = 1#0
                if SKIP == 0:
                    SKIP = 1
                coordinateinfo = ""     
                clist = []
                logging.info(SKIP)
                for data in rs2:
                    longitude= data[0]
                    latitude = data[1]
                    altitude = data[2]
                    time = data[3]
                    real_altitude = altitude
                    
                    if self.isZero == 1:
                        altitude = 0
                    # damn, some flights have pretty strange GPS-info! define a value range
                    if latitude > 40 and latitude < 50 and longitude > 8 and longitude < 15: 
                        if c % SKIP == 0:
                            coordinateinfo += "%f,%f,%d \n" %(longitude, latitude, altitude)
                            clist.append( (longitude, latitude, real_altitude, time) )
                        c += 1
                cursor2.close()
                placemark += """<Placemark>
          <description>-</description>
          <name>-</name>
          <styleUrl>#red</styleUrl>
          <LineString>
            <extrude>0</extrude>
            <tessellate>1</tessellate>
            <altitudeMode>absolute</altitudeMode>
            <coordinates>%s</coordinates>
          </LineString>
        </Placemark>""" %coordinateinfo
                # recover altitude from backup ;)
                if self.isPlacemarks:
                    # append location icon which is most close to longitude 9.6
                    longitude = latitude = altitude = 0
                    isAscending = 1
                    # find out if flight flies W->E or E->W
                    if len(clist) > 1:
                        long1 = clist[0][0]
                        long2 = clist[1][0]
                        if long1 > long2:
                            isAscending = 0
                        
                    for long, lat, alt, time in clist:
                        longitude = long
                        latitude = lat
                        altitude = alt
                        ts = time
                        if isAscending == 1 and long > 9.7:
                            break
                        elif isAscending == 0 and long < 9.7:
                            break
                    #long, lat, alt = clist[len(clist)/2]
                    long, lat, alt = longitude, latitude, altitude
                    if self.isZero == 1:
                        alt = 0
                    #self.htmlcreator.addFlight( callsign, hexident, ts)
                    if callsign == None:
                        callsign = "non000"
                    airline = callsign[0:3]
                    number = callsign[3:]
                    # remove leading zeros
                    try:
                        number = str(int(number))
                    except ValueError:
                        pass
                    date = str(ts)[0:10]
                    flightstatslink = "<![CDATA[<a href='http://www.flightstats.com/go/FlightStatus/flightStatusByFlight.do?airline=%s&flightNumber=%s&departureDate=%s'>flightstats</a> | %s | %s | altitude: %sft (%dm)]]>" % (airline, number, date, hexident, ts, altitude, int(altitude) / 3.2808399)
                    #<styleUrl>#normalPlacemark</styleUrl>
                    placemark += """\n<Placemark>
                    <description>%s</description>
                    <name>callsign: %s</name>
                    
                    <Point>
                    <coordinates>%f,%f,%d</coordinates>
                    </Point>
                    </Placemark>
                """ % (flightstatslink, callsign, long, lat, alt)
            cursor.close() 
        
        # flights _NOT_ crossing Vorarlberg
        if 0 in overVlbg:
            cursor = self.db.cursor()
            sql = basesql + " AND flights.overVlbg=0"
            cursor.execute(sql)
            rs = cursor.fetchall()
            # loop over all flights and check'em 
            for record in rs:
                flightid = record[0]
                logging.log(logging.INFO, flightid)
                sql = "SELECT longitude, latitude, altitude FROM flightdata WHERE flightid=%i" %flightid
                logging.info(sql)
                cursor2 = self.db.cursor()
                cursor2.execute(sql)
                rs2 = cursor2.fetchall()
                # only use 10% of data!
                c = 0
                SKIP = 30
                length = len(rs2)
                SKIP = length / 5
                coordinateinfo = ""     
                logging.info(SKIP)
		SKIP = 1
                for data in rs2:
                    longitude= data[0]
                    latitude = data[1]
                    altitude = data[2]
                    # damn, some flights have pretty strange GPS-info! define a value range
                    #if latitude > 40 and latitude < 50 and longitude > 8 and longitude < 15: 
                    if SKIP and c % SKIP == 0:
                        coordinateinfo += "%f,%f,%f \n" %(longitude, latitude, altitude)
                    c += 1
                cursor2.close()
                if length > 50:
                    placemark += """<Placemark>
          <name>-</name>
          <description>-</description>
          <styleUrl>#blue</styleUrl>
          <LineString>
            <extrude>0</extrude>
            <tessellate>1</tessellate>
            <altitudeMode>absolute</altitudeMode>
            <coordinates>%s</coordinates>
          </LineString>
        </Placemark>""" %coordinateinfo
                
            cursor.close() 
    
        f = open(self.name + ".kml", 'w')
        kml = """<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://earth.google.com/kml/2.1">
  <Document>
    <name>Paths</name>
    <description>Flight Display</description>
    <Style id="red">
      <LineStyle>
        <color>7d0000ff</color>
        <width>1</width>
      </LineStyle>
      <PolyStyle>
        <color>7d0000ff</color>
      </PolyStyle>
    </Style>
    <Style id="blue">
      <LineStyle>
        <color>7dff0000</color>
        <width>1</width>
      </LineStyle>
      <PolyStyle>
        <color>7dff0000</color>
      </PolyStyle>
    </Style>
    <Style id="normalPlacemark">
      <IconStyle>
        <Icon>
          <href>http://www.wahuu.at/~db/flights/icon.png</href>
        </Icon>
      </IconStyle>
      <LabelStyle>
          <color>7dff0000</color>
    </LabelStyle>
    </Style>
    <Folder>
    <description>flights</description>
    <name>Flights</name>
    <open>1</open>
    %s
    </Folder>
  </Document>
</kml>
""" % placemark
        logging.log(logging.INFO, kml)
        f.write(kml)
        f.close()
        
        HTMLFILENAME = self.name + ".html"
        f = open(HTMLFILENAME, 'w')
        f.write(self.htmlcreator.createHTML(self.name))
        f.close()
        
def main():
    ''' kml creator '''
    
    logging.info("### KMLCreator started")
    # parsing options
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-s", "--startdate", dest="startdate", help="startdate", metavar="STARTDATE")
    parser.add_option("-e", "--enddate", dest="enddate", help="enddate", metavar="ENDDATE")
    parser.add_option("-n", "--name", dest="filename", help="filename", metavar="NAME")
    parser.add_option("-p", "--placemarks", action="store_false", dest="isPlacemarks", default=True, help="add placemarks to each flight")
    parser.add_option("-z","--zero", dest="isZero", default=False, help="set altitude to 0")
    options, args = parser.parse_args()
    startdate = options.startdate
    enddate = options.enddate
    if startdate == None or enddate == None or options.filename == None:
        logging.info("missing arguments")
        return
    creator = KMLCreator(options.filename)
    creator.setLimits(options.startdate, options.enddate)
    creator.setIsPlacemarks(options.isPlacemarks)
    creator.setIsZero(options.isZero)
    creator.createFile()
    logging.info("### KMLCreator finished")
 
if __name__ == '__main__':
    main()
