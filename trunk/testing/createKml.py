#!/usr/bin/python
# Create kml-file from flightdata 
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
import MySQLdb
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
FILENAME = 'flights.kml'
overVlbg = (0, 1)

class KMLCreator:
    ''' creator of kml files '''
    
    host = 'localhost'
    database = 'flightdb'
    user = 'flight'
    password = 'flyaway'
 
    def __init__(self):
        self.db = MySQLdb.connect(host = self.host, db = self.database, user = self.user, passwd = self.password)
        self.startdate = self.enddate = None
        
    def setLimits(self, startdate, enddate):
        ''' start- + enddate '''
        self.startdate = startdate
        self.enddate = enddate
    
    def createFile(self):
        ''' create kml file! '''
        basesql = "SELECT distinct flights.id, callsign, aircrafts.hexident, flights.ts FROM flights LEFT JOIN flightdata ON flights.id=flightdata.flightid LEFT JOIN aircrafts ON flights.aircraftid=aircrafts.id WHERE ts BETWEEN '%s' AND '%s'" % (self.startdate, self.enddate)
        #logging.info(basesql)
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
                sql = "SELECT longitude, latitude FROM flightdata WHERE flightid=%i" %flightid
                logging.info(sql)
                cursor2 = self.db.cursor()
                cursor2.execute(sql)
                rs2 = cursor2.fetchall()
                
                # only use 10% of data!
                c = 0
                SKIP = 30
                length = len(rs2)
                SKIP = length / 25
                coordinateinfo = ""     
                clist = []
                logging.info(SKIP)
                for data in rs2:
                    longitude= data[0]
                    latitude = data[1]
                    # damn, some flights have pretty strange GPS-info! define a value range
                    if latitude > 40 and latitude < 50 and longitude > 8 and longitude < 15: 
                        if c % SKIP == 0:
                            coordinateinfo += "%f,%f,20000 \n" %(longitude, latitude)
                            clist.append( (longitude, latitude) )
                        c += 1
                cursor2.close()
                placemark += """<Placemark>
          <name>-</name>
          <description>-</description>
          <styleUrl>#red</styleUrl>
          <LineString>
            <extrude>0</extrude>
            <tessellate>1</tessellate>
            <altitudeMode>absolute</altitudeMode>
            <coordinates>%s</coordinates>
          </LineString>
        </Placemark>""" %coordinateinfo
            
                # append location icon
                long, lat = clist[len(clist)/2]
                placemark += """<Placemark>
                <name>flight: %s</name>
                <description>aircraft: %s  spotted at: %s</description>
                <styleUrl>#normalPlacemark</styleUrl>
                <Point>
                <coordinates>%f,%f,20000</coordinates>
                </Point>
                </Placemark>
            """ % (callsign, hexident, ts, long, lat)
            
            cursor.close() 
        
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
                for data in rs2:
                    longitude= data[0]
                    latitude = data[1]
                    altitude = data[2]
                    # damn, some flights have pretty strange GPS-info! define a value range
                    if latitude > 40 and latitude < 50 and longitude > 8 and longitude < 15: 
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
    
        f = open(FILENAME, 'w')
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
    </Style>
    <Folder>
    <description>flights</description>
    <name>Flights</name>
    <open>1</open>
    %s
    </Folder>
  </Document>
</kml>""" % placemark
        logging.log(logging.INFO, kml)
        f.write(kml)
        f.close()
        
        
    
def main():
    ''' kml creator '''
    
    logging.info("### KMLCreator started")
    # parsing options
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-s", "--startdate", dest="startdate", help="startdate", metavar="STARTDATE")
    parser.add_option("-e", "--enddate", dest="enddate", help="enddate", metavar="ENDDATE")
    options, args = parser.parse_args()
   
    startdate = options.startdate
    enddate = options.enddate
    if startdate == None or enddate == None:
        logging.info("missing arguments")
        return
    creator = KMLCreator()
    creator.setLimits(options.startdate, options.enddate)
    creator.createFile()
    logging.info("### KMLCreator finished")
 
if __name__ == '__main__':
    main()
