#!/usr/bin/python
# Create kml-file from flightdata 
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
import MySQLdb
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

class KMLCreator:
    ''' creator of kml files '''
    
    host = 'localhost'
    database = 'flightdb'
    user = 'flight'
    password = 'flyaway'
    basesql = "SELECT distinct flights.id, callsign, aircrafts.hexident, flights.ts FROM flights LEFT JOIN flightdata ON flights.id=flightdata.flightid LEFT JOIN aircrafts ON flights.aircraftid=aircrafts.id WHERE ts BETWEEN '2007-04-17 00:00' AND '2007-04-18 00:00'"
 
    def __init__(self):
        self.db = MySQLdb.connect(host = self.host, db = self.database, user = self.user, passwd = self.password)
      
    def createFile(self):
        ''' create kml file! '''
       
	sql = self.basesql + " AND flights.overVlbg=1" 
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
            <name>flight: %s  aircraft: %s  spotter time: %s</name>
            <styleUrl>#normalPlacemark</styleUrl>
            <Point>
            <coordinates>%f,%f,20000</coordinates>
            </Point>
            </Placemark>
        """ % (callsign, hexident, ts, long, lat)
        
        cursor.close() 
    
        cursor = self.db.cursor()
        sql = self.basesql + " AND flights.overVlbg=0"
        cursor.execute(sql)
        rs = cursor.fetchall()
        # loop over all flights and check'em 
        for record in rs:
            flightid = record[0]
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
            SKIP = length / 5
            coordinateinfo = ""     
            logging.info(SKIP)
            for data in rs2:
                longitude= data[0]
                latitude = data[1]
                # damn, some flights have pretty strange GPS-info! define a value range
                if latitude > 40 and latitude < 50 and longitude > 8 and longitude < 15: 
                    if SKIP and c % SKIP == 0:
                        coordinateinfo += "%f,%f,20000 \n" %(longitude, latitude)
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
    
        FILENAME = 'flights.kml'
        f = open(FILENAME, 'w')
        kml = """<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://earth.google.com/kml/2.1">
  <Document>
    <name>Paths</name>
    <description>Examples of paths. Note that the tessellate tag is by default
      set to 0. If you want to create tessellated lines, they must be authored
      (or edited) directly in KML.</description>
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
        <scale>0.5</scale>
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
    
    creator = KMLCreator()
    creator.createFile()
    logging.info("### KMLCreator finished")
 
if __name__ == '__main__':
    main()
