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
    
    def __init__(self):
        self.db = MySQLdb.connect(host = self.host, db = self.database, user = self.user, passwd = self.password)
      
    def createFile(self):
        ''' create kml file! '''
        
        cursor = self.db.cursor()
        sql = "SELECT distinct flights.id, callsign, aircrafts.hexident, flights.ts FROM flights LEFT JOIN flightdata ON flights.id=flightdata.flightid LEFT JOIN aircrafts ON flights.aircraftid=aircrafts.id WHERE ts BETWEEN '2007-04-17 00:00' AND '2007-04-18 00:00' AND flights.overVlbg=1;"
        cursor.execute(sql)
        rs = cursor.fetchall()
        # loop over all flights and check'em 
        placemark = ""
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
            SKIP = length / 20
            coordinateinfo = ""     
            logging.info(SKIP)
            for data in rs2:
                logging.log(logging.INFO, "loop")
                longitude= data[0]
                latitude = data[1]
                if latitude > 0 and longitude > 0 and c % SKIP == 0:
                    coordinateinfo += "%f,%f,0 \n" %(longitude, latitude)
                c += 1
            cursor2.close()
            placemark += """<Placemark>
      <name>Absolute Extruded</name>
      <description>Transparent green wall with yellow outlines</description>
      <styleUrl>#yellowLineGreenPoly</styleUrl>
      <LineString>
        <extrude>1</extrude>
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
    <Style id="yellowLineGreenPoly">
      <LineStyle>
        <color>7dff0000</color>
        <width>1</width>
      </LineStyle>
      <PolyStyle>
        <color>7dff0000</color>
      </PolyStyle>
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