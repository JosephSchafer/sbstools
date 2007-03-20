# $Id$
# Flight Observation Service
# SBS-1 is a appliance that receives ADS-B signals from aircraft.
# The software BaseStation monitors and displays these flights
# on a screen. Via port 30003 (default) the logged data can be obtained (5' delay)
# This software listens on this port and stores the information retrieved
# in a separate database for further processing. (e.g. filtering by geographical criteria)
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>

#!/usr/bin/python
import sys
import telnetlib
import MySQLdb
HOST = "192.168.2.102" # ip-address Basestation is running at
PORT = 30003 # port 30003 is Basestation's default

tn = telnetlib.Telnet(HOST, PORT)

class MessageHandler:
    ''' process messages '''
    
    def _createMap(self, msgparts, fields):
        ''' map msg parts to fields '''
        mapping = dict(zip(fields, msgparts))
        return mapping
        
    def processMessage(self, msg):
        ''' message specs at http://www.kinetic-avionics.co.uk/forums/viewtopic.php?t=1402 '''
    
        DELIMITER = ',' # parts of message are separated by this char
        parts = msg.split(DELIMITER)
        # first part of message indicates MESSAGE TYPE
        # according to the specs the following types are known: 
        # selection change message (SEL), new aircraft message (AIR), new id message (ID) transmission message (MSG)
        msgtype = parts[0] 
        
        if msgtype == 'SEL':
            # selection of new aircraft in Basestation GUI is irrelevant, so we ignore it!
            pass
            
        elif msgtype == 'AIR':
            # new aircraft appears in the right-handed aircraft list for the first time
            # Q: what about if aircraft disappears for some seconds?
            fields = ['msgtype', '-', 'sessionid', 'aircraftid', 'hexident', 'flightid', 'datemessagegenerated', 'timemessagegenerated', 'datemessagelogged', 'timemessagelogged']
            mapping = self._createMap(parts, fields)
            print mapping
        
        elif msgtype == 'ID':
            collector = DataCollector()
            # when an aircraft changes or sets its callsign.
            fields = ['msgtype', '-', 'sessionid', 'aircraftid', 'hexident', 'flightid', 'datemessagegenerated', 'timemessagegenerated', 'datemessagelogged', 'timemessagelogged', 'callsign']
            mapping = self._createMap(parts, fields)
            print mapping
            collector.newAircraft(mapping.get('aircraftid'), mapping.get('hexident'))
    
        elif msgtype == 'MSG':
            collector = DataCollector()
            # delayed output of every message from aircraft
            fields = ['msgtype', 'transmissiontype', 'sessionid', 'aircraftid', 'hexident', 'flightid', 'datemessagegenerated', 'timemessagegenerated', 'datemessagelogged', 'timemessagelogged', 'callsign', 'altitude', 'groundspeed', 'track', 'lat', 'long', 'verticalrate', 'squawk', 'alert', 'emergency', 'spi', 'isonground']
            mapping = self._createMap(parts, fields)
            
            # typeconversion of some fields
            for field in mapping.keys():
                if field in ['groundspeed', 'lat', 'long']:
                    try:
                        mapping[field] = float(mapping.get(field))
                    except ValueError:
                        pass
                if field in ['transmissiontype', 'sessionid', 'aircraftid', 'flightid', 'altitude', 'verticalrate']:
                    try:
                        mapping[field] = int(mapping.get(field))
                    except ValueError:
                        pass
                
            # there are 8 different transmissiontypes of type MSG
            # 1 IDMessage: callsign
            # 2 SurfacePositionMessage: altitude, groundspeed, track, lat, long
            # 3 AirbornePositionMessage: altitute, lat, long, alert, emergency, spi
            # 4 AirborneVelocityManger: groundspeed, track, verticalrate
            # 5 SurveillanceAltMessage: altitude, alert, spi
            # 6 SurveillanceIDMessage: altitude, squawk, alert, emergency, spi
            # 7 AirToAirMessage: altitude
            # 8 AirCallReplay: none at the moment
            # specs from http://www.kinetic-avionics.co.uk/forums/viewtopic.php?t=1402
            transmissiontypes = {1: 'IDMessage', 2: 'SurfacePositionMessage', 3: 'AirbornePositionMessage', 4: 'AirborneVelocityMessage', 5: 'SurveillanceAltMessage', 6: 'SurveillanceIDMessage', 7: 'AirToAirMessage', 8: 'AllCallReply'}
            transmissiontype = mapping.get('transmissiontype')
    
            # transmissiontype 2 and 3 contain geographical information (lat, long)
            print '---------'
            print 'transmissiontype %i:' %(transmissiontype)
            for a, b in mapping.items():
                print '%s: %s' %(a,b)
            print '---------'
            
            if transmissiontype in [2, 3]:
                print 'lat: %f' %mapping.get('lat')
                print 'long: %f' %mapping.get('long')
                collector.logFlightdata(mapping.get('flightid'), mapping.get('lat'), mapping.get('long'), mapping.get('datemessagegenerated') + ' ' + mapping.get('timemessagegenerated') )
            if transmissiontype == 1:
                print 'callsign: %s' %mapping.get('callsign')
        else:
            # unknown msgtype!
            pass
    # listen on socket until user terminates process


class DataCollector:
    ''' database agent '''
    
    database = 'flightdb'
    user = 'flight'
    password = 'flyaway'
    
    def __init__(self):
        self.db = MySQLdb.connect(host = 'localhost', db = self.database, user = self.user, passwd = self.password)
      
    def newAircraft(self, aircraftid, hexident):
        ''' new aircraft appears '''
        cursor = self.db.cursor()
        sql = "INSERT INTO aircrafts (ID, hexident) VALUES (%i, '%s')" % (int(aircraftid), hexident)
        print sql
        cursor.execute(sql)
        cursor.close()
        
    def logFlightdata(self, flightid, latitude, longitude, time):
        """ store data in mysql """
        # get database cursor
        cursor = self.db.cursor()
        sql = "INSERT INTO flightdata" + " (flightid, latitude, longitude, time) VALUES " + "(" + str(flightid) + ", " + str(latitude) + ", " + str(longitude) + ", '" + time + "')"
        print sql
        cursor.execute(sql)
        cursor.close()

def main():
    handler = MessageHandler()
    while 1:
        message = tn.read_until('\n')
        message = message.replace("\r\n", "")
        handler.processMessage(message)

if __name__ == '__main__':
    main()
