#!/usr/bin/python
# Flight Observation Service
# SBS-1 is a appliance that receives ADS-B signals from aircraft.
# The software BaseStation monitors and displays these flights
# on a screen. Via port 30003 (default) the logged data can be obtained (5' delay)
# This software listens on this port and stores the information retrieved
# in a separate database for further processing. (e.g. filtering by geographical criteria)
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>
import sys
import telnetlib
import logging
import MySQLdb

HOST = "192.168.2.102" # ip-address Basestation is running at
PORT = 30003 # port 30003 is Basestation's default

# this Python logging facility rocks! :)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

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
            collector = DataCollector()
            # new aircraft appears in the right-handed aircraft list for the first time
            # Q: what about if aircraft disappears for some seconds?
            fields = ['msgtype', '-', 'sessionid', 'aircraftid', 'hexident', 'flightid', 'datemessagegenerated', 'timemessagegenerated', 'datemessagelogged', 'timemessagelogged']
            mapping = self._createMap(parts, fields)
            logging.debug(mapping)
            
            flightid = int(mapping.get('flightid'))
            aircraftid = int(mapping.get('aircraftid'))
            collector.logNewFlight(flightid, aircraftid)
        
        elif msgtype == 'ID':
            collector = DataCollector()
            # when an aircraft changes or sets its callsign.
            fields = ['msgtype', '-', 'sessionid', 'aircraftid', 'hexident', 'flightid', 'datemessagegenerated', 'timemessagegenerated', 'datemessagelogged', 'timemessagelogged', 'callsign']
            mapping = self._createMap(parts, fields)
            logging.debug(mapping)
            
            aircraftid = int(mapping.get('aircraftid'))
            hexident = mapping.get('hexident')
            callsign = mapping.get('callsign')
            flightid = int(mapping.get('flightid'))
            collector.updateFlightdata(flightid, aircraftid, callsign, hexident)
    
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
    
            logging.debug('transmissiontype %i:' %(transmissiontype))
            for a, b in mapping.items():
                logging.debug('%s: %s' %(a,b))
            
            # split millisecond part from timemessagegenerated
            time_ms = int(mapping.get('timemessagegenerated').split('.')[1])
            # transmissiontype 2 and 3 contain geographical information (lat, long)
            if transmissiontype in [2, 3]:
                logging.debug('lat: %f' %mapping.get('lat'))
                logging.debug('long: %f' %mapping.get('long'))
                collector.logFlightdata(mapping.get('flightid'), mapping.get('lat'), mapping.get('long'), mapping.get('datemessagegenerated') + ' ' + mapping.get('timemessagegenerated'), time_ms, transmissiontype)
            elif transmissiontype == 1:
                logging.debug('callsign: %s' %mapping.get('callsign'))
            elif transmissiontype == 4:
                logging.debug('track: %s' %mapping.get('track'))
                collector.logAirborneVelocityMessage(mapping.get('flightid'), mapping.get('groundspeed'), mapping.get('verticalrate'), mapping.get('track'))
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
      
    def logNewFlight(self, flightid, aircraftid):
        ''' flight appears on radar screen for the first time '''
        cursor = self.db.cursor()
        sql = "INSERT DELAYED INTO flights (ID, aircraftid) VALUES (%i, %i)" %(flightid, aircraftid)
        logging.info(sql)
        cursor.execute(sql)
        cursor.close()
    
    def updateFlightdata(self, flightid, aircraftid, callsign, hexident):
        ''' update flight info after callsign was set '''
        cursor = self.db.cursor()
        sql = "UPDATE flights SET callsign='%s' WHERE ID=%i" %(callsign, flightid)
        logging.info(sql)
        cursor.execute(sql)
        cursor.close()
        self.newAircraft(aircraftid, hexident)
        
    def newAircraft(self, aircraftid, hexident):
        ''' new aircraft appears '''
        cursor = self.db.cursor()
        sql = "INSERT DELAYED INTO aircrafts (ID, hexident) VALUES (%i, '%s')" % (int(aircraftid), hexident)
        logging.info(sql)
        try:
            cursor.execute(sql)
        except MySQLdb.IntegrityError, e:
            logging.warn(str(e))
        cursor.close()
         
    def logFlightdata(self, flightid, latitude, longitude, time, time_ms=0, transmissiontype=0):
        """ store data in mysql """
        # get database cursor
        cursor = self.db.cursor()
        sql = "INSERT DELAYED INTO flightdata (flightid, latitude, longitude, time, time_ms, transmissiontype) VALUES (%s, %s, %s, '%s', %i, %i)" %(str(flightid), str(latitude), str(longitude), time, time_ms, transmissiontype)
        logging.info(sql)
        cursor.execute(sql)
        cursor.close()
    
    def logAirborneVelocityMessage(self, flightid, groundspeed, verticalrate, track):
        ''' store transmission type 4 '''
        cursor = self.db.cursor()
        sql = "INSERT DELAYED INTO airbornevelocitymessage (flightid, groundspeed, verticalrate, track) VALUES (%s, %s, %s, %s)" %(flightid, groundspeed, verticalrate, track)
        logging.info(sql)
        try:
            cursor.execute(sql)
        except MySQLdb.IntegrityError, e:
            logger.warn(str(e))
        cursor.close()
        
def main():
    tn = telnetlib.Telnet(HOST, PORT)
    handler = MessageHandler()
    while 1:
        message = tn.read_until('\n')
        message = message.replace("\r\n", "")
        handler.processMessage(message)

if __name__ == '__main__':
    main()
