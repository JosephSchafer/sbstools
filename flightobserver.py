# Flight Observation Service
# SBS-1 is a appliance that receives ADS-B signals from aircraft.
# The software BaseStation monitors and displays these flights
# on a screen. Via port 30003 (default) the logged data can be obtained (5' delay)
# This software listens on this port and stores the information retrieved
# in a separate database for further processing.
# Copyright (GPL) 2007 Dominik Bartenstein <db@wahuu.at>

#!/usr/bin/python
import sys
import telnetlib
import MySQLdb
HOST = "192.168.2.102"
PORT = 30003 
# port 30003 is default

tn = telnetlib.Telnet(HOST, PORT)
# message information at http://www.kinetic-avionics.co.uk/forums/viewtopic.php?t=1402
def decodeMessage(line):
    ''' parse message '''
    DELIMITER = ','
    parts = line.split(DELIMITER)
    msgtype = parts[0]
    
    if msgtype == 'SEL':
        # selection of new aircraft in basestation interface is irrelevant!
        pass
    elif msgtype == 'AIR':
        # new aircraft appears for the first time in the right-handed aircraft list
        fields = ['msgtype', '-', 'sessionid', 'aircraftid', 'hexident', 'flightid', 'datemessagegenerated', 'timemessagegenerated', 'datemessagelogged', 'timemessagelogged']
        map = {}
        for i in range(len(fields)):
            map[fields[i]] = parts[i]
        print map
    elif msgtype == 'ID':
        # when an aircraft changes, or sets, its callsign.
        fields = ['msgtype', '-', 'sessionid', 'aircraftid', 'hexident', 'flightid', 'datemessagegenerated', 'timemessagegenerated', 'datemessagelogged', 'timemessagelogged', 'callsign']
        map = {}
        for i in range(len(fields)):
            map[fields[i]] = parts[i]
        print map
    elif msgtype == 'MSG':
        fields = ['msgtype', 'transmissiontype', 'sessionid', 'aircraftid', 'hexident', 'flightid', 'datemessagegenerated', 'timemessagegenerated', 'datemessagelogged', 'timemessagelogged', 'callsign', 'altitude', 'groundspeed', 'track', 'lat', 'long', 'verticalrate', 'squawk', 'alert', 'emergency', 'spi', 'isonground']
        map = {}
        # map values to fields
        for i in range(len(fields)):
            map[fields[i]] = parts[i]
        
        # auto-conversion of special fields
        for field in map.keys():
            if field in ['groundspeed', 'lat', 'long']:
                try:
                    map[field] = float(map.get(field))
                except ValueError:
                    pass
            if field in ['transmissiontype', 'sessionid', 'aircraftid', 'flightid', 'altitude', 'verticalrate']:
                try:
                    map[field] = int(map.get(field))
                except ValueError:
                    pass
            
        transmissiontypes = {1: 'IDMessage', 2: 'SurfacePositionMessage', 3: 'AirbornePositionMessage', 4: 'AirborneVelocityMessage', 5: 'SurveillanceAltMessage', 6: 'SurveillanceIDMessage', 7: 'AirToAirMessage', 8: 'AllCallReply'}
        transmissiontype = map['transmissiontype']
        #print transmissiontypes.get(transmissiontype)
        if transmissiontype in [2, 3]:
            print 'lat: %f' %map.get('lat')
            print 'long: %f' %map.get('long')
            reporter = Reporter()
            reporter.logFlightdata(map.get('flightid'), map.get('lat'), map.get('long'), map.get('datemessagegenerated') + ' ' + map.get('timemessagegenerated'))
        if transmissiontype == 1:
            print 'callsign: %s' %map.get('callsign')
        #print map
    else:
        # unknown msgtype!
        pass
# listen on socket until user terminates process

class Reporter:
    
    database = 'flightdb'
    user = 'root'
    password = 'Timsql18'
    
    def __init__(self):
        self.db = MySQLdb.connect(host = 'localhost', db = self.database, user = self.user, passwd = self.password)
      
    def logFlightdata(self, flightid, latitude, longitude, time):
        """ store data in mysql """
        print time
        # get database cursor
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO flightdata" + 
                                 " (flightid, latitude, longitude, time) VALUES " +
                                 "(" + str(flightid) + ", " + str(latitude) + ", " + str(longitude) + ", " + time + ")")
        cursor.close()

def main():
    while 1:
        message = tn.read_until('\n')
        message = message.replace("\r\n", "")
        decodeMessage(message)

if __name__ == '__main__':
    main()

