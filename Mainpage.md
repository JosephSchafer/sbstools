# Project packages #

## Package1 ##

**Description**

  * Setup of hardware (see [systemconfiguration](systemconfiguration.md))
  * Software for connecting to Basestation via port and storing all relevant data in a mysql database (_flightobserver.py_)
  * Socket output format - http://www.kinetic-avionics.co.uk/forums/viewtopic.php?t=1402
  * [PerformanceOptimiziations](PerformanceOptimiziations.md)
  * [FlightTableIssue](FlightTableIssue.md)

**Timeframe**

beta version till April 1st, 2007 to start recording.
continuous improvements

## Package2 ##

**Description**

  * Software to filter flight data stored in mysql database by geographical aspects (_flightprocesser.py_).
  * [GISSoftwareEvaluation](GISSoftwareEvaluation.md)
  * Data reduction: remove 90% of flightdata&airbornevelocitymessage for flights not crossing Vorarlberg (_flightdatareducer.py_)
  * ~~remove senseless flightdata! (DELETE FROM flightdata WHERE LONGITUDE <= 3 OR LONGITUDE >=15 OR LATITUDE <= 40 OR LATITUDE >= 50;)~~
  * [TagSBSNonesense](TagSBSNonesense.md): do not remove obviously incorrect data but tag these flights

**Timeframe**

beta version till April 15th, 2007.
continuous improvements

## Package3 ##

**Description**
  * Reporting: software or existing applications to create useful statistics

**Useful websites**

  * Lookup flights by callsign: http://www.flightstats.com/go/FlightStatus/flightStatusByFlight.do
  * Lookup aircraft by hexident: http://www.airframes.org
  * Historical ICAO Callsign's: http://www.airlinecodes.co.uk/callsignlistres.asp

**Technical**
  * [Reporting](Reporting.md)

**Timeframe**

beta version till May 2007
continuous improvements

## Package5 ##

**Description**

  * odometer: flight kilometres over Vorarlberg

**Timeframe**

till end of 2007

## Future ##

  * Usage of an object relational mapper: http://www.sqlalchemy.org http://entwickler.de/zonen/portale/psecom,id,101,online,1033.html

  * Making flightobserver independent of ids provided by Basestation software!