# Description #

Public discussion: http://www.kinetic-avionics.co.uk/forums/viewtopic.php?t=3782

Sometimes aircrafts seem to change their callsign many times during a flight. Unfortunately the Basestation software then introduces a new flightid. As the flightobserver-daemon only inserts new flights when a new aircraft appears on the Basestation screen, there are important flightdata-records not associated with an existing entry in the flights-table.

The following sql statement presents all flightdata not connected to an existing flight:
```
SELECT DISTINCT flightid FROM flightdata WHERE flightid NOT IN (SELECT id FROM flights);
```

## Step 1 ##

  * flightobserver.py creates new entry to table flights when the callsign is set

## Step 2 ##

### Issue ###
Example: Flight ELY008 changed its callsign several times and so many new flight entries were created. => summarize these flights into one and update flightdata + airbornevelocity tables!

```
SELECT distinct flights.id, callsign, aircrafts.hexident, flights.ts FROM flights 
LEFT JOIN flightdata ON flights.id=flightdata.flightid 
LEFT JOIN aircrafts ON flights.aircraftid=aircrafts.id 
WHERE ts BETWEEN '2007-04-08 00:00' AND '2007-04-09 00:00' 
ORDER BY hexident, flights.ts;
...
| 21705 | ELY008   | 738044   | 2007-04-08 12:56:05 |
| 21715 | ELY008 N | 738044   | 2007-04-08 12:57:59 |
| 21716 | ELY008   | 738044   | 2007-04-08 12:58:09 |
| 21718 | ELY008 O | 738044   | 2007-04-08 12:58:19 |
| 21719 | ELY008   | 738044   | 2007-04-08 12:58:29 |
| 21721 | ELY LY00 | 738044   | 2007-04-08 12:59:19 |
| 21722 | ELY008   | 738044   | 2007-04-08 12:59:29 |
| 21725 | ELY0LY00 | 738044   | 2007-04-08 13:00:00 |
| 21726 | FY0LY00  | 738044   | 2007-04-08 13:00:10 |
| 21727 | ELY008   | 738044   | 2007-04-08 13:00:19 |
| 21735 | ELY008 5 | 738044   | 2007-04-08 13:03:30 |
| 21736 | ELY008 1 | 738044   | 2007-04-08 13:03:39 |
| 21738 | ELY008 8 | 738044   | 2007-04-08 13:03:50 |
| 21739 | ELY008 D | 738044   | 2007-04-08 13:04:00 |
| 21740 | ELY008   | 738044   | 2007-04-08 13:04:10 |
| 21741 | QY LY00  | 738044   | 2007-04-08 13:04:40 |
| 21742 | ELY LY00 | 738044   | 2007-04-08 13:04:45 |
| 21743 | ELY008   | 738044   | 2007-04-08 13:05:05 |
| 21744 | ELY0LY00 | 738044   | 2007-04-08 13:05:35 |
| 21747 | ELY008   | 738044   | 2007-04-08 13:05:45 |
| 21754 | ELY00800 | 738044   | 2007-04-08 13:08:51 |
| 21756 | ELY008   | 738044   | 2007-04-08 13:09:01 |
| 21760 | ELY00800 | 738044   | 2007-04-08 13:10:10 |
| 21761 | ELY 0800 | 738044   | 2007-04-08 13:10:10 |
| 21762 | ELY00800 | 738044   | 2007-04-08 13:10:13 |
| 21764 | ELY LY00 | 738044   | 2007-04-08 13:10:21 |
...
```

## Search for callsign-flickering flights ##

based on the assumption that an aircraft does not appear within 45 minutes with a different callsign. i.e. landing and taking off cannot be that quick.

```
SELECT a.ts, aircrafts.hexident, a.callsign, a.id, b.id, timestampdiff(MINUTE, a.ts, b.ts) AS dif 
FROM flights AS a, flights as b INNER JOIN aircrafts ON aircraftid = aircrafts.id 
WHERE a.aircraftid=b.aircraftid AND a.id != b.id AND timestampdiff(MINUTE, a.ts, b.ts) BETWEEN 0 AND 45 
ORDER BY a.ts;          
```