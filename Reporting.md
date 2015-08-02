# Basics #

  * How to setup the database with user&tables: [DatabaseSetup](DatabaseSetup.md)

## SQL ##

sql-statement for finding all flights not yet geographically classified
```
SELECT callsign, hexident FROM flights INNER JOIN aircrafts ON flights.aircraftid=aircrafts.id 
WHERE flights.overVlbg IS NULL 
AND flights.id in 
(SELECT DISTINCT flightid FROM flightdata 
WHERE time BETWEEN '2007-03-24 14:00' 
AND '2007-03-24 18:00');
```

sql-statement for finding non-existing flights (at least in the database) but where some flightdata entries link to
```
SELECT DISTINCT flightid FROM flightdata WHERE time BETWEEN '2007-04-09 00:00' AND '2007-04-10 00:00' AND flightid NOT IN (SELECT id FROM flights);
```

sql-statement for displaying number of overflights grouped by day
```
SELECT DAYNAME(flights.ts) AS Wochentag, DAYOFMONTH(flights.ts) AS Tag, MONTH(flights.ts) AS Monat, YEAR(flights.ts) AS Jahr, count(*) 
FROM flights WHERE flights.overVlbg=1 
GROUP BY MONTH(flights.ts), DAYOFMONTH(flights.ts);      
```