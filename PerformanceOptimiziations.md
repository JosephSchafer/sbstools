# Optimiziations #

  * intelligent usage of field types in flightdb: Is it really necessary to use BIGINT for primary key fields?

## Data flood solution ##

  * every day the database grows by ~100MB

  * reduce flight data by about 90% for flights not crossing Vorarlberg
```
  # SQL for retrieving all flights that have to be reduced
  SELECT id FROM flights WHERE overVlbg=0 AND status=0
  # SQL for retrieving all flightdata for flights
  SELECT id FROM flightdata WHERE flightid=<ID>

  # loop over it and select all ids which can be deleted
  skip = 5
  ids = [...]
  removeableids = []
  for id in ids:
     if id % skip != 0:
        removeableids.append(ids[i])
  if ids[-1] in removeableids:
     removeableids.remove(ids[-1])

  # remove all unnecessary flightdata
  DELETE FROM flightdata WHERE id in (<removeableids>)

  UPDATE flights SET status=1 WHERE id IN (flights)
```