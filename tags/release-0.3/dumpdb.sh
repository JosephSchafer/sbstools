#!/bin/bash
# non-locking, ressource-friendly version of dumping the db
# still, should not be run when production data is being collected
# mysqldump -u root -p flightdb --quick --skip-add-locks --skip-lock-tables > /tmp/flightdb.sql
# 2008-03-03 compress sql-dump on the fly
mysqldump -u root -p flightdb --quick --skip-add-locks --skip-lock-tables | bzip2 -c > /var/lib/mysql/flightdb-full-dump.bz2 
