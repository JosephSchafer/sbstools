#!/bin/bash
# non-locking, ressource-friendly version of dumping the db
# still, should not be run when production data is being collected
mysqldump -u root -p flightdb --quick --skip-add-locks --skip-lock-tables > /tmp/flightdb.sql
