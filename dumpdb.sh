#!/bin/bash
# non-locking, ressource-friendly version of dumping the db
mysqldump -u root -p flightdb --quick --skip-add-locks --skip-lock-tables > /tmp/flightdb.sql
