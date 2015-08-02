
```
# 1. create database flightdb
mysqladmin -u root -p create flightdb

# 2. create user with limited access to the flightdb
mysql -u root -p
GRANT INSERT,UPDATE,SELECT, DELETE ON flightdb.* TO flight@localhost IDENTIFIED BY 'flyaway';

# 3. create database structure (i.e. tables) from file and if necessary fill it with data
mysql -u root -p flightdb < flightdb.sql

```