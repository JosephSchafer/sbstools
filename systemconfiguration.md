# Sysconfig #

  * Hardware: Dell Server + connected SBS-1
  * OS: Ubuntu 12,94 LTS + Virtualbox running XP
  * [InstallationTutorial](InstallationTutorial.md)

## XP (running in Virtualbox) ##

  * http://www.kinetic-avionics.co.uk/basestation.php

## Linux ##
  * mysqld
  * sbstools consisting of:
    1. flightobserver.py - daemon reading data from Basestation's port #30003 and writing relevant info to database
    1. flightprocessor.py - processsing (cleaning up, merging, geoclassify) flightdata, running as hourly cronjob
    1. flightdatareducer.py - reduction of flight data, running as daily cronjob