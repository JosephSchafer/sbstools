#nonsense data has to be tagged

# Introduction #

Some flights send incorrect GPS-data, see: http://www.kinetic-avionics.co.uk/forums/viewtopic.php?t=3491
My first approach was to remove GPS data out of a certain range but this was not sufficient.

# Details #

  1. calculate distance between adjacent points (longitude, latitude) http://mathforum.org/library/drmath/view/55417.html
  1. check for GPS coordinates out of range
  1. tag these flights (introduce new field named _gpsaccuracy_)
    * 10 perfect
    * 9,8,7,6
    * 1 inaccurate
    * -1 not classified

# Comments #

The credo is: **Tag but do not remove incorrect data**

  * At least the first flightdata entry seems to be (0, 0)! - delete all (0, 0)-records?