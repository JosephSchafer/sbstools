-- MySQL dump 10.11
--
-- Host: localhost    Database: flightdb
-- ------------------------------------------------------
-- Server version	5.0.38-Ubuntu_0ubuntu1-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `airbornevelocitymessage`
--

DROP TABLE IF EXISTS `airbornevelocitymessage`;
CREATE TABLE `airbornevelocitymessage` (
  `id` int(10) unsigned NOT NULL auto_increment,
  `flightid` int(10) unsigned default NULL,
  `groundspeed` smallint(5) unsigned default NULL,
  `verticalrate` smallint(6) default NULL,
  `track` smallint(5) unsigned default NULL,
  `time` datetime default NULL,
  PRIMARY KEY  (`id`),
  KEY `flightid` (`flightid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Table structure for table `aircrafts`
--

DROP TABLE IF EXISTS `aircrafts`;
CREATE TABLE `aircrafts` (
  `id` int(10) unsigned NOT NULL auto_increment,
  `hexident` varchar(6) default NULL,
  PRIMARY KEY  (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Table structure for table `flightdata`
--

DROP TABLE IF EXISTS `flightdata`;
CREATE TABLE `flightdata` (
  `id` int(10) unsigned NOT NULL auto_increment,
  `latitude` float default NULL,
  `longitude` float default NULL,
  `flightid` int(10) unsigned default NULL,
  `time` datetime default NULL,
  `time_ms` smallint(5) unsigned default NULL,
  `transmissiontype` tinyint(3) unsigned default NULL,
  `altitude` smallint(5) unsigned NOT NULL,
  PRIMARY KEY  (`id`),
  KEY `time` (`time`),
  KEY `flightid` (`flightid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Table structure for table `flights`
--

DROP TABLE IF EXISTS `flights`;
CREATE TABLE `flights` (
  `id` int(10) unsigned NOT NULL auto_increment,
  `aircraftid` int(11) default NULL,
  `callsign` varchar(20) default NULL,
  `overvlbg` tinyint(1) default NULL,
  `ts` timestamp NOT NULL default CURRENT_TIMESTAMP,
  `state` tinyint(3) unsigned default NULL,
  `mergestate` tinyint(3) unsigned default NULL,
  `gpsaccuracy` tinyint(1) default NULL,
  PRIMARY KEY  (`id`),
  KEY `aircraftid` (`aircraftid`),
  KEY `overvlbg` (`overvlbg`),
  KEY `ts` (`ts`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Table structure for table `kexi__blobs`
--

DROP TABLE IF EXISTS `kexi__blobs`;
CREATE TABLE `kexi__blobs` (
  `o_id` int(10) unsigned NOT NULL auto_increment,
  `o_data` blob,
  `o_name` varchar(200) default NULL,
  `o_caption` varchar(200) default NULL,
  `o_mime` varchar(200) NOT NULL,
  `o_folder_id` int(10) unsigned default NULL,
  PRIMARY KEY  (`o_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Table structure for table `kexi__db`
--

DROP TABLE IF EXISTS `kexi__db`;
CREATE TABLE `kexi__db` (
  `db_property` varchar(32) default NULL,
  `db_value` longtext
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Table structure for table `kexi__fields`
--

DROP TABLE IF EXISTS `kexi__fields`;
CREATE TABLE `kexi__fields` (
  `t_id` int(10) unsigned default NULL,
  `f_type` tinyint(3) unsigned default NULL,
  `f_name` varchar(200) default NULL,
  `f_length` int(11) default NULL,
  `f_precision` int(11) default NULL,
  `f_constraints` int(11) default NULL,
  `f_options` int(11) default NULL,
  `f_default` varchar(200) default NULL,
  `f_order` int(11) default NULL,
  `f_caption` varchar(200) default NULL,
  `f_help` longtext
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Table structure for table `kexi__objectdata`
--

DROP TABLE IF EXISTS `kexi__objectdata`;
CREATE TABLE `kexi__objectdata` (
  `o_id` int(10) unsigned NOT NULL,
  `o_data` blob,
  `o_sub_id` varchar(200) default NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Table structure for table `kexi__objects`
--

DROP TABLE IF EXISTS `kexi__objects`;
CREATE TABLE `kexi__objects` (
  `o_id` int(10) unsigned NOT NULL auto_increment,
  `o_type` tinyint(3) unsigned default NULL,
  `o_name` varchar(200) default NULL,
  `o_caption` varchar(200) default NULL,
  `o_desc` longtext,
  PRIMARY KEY  (`o_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Table structure for table `kexi__parts`
--

DROP TABLE IF EXISTS `kexi__parts`;
CREATE TABLE `kexi__parts` (
  `p_id` int(10) unsigned NOT NULL auto_increment,
  `p_name` varchar(200) default NULL,
  `p_mime` varchar(200) default NULL,
  `p_url` varchar(200) default NULL,
  PRIMARY KEY  (`p_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2007-07-04 13:47:54
