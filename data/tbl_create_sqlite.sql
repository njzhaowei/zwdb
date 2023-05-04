-- MySQL dump 10.13  Distrib 5.7.17, for macos10.12 (x86_64)
--
-- Host: 127.0.0.1    Database: testdb
-- ------------------------------------------------------
-- Server version	5.7.16

--
-- Table structure for table `tbl_create`
--

DROP TABLE IF EXISTS `tbl_create`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tbl_create` (
  `id` int(11) NOT NULL,
  `txt` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`)
);

-- Dump completed on 2020-04-19 19:43:59
