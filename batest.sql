/*
 Navicat MySQL Data Transfer

 Source Server         : Local MYSQL
 Source Server Type    : MySQL
 Source Server Version : 80028
 Source Host           : localhost:3306
 Source Schema         : batest

 Target Server Type    : MySQL
 Target Server Version : 80028
 File Encoding         : 65001

 Date: 09/02/2022 09:55:15
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for Batches
-- ----------------------------

DROP TABLE IF EXISTS `Batches`;
CREATE TABLE `Batches` (
  `id` int NOT NULL AUTO_INCREMENT,
  `mobile_number` varchar(255) DEFAULT NULL,
  `product` varchar(255) DEFAULT NULL,
  `amount` varchar(255) DEFAULT NULL,
  `reference` varchar(255) DEFAULT NULL,
  `hasBeenProccessed` tinyint(1) DEFAULT NULL,
  `telco` varchar(255) DEFAULT NULL,
  `batchId` varchar(255) DEFAULT NULL,
  `user_id` int DEFAULT NULL,
  `message` varchar(255) DEFAULT NULL,
  `createdAt` datetime NOT NULL,
  `updatedAt` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `reference` (`reference`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----------------------------
-- Records of Batches
-- ----------------------------
BEGIN;
INSERT INTO `Batches` (`id`, `mobile_number`, `product`, `amount`, `reference`, `hasBeenProccessed`, `telco`, `batchId`, `user_id`, `message`, `createdAt`, `updatedAt`) VALUES (5, '08107176089', NULL, '2', '297589474', 1, NULL, '122955', 13, 'RECHARGE_COMPLETE', '2022-02-07 13:35:17', '2022-02-07 14:04:04');
INSERT INTO `Batches` (`id`, `mobile_number`, `product`, `amount`, `reference`, `hasBeenProccessed`, `telco`, `batchId`, `user_id`, `message`, `createdAt`, `updatedAt`) VALUES (6, '08107173389', NULL, '2', '329829813', 1, NULL, '122955', 13, 'RECHARGE_COMPLETE', '2022-02-07 13:35:17', '2022-02-07 14:04:07');
INSERT INTO `Batches` (`id`, `mobile_number`, `product`, `amount`, `reference`, `hasBeenProccessed`, `telco`, `batchId`, `user_id`, `message`, `createdAt`, `updatedAt`) VALUES (7, '08107176089', NULL, '2', '683756014', 0, NULL, '699883', 13, NULL, '2022-02-07 20:14:23', '2022-02-07 20:14:23');
INSERT INTO `Batches` (`id`, `mobile_number`, `product`, `amount`, `reference`, `hasBeenProccessed`, `telco`, `batchId`, `user_id`, `message`, `createdAt`, `updatedAt`) VALUES (8, '08107173389', NULL, '2', '112293858', 0, NULL, '699883', 13, NULL, '2022-02-07 20:14:23', '2022-02-07 20:14:23');
COMMIT;

-- ----------------------------
-- Table structure for Insufficients
-- ----------------------------
DROP TABLE IF EXISTS `Insufficients`;
CREATE TABLE `Insufficients` (
  `id` int NOT NULL AUTO_INCREMENT,
  `product_id` varchar(255) DEFAULT NULL,
  `send_sms` varchar(255) DEFAULT NULL,
  `sms_text` varchar(255) DEFAULT NULL,
  `customer_reference` varchar(255) DEFAULT NULL,
  `type` varchar(255) DEFAULT NULL,
  `createdAt` datetime NOT NULL,
  `updatedAt` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----------------------------
-- Records of Insufficients
-- ----------------------------
BEGIN;
INSERT INTO `Insufficients` (`id`, `product_id`, `send_sms`, `sms_text`, `customer_reference`, `type`, `createdAt`, `updatedAt`) VALUES (1, 'D-MFIN-5-100MB', '0', '0', 'BILL_128231988', 'DATAE', '2022-02-02 18:15:47', '2022-02-02 18:15:47');
INSERT INTO `Insufficients` (`id`, `product_id`, `send_sms`, `sms_text`, `customer_reference`, `type`, `createdAt`, `updatedAt`) VALUES (2, 'D-MFIN-5-100MB', '0', '0', 'BILL_792284201', 'DATAE', '2022-02-02 18:16:04', '2022-02-02 18:16:04');
INSERT INTO `Insufficients` (`id`, `product_id`, `send_sms`, `sms_text`, `customer_reference`, `type`, `createdAt`, `updatedAt`) VALUES (3, 'D-MFIN-5-100MB', '0', '0', 'BILL_275215175', 'DATA', '2022-02-02 18:17:49', '2022-02-02 18:17:49');
INSERT INTO `Insufficients` (`id`, `product_id`, `send_sms`, `sms_text`, `customer_reference`, `type`, `createdAt`, `updatedAt`) VALUES (4, 'D-MFIN-5-100MB', '0', '0', 'BILL_788020054', 'DATA', '2022-02-02 18:19:49', '2022-02-02 18:19:49');
INSERT INTO `Insufficients` (`id`, `product_id`, `send_sms`, `sms_text`, `customer_reference`, `type`, `createdAt`, `updatedAt`) VALUES (5, 'D-MFIN-5-100MB', '0', '0', 'BILL_798195995', 'DATA', '2022-02-02 18:20:23', '2022-02-02 18:20:23');
INSERT INTO `Insufficients` (`id`, `product_id`, `send_sms`, `sms_text`, `customer_reference`, `type`, `createdAt`, `updatedAt`) VALUES (6, 'MFIN-1-OR', '0', '0', 'BILL_undefined', 'AIRTIME', '2022-02-07 20:04:23', '2022-02-07 20:04:23');
COMMIT;

-- ----------------------------
-- Table structure for PrimeAirtimes
-- ----------------------------
DROP TABLE IF EXISTS `PrimeAirtimes`;
CREATE TABLE `PrimeAirtimes` (
  `id` int NOT NULL AUTO_INCREMENT,
  `stsatus` varchar(255) DEFAULT NULL,
  `message` varchar(255) DEFAULT NULL,
  `coder` varchar(255) DEFAULT NULL,
  `paid_amount` varchar(255) DEFAULT NULL,
  `paid_currency` varchar(255) DEFAULT NULL,
  `topup_currency` varchar(255) DEFAULT NULL,
  `target` varchar(255) DEFAULT NULL,
  `product_id` varchar(255) DEFAULT NULL,
  `time` varchar(255) DEFAULT NULL,
  `country` varchar(255) DEFAULT NULL,
  `operator_name` varchar(255) DEFAULT NULL,
  `completed_in` varchar(255) DEFAULT NULL,
  `customer_reference` varchar(255) DEFAULT NULL,
  `api_transactionid` varchar(255) DEFAULT NULL,
  `pin_based` varchar(255) DEFAULT NULL,
  `user_id` varchar(255) DEFAULT NULL,
  `createdAt` datetime NOT NULL,
  `updatedAt` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=20 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----------------------------
-- Records of PrimeAirtimes
-- ----------------------------
BEGIN;
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (1, NULL, 'Operation Successful, Recharge created, Reference : cf0cf970-8302-11ec-8d7d-3dee61a2c1db', NULL, '1', 'NGN', 'NGN', '2348069308371', 'MFIN-5-OR', '2022-02-01T02:01:05.254Z', 'Nigeria', 'MTN', '1319', 'BILL_534531915', '2022020103010467202258207', '0', NULL, '2022-02-01 02:01:05', '2022-02-01 02:01:05');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (2, NULL, 'Operation Successful, Recharge created, Reference : bf36d110-83b6-11ec-9aa3-17b10c2533a9', NULL, '2', 'NGN', 'NGN', '2348107176019', 'MFIN-5-OR', '2022-02-01T23:29:08.085Z', 'Nigeria', 'MTN', '1327', 'BILL_534667092', '2022020200290751401788662', '0', NULL, '2022-02-01 23:29:08', '2022-02-01 23:29:08');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (3, NULL, 'Operation Successful, Recharge created, Reference : c0926970-83b6-11ec-91fd-d93e823085f1', NULL, '2', 'NGN', 'NGN', '2347039499403', 'MFIN-2-OR', '2022-02-01T23:29:09.400Z', 'Nigeria', '9mobile', '351', 'BILL_174174507', '0202002908014012', '0', NULL, '2022-02-01 23:29:09', '2022-02-01 23:29:09');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (4, NULL, 'Operation Successful, Recharge created, Reference : 28dc1ad0-83b7-11ec-ba5e-59ad18a259b6', NULL, '2', 'NGN', 'NGN', '2348107176019', 'MFIN-5-OR', '2022-02-01T23:32:04.595Z', 'Nigeria', 'MTN', '598', 'BILL_570767249', '2022020200320410503000688', '0', NULL, '2022-02-01 23:32:04', '2022-02-01 23:32:04');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (5, NULL, 'Operation Successful, Recharge created, Reference : 2a4017a0-83b7-11ec-b282-774deb9c0bf3', NULL, '2', 'NGN', 'NGN', '2347039499403', 'MFIN-2-OR', '2022-02-01T23:32:07.521Z', 'Nigeria', '9mobile', '1157', 'BILL_538703786', '0202003207014357', '0', NULL, '2022-02-01 23:32:07', '2022-02-01 23:32:07');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (6, NULL, 'Operation Successful, Recharge created, Reference : 88acc5a0-878f-11ec-a2b9-13dd6833549f', NULL, '2', 'NGN', 'NGN', '2348107176019', 'MFIN-5-OR', '2022-02-06T20:58:29.883Z', 'Nigeria', 'MTN', '352', 'BILL_780097228', '2022020621582945801542749', '0', NULL, '2022-02-06 20:58:30', '2022-02-06 20:58:30');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (7, NULL, 'Operation Successful, Recharge created, Reference : bb988ca0-8790-11ec-9c85-d32be8741752', NULL, '2', 'NGN', 'NGN', '2348107176019', 'MFIN-5-OR', '2022-02-06T21:07:05.712Z', 'Nigeria', 'MTN', '1262', 'BILL_395931343', '2022020622070503706713159', '0', NULL, '2022-02-06 21:07:05', '2022-02-06 21:07:05');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (8, NULL, 'Operation Successful, Recharge created, Reference : bc99f710-8790-11ec-994c-c58a3ccf89db', NULL, '2', 'NGN', 'NGN', '2347039499403', 'MFIN-2-OR', '2022-02-06T21:07:07.123Z', 'Nigeria', '9mobile', '981', 'BILL_980590994', '0206220706024484', '0', NULL, '2022-02-06 21:07:07', '2022-02-06 21:07:07');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (9, NULL, 'Operation Successful, Recharge created, Reference : 50a0fb80-879f-11ec-87dd-77cca5bf20a7', NULL, '1', 'NGN', 'NGN', '2348107176019', 'MFIN-5-OR', '2022-02-06T22:51:28.821Z', 'Nigeria', 'MTN', '1360', 'BILL_812237128', '2022020623512827806768966', '0', NULL, '2022-02-06 22:51:28', '2022-02-06 22:51:28');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (10, NULL, 'Operation Successful, Recharge created, Reference : c8bcdb60-87a0-11ec-88cb-bda302011d5e', NULL, '1', 'NGN', 'NGN', '2348107176019', 'MFIN-5-OR', '2022-02-06T23:01:59.744Z', 'Nigeria', 'MTN', '1288', 'BILL_559230811', '2022020700015931108555872', '0', NULL, '2022-02-06 23:01:59', '2022-02-06 23:01:59');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (11, NULL, 'Operation Successful, Recharge created, Reference : 2fc4e360-8801-11ec-b322-171b4bdf9967', NULL, '1', 'NGN', 'NGN', '2348107176019', 'MFIN-5-OR', '2022-02-07T10:32:03.613Z', 'Nigeria', 'MTN', '624', 'BILL_748879540', '2022020711320315307952414', '0', NULL, '2022-02-07 10:32:03', '2022-02-07 10:32:03');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (12, NULL, 'Operation Successful, Recharge created, Reference : 56d05a10-8802-11ec-95c2-5b50256508f7', NULL, '1', 'NGN', 'NGN', '2348107176019', 'MFIN-5-OR', '2022-02-07T10:40:18.598Z', 'Nigeria', 'MTN', '610', 'BILL_283204954', '2022020711401816708882850', '0', NULL, '2022-02-07 10:40:18', '2022-02-07 10:40:18');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (13, NULL, 'Operation Successful, Recharge created, Reference : 0e370910-8803-11ec-a528-4369281f8e6e', NULL, '1', 'NGN', 'NGN', '2348107176019', 'MFIN-5-OR', '2022-02-07T10:45:26.013Z', 'Nigeria', 'MTN', '1280', 'BILL_541342836', '2022020711452563106089376', '0', NULL, '2022-02-07 10:45:25', '2022-02-07 10:45:25');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (14, NULL, 'Operation Successful, Recharge created, Reference : caf19210-8816-11ec-8328-d917b11ccb41', NULL, '1', 'NGN', 'NGN', '2348107176019', 'MFIN-5-OR', '2022-02-07T13:06:43.076Z', 'Nigeria', 'MTN', '315', 'BILL_213568087', '2022020714064267108020374', '0', NULL, '2022-02-07 13:06:42', '2022-02-07 13:06:42');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (15, NULL, 'Operation Successful, Recharge created, Reference : e0dd1d30-8819-11ec-8836-11f0504c4868', NULL, '100', 'NGN', 'NGN', '2348107176019', 'D-MFIN-5-100MB', '2022-02-07T13:28:48.943Z', 'Nigeria', 'MTN', '1154', 'BILL_512912106', '2022020714284808105053910', '0', NULL, '2022-02-07 13:28:48', '2022-02-07 13:28:48');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (16, NULL, 'Operation Successful, Recharge created, Reference : ce291cc0-881e-11ec-aa84-2f5cfb823d7e', NULL, '2', 'NGN', 'NGN', '2348107176019', 'MFIN-5-OR', '2022-02-07T14:04:04.828Z', 'Nigeria', 'MTN', '667', 'BILL_297589474', '2022020715040442202245094', '0', NULL, '2022-02-07 14:04:04', '2022-02-07 14:04:04');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (17, NULL, 'Operation Successful, Recharge created, Reference : cfc7b190-881e-11ec-af6f-adcac86cbcfa', NULL, '2', 'NGN', 'NGN', '2347039499403', 'MFIN-2-OR', '2022-02-07T14:04:07.186Z', 'Nigeria', '9mobile', '335', 'BILL_329829813', '0207150406065722', '0', NULL, '2022-02-07 14:04:07', '2022-02-07 14:04:07');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (18, NULL, 'Operation Successful, Recharge created, Reference : 07a8bc30-8851-11ec-aa84-2f5cfb823d7e', NULL, '1', 'NGN', 'NGN', '2348107176019', 'MFIN-5-OR', '2022-02-07T20:03:36.010Z', 'Nigeria', 'MTN', '569', 'BILL_434419527', '2022020721033579007487569', '0', NULL, '2022-02-07 20:03:36', '2022-02-07 20:03:36');
INSERT INTO `PrimeAirtimes` (`id`, `stsatus`, `message`, `coder`, `paid_amount`, `paid_currency`, `topup_currency`, `target`, `product_id`, `time`, `country`, `operator_name`, `completed_in`, `customer_reference`, `api_transactionid`, `pin_based`, `user_id`, `createdAt`, `updatedAt`) VALUES (19, NULL, 'Operation Successful, Recharge created, Reference : bd6fec00-8851-11ec-bdf2-21ea1c9869ee', NULL, '1.04', 'NGN', 'NGN', '2348075812552', 'MFIN-6-OR', '2022-02-07T20:08:40.798Z', 'Nigeria', 'Globacom', '364', 'BILL_294504858', '2022020721070523603471315', '0', NULL, '2022-02-07 20:08:40', '2022-02-07 20:08:40');
COMMIT;

-- ----------------------------
-- Table structure for Primeauths
-- ----------------------------
DROP TABLE IF EXISTS `Primeauths`;
CREATE TABLE `Primeauths` (
  `id` int NOT NULL AUTO_INCREMENT,
  `token` varchar(255) DEFAULT NULL,
  `isActive` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----------------------------
-- Records of Primeauths
-- ----------------------------
BEGIN;
INSERT INTO `Primeauths` (`id`, `token`, `isActive`) VALUES (1, 'eyJN0', 1);
COMMIT;

-- ----------------------------
-- Table structure for Primedata
-- ----------------------------
DROP TABLE IF EXISTS `Primedata`;
CREATE TABLE `Primedata` (
  `id` int NOT NULL AUTO_INCREMENT,
  `product_id` varchar(255) DEFAULT NULL,
  `validity` varchar(255) DEFAULT NULL,
  `topup_value` varchar(255) DEFAULT NULL,
  `price` varchar(255) DEFAULT NULL,
  `data_amount` varchar(255) DEFAULT NULL,
  `telco` varchar(255) DEFAULT NULL,
  `planId` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=429 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----------------------------
-- Records of Primedata
-- ----------------------------
BEGIN;
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (1, 'D-MFIN-5-100MB', '1 day', '100', '100', '100', 'MTN', '974');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (2, 'D-MFIN-5-750MB', '14 days', '500', '500', '750', 'MTN', '226');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (3, 'D-MFIN-5-1.5GB', '30 days', '1000', '1000', '1500', 'MTN', '462');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (4, 'D-MFIN-5-20GB', '30 days', '5000', '5000', '20000', 'MTN', '742');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (5, 'D-MFIN-5-40GB', '30 days', '10000', '10000', '40000', 'MTN', '996');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (6, 'D-MFIN-5-75GB2M', '60 days', '20000', '20000', '75000', 'MTN', '191');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (7, 'D-MFIN-5-150GB3M', '90 days', '50000', '50000', '150000', 'MTN', '369');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (8, 'D-MFIN-5-250GB3M', '90 days', '75000', '75000', '250000', 'MTN', '302');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (9, 'D-MFIN-5-200MB', '2 days', '200', '200', '200', 'MTN', '212');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (10, 'D-MFIN-5-350MB', '7 days', '300', '300', '350', 'MTN', '199');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (11, 'D-MFIN-5-75GB', '30 days', '15000', '15000', '75000', 'MTN', '229');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (12, 'D-MFIN-5-120GB2M', '60 days', '30000', '30000', '120000', 'MTN', '803');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (13, 'D-MFIN-5-3GB1M', '30 days', '1500', '1500', '3000', 'MTN', '441');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (14, 'D-MFIN-5-25GB1M', '30 days', '6000', '6000', '25000', 'MTN', '143');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (15, 'D-MFIN-5-2GB1M', '30 days', '1200', '1200', '2000', 'MTN', '207');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (16, 'D-MFIN-5-6GB', '7 days', '1500', '1500', '6000', 'MTN', '815');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (17, 'D-MFIN-5-6GB1M', '30 days', '2500', '2500', '6000', 'MTN', '609');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (18, 'D-MFIN-5-10GB1M', '30 days', '3000', '3000', '10000', 'MTN', '823');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (19, 'D-MFIN-5-110GB1M', '30 days', '20000', '20000', '110000', 'MTN', '456');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (20, 'D-MFIN-5-30GB1M', '30 days', '13500', '13500', '30000', 'MTN', '360');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (21, 'D-MFIN-5-90GB1M', '30 days', '40000', '40000', '90000', 'MTN', '222');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (22, 'D-MFIN-5-150GB3M', '90 days', '50000', '50000', '150000', 'MTN', '734');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (23, 'D-MFIN-5-325GB6M', '180 days', '100000', '100000', '325000', 'MTN', '175');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (24, 'D-MFIN-5-1T1Y', '1 year', '100000', '100000', '1000000', 'MTN', '643');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (25, 'D-MFIN-5-1.5T1Y', '1 year', '450000', '450000', '1500000', 'MTN', '690');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (26, 'D-MFIN-5-400GB1Y', '1 year', '120000', '120000', '400000', 'MTN', '805');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (27, 'D-MFIN-5-1GB1W', '7 days', '500', '500', '1000', 'MTN', '138');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (28, 'D-MFIN-5-2.5GB2D', '2 days', '500', '500', '2500', 'MTN', '261');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (29, 'D-MFIN-5-30GB2M', '60 days', '8000', '8000', '30000', 'MTN', '544');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (30, 'D-MFIN-5-12GB1M', '30 days', '3500', '3500', '12000', 'MTN', '825');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (31, 'D-MFIN-5-4.5GB1M', '30 days', '2000', '2000', '4500', 'MTN', '389');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (32, 'D-MFIN-5-750MB', '7 days Xtradata 500', '500', '500', '750', 'MTN', '344');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (33, 'D-MFIN-5-300MB', '7 days, Xtradata 300', '300', '300', '300', 'MTN', '177');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (34, 'D-MFIN-5-1.5GB', '30 days Xtradata 1000', '1000', '1000', '1500', 'MTN', '793');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (35, 'D-MFIN-5-4.5GB', '30 days Xtradata 2000', '2000', '2000', '4500', 'MTN', '131');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (36, 'D-MFIN-5-15GB', '30 days Xtradata 5000', '5000', '5000', '15000', 'MTN', '810');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (37, 'D-MFIN-5-30GB', '30 days Xtradata 10000', '10000', '10000', '30000', 'MTN', '820');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (38, 'D-MFIN-5-50GB', '30 days Xtradata 15000', '15000', '15000', '50000', 'MTN', '590');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (39, 'D-MFIN-5-70GB', '30 days Xtradata 20000', '20000', '20000', '70000', 'MTN', '594');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (40, 'D-MFIN-1-40MB', '1 day', '50', '50.49', '40', 'Airtel', '954');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (41, 'D-MFIN-1-100MB', '1 day', '100', '99.99', '100', 'Airtel', '369');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (42, 'D-MFIN-1-200MB', '3 days', '200', '201.02', '200', 'Airtel', '122');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (43, 'D-MFIN-1-350MB', '7 days', '300', '302.01', '350', 'Airtel', '704');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (44, 'D-MFIN-1-750MB', '14 days', '500', '503.99', '750', 'Airtel', '489');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (45, 'D-MFIN-1-1.5GB', '30 days', '1000', '1008.99', '1500', 'Airtel', '398');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (46, 'D-MFIN-1-3GB', '30 days', '1500', '1514', '3000', 'Airtel', '449');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (47, 'D-MFIN-1-6GB', '30 days', '2500', '2524', '6000', 'Airtel', '565');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (48, 'D-MFIN-1-11GB', '30 days', '4000', '4039', '11000', 'Airtel', '494');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (49, 'D-MFIN-1-20GB', '30 days', '5000', '5048.99', '20000', 'Airtel', '825');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (50, 'D-MFIN-1-40GB', '30 days', '10000', '10098.99', '40000', 'Airtel', '688');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (51, 'D-MFIN-1-75GB', '30 days', '15000', '15148.99', '75000', 'Airtel', '333');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (52, 'D-MFIN-1-8GB', '30 days', '3000', '3029.01', '8000', 'Airtel', '674');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (53, 'D-MFIN-1-120GB', '30 days', '20000', '20199.01', '120000', 'Airtel', '810');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (54, 'D-MFIN-1-1GB1D', '1 day', '300', '302.02', '1000', 'Airtel', '442');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (55, 'D-MFIN-1-2GB1D', '1 day', '500', '504.02', '2000', 'Airtel', '836');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (56, 'D-MFIN-1-6GB1W', '7 days', '1500', '1514.02', '6000', 'Airtel', '192');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (57, 'D-MFIN-1-2GB1M', '30 days', '1200', '1210.99', '2000', 'Airtel', '712');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (58, 'D-MFIN-1-4.5GB', '30 days', '2000', '2018.99', '4500', 'Airtel', '367');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (59, 'D-MFIN-1-25GB1M', '30 days', '8000', '8079.01', '25000', 'Airtel', '356');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (60, 'D-MFIN-1-200GB1M', '30 days', '30000', '30299.01', '200000', 'Airtel', '835');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (61, 'D-MFIN-1-280GB1M', '30 days', '36000', '36359.01', '280000', 'Airtel', '513');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (62, 'D-MFIN-1-400GB3M', '90 days', '50000', '50499.01', '400000', 'Airtel', '379');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (63, 'D-MFIN-1-500GB4M', '120 days', '60000', '60599.01', '500000', 'Airtel', '802');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (64, 'D-MFIN-1-1TB1Y', '365 days', '100000', '100999.01', '1000000', 'Airtel', '777');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (65, 'D-MFIN-2-1.5GB', '30 days', '1000', '1000', '1500', '9mobile', '909');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (66, 'D-MFIN-2-2GB', '30 days', '1200', '1200', '2000', '9mobile', '231');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (67, 'D-MFIN-2-4.5GB', '30 days', '2000', '2000', '4500', '9mobile', '974');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (68, 'D-MFIN-2-40GB', '30 days', '10000', '10000', '40000', '9mobile', '965');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (69, 'D-MFIN-2-165GB', '180 days', '50000', '50000', '165000', '9mobile', '190');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (70, 'D-MFIN-2-365GB', '365 days', '100000', '100000', '365000', '9mobile', '948');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (71, 'D-MFIN-2-11GB', '30 days', '4000', '4000', '11000', '9mobile', '575');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (72, 'D-MFIN-2-25MB', '1 day', '50', '50', '25', '9mobile', '465');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (73, 'D-MFIN-2-100MB', '1 day', '100', '100', '100', '9mobile', '271');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (74, 'D-MFIN-2-650MB', '1 day', '200', '200', '650', '9mobile', '127');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (75, 'D-MFIN-2-15GB', '30 days', '5000', '5000', '15000', '9mobile', '309');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (76, 'D-MFIN-2-100GB', '100 days', '84992', '84992', '100000', '9mobile', '775');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (77, 'D-MFIN-2-7GB1W', '7 days', '1500', '1500', '7000', '9mobile', '171');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (78, 'D-MFIN-2-5M', '30 days', '500', '500', '500', '9mobile', '496');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (79, 'D-MFIN-2-75M30', '30 days', '15000', '15000', '75000', '9mobile', '496');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (80, 'D-MFIN-2-75M', '90 days', '25000', '25000', '75000', '9mobile', '640');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (81, 'D-MFIN-6-150MB1D', '1 day', '100', '104', '150', 'Globacom', '493');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (82, 'D-MFIN-6-350MB2D', '2 days', '200', '208', '350', 'Globacom', '505');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (83, 'D-MFIN-6-1.35GB2W', '14 days', '500', '520', '1350', 'Globacom', '691');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (84, 'D-MFIN-6-2.9GB', '30 days', '1000', '1040', '2900', 'Globacom', '317');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (85, 'D-MFIN-6-5.8GB', '30 days', '2000', '2080', '5800', 'Globacom', '358');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (86, 'D-MFIN-6-7.7GB', '30 days', '2500', '2600', '7700', 'Globacom', '492');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (87, 'D-MFIN-6-10GB', '30 days', '3000', '3120', '10000', 'Globacom', '339');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (88, 'D-MFIN-6-13.25GB', '30 days', '4000', '4160', '13250', 'Globacom', '280');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (89, 'D-MFIN-6-18.25GB', '30 days', '5000', '5200', '18250', 'Globacom', '846');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (90, 'D-MFIN-6-29.5GB', '30 days', '8000', '8320', '29500', 'Globacom', '577');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (91, 'D-MFIN-6-50GB', '30 days', '10000', '10400', '50000', 'Globacom', '810');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (92, 'D-MFIN-6-93GB', '30 days', '15000', '15600', '93000', 'Globacom', '303');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (93, 'D-MFIN-6-119GB', '30 days', '18000', '18720', '119000', 'Globacom', '108');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (94, 'D-MFIN-6-50MB1D', '1 day', '50', '52', '50', 'Globacom', '719');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (95, 'D-MFIN-6-138GB', '30 days', '20000', '20800', '138000', 'Globacom', '682');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (96, 'D-MFIN-6-250MB1N', '1 night', '25', '26', '250', 'Globacom', '836');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (97, 'D-MFIN-6-500MB1N', '1 night', '50', '52', '500', 'Globacom', '558');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (98, 'D-MFIN-6-1GB5N', '5 nights', '100', '104', '1000', 'Globacom', '152');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (99, 'D-MFIN-6-7GB', '7 days', '1500', '1560', '7000', 'Globacom', '569');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (100, 'D-MFIN-6-4.1GB', '30 days', '1500', '1560', '4100', 'Globacom', '971');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (101, 'D-MFIN-6-1.25GB1SUN', '1 day (Sun)', '200', '208', '1250', 'Globacom', '278');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (102, 'D-MFIN-6-225GBOT', '30 days', '30000', '31200', '225000', 'Globacom', '487');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (103, 'D-MFIN-6-300GBOT', '30 days', '36000', '37440', '300000', 'Globacom', '481');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (104, 'D-MFIN-6-425GBOT', '90 days', '50000', '52000', '425000', 'Globacom', '610');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (105, 'D-MFIN-6-525GBOT', '120 days', '60000', '62400', '525000', 'Globacom', '149');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (106, 'D-MFIN-6-675GBOT', '120 days', '75000', '78000', '675000', 'Globacom', '890');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (107, 'D-MFIN-6-1TBOT', '365 days', '100000', '104000', '1000000', 'Globacom', '961');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (108, 'D-MFIN-5-100MB', '1 day', '100', '100', '100', 'MTN', '974');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (109, 'D-MFIN-5-750MB', '14 days', '500', '500', '750', 'MTN', '226');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (110, 'D-MFIN-5-1.5GB', '30 days', '1000', '1000', '1500', 'MTN', '462');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (111, 'D-MFIN-5-20GB', '30 days', '5000', '5000', '20000', 'MTN', '742');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (112, 'D-MFIN-5-40GB', '30 days', '10000', '10000', '40000', 'MTN', '996');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (113, 'D-MFIN-5-75GB2M', '60 days', '20000', '20000', '75000', 'MTN', '191');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (114, 'D-MFIN-5-150GB3M', '90 days', '50000', '50000', '150000', 'MTN', '369');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (115, 'D-MFIN-5-250GB3M', '90 days', '75000', '75000', '250000', 'MTN', '302');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (116, 'D-MFIN-5-200MB', '2 days', '200', '200', '200', 'MTN', '212');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (117, 'D-MFIN-5-350MB', '7 days', '300', '300', '350', 'MTN', '199');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (118, 'D-MFIN-5-75GB', '30 days', '15000', '15000', '75000', 'MTN', '229');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (119, 'D-MFIN-5-120GB2M', '60 days', '30000', '30000', '120000', 'MTN', '803');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (120, 'D-MFIN-5-3GB1M', '30 days', '1500', '1500', '3000', 'MTN', '441');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (121, 'D-MFIN-5-25GB1M', '30 days', '6000', '6000', '25000', 'MTN', '143');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (122, 'D-MFIN-5-2GB1M', '30 days', '1200', '1200', '2000', 'MTN', '207');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (123, 'D-MFIN-5-6GB', '7 days', '1500', '1500', '6000', 'MTN', '815');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (124, 'D-MFIN-5-6GB1M', '30 days', '2500', '2500', '6000', 'MTN', '609');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (125, 'D-MFIN-5-10GB1M', '30 days', '3000', '3000', '10000', 'MTN', '823');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (126, 'D-MFIN-5-110GB1M', '30 days', '20000', '20000', '110000', 'MTN', '456');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (127, 'D-MFIN-5-30GB1M', '30 days', '13500', '13500', '30000', 'MTN', '360');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (128, 'D-MFIN-5-90GB1M', '30 days', '40000', '40000', '90000', 'MTN', '222');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (129, 'D-MFIN-5-150GB3M', '90 days', '50000', '50000', '150000', 'MTN', '734');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (130, 'D-MFIN-5-325GB6M', '180 days', '100000', '100000', '325000', 'MTN', '175');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (131, 'D-MFIN-5-1T1Y', '1 year', '100000', '100000', '1000000', 'MTN', '643');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (132, 'D-MFIN-5-1.5T1Y', '1 year', '450000', '450000', '1500000', 'MTN', '690');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (133, 'D-MFIN-5-400GB1Y', '1 year', '120000', '120000', '400000', 'MTN', '805');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (134, 'D-MFIN-5-1GB1W', '7 days', '500', '500', '1000', 'MTN', '138');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (135, 'D-MFIN-5-2.5GB2D', '2 days', '500', '500', '2500', 'MTN', '261');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (136, 'D-MFIN-5-30GB2M', '60 days', '8000', '8000', '30000', 'MTN', '544');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (137, 'D-MFIN-5-12GB1M', '30 days', '3500', '3500', '12000', 'MTN', '825');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (138, 'D-MFIN-5-4.5GB1M', '30 days', '2000', '2000', '4500', 'MTN', '389');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (139, 'D-MFIN-5-750MB', '7 days Xtradata 500', '500', '500', '750', 'MTN', '344');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (140, 'D-MFIN-5-300MB', '7 days, Xtradata 300', '300', '300', '300', 'MTN', '177');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (141, 'D-MFIN-5-1.5GB', '30 days Xtradata 1000', '1000', '1000', '1500', 'MTN', '793');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (142, 'D-MFIN-5-4.5GB', '30 days Xtradata 2000', '2000', '2000', '4500', 'MTN', '131');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (143, 'D-MFIN-5-15GB', '30 days Xtradata 5000', '5000', '5000', '15000', 'MTN', '810');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (144, 'D-MFIN-5-30GB', '30 days Xtradata 10000', '10000', '10000', '30000', 'MTN', '820');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (145, 'D-MFIN-5-50GB', '30 days Xtradata 15000', '15000', '15000', '50000', 'MTN', '590');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (146, 'D-MFIN-5-70GB', '30 days Xtradata 20000', '20000', '20000', '70000', 'MTN', '594');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (147, 'D-MFIN-1-40MB', '1 day', '50', '50.49', '40', 'Airtel', '954');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (148, 'D-MFIN-1-100MB', '1 day', '100', '99.99', '100', 'Airtel', '369');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (149, 'D-MFIN-1-200MB', '3 days', '200', '201.02', '200', 'Airtel', '122');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (150, 'D-MFIN-1-350MB', '7 days', '300', '302.01', '350', 'Airtel', '704');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (151, 'D-MFIN-1-750MB', '14 days', '500', '503.99', '750', 'Airtel', '489');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (152, 'D-MFIN-1-1.5GB', '30 days', '1000', '1008.99', '1500', 'Airtel', '398');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (153, 'D-MFIN-1-3GB', '30 days', '1500', '1514', '3000', 'Airtel', '449');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (154, 'D-MFIN-1-6GB', '30 days', '2500', '2524', '6000', 'Airtel', '565');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (155, 'D-MFIN-1-11GB', '30 days', '4000', '4039', '11000', 'Airtel', '494');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (156, 'D-MFIN-1-20GB', '30 days', '5000', '5048.99', '20000', 'Airtel', '825');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (157, 'D-MFIN-1-40GB', '30 days', '10000', '10098.99', '40000', 'Airtel', '688');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (158, 'D-MFIN-1-75GB', '30 days', '15000', '15148.99', '75000', 'Airtel', '333');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (159, 'D-MFIN-1-8GB', '30 days', '3000', '3029.01', '8000', 'Airtel', '674');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (160, 'D-MFIN-1-120GB', '30 days', '20000', '20199.01', '120000', 'Airtel', '810');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (161, 'D-MFIN-1-1GB1D', '1 day', '300', '302.02', '1000', 'Airtel', '442');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (162, 'D-MFIN-1-2GB1D', '1 day', '500', '504.02', '2000', 'Airtel', '836');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (163, 'D-MFIN-1-6GB1W', '7 days', '1500', '1514.02', '6000', 'Airtel', '192');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (164, 'D-MFIN-1-2GB1M', '30 days', '1200', '1210.99', '2000', 'Airtel', '712');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (165, 'D-MFIN-1-4.5GB', '30 days', '2000', '2018.99', '4500', 'Airtel', '367');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (166, 'D-MFIN-1-25GB1M', '30 days', '8000', '8079.01', '25000', 'Airtel', '356');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (167, 'D-MFIN-1-200GB1M', '30 days', '30000', '30299.01', '200000', 'Airtel', '835');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (168, 'D-MFIN-1-280GB1M', '30 days', '36000', '36359.01', '280000', 'Airtel', '513');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (169, 'D-MFIN-1-400GB3M', '90 days', '50000', '50499.01', '400000', 'Airtel', '379');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (170, 'D-MFIN-1-500GB4M', '120 days', '60000', '60599.01', '500000', 'Airtel', '802');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (171, 'D-MFIN-1-1TB1Y', '365 days', '100000', '100999.01', '1000000', 'Airtel', '777');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (172, 'D-MFIN-2-1.5GB', '30 days', '1000', '1000', '1500', '9mobile', '909');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (173, 'D-MFIN-2-2GB', '30 days', '1200', '1200', '2000', '9mobile', '231');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (174, 'D-MFIN-2-4.5GB', '30 days', '2000', '2000', '4500', '9mobile', '974');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (175, 'D-MFIN-2-40GB', '30 days', '10000', '10000', '40000', '9mobile', '965');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (176, 'D-MFIN-2-165GB', '180 days', '50000', '50000', '165000', '9mobile', '190');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (177, 'D-MFIN-2-365GB', '365 days', '100000', '100000', '365000', '9mobile', '948');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (178, 'D-MFIN-2-11GB', '30 days', '4000', '4000', '11000', '9mobile', '575');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (179, 'D-MFIN-2-25MB', '1 day', '50', '50', '25', '9mobile', '465');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (180, 'D-MFIN-2-100MB', '1 day', '100', '100', '100', '9mobile', '271');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (181, 'D-MFIN-2-650MB', '1 day', '200', '200', '650', '9mobile', '127');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (182, 'D-MFIN-2-15GB', '30 days', '5000', '5000', '15000', '9mobile', '309');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (183, 'D-MFIN-2-100GB', '100 days', '84992', '84992', '100000', '9mobile', '775');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (184, 'D-MFIN-2-7GB1W', '7 days', '1500', '1500', '7000', '9mobile', '171');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (185, 'D-MFIN-2-5M', '30 days', '500', '500', '500', '9mobile', '496');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (186, 'D-MFIN-2-75M30', '30 days', '15000', '15000', '75000', '9mobile', '496');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (187, 'D-MFIN-2-75M', '90 days', '25000', '25000', '75000', '9mobile', '640');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (188, 'D-MFIN-6-150MB1D', '1 day', '100', '104', '150', 'Globacom', '493');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (189, 'D-MFIN-6-350MB2D', '2 days', '200', '208', '350', 'Globacom', '505');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (190, 'D-MFIN-6-1.35GB2W', '14 days', '500', '520', '1350', 'Globacom', '691');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (191, 'D-MFIN-6-2.9GB', '30 days', '1000', '1040', '2900', 'Globacom', '317');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (192, 'D-MFIN-6-5.8GB', '30 days', '2000', '2080', '5800', 'Globacom', '358');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (193, 'D-MFIN-6-7.7GB', '30 days', '2500', '2600', '7700', 'Globacom', '492');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (194, 'D-MFIN-6-10GB', '30 days', '3000', '3120', '10000', 'Globacom', '339');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (195, 'D-MFIN-6-13.25GB', '30 days', '4000', '4160', '13250', 'Globacom', '280');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (196, 'D-MFIN-6-18.25GB', '30 days', '5000', '5200', '18250', 'Globacom', '846');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (197, 'D-MFIN-6-29.5GB', '30 days', '8000', '8320', '29500', 'Globacom', '577');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (198, 'D-MFIN-6-50GB', '30 days', '10000', '10400', '50000', 'Globacom', '810');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (199, 'D-MFIN-6-93GB', '30 days', '15000', '15600', '93000', 'Globacom', '303');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (200, 'D-MFIN-6-119GB', '30 days', '18000', '18720', '119000', 'Globacom', '108');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (201, 'D-MFIN-6-50MB1D', '1 day', '50', '52', '50', 'Globacom', '719');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (202, 'D-MFIN-6-138GB', '30 days', '20000', '20800', '138000', 'Globacom', '682');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (203, 'D-MFIN-6-250MB1N', '1 night', '25', '26', '250', 'Globacom', '836');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (204, 'D-MFIN-6-500MB1N', '1 night', '50', '52', '500', 'Globacom', '558');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (205, 'D-MFIN-6-1GB5N', '5 nights', '100', '104', '1000', 'Globacom', '152');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (206, 'D-MFIN-6-7GB', '7 days', '1500', '1560', '7000', 'Globacom', '569');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (207, 'D-MFIN-6-4.1GB', '30 days', '1500', '1560', '4100', 'Globacom', '971');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (208, 'D-MFIN-6-1.25GB1SUN', '1 day (Sun)', '200', '208', '1250', 'Globacom', '278');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (209, 'D-MFIN-6-225GBOT', '30 days', '30000', '31200', '225000', 'Globacom', '487');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (210, 'D-MFIN-6-300GBOT', '30 days', '36000', '37440', '300000', 'Globacom', '481');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (211, 'D-MFIN-6-425GBOT', '90 days', '50000', '52000', '425000', 'Globacom', '610');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (212, 'D-MFIN-6-525GBOT', '120 days', '60000', '62400', '525000', 'Globacom', '149');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (213, 'D-MFIN-6-675GBOT', '120 days', '75000', '78000', '675000', 'Globacom', '890');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (214, 'D-MFIN-6-1TBOT', '365 days', '100000', '104000', '1000000', 'Globacom', '961');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (215, 'D-MFIN-5-100MB', '1 day', '100', '100', '100', 'MTN', '974');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (216, 'D-MFIN-5-750MB', '14 days', '500', '500', '750', 'MTN', '226');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (217, 'D-MFIN-5-1.5GB', '30 days', '1000', '1000', '1500', 'MTN', '462');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (218, 'D-MFIN-5-20GB', '30 days', '5000', '5000', '20000', 'MTN', '742');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (219, 'D-MFIN-5-40GB', '30 days', '10000', '10000', '40000', 'MTN', '996');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (220, 'D-MFIN-5-75GB2M', '60 days', '20000', '20000', '75000', 'MTN', '191');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (221, 'D-MFIN-5-150GB3M', '90 days', '50000', '50000', '150000', 'MTN', '369');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (222, 'D-MFIN-5-250GB3M', '90 days', '75000', '75000', '250000', 'MTN', '302');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (223, 'D-MFIN-5-200MB', '2 days', '200', '200', '200', 'MTN', '212');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (224, 'D-MFIN-5-350MB', '7 days', '300', '300', '350', 'MTN', '199');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (225, 'D-MFIN-5-75GB', '30 days', '15000', '15000', '75000', 'MTN', '229');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (226, 'D-MFIN-5-120GB2M', '60 days', '30000', '30000', '120000', 'MTN', '803');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (227, 'D-MFIN-5-3GB1M', '30 days', '1500', '1500', '3000', 'MTN', '441');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (228, 'D-MFIN-5-25GB1M', '30 days', '6000', '6000', '25000', 'MTN', '143');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (229, 'D-MFIN-5-2GB1M', '30 days', '1200', '1200', '2000', 'MTN', '207');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (230, 'D-MFIN-5-6GB', '7 days', '1500', '1500', '6000', 'MTN', '815');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (231, 'D-MFIN-5-6GB1M', '30 days', '2500', '2500', '6000', 'MTN', '609');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (232, 'D-MFIN-5-10GB1M', '30 days', '3000', '3000', '10000', 'MTN', '823');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (233, 'D-MFIN-5-110GB1M', '30 days', '20000', '20000', '110000', 'MTN', '456');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (234, 'D-MFIN-5-30GB1M', '30 days', '13500', '13500', '30000', 'MTN', '360');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (235, 'D-MFIN-5-90GB1M', '30 days', '40000', '40000', '90000', 'MTN', '222');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (236, 'D-MFIN-5-150GB3M', '90 days', '50000', '50000', '150000', 'MTN', '734');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (237, 'D-MFIN-5-325GB6M', '180 days', '100000', '100000', '325000', 'MTN', '175');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (238, 'D-MFIN-5-1T1Y', '1 year', '100000', '100000', '1000000', 'MTN', '643');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (239, 'D-MFIN-5-1.5T1Y', '1 year', '450000', '450000', '1500000', 'MTN', '690');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (240, 'D-MFIN-5-400GB1Y', '1 year', '120000', '120000', '400000', 'MTN', '805');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (241, 'D-MFIN-5-1GB1W', '7 days', '500', '500', '1000', 'MTN', '138');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (242, 'D-MFIN-5-2.5GB2D', '2 days', '500', '500', '2500', 'MTN', '261');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (243, 'D-MFIN-5-30GB2M', '60 days', '8000', '8000', '30000', 'MTN', '544');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (244, 'D-MFIN-5-12GB1M', '30 days', '3500', '3500', '12000', 'MTN', '825');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (245, 'D-MFIN-5-4.5GB1M', '30 days', '2000', '2000', '4500', 'MTN', '389');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (246, 'D-MFIN-5-750MB', '7 days Xtradata 500', '500', '500', '750', 'MTN', '344');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (247, 'D-MFIN-5-300MB', '7 days, Xtradata 300', '300', '300', '300', 'MTN', '177');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (248, 'D-MFIN-5-1.5GB', '30 days Xtradata 1000', '1000', '1000', '1500', 'MTN', '793');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (249, 'D-MFIN-5-4.5GB', '30 days Xtradata 2000', '2000', '2000', '4500', 'MTN', '131');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (250, 'D-MFIN-5-15GB', '30 days Xtradata 5000', '5000', '5000', '15000', 'MTN', '810');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (251, 'D-MFIN-5-30GB', '30 days Xtradata 10000', '10000', '10000', '30000', 'MTN', '820');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (252, 'D-MFIN-5-50GB', '30 days Xtradata 15000', '15000', '15000', '50000', 'MTN', '590');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (253, 'D-MFIN-5-70GB', '30 days Xtradata 20000', '20000', '20000', '70000', 'MTN', '594');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (254, 'D-MFIN-1-40MB', '1 day', '50', '50.49', '40', 'Airtel', '954');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (255, 'D-MFIN-1-100MB', '1 day', '100', '99.99', '100', 'Airtel', '369');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (256, 'D-MFIN-1-200MB', '3 days', '200', '201.02', '200', 'Airtel', '122');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (257, 'D-MFIN-1-350MB', '7 days', '300', '302.01', '350', 'Airtel', '704');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (258, 'D-MFIN-1-750MB', '14 days', '500', '503.99', '750', 'Airtel', '489');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (259, 'D-MFIN-1-1.5GB', '30 days', '1000', '1008.99', '1500', 'Airtel', '398');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (260, 'D-MFIN-1-3GB', '30 days', '1500', '1514', '3000', 'Airtel', '449');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (261, 'D-MFIN-1-6GB', '30 days', '2500', '2524', '6000', 'Airtel', '565');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (262, 'D-MFIN-1-11GB', '30 days', '4000', '4039', '11000', 'Airtel', '494');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (263, 'D-MFIN-1-20GB', '30 days', '5000', '5048.99', '20000', 'Airtel', '825');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (264, 'D-MFIN-1-40GB', '30 days', '10000', '10098.99', '40000', 'Airtel', '688');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (265, 'D-MFIN-1-75GB', '30 days', '15000', '15148.99', '75000', 'Airtel', '333');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (266, 'D-MFIN-1-8GB', '30 days', '3000', '3029.01', '8000', 'Airtel', '674');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (267, 'D-MFIN-1-120GB', '30 days', '20000', '20199.01', '120000', 'Airtel', '810');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (268, 'D-MFIN-1-1GB1D', '1 day', '300', '302.02', '1000', 'Airtel', '442');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (269, 'D-MFIN-1-2GB1D', '1 day', '500', '504.02', '2000', 'Airtel', '836');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (270, 'D-MFIN-1-6GB1W', '7 days', '1500', '1514.02', '6000', 'Airtel', '192');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (271, 'D-MFIN-1-2GB1M', '30 days', '1200', '1210.99', '2000', 'Airtel', '712');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (272, 'D-MFIN-1-4.5GB', '30 days', '2000', '2018.99', '4500', 'Airtel', '367');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (273, 'D-MFIN-1-25GB1M', '30 days', '8000', '8079.01', '25000', 'Airtel', '356');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (274, 'D-MFIN-1-200GB1M', '30 days', '30000', '30299.01', '200000', 'Airtel', '835');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (275, 'D-MFIN-1-280GB1M', '30 days', '36000', '36359.01', '280000', 'Airtel', '513');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (276, 'D-MFIN-1-400GB3M', '90 days', '50000', '50499.01', '400000', 'Airtel', '379');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (277, 'D-MFIN-1-500GB4M', '120 days', '60000', '60599.01', '500000', 'Airtel', '802');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (278, 'D-MFIN-1-1TB1Y', '365 days', '100000', '100999.01', '1000000', 'Airtel', '777');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (279, 'D-MFIN-2-1.5GB', '30 days', '1000', '1000', '1500', '9mobile', '909');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (280, 'D-MFIN-2-2GB', '30 days', '1200', '1200', '2000', '9mobile', '231');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (281, 'D-MFIN-2-4.5GB', '30 days', '2000', '2000', '4500', '9mobile', '974');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (282, 'D-MFIN-2-40GB', '30 days', '10000', '10000', '40000', '9mobile', '965');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (283, 'D-MFIN-2-165GB', '180 days', '50000', '50000', '165000', '9mobile', '190');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (284, 'D-MFIN-2-365GB', '365 days', '100000', '100000', '365000', '9mobile', '948');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (285, 'D-MFIN-2-11GB', '30 days', '4000', '4000', '11000', '9mobile', '575');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (286, 'D-MFIN-2-25MB', '1 day', '50', '50', '25', '9mobile', '465');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (287, 'D-MFIN-2-100MB', '1 day', '100', '100', '100', '9mobile', '271');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (288, 'D-MFIN-2-650MB', '1 day', '200', '200', '650', '9mobile', '127');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (289, 'D-MFIN-2-15GB', '30 days', '5000', '5000', '15000', '9mobile', '309');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (290, 'D-MFIN-2-100GB', '100 days', '84992', '84992', '100000', '9mobile', '775');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (291, 'D-MFIN-2-7GB1W', '7 days', '1500', '1500', '7000', '9mobile', '171');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (292, 'D-MFIN-2-5M', '30 days', '500', '500', '500', '9mobile', '496');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (293, 'D-MFIN-2-75M30', '30 days', '15000', '15000', '75000', '9mobile', '496');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (294, 'D-MFIN-2-75M', '90 days', '25000', '25000', '75000', '9mobile', '640');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (295, 'D-MFIN-6-150MB1D', '1 day', '100', '104', '150', 'Globacom', '493');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (296, 'D-MFIN-6-350MB2D', '2 days', '200', '208', '350', 'Globacom', '505');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (297, 'D-MFIN-6-1.35GB2W', '14 days', '500', '520', '1350', 'Globacom', '691');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (298, 'D-MFIN-6-2.9GB', '30 days', '1000', '1040', '2900', 'Globacom', '317');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (299, 'D-MFIN-6-5.8GB', '30 days', '2000', '2080', '5800', 'Globacom', '358');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (300, 'D-MFIN-6-7.7GB', '30 days', '2500', '2600', '7700', 'Globacom', '492');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (301, 'D-MFIN-6-10GB', '30 days', '3000', '3120', '10000', 'Globacom', '339');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (302, 'D-MFIN-6-13.25GB', '30 days', '4000', '4160', '13250', 'Globacom', '280');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (303, 'D-MFIN-6-18.25GB', '30 days', '5000', '5200', '18250', 'Globacom', '846');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (304, 'D-MFIN-6-29.5GB', '30 days', '8000', '8320', '29500', 'Globacom', '577');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (305, 'D-MFIN-6-50GB', '30 days', '10000', '10400', '50000', 'Globacom', '810');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (306, 'D-MFIN-6-93GB', '30 days', '15000', '15600', '93000', 'Globacom', '303');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (307, 'D-MFIN-6-119GB', '30 days', '18000', '18720', '119000', 'Globacom', '108');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (308, 'D-MFIN-6-50MB1D', '1 day', '50', '52', '50', 'Globacom', '719');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (309, 'D-MFIN-6-138GB', '30 days', '20000', '20800', '138000', 'Globacom', '682');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (310, 'D-MFIN-6-250MB1N', '1 night', '25', '26', '250', 'Globacom', '836');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (311, 'D-MFIN-6-500MB1N', '1 night', '50', '52', '500', 'Globacom', '558');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (312, 'D-MFIN-6-1GB5N', '5 nights', '100', '104', '1000', 'Globacom', '152');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (313, 'D-MFIN-6-7GB', '7 days', '1500', '1560', '7000', 'Globacom', '569');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (314, 'D-MFIN-6-4.1GB', '30 days', '1500', '1560', '4100', 'Globacom', '971');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (315, 'D-MFIN-6-1.25GB1SUN', '1 day (Sun)', '200', '208', '1250', 'Globacom', '278');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (316, 'D-MFIN-6-225GBOT', '30 days', '30000', '31200', '225000', 'Globacom', '487');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (317, 'D-MFIN-6-300GBOT', '30 days', '36000', '37440', '300000', 'Globacom', '481');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (318, 'D-MFIN-6-425GBOT', '90 days', '50000', '52000', '425000', 'Globacom', '610');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (319, 'D-MFIN-6-525GBOT', '120 days', '60000', '62400', '525000', 'Globacom', '149');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (320, 'D-MFIN-6-675GBOT', '120 days', '75000', '78000', '675000', 'Globacom', '890');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (321, 'D-MFIN-6-1TBOT', '365 days', '100000', '104000', '1000000', 'Globacom', '961');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (322, 'D-MFIN-5-100MB', '1 day', '100', '100', '100', 'MTN', '974');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (323, 'D-MFIN-5-750MB', '14 days', '500', '500', '750', 'MTN', '226');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (324, 'D-MFIN-5-1.5GB', '30 days', '1000', '1000', '1500', 'MTN', '462');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (325, 'D-MFIN-5-20GB', '30 days', '5000', '5000', '20000', 'MTN', '742');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (326, 'D-MFIN-5-40GB', '30 days', '10000', '10000', '40000', 'MTN', '996');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (327, 'D-MFIN-5-75GB2M', '60 days', '20000', '20000', '75000', 'MTN', '191');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (328, 'D-MFIN-5-150GB3M', '90 days', '50000', '50000', '150000', 'MTN', '369');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (329, 'D-MFIN-5-250GB3M', '90 days', '75000', '75000', '250000', 'MTN', '302');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (330, 'D-MFIN-5-200MB', '2 days', '200', '200', '200', 'MTN', '212');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (331, 'D-MFIN-5-350MB', '7 days', '300', '300', '350', 'MTN', '199');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (332, 'D-MFIN-5-75GB', '30 days', '15000', '15000', '75000', 'MTN', '229');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (333, 'D-MFIN-5-120GB2M', '60 days', '30000', '30000', '120000', 'MTN', '803');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (334, 'D-MFIN-5-3GB1M', '30 days', '1500', '1500', '3000', 'MTN', '441');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (335, 'D-MFIN-5-25GB1M', '30 days', '6000', '6000', '25000', 'MTN', '143');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (336, 'D-MFIN-5-2GB1M', '30 days', '1200', '1200', '2000', 'MTN', '207');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (337, 'D-MFIN-5-6GB', '7 days', '1500', '1500', '6000', 'MTN', '815');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (338, 'D-MFIN-5-6GB1M', '30 days', '2500', '2500', '6000', 'MTN', '609');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (339, 'D-MFIN-5-10GB1M', '30 days', '3000', '3000', '10000', 'MTN', '823');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (340, 'D-MFIN-5-110GB1M', '30 days', '20000', '20000', '110000', 'MTN', '456');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (341, 'D-MFIN-5-30GB1M', '30 days', '13500', '13500', '30000', 'MTN', '360');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (342, 'D-MFIN-5-90GB1M', '30 days', '40000', '40000', '90000', 'MTN', '222');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (343, 'D-MFIN-5-150GB3M', '90 days', '50000', '50000', '150000', 'MTN', '734');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (344, 'D-MFIN-5-325GB6M', '180 days', '100000', '100000', '325000', 'MTN', '175');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (345, 'D-MFIN-5-1T1Y', '1 year', '100000', '100000', '1000000', 'MTN', '643');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (346, 'D-MFIN-5-1.5T1Y', '1 year', '450000', '450000', '1500000', 'MTN', '690');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (347, 'D-MFIN-5-400GB1Y', '1 year', '120000', '120000', '400000', 'MTN', '805');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (348, 'D-MFIN-5-1GB1W', '7 days', '500', '500', '1000', 'MTN', '138');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (349, 'D-MFIN-5-2.5GB2D', '2 days', '500', '500', '2500', 'MTN', '261');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (350, 'D-MFIN-5-30GB2M', '60 days', '8000', '8000', '30000', 'MTN', '544');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (351, 'D-MFIN-5-12GB1M', '30 days', '3500', '3500', '12000', 'MTN', '825');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (352, 'D-MFIN-5-4.5GB1M', '30 days', '2000', '2000', '4500', 'MTN', '389');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (353, 'D-MFIN-5-750MB', '7 days Xtradata 500', '500', '500', '750', 'MTN', '344');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (354, 'D-MFIN-5-300MB', '7 days, Xtradata 300', '300', '300', '300', 'MTN', '177');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (355, 'D-MFIN-5-1.5GB', '30 days Xtradata 1000', '1000', '1000', '1500', 'MTN', '793');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (356, 'D-MFIN-5-4.5GB', '30 days Xtradata 2000', '2000', '2000', '4500', 'MTN', '131');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (357, 'D-MFIN-5-15GB', '30 days Xtradata 5000', '5000', '5000', '15000', 'MTN', '810');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (358, 'D-MFIN-5-30GB', '30 days Xtradata 10000', '10000', '10000', '30000', 'MTN', '820');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (359, 'D-MFIN-5-50GB', '30 days Xtradata 15000', '15000', '15000', '50000', 'MTN', '590');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (360, 'D-MFIN-5-70GB', '30 days Xtradata 20000', '20000', '20000', '70000', 'MTN', '594');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (361, 'D-MFIN-1-40MB', '1 day', '50', '50.49', '40', 'Airtel', '954');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (362, 'D-MFIN-1-100MB', '1 day', '100', '99.99', '100', 'Airtel', '369');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (363, 'D-MFIN-1-200MB', '3 days', '200', '201.02', '200', 'Airtel', '122');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (364, 'D-MFIN-1-350MB', '7 days', '300', '302.01', '350', 'Airtel', '704');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (365, 'D-MFIN-1-750MB', '14 days', '500', '503.99', '750', 'Airtel', '489');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (366, 'D-MFIN-1-1.5GB', '30 days', '1000', '1008.99', '1500', 'Airtel', '398');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (367, 'D-MFIN-1-3GB', '30 days', '1500', '1514', '3000', 'Airtel', '449');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (368, 'D-MFIN-1-6GB', '30 days', '2500', '2524', '6000', 'Airtel', '565');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (369, 'D-MFIN-1-11GB', '30 days', '4000', '4039', '11000', 'Airtel', '494');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (370, 'D-MFIN-1-20GB', '30 days', '5000', '5048.99', '20000', 'Airtel', '825');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (371, 'D-MFIN-1-40GB', '30 days', '10000', '10098.99', '40000', 'Airtel', '688');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (372, 'D-MFIN-1-75GB', '30 days', '15000', '15148.99', '75000', 'Airtel', '333');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (373, 'D-MFIN-1-8GB', '30 days', '3000', '3029.01', '8000', 'Airtel', '674');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (374, 'D-MFIN-1-120GB', '30 days', '20000', '20199.01', '120000', 'Airtel', '810');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (375, 'D-MFIN-1-1GB1D', '1 day', '300', '302.02', '1000', 'Airtel', '442');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (376, 'D-MFIN-1-2GB1D', '1 day', '500', '504.02', '2000', 'Airtel', '836');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (377, 'D-MFIN-1-6GB1W', '7 days', '1500', '1514.02', '6000', 'Airtel', '192');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (378, 'D-MFIN-1-2GB1M', '30 days', '1200', '1210.99', '2000', 'Airtel', '712');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (379, 'D-MFIN-1-4.5GB', '30 days', '2000', '2018.99', '4500', 'Airtel', '367');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (380, 'D-MFIN-1-25GB1M', '30 days', '8000', '8079.01', '25000', 'Airtel', '356');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (381, 'D-MFIN-1-200GB1M', '30 days', '30000', '30299.01', '200000', 'Airtel', '835');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (382, 'D-MFIN-1-280GB1M', '30 days', '36000', '36359.01', '280000', 'Airtel', '513');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (383, 'D-MFIN-1-400GB3M', '90 days', '50000', '50499.01', '400000', 'Airtel', '379');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (384, 'D-MFIN-1-500GB4M', '120 days', '60000', '60599.01', '500000', 'Airtel', '802');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (385, 'D-MFIN-1-1TB1Y', '365 days', '100000', '100999.01', '1000000', 'Airtel', '777');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (386, 'D-MFIN-2-1.5GB', '30 days', '1000', '1000', '1500', '9mobile', '909');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (387, 'D-MFIN-2-2GB', '30 days', '1200', '1200', '2000', '9mobile', '231');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (388, 'D-MFIN-2-4.5GB', '30 days', '2000', '2000', '4500', '9mobile', '974');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (389, 'D-MFIN-2-40GB', '30 days', '10000', '10000', '40000', '9mobile', '965');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (390, 'D-MFIN-2-165GB', '180 days', '50000', '50000', '165000', '9mobile', '190');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (391, 'D-MFIN-2-365GB', '365 days', '100000', '100000', '365000', '9mobile', '948');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (392, 'D-MFIN-2-11GB', '30 days', '4000', '4000', '11000', '9mobile', '575');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (393, 'D-MFIN-2-25MB', '1 day', '50', '50', '25', '9mobile', '465');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (394, 'D-MFIN-2-100MB', '1 day', '100', '100', '100', '9mobile', '271');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (395, 'D-MFIN-2-650MB', '1 day', '200', '200', '650', '9mobile', '127');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (396, 'D-MFIN-2-15GB', '30 days', '5000', '5000', '15000', '9mobile', '309');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (397, 'D-MFIN-2-100GB', '100 days', '84992', '84992', '100000', '9mobile', '775');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (398, 'D-MFIN-2-7GB1W', '7 days', '1500', '1500', '7000', '9mobile', '171');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (399, 'D-MFIN-2-5M', '30 days', '500', '500', '500', '9mobile', '496');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (400, 'D-MFIN-2-75M30', '30 days', '15000', '15000', '75000', '9mobile', '496');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (401, 'D-MFIN-2-75M', '90 days', '25000', '25000', '75000', '9mobile', '640');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (402, 'D-MFIN-6-150MB1D', '1 day', '100', '104', '150', 'Globacom', '493');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (403, 'D-MFIN-6-350MB2D', '2 days', '200', '208', '350', 'Globacom', '505');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (404, 'D-MFIN-6-1.35GB2W', '14 days', '500', '520', '1350', 'Globacom', '691');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (405, 'D-MFIN-6-2.9GB', '30 days', '1000', '1040', '2900', 'Globacom', '317');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (406, 'D-MFIN-6-5.8GB', '30 days', '2000', '2080', '5800', 'Globacom', '358');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (407, 'D-MFIN-6-7.7GB', '30 days', '2500', '2600', '7700', 'Globacom', '492');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (408, 'D-MFIN-6-10GB', '30 days', '3000', '3120', '10000', 'Globacom', '339');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (409, 'D-MFIN-6-13.25GB', '30 days', '4000', '4160', '13250', 'Globacom', '280');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (410, 'D-MFIN-6-18.25GB', '30 days', '5000', '5200', '18250', 'Globacom', '846');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (411, 'D-MFIN-6-29.5GB', '30 days', '8000', '8320', '29500', 'Globacom', '577');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (412, 'D-MFIN-6-50GB', '30 days', '10000', '10400', '50000', 'Globacom', '810');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (413, 'D-MFIN-6-93GB', '30 days', '15000', '15600', '93000', 'Globacom', '303');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (414, 'D-MFIN-6-119GB', '30 days', '18000', '18720', '119000', 'Globacom', '108');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (415, 'D-MFIN-6-50MB1D', '1 day', '50', '52', '50', 'Globacom', '719');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (416, 'D-MFIN-6-138GB', '30 days', '20000', '20800', '138000', 'Globacom', '682');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (417, 'D-MFIN-6-250MB1N', '1 night', '25', '26', '250', 'Globacom', '836');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (418, 'D-MFIN-6-500MB1N', '1 night', '50', '52', '500', 'Globacom', '558');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (419, 'D-MFIN-6-1GB5N', '5 nights', '100', '104', '1000', 'Globacom', '152');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (420, 'D-MFIN-6-7GB', '7 days', '1500', '1560', '7000', 'Globacom', '569');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (421, 'D-MFIN-6-4.1GB', '30 days', '1500', '1560', '4100', 'Globacom', '971');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (422, 'D-MFIN-6-1.25GB1SUN', '1 day (Sun)', '200', '208', '1250', 'Globacom', '278');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (423, 'D-MFIN-6-225GBOT', '30 days', '30000', '31200', '225000', 'Globacom', '487');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (424, 'D-MFIN-6-300GBOT', '30 days', '36000', '37440', '300000', 'Globacom', '481');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (425, 'D-MFIN-6-425GBOT', '90 days', '50000', '52000', '425000', 'Globacom', '610');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (426, 'D-MFIN-6-525GBOT', '120 days', '60000', '62400', '525000', 'Globacom', '149');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (427, 'D-MFIN-6-675GBOT', '120 days', '75000', '78000', '675000', 'Globacom', '890');
INSERT INTO `Primedata` (`id`, `product_id`, `validity`, `topup_value`, `price`, `data_amount`, `telco`, `planId`) VALUES (428, 'D-MFIN-6-1TBOT', '365 days', '100000', '104000', '1000000', 'Globacom', '961');
COMMIT;

-- ----------------------------
-- Table structure for SequelizeMeta
-- ----------------------------
DROP TABLE IF EXISTS `SequelizeMeta`;
CREATE TABLE `SequelizeMeta` (
  `name` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`name`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8_unicode_ci;

-- ----------------------------
-- Records of SequelizeMeta
-- ----------------------------
BEGIN;
INSERT INTO `SequelizeMeta` (`name`) VALUES ('20220130220437-create-user.js');
INSERT INTO `SequelizeMeta` (`name`) VALUES ('20220130223401-create-transaction.js');
INSERT INTO `SequelizeMeta` (`name`) VALUES ('20220130225828-create-primeauth.js');
INSERT INTO `SequelizeMeta` (`name`) VALUES ('20220130230344-create-prime-airtime.js');
INSERT INTO `SequelizeMeta` (`name`) VALUES ('20220130230448-create-batch.js');
INSERT INTO `SequelizeMeta` (`name`) VALUES ('20220202161005-create-primedata.js');
INSERT INTO `SequelizeMeta` (`name`) VALUES ('20220202181017-create-insufficient.js');
COMMIT;

-- ----------------------------
-- Table structure for Transactions
-- ----------------------------
DROP TABLE IF EXISTS `Transactions`;
CREATE TABLE `Transactions` (
  `id` int NOT NULL AUTO_INCREMENT,
  `amount` varchar(255) NOT NULL,
  `description` varchar(255) DEFAULT NULL,
  `is_completed` tinyint(1) DEFAULT NULL,
  `ip` varchar(255) DEFAULT NULL,
  `user_id` int NOT NULL,
  `product_id` varchar(255) DEFAULT NULL,
  `is_reversed` tinyint(1) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `reference` varchar(255) NOT NULL,
  `telco` varchar(255) DEFAULT NULL,
  `is_debited` tinyint(1) DEFAULT NULL,
  `type` varchar(255) DEFAULT NULL,
  `phone` varchar(255) DEFAULT NULL,
  `plan` varchar(255) DEFAULT NULL,
  `bundle` varchar(255) DEFAULT NULL,
  `core_trnx_id` varchar(255) DEFAULT NULL,
  `createdAt` datetime NOT NULL,
  `updatedAt` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `reference` (`reference`),
  UNIQUE KEY `core_trnx_id` (`core_trnx_id`)
) ENGINE=InnoDB AUTO_INCREMENT=16 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----------------------------
-- Records of Transactions
-- ----------------------------
BEGIN;
INSERT INTO `Transactions` (`id`, `amount`, `description`, `is_completed`, `ip`, `user_id`, `product_id`, `is_reversed`, `status`, `reference`, `telco`, `is_debited`, `type`, `phone`, `plan`, `bundle`, `core_trnx_id`, `createdAt`, `updatedAt`) VALUES (1, '1100', 'WALLET TOP UP', 0, '197.253.58.228', 13, NULL, 0, 'SUCCESFULL', 'TPUP_890875051_836073570', NULL, 1, 'top up', NULL, NULL, NULL, '95898BBF1DB5', '2022-02-06 20:37:56', '2022-02-06 20:37:56');
INSERT INTO `Transactions` (`id`, `amount`, `description`, `is_completed`, `ip`, `user_id`, `product_id`, `is_reversed`, `status`, `reference`, `telco`, `is_debited`, `type`, `phone`, `plan`, `bundle`, `core_trnx_id`, `createdAt`, `updatedAt`) VALUES (2, '2', 'AIRTIME TOPUP', 1, '', 13, NULL, 0, '201', 'BILL_BILL_780097228', 'MTN', 1, 'airtime', '08107176089', NULL, NULL, '95899314DA19', '2022-02-06 20:58:30', '2022-02-06 20:58:30');
INSERT INTO `Transactions` (`id`, `amount`, `description`, `is_completed`, `ip`, `user_id`, `product_id`, `is_reversed`, `status`, `reference`, `telco`, `is_debited`, `type`, `phone`, `plan`, `bundle`, `core_trnx_id`, `createdAt`, `updatedAt`) VALUES (3, '2', 'AIRTIME TOPUP', 1, '', 13, NULL, 0, '201', 'BILL_BILL_395931343', 'MTN', 1, 'airtime', '08107176089', NULL, NULL, '958996268D29', '2022-02-06 21:07:05', '2022-02-06 21:07:05');
INSERT INTO `Transactions` (`id`, `amount`, `description`, `is_completed`, `ip`, `user_id`, `product_id`, `is_reversed`, `status`, `reference`, `telco`, `is_debited`, `type`, `phone`, `plan`, `bundle`, `core_trnx_id`, `createdAt`, `updatedAt`) VALUES (4, '2', 'AIRTIME TOPUP', 1, '', 13, NULL, 0, '201', 'BILL_BILL_980590994', '9mobile', 1, 'airtime', '08107173389', NULL, NULL, '958996291B11', '2022-02-06 21:07:07', '2022-02-06 21:07:07');
INSERT INTO `Transactions` (`id`, `amount`, `description`, `is_completed`, `ip`, `user_id`, `product_id`, `is_reversed`, `status`, `reference`, `telco`, `is_debited`, `type`, `phone`, `plan`, `bundle`, `core_trnx_id`, `createdAt`, `updatedAt`) VALUES (5, '1', 'AIRTIME TOPUP', 1, '', 13, NULL, 0, '201', 'BILL_BILL_812237128', NULL, 1, 'airtime', '08107176089', NULL, NULL, '9589BB7BB789', '2022-02-06 22:51:28', '2022-02-06 22:51:28');
INSERT INTO `Transactions` (`id`, `amount`, `description`, `is_completed`, `ip`, `user_id`, `product_id`, `is_reversed`, `status`, `reference`, `telco`, `is_debited`, `type`, `phone`, `plan`, `bundle`, `core_trnx_id`, `createdAt`, `updatedAt`) VALUES (6, '1', 'AIRTIME TOPUP', 0, '', 13, NULL, 0, 'RECHARGE_COMPLETE', 'BILL_BILL_559230811', 'MTN', 1, 'airtime', '08107176089', NULL, NULL, '9589BF3DF521', '2022-02-06 23:01:59', '2022-02-06 23:01:59');
INSERT INTO `Transactions` (`id`, `amount`, `description`, `is_completed`, `ip`, `user_id`, `product_id`, `is_reversed`, `status`, `reference`, `telco`, `is_debited`, `type`, `phone`, `plan`, `bundle`, `core_trnx_id`, `createdAt`, `updatedAt`) VALUES (7, '1', 'AIRTIME TOPUP', 1, '', 13, NULL, 0, 'RECHARGE_COMPLETE', 'BILL_BILL_748879540', 'MTN', 1, 'airtime', '08107176089', NULL, NULL, '958AB607AA7D', '2022-02-07 10:32:03', '2022-02-07 10:32:03');
INSERT INTO `Transactions` (`id`, `amount`, `description`, `is_completed`, `ip`, `user_id`, `product_id`, `is_reversed`, `status`, `reference`, `telco`, `is_debited`, `type`, `phone`, `plan`, `bundle`, `core_trnx_id`, `createdAt`, `updatedAt`) VALUES (8, '1', 'AIRTIME TOPUP', 1, '', 13, NULL, 0, 'RECHARGE_COMPLETE', 'BILL_BILL_283204954', 'MTN', 1, 'airtime', '08107176089', NULL, NULL, '958AB8FB7809', '2022-02-07 10:40:18', '2022-02-07 10:40:18');
INSERT INTO `Transactions` (`id`, `amount`, `description`, `is_completed`, `ip`, `user_id`, `product_id`, `is_reversed`, `status`, `reference`, `telco`, `is_debited`, `type`, `phone`, `plan`, `bundle`, `core_trnx_id`, `createdAt`, `updatedAt`) VALUES (9, '1', 'AIRTIME TOPUP', 1, '', 13, NULL, 0, 'RECHARGE_COMPLETE', 'BILL_BILL_541342836', 'MTN', 1, 'airtime', '08107176089', NULL, NULL, '958ABAD14915', '2022-02-07 10:45:25', '2022-02-07 10:45:25');
INSERT INTO `Transactions` (`id`, `amount`, `description`, `is_completed`, `ip`, `user_id`, `product_id`, `is_reversed`, `status`, `reference`, `telco`, `is_debited`, `type`, `phone`, `plan`, `bundle`, `core_trnx_id`, `createdAt`, `updatedAt`) VALUES (10, '1', 'AIRTIME TOPUP', 1, '', 13, NULL, 0, 'RECHARGE_COMPLETE', 'BILL_BILL_213568087', 'MTN', 1, 'airtime', '08107176089', NULL, NULL, '958AED586A71', '2022-02-07 13:06:42', '2022-02-07 13:06:42');
INSERT INTO `Transactions` (`id`, `amount`, `description`, `is_completed`, `ip`, `user_id`, `product_id`, `is_reversed`, `status`, `reference`, `telco`, `is_debited`, `type`, `phone`, `plan`, `bundle`, `core_trnx_id`, `createdAt`, `updatedAt`) VALUES (11, '100', 'DATA TOPUP', 1, '', 13, NULL, 0, 'RECHARGE_COMPLETE', 'BILL_BILL_512912106', 'MTN', 1, 'airtime', '08107176089', 'MB100', '100', '958AF53EE32D', '2022-02-07 13:28:48', '2022-02-07 13:28:48');
INSERT INTO `Transactions` (`id`, `amount`, `description`, `is_completed`, `ip`, `user_id`, `product_id`, `is_reversed`, `status`, `reference`, `telco`, `is_debited`, `type`, `phone`, `plan`, `bundle`, `core_trnx_id`, `createdAt`, `updatedAt`) VALUES (12, '2', 'AIRTIME TOPUP', 1, '', 13, NULL, 0, 'RECHARGE_COMPLETE', 'BILL_BILL_297589474', 'MTN', 1, 'airtime', '08107176089', NULL, NULL, '958B01DBBD81', '2022-02-07 14:04:04', '2022-02-07 14:04:04');
INSERT INTO `Transactions` (`id`, `amount`, `description`, `is_completed`, `ip`, `user_id`, `product_id`, `is_reversed`, `status`, `reference`, `telco`, `is_debited`, `type`, `phone`, `plan`, `bundle`, `core_trnx_id`, `createdAt`, `updatedAt`) VALUES (13, '2', 'AIRTIME TOPUP', 1, '', 13, NULL, 0, 'RECHARGE_COMPLETE', 'BILL_BILL_329829813', '9mobile', 1, 'airtime', '08107173389', NULL, NULL, '958B01DEB6D5', '2022-02-07 14:04:07', '2022-02-07 14:04:07');
INSERT INTO `Transactions` (`id`, `amount`, `description`, `is_completed`, `ip`, `user_id`, `product_id`, `is_reversed`, `status`, `reference`, `telco`, `is_debited`, `type`, `phone`, `plan`, `bundle`, `core_trnx_id`, `createdAt`, `updatedAt`) VALUES (14, '1', 'AIRTIME TOPUP', 0, NULL, 13, NULL, 0, '201', 'BILL_BILL_434419527', 'MTN', 1, 'airtime', NULL, NULL, NULL, '958B826E4F95', '2022-02-07 20:03:35', '2022-02-07 20:03:35');
INSERT INTO `Transactions` (`id`, `amount`, `description`, `is_completed`, `ip`, `user_id`, `product_id`, `is_reversed`, `status`, `reference`, `telco`, `is_debited`, `type`, `phone`, `plan`, `bundle`, `core_trnx_id`, `createdAt`, `updatedAt`) VALUES (15, '1', 'AIRTIME TOPUP', 1, NULL, 13, NULL, 0, '201', 'BILL_BILL_294504858', 'Globacom', 1, 'airtime', '08075812552', NULL, NULL, '958B843FA431', '2022-02-07 20:08:40', '2022-02-07 20:08:40');
COMMIT;

-- ----------------------------
-- Table structure for Users
-- ----------------------------
DROP TABLE IF EXISTS `Users`;
CREATE TABLE `Users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `first_name` varchar(255) DEFAULT NULL,
  `last_name` varchar(255) DEFAULT NULL,
  `email` varchar(255) NOT NULL,
  `username` varchar(255) DEFAULT NULL,
  `picture` text,
  `isActive` tinyint(1) DEFAULT NULL,
  `core_account_no` varchar(255) NOT NULL,
  `core_wallet_id` varchar(255) NOT NULL,
  `company_name` varchar(255) DEFAULT NULL,
  `phone` varchar(255) DEFAULT NULL,
  `address` varchar(255) DEFAULT NULL,
  `password` varchar(255) NOT NULL,
  `confirm_password` varchar(255) NOT NULL,
  `pin` varchar(255) DEFAULT NULL,
  `createdAt` datetime NOT NULL,
  `updatedAt` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `email` (`email`),
  UNIQUE KEY `core_account_no` (`core_account_no`),
  UNIQUE KEY `core_wallet_id` (`core_wallet_id`)
) ENGINE=InnoDB AUTO_INCREMENT=14 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----------------------------
-- Records of Users
-- ----------------------------
BEGIN;
INSERT INTO `Users` (`id`, `first_name`, `last_name`, `email`, `username`, `picture`, `isActive`, `core_account_no`, `core_wallet_id`, `company_name`, `phone`, `address`, `password`, `confirm_password`, `pin`, `createdAt`, `updatedAt`) VALUES (3, 'sam', 'sam', 'sam@f.com', NULL, NULL, 1, '000000389', '389', 'tfyuio', 'retyuio', 'dtfyguhjkl', '$2a$10$KSswNQGS/gighh/gNo9nVO.MbOIYTPenXKJ8YGDFLDu76nXaExpiW', '$2a$10$KSswNQGS/gighh/gNo9nVO.MbOIYTPenXKJ8YGDFLDu76nXaExpiW', '$2a$10$zrsvpVf8e9In.vY7kLJmZecRY2jAyO5x8wEwpAUlMzou1XjJ0p0J2', '2022-02-01 00:40:01', '2022-02-01 00:40:01');
INSERT INTO `Users` (`id`, `first_name`, `last_name`, `email`, `username`, `picture`, `isActive`, `core_account_no`, `core_wallet_id`, `company_name`, `phone`, `address`, `password`, `confirm_password`, `pin`, `createdAt`, `updatedAt`) VALUES (12, 'battyj', 'battyj', 'battyj@gmail.com', NULL, NULL, 1, '000000398', '398', 'Woodc', '90909090', 'Burdioi', '$2a$10$dklArK3jh1lri7MFkJsTCenDs2y.R/P.KDDB/q9I9hYGyMjU16voq', '$2a$10$dklArK3jh1lri7MFkJsTCenDs2y.R/P.KDDB/q9I9hYGyMjU16voq', '$2a$10$1k1SKQMX7/WZFhFjjPKCSOTMPclwvcI2MTlQWLi1qSfAQCH8k/282', '2022-02-01 01:12:30', '2022-02-01 01:12:30');
INSERT INTO `Users` (`id`, `first_name`, `last_name`, `email`, `username`, `picture`, `isActive`, `core_account_no`, `core_wallet_id`, `company_name`, `phone`, `address`, `password`, `confirm_password`, `pin`, `createdAt`, `updatedAt`) VALUES (13, 'James', 'Adanay', 'alt.z9-ej4bifa@yopmail.com', NULL, NULL, 1, '000000401', '401', 'WC', '08178787177', 'Prime Waters', '$2a$10$cNE0vXAXJgM2aUKOIeJLneyna6/YPmoTqzcTm/O1H8L3CQ05V99mm', '$2a$10$cNE0vXAXJgM2aUKOIeJLneyna6/YPmoTqzcTm/O1H8L3CQ05V99mm', '$2a$10$PtX/yvjsbf0Br0tlHewqpuz030FdaSHq0LeFc0FOmkhm2Xi4qVY9e', '2022-02-06 20:36:39', '2022-02-06 20:36:39');
COMMIT;

SET FOREIGN_KEY_CHECKS = 1;
