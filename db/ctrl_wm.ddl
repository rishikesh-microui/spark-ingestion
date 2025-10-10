-- monitoring2.control_watermarks definition
CREATE TABLE `control_watermarks` (
  `schema_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `table_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `watermark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `sourceId` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `last_loaded_date` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  SHARD KEY `__SHARDKEY` (`schema_name`,`table_name`),
  SORT KEY `__UNORDERED` ()
) AUTOSTATS_CARDINALITY_MODE=INCREMENTAL AUTOSTATS_HISTOGRAM_MODE=CREATE AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES,NO_AUTO_CREATE_USER';
