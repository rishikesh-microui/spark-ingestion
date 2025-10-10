-- monitoring2.control_etl_events definition
CREATE TABLE `control_etl_events` (
  `schema_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `table_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `load_date` date NOT NULL,
  `mode` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `phase` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `status` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `rows_written` bigint(20) DEFAULT NULL,
  `watermark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `location` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `strategy` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `error` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `metadata_json` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `sourceId` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `run_id` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `event_seq` bigint(20) DEFAULT NULL,
  `event_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  SHARD KEY `__SHARDKEY` (`schema_name`,`table_name`),
  SORT KEY `__UNORDERED` ()
) AUTOSTATS_CARDINALITY_MODE=INCREMENTAL AUTOSTATS_HISTOGRAM_MODE=CREATE AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES,NO_AUTO_CREATE_USER';
