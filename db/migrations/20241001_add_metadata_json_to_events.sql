-- Add metadata_json column to control_etl_events for richer event payloads.
DELIMITER //
CREATE PROCEDURE IF NOT EXISTS add_metadata_column()
BEGIN
  DECLARE col_count INT;
  SELECT COUNT(*) INTO col_count
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = 'monitoring2'
    AND TABLE_NAME = 'control_etl_events'
    AND COLUMN_NAME = 'metadata_json';

  IF col_count = 0 THEN
    ALTER TABLE monitoring2.control_etl_events
      ADD COLUMN metadata_json TEXT;
  END IF;
END//
DELIMITER ;

CALL add_metadata_column();
DROP PROCEDURE IF EXISTS add_metadata_column;
