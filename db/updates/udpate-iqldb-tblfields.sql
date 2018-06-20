CREATE TABLE `tblfields` (
  `dataset` VARCHAR(100) NOT NULL COMMENT 'Dataset that the field used in select queries belongs to',
  `field` VARCHAR(250) NOT NULL COMMENT 'field that is used in select queries',
  `used_date` DATE NOT NULL COMMENT 'Date when the field is used',
  `count` INT(11) NOT NULL COMMENT 'Count of field usage on this date'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT 'Contains fields used in select queries';