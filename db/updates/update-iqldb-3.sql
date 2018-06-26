ALTER TABLE `tblquerylog` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
ALTER TABLE `tblrunning` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
ALTER TABLE `tbllimits` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE `tblrunning` CHANGE `query` `query` VARCHAR(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT 'Query being performed (with large lists truncated)';
ALTER TABLE `tblrunning` CHANGE `username` `username` VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT 'Username of the user executing the query';
ALTER TABLE `tbllimits` CHANGE `name` `name` VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT 'Specific username or client name to apply the limit to; Or a general role to be referenced in other rows';
ALTER TABLE `tblquerylog` CHANGE `dataset` `dataset` VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL;
ALTER TABLE `tblquerylog` CHANGE `query` `query` MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL;
ALTER TABLE `tblquerylog` CHANGE `user` `user` VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL;
ALTER TABLE `tblquerylog` CHANGE `version` `version` VARCHAR(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL;

