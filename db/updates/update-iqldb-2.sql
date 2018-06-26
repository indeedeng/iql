CREATE TABLE `tbllimits` (
  `id` int(11) NOT NULL,
  `name` varchar(100) NOT NULL COMMENT 'Specific username or client name to apply the limit to; Or a general role to be referenced in other rows',
  `parent_id` int(11) DEFAULT NULL COMMENT 'Id of a general role to be assigned to the username (or client name) specified in the `name` column',
  `query_document_count_limit_billions` int(11) DEFAULT NULL,
  `query_in_memory_rows_limit` int(11) DEFAULT NULL,
  `query_ftgs_iql_limit_mb` int(11) DEFAULT NULL,
  `query_ftgs_imhotep_daemon_limit_mb` int(11) DEFAULT NULL,
  `concurrent_queries_limit` int(11) DEFAULT NULL,
  `concurrent_imhotep_sessions_limit` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Contains custom limits for specific users and clients';

ALTER TABLE `tbllimits`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `name` (`name`),
  ADD KEY `parent_id` (`parent_id`);

ALTER TABLE `tbllimits`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=1;

ALTER TABLE `tbllimits`
  ADD CONSTRAINT `tbllimits_ibfk_1` FOREIGN KEY (`parent_id`) REFERENCES `tbllimits` (`id`);
