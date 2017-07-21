CREATE TABLE `tblrunning` (
  `id` bigint(20) NOT NULL,
  `query` varchar(1000) COLLATE utf8_bin NOT NULL COMMENT 'Query being performed (with large lists truncated)',
  `qhash` varchar(30) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL COMMENT 'Hash of the query for duplicate detection',
  `username` varchar(100) COLLATE utf8_bin NOT NULL COMMENT 'Username of the user executing the query',
  `client` varchar(100) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL COMMENT 'Name of the IQL client application that started the query',
  `submit_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Time the query execution was requested by the user',
  `execution_start_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Time the query execution actually started',
  `hostname` varchar(20) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL COMMENT 'Hostname of the IQL daemon executing the query',
  `sessions` tinyint(4) NOT NULL DEFAULT '1' COMMENT 'Number of Imhotep sessions used by this query',
  `killed` tinyint(1) NOT NULL DEFAULT '0' COMMENT '1 if query was requested to be stopped'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='Queries currently running';

ALTER TABLE `tblrunning`
  ADD PRIMARY KEY (`id`);

ALTER TABLE `tblrunning`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=1;