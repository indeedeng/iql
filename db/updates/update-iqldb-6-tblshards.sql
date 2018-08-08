create table tblshards
(
  path           varchar(357) not null,
  numDocs        int          not null,
  addedtimestamp bigint       not null
);

create index tblshards_addedtimestamp_path_index
  on tblshards (addedtimestamp, path);

create index tblshards_path_index
  on tblshards (path);
