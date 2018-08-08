create table tblshards
(
  path           varchar(512) null,
  numDocs        int          null,
  addedtimestamp bigint       null
);

create index tblshards_addedtimestamp_path_index
  on tblshards (addedtimestamp, path);

create index tblshards_path_index
  on tblshards (path);
