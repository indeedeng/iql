create table tblfields
(
  dataset            varchar(100)           not null,
  fieldname          varchar(256)           not null,
  type               enum ('INT', 'STRING') not null,
  lastshardstarttime bigint                 null,
  primary key (dataset, fieldname, type)
);
