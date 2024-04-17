create schema partition_reuse_columns;
set search_path to partition_reuse_columns;

create table t1(a int, b int) partition by range(a) ;
create table p1 partition of t1 for values from (1) to (5);
create table p2 partition of t1 for values from (5) to (10) partition by range(b);
create table p2_1 partition of p2 for values from (1) to (3);
create table p2_2 partition of p2 for values from (3) to (6);
select relname, relreuseattrs from pg_class where relnamespace = 'partition_reuse_columns'::regnamespace order by relname;

create table p3(a int, b int);
-- reuse columns
alter table t1 attach partition p3 for values from (10) to (15);
select relname, relreuseattrs from pg_class where oid = 'p3'::regclass::oid;

create table p4(b int, a int, c int);
alter table p4 drop column c;
-- not reuse columns
alter table t1 attach partition p4 for values from (15) to (20);
select relname, relreuseattrs from pg_class where oid = 'p4'::regclass::oid;

-- reuse columns, include the dropped column
create table p5(a int, b int, c int);
alter table p5 drop column c;
alter table t1 add column c int;
alter table t1 drop column c;
alter table t1 attach partition p5 for values from (20) to (25);
select relname, relreuseattrs from pg_class where oid = 'p5'::regclass::oid;

-- check after detach
alter table t1 detach partition p1;
select relname, relreuseattrs from pg_class where oid = 'p1'::regclass::oid;

-- check after re-attach
alter table t1 attach partition p1 for values from (1) to (5);
select relname, relreuseattrs from pg_class where oid = 'p1'::regclass::oid;

-- check after detach and alter tables
alter table t1 detach partition p1;
alter table p1 add column d int;
alter table p1 drop column d;
alter table t1 attach partition p1 for values from (1) to (5);
-- not reuse columns
select relname, relreuseattrs from pg_class where oid = 'p1'::regclass::oid;

-- check alter columns
create table t2(a int, b int) partition by range(a) ;
create table p6 (a int, b int);
alter table t2 attach partition p6 for values from (25) to (30);

--
-- test alter default
--
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;
alter table p6 alter column b set default 10;
-- should not reuse
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;
-- should reuse after parent alter column
alter table t2 alter column b set default 100;
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;
-- should not reuse again
alter table p6 alter column b set default 99;
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;
-- should reuse again
alter table p6 alter column b set default 100;
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;

--
-- test alter statistics 
--
-- should not reuse
alter table p6 alter column b set STATISTICS 99;
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;
-- should reuse again
alter table p6 alter column b set STATISTICS -1;
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;

--
-- test alter NULL/NOT NULL
--
-- should not reuse
alter table p6 alter column b set NOT NULL;
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;
-- should reuse again
alter table p6 alter column b drop NOT NULL;
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;

--
-- test alter storage
--
alter table t2 add column c text;
select attstorage from pg_attribute where attrelid = 't2'::regclass::oid and attname = 'c'; 
select attstorage from pg_attribute where attrelid = 'p6'::regclass::oid and attname = 'c'; 
alter table p6 alter column c set storage main;
select attstorage from pg_attribute where attrelid = 'p6'::regclass::oid and attname = 'c'; 
-- should not reuse
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;
-- should reuse again
alter table p6 alter column c set storage extended;
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;

ALTER TABLE t2 add COLUMN d int;
ALTER TABLE t2 ALTER COLUMN d set NOT NULL;
select attname, attnotnull from pg_attribute where attrelid = 'p6'::regclass::oid and attnum > 0;
select attname, attnotnull from pg_attribute where attrelid = 't2'::regclass::oid and attnum >0;
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;
ALTER TABLE t2 ALTER COLUMN d drop NOT NULL;
select attname, attnotnull from pg_attribute where attrelid = 'p6'::regclass::oid and attnum > 0; 
select attname, attnotnull from pg_attribute where attrelid = 't2'::regclass::oid and attnum > 0;
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;

--
-- test IDENTITY
--
ALTER TABLE t2 ALTER COLUMN d set NOT NULL;
select attname, attidentity from pg_attribute where attrelid = 'p6'::regclass::oid and attnum > 0; 
select attname, attidentity from pg_attribute where attrelid = 't2'::regclass::oid and attnum > 0;
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;
ALTER TABLE p6 ALTER COLUMN d ADD GENERATED ALWAYS AS IDENTITY;
select attname, attidentity from pg_attribute where attrelid = 'p6'::regclass::oid and attnum > 0; 
select attname, attidentity from pg_attribute where attrelid = 't2'::regclass::oid and attnum > 0;
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;
ALTER TABLE p6 ALTER COLUMN d DROP IDENTITY;
select attname, attidentity from pg_attribute where attrelid = 'p6'::regclass::oid and attnum > 0; 
select attname, attidentity from pg_attribute where attrelid = 't2'::regclass::oid and attnum > 0;
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;

--
-- test COMPRESSION
--
select attname, attcompression from pg_attribute where attrelid = 'p6'::regclass::oid and attnum > 0; 
select attname, attcompression from pg_attribute where attrelid = 't2'::regclass::oid and attnum > 0;
ALTER TABLE p6 ALTER COLUMN c SET compression pglz;
select attname, attcompression from pg_attribute where attrelid = 'p6'::regclass::oid and attnum > 0; 
select attname, attcompression from pg_attribute where attrelid = 't2'::regclass::oid and attnum > 0;
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;
ALTER TABLE p6 ALTER COLUMN c SET compression default;
select attname, attcompression from pg_attribute where attrelid = 'p6'::regclass::oid and attnum > 0; 
select attname, attcompression from pg_attribute where attrelid = 't2'::regclass::oid and attnum > 0;
select relname, relreuseattrs from pg_class where oid = 'p6'::regclass::oid;

-- GPDB partition grammar
create table region
(
	r_regionkey integer not null,
	r_name char(25),
	r_comment varchar(152)
)
partition by range (r_regionkey)
subpartition by list (r_name) subpartition template
(
	subpartition africa values ('AFRICA'),
	subpartition america values ('AMERICA'),
	subpartition asia values ('ASIA'),
	subpartition europe values ('EUROPE'),
	subpartition mideast values ('MIDDLE EAST'),
	subpartition australia values ('AUSTRALIA'),
	subpartition antarctica values ('ANTARCTICA')
)
(
	partition region1 start (0),
	partition region2 start (3),
	partition region3 start (5) end (8)
);
select relname, relreuseattrs from pg_class where relnamespace = 'partition_reuse_columns'::regnamespace and relname like 'region%';

create table foo_p (i int, j int)
partition by range(j)
(start(1) end(10) every(1));
select relname, relreuseattrs from pg_class where relnamespace = 'partition_reuse_columns'::regnamespace and relname like 'foo_p%';

--
-- test partition tables with children
--
create table p7 partition of t2 for values from (30) to (40) partition by range (a);
-- should not reuse as t2 has dropped columns
select relname, relreuseattrs from pg_class where oid in
 ('p7'::regclass::oid);
drop table p7;

create table t3(a int, b int, c text) partition by range(a) ;
create table p7 partition of t3 for values from (1) to (40) partition by range (a);
create table p7_1 partition of p7 for values from (1) to (35);
create table p7_2 partition of p7 for values from (35) to (40);
select relname, relreuseattrs from pg_class where oid in
 ('p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid);

-- test compression
select attrelid::regclass, attname, attcompression from pg_attribute where attrelid in
 ('t3'::regclass::oid, 
 'p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid)
 and attnum > 0;
ALTER TABLE p7 ALTER COLUMN c SET compression pglz;
select attrelid::regclass, attname, attcompression from pg_attribute where attrelid in
 ('t3'::regclass::oid, 
 'p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid)
 and attnum > 0;
-- shoud not reuse
select relname, relreuseattrs from pg_class where oid in
 ('p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid);
-- p7_1 shoud reuse
ALTER TABLE p7_1 ALTER COLUMN c SET compression pglz;
select attrelid::regclass, attname, attcompression from pg_attribute where attrelid in
 ('t3'::regclass::oid, 
 'p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid)
 and attnum > 0;
select relname, relreuseattrs from pg_class where oid in
 ('p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid);

-- p7, p7_1 should reuse, but p7_2 not
ALTER TABLE t3 ALTER COLUMN c SET compression pglz;
select attrelid::regclass, attname, attcompression from pg_attribute where attrelid in
 ('t3'::regclass::oid, 
 'p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid)
 and attnum > 0;
select relname, relreuseattrs from pg_class where oid in
 ('p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid);

-- test not null
ALTER TABLE p7_2 ALTER COLUMN c SET compression pglz;
select relname, relreuseattrs from pg_class where oid in
 ('p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid);
select attrelid::regclass, attname, attnotnull from pg_attribute where attrelid in
 ('t3'::regclass::oid, 
 'p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid)
 and attnum = 3;
ALTER TABLE p7 ALTER COLUMN c SET not null;
select attrelid::regclass, attname, attnotnull from pg_attribute where attrelid in
 ('t3'::regclass::oid, 
 'p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid)
 and attnum = 3;
select relname, relreuseattrs from pg_class where oid in
 ('p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid);
-- drop not null on a leaf
ALTER TABLE p7 ALTER COLUMN c drop not null;
select attrelid::regclass, attname, attnotnull from pg_attribute where attrelid in
 ('t3'::regclass::oid, 
 'p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid)
 and attnum = 3;
select relname, relreuseattrs from pg_class where oid in
 ('p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid);
ALTER TABLE p7_2 ALTER COLUMN c set not null;
select attrelid::regclass, attname, attnotnull from pg_attribute where attrelid in
 ('t3'::regclass::oid, 
 'p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid)
 and attnum = 3;
select relname, relreuseattrs from pg_class where oid in
 ('p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid);
ALTER TABLE p7_2 ALTER COLUMN c drop not null;
select attrelid::regclass, attname, attnotnull from pg_attribute where attrelid in
 ('t3'::regclass::oid, 
 'p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid)
 and attnum = 3;
select relname, relreuseattrs from pg_class where oid in
 ('p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid);

-- test static
ALTER TABLE p7 ALTER COLUMN c set STATISTICS 11;
select attrelid::regclass, attname, attstattarget from pg_attribute where attrelid in
 ('t3'::regclass::oid, 
 'p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid)
 and attnum = 3;
select relname, relreuseattrs from pg_class where oid in
 ('p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid);
ALTER TABLE p7 ALTER COLUMN c set STATISTICS -1;
select attrelid::regclass, attname, attstattarget from pg_attribute where attrelid in
 ('t3'::regclass::oid, 
 'p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid)
 and attnum = 3;
select relname, relreuseattrs from pg_class where oid in
 ('p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid);

-- test IDENTITY
ALTER TABLE t3 ALTER COLUMN b set NOT NULL;
select attrelid::regclass, attname, attidentity from pg_attribute where attrelid in
 ('t3'::regclass::oid, 
 'p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid)
 and attnum = 2;
ALTER TABLE p7 ALTER COLUMN b ADD GENERATED ALWAYS AS IDENTITY;
select attrelid::regclass, attname, attidentity from pg_attribute where attrelid in
 ('t3'::regclass::oid, 
 'p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid)
 and attnum = 2;
select relname, relreuseattrs from pg_class where oid in
 ('p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid);
-- set IDENTITY
ALTER TABLE p7 ALTER COLUMN b SET GENERATED BY DEFAULT;
select attrelid::regclass, attname, attidentity from pg_attribute where attrelid in
 ('t3'::regclass::oid, 
 'p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid)
 and attnum = 2;
select relname, relreuseattrs from pg_class where oid in
 ('p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid);
 -- drop IDENTITY
ALTER TABLE p7 ALTER COLUMN b DROP IDENTITY;
select attrelid::regclass, attname, attidentity from pg_attribute where attrelid in
 ('t3'::regclass::oid, 
 'p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid)
 and attnum = 2;
select relname, relreuseattrs from pg_class where oid in
 ('p7'::regclass::oid, 
 'p7_1'::regclass::oid, 
 'p7_2'::regclass::oid);

drop schema partition_reuse_columns cascade;
