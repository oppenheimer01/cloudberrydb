-- start_matchsubs
-- s/database oid:\d+/database oid:#####/
-- m/database oid:\s+[0-9].*/
-- s/database oid:\s+[0-9].*/database oid:/
-- end_matchsubs
set tde_get_master_key_from_cm to false;
DROP DATABASE IF EXISTS encryptdb1;

CREATE DATABASE encryptdb1 WITH ENCRYPTION_ENABLE 'aes' TABLESPACE regress_oss_test;

select datname,is_encrypt,dek_version from hashdata_encrypt_database where datname = 'encryptdb1';

\c encryptdb1
set tde_get_master_key_from_cm to false;
set warehouse to test;

-- success
create table t1 (id int) with (storage_format=vbf) tablespace regress_oss_test;

insert into t1 select generate_series(1,5);

select * from t1 order by id;

set debug_tde_print_encrypt_data = true;

insert into t1 select generate_series(6,10);

select * from t1 order by id;

drop table t1;

-- fail, do not setting the tablespace of database, can not create the encrypted db.
reset default_tablespace;
CREATE DATABASE encryptdb3 WITH ENCRYPTION_ENABLE 'aes';

\c postgres
set tde_get_master_key_from_cm to false;
DROP DATABASE encryptdb1;