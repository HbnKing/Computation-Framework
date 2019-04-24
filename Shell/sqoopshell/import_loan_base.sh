#!/bin/bash
url="jdbc:oracle:thin:@自己的数据库IP:1521:zdxdb"
database="XD_CORE"
tables=("INCREAT_TABLE")
tables_num=${#tables[@]}
username="frontbank"
password="sdff23s"
for((i=0;i<tables_num;i++));
do
sqoop import \
--connect ${url} \
--username ${username} \
--password ${password} \
--query 'SELECT * FROM  FRONTBANK.INCREAT_TABLE where 1=1 and $CONDITIONS' \
--target-dir /user/hzp/test1 \
--hive-drop-import-delims \
--fields-terminated-by ',' \
--m 1 \
--split-by id \
--hive-import \
--hive-overwrite \
--hive-database test \
--create-hive-table \
--hive-table ${tables[i]} \
--null-non-string '\\N' \
--null-string '\\N' \
--verbose 
done