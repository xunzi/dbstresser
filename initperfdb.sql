-- sqlite3
-- drop table performance;
create table performance (
       target varchar(20),
       insertions bigint,
       workers int(3),
       commits bigint,
       rundate date,
       runtime bigint       
       );
