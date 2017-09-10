connect sys/welcome1@//ora122server/orcl as sysdba;

ALTER SYSTEM SWITCH LOGFILE;

-- truncate all tables

connect anpr/anpr@//ora122server/soe;

TRUNCATE TABLE ANPR_RELATIONAL reuse storage;
TRUNCATE TABLE ANPR_COLLECTION reuse storage;
TRUNCATE TABLE SIMPLETABLE reuse storage;
TRUNCATE TABLE INSERTABLE reuse storage;

connect sys/welcome1@//ora122server/orcl as sysdba;

ALTER SYSTEM SWITCH LOGFILE;

EXIT;