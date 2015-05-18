--oracle sql dialect

DROP TABLE stress123;
DROP SEQUENCE stress123_seq;

-- table
CREATE TABLE stress123 (
       id NUMBER(8),
       txt VARCHAR(1024),
       txt2 VARCHAR(1024),
       ts TIMESTAMP DEFAULT SYSTIMESTAMP,
       CONSTRAINT id_pk PRIMARY KEY(id))
       TABLESPACE users;

-- sequence
CREATE SEQUENCE stress123_seq START WITH 1 INCREMENT BY 1;


-- trigger
CREATE OR REPLACE TRIGGER stress123_trig
BEFORE INSERT ON stress123
FOR EACH ROW

BEGIN
	SELECT stress123_seq.NEXTVAL
	INTO :new.id
	FROM dual;
END;

/
