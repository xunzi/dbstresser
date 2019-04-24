#!/usr/bin/env python

import random
import time
import os
import sys
import argparse
import multiprocessing
import string

q = multiprocessing.Queue()

def init_words(infile):
	with open(infile) as fh:
		words = fh.read().split()
		fh.seek(0)
		sentences = fh.read().split(".")
		fh.close()
	return (words,sentences)	

def mysql_connect():
        _con = mysql.connect(args.dbhost, args.dbuser, args.dbpass, args.dbname)
        _cur = _con.cursor()
        return (_con,_cur)

def debugprint(msg):
	if args.debug:
		sys.stdout.write("%s\n" % msg)

def oracle_connect():
        _dsn = cx_Oracle.makedsn(args.dbhost, 1521, args.dbname)
        debugprint(_dsn)
        _con = cx_Oracle.connect(args.dbuser, args.dbpass, _dsn)
        _cur = _con.cursor()
        return(_con, _cur)

def insert_data(tablename, flavour):
	start = time.time()

        if flavour == "mysql":
                _sql = "INSERT INTO %s" % tablename 
                _sql += "(txt, txt2) VALUES (%s, %s)"
                (_con, _cur) = mysql_connect()
        elif flavour == "oracle":
                (_con, _cur) = oracle_connect()
                _sql = """INSERT INTO %s (txt, txt2) VALUES (:1, :2)""" % tablename
	_counter = 0
	for i in range(args.numrows):
		s1 = random.choice(sentences)
		s1 = s1.strip()
		if len(s1) > 1024:
			s1 = s1[:1023]
		w2 = random.choice(words)
		w2 = w2.strip()
                if flavour == "oracle":
                        _res = _cur.execute("SELECT %s_seq.nextval FROM dual" % tablename)
                        _id = _res.fetchone()[0]
                else:
                        _id = _con.insert_id()
                #print _sql        
                _cur.execute(_sql, (s1,w2))
		if _id % args.checkpoint == 0:
			_con.commit()
                        if args.log:
                                log2db(_cur, _id, "%s|%s" % (s1,w2))
                                _con.commit()
		_counter += 1
	_cur.close()
        _con.close()
	end = time.time()
	return (end - start)

def log2db(cursor,insertid,message):
        _sql = "INSERT INTO %slog" % table
        _sql += """(insertid, pid, logmessage) VALUES (%s, %s, %s)""" 
        cursor.execute(_sql, (insertid, os.getpid(), message))        

def insert_job(pid, q, tablename, flavour):
	debugprint("Process ID %d, PID %d" % (pid, os.getpid()))
	q.put(insert_data(tablename, flavour))

def init_table(flavour, tname):
        drop_table_mysql = """ DROP TABLE IF EXISTS %s;""" % tname
	create_table_mysql = """ CREATE TABLE %s (
        id bigint(8) NOT NULL AUTO_INCREMENT,
        ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        txt varchar(1024) DEFAULT NULL,
        txt2 varchar(1024) DEFAULT NULL,
        PRIMARY KEY (id),
        KEY txt_idx (txt(15)),
        KEY txt2_idx (txt2(15))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;""" % tname
        
        create_log_table_mysql = """ CREATE TABLE %slog (
        id bigint(8) NOT NULL AUTO_INCREMENT,
        ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        insertid bigint(8),
        pid bigint(6),
        logmessage varchar(2048),
        PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;""" % tname

        drop_table_oracle = """DROP TABLE %s""" % tname
        
        create_table_oracle = """CREATE TABLE %s (
        id NUMBER(8) NOT NULL PRIMARY KEY,
        txt VARCHAR(1024),
        txt2 VARCHAR(1024),
        ts TIMESTAMP DEFAULT SYSTIMESTAMP
        ) TABLESPACE USERS""" % tname

        create_log_table_oracle = """CREATE TABLE %slog (
        ts TIMESTAMP DEFAULT SYSTIMESTAMP,
        insertid NUMBER(8),
        pid NUMBER(6),
        logmessage VARCHAR(2048)
        ) TABLESPACE USERS""" % tname
        
        create_sequence_oracle = """CREATE SEQUENCE %s_seq START WITH 1 INCREMENT BY 1""" % tname
        
        create_trigger_oracle = """CREATE OR REPLACE TRIGGER %s_trig
        BEFORE INSERT ON %s
        FOR EACH ROW
        BEGIN
         SELECT %s_seq.NEXTVAL
         INTO :new.id
         FROM dual;
        END
        /"""   % (tname, tname, tname)

        drop_table_oracle = """DROP TABLE %s """ % tname
        
        drop_sequence_oracle = """DROP SEQUENCE %s""" % tname
        
        drop_trigger_oracle = """DROP TRIGGER %s""" % tname
        if flavour=="mysql":        
                (_con,_cur) = mysql_connect()
                _cur.execute(drop_table_mysql)
                _cur.execute(create_table_mysql)
        elif flavour == "oracle":
                (_con, _cur) = oracle_connect()
                try:
                        _cur.execute(drop_table_oracle)
                        _cur.execute(drop_sequence_oracle)
                        _cur.execute(drop_trigger_oracle)
                except cx_Oracle.DatabaseError:
                        print "Exception occurred"
                debugprint ("Creating Table %s, Sequence %s_seq and Trigger %s_trig" % (tname, tname, tname))
                _cur.execute(create_table_oracle)
                _cur.execute(create_sequence_oracle)
                _cur.execute(create_trigger_oracle)
        else:
                print "Unknown DB Type, exiting"
                sys.exit(3)
        if args.log:
                if args.mode == "mysql":
                        _cur.execute(create_log_table_mysql)                        
                elif args.mode == "oracle":
                        debugprint (create_log_table_oracle)
                        _cur.execute(create_log_table_oracle)
                debugprint( "created logging table %slog" % tname)
	_cur.close()
        _con.close()
	return tname

        


def drop_table(tname):
	_sql = "DROP TABLE %s" % tname
	(_con,_cur) = mysql_connect()
	_cur.execute(_sql)
	debugprint ("dropped table %s" %  tname)
	_cur.close()
        _con.close()

def delete_random_lines(tname, deletions):
        print "delete job running as PID %d" % os.getpid()
        _start = time.time()
        _sql = "DELETE FROM %s" % tname
        _sql += " WHERE id = %s"
        (_con,_cur) = mysql_connect()
        for i in range(deletions):
                #print _sql
                _cur.execute(_sql, (random.randint(1,args.numrows),))
                #time.sleep(1)
        _end = time.time()
        _runtime = _end - _start
        debugprint( "deleted %d rows in %d seconds" % (deletions, _runtime))
        _cur.close()
        _con.close()


def save_perfvalues(target, numrows, checkpoint, avgruntime):
        pass
        
if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Inserts random data into table")
	parser.add_argument("-P", "--parallel", help = "Parallelism", default = 1, dest = "parallel", type = int)
	parser.add_argument("-f", "--file", help = "file to use as input", dest = "infile")
	parser.add_argument("-u", "--user", help = "User name to connect to the DB", dest = "dbuser")
	parser.add_argument("-p", "--password", help = "Password for the DB", dest = "dbpass")
	parser.add_argument("-d", "--database", help = "Database Name", dest = "dbname", default = "test")
	parser.add_argument("-H", "--host", help = "Host to connect to", dest = "dbhost", default = "localhost")
	parser.add_argument("-t", "--table", help = "Table Name, will be generated if empty", dest = "tname", default = "stresser%s" % time.strftime("%s"))
	parser.add_argument("-n", "--numrows", help = "Number of rows to be inserted per process, i.e. the number of insertions will be Parallelism * Number of rows", dest = "numrows", default = 1000, type = int)
	parser.add_argument("-c", "--checkpoint", help = "Commit every checkpoint insertion", dest = "checkpoint", default = 10, type = int)
	parser.add_argument("-D", "--drop", help = "Drop table after completion", dest = "drop", default = False, action = "store_true")
        parser.add_argument("-l", "--log", help = "Log every checkpoint commit to table named <table>log", dest = "log", default = False, action = "store_true")
        parser.add_argument("-e", "--delete", help = "number of  random entries to be deleted", dest = "delete", type = int, default = 0)
        parser.add_argument("-m", "--mode", help = "database mode, mysql or oracle, default = mysql", dest = "mode", default="mysql")
	parser.add_argument("-V", "--debug", help = "Debug Mode, verbose output", dest = "debug", default = False, action = "store_true")

	args = parser.parse_args()

        if args.mode == "mysql":
                import MySQLdb as mysql
        elif args.mode == "oracle":
                import cx_Oracle
        else:
                sys.stderr.write("No valid db type, should be either mysql or oracle\n")
                sys.exit(1)
	table = init_table(flavour=args.mode, tname=args.tname)                
	debugprint("Created table %s" % table)
	(words, sentences) = init_words(args.infile)
	debugprint( "%d words and %d sentences" % (len(words), len(sentences)))
	debugprint ("running %d parallel insert jobs inserting %d rows each and committing every %d transaction against table %s" % (args.parallel, args.numrows, args.checkpoint, table))
	plist = []
        debugprint ("starting insertion jobs")
	for i in range(args.parallel):
		p = multiprocessing.Process(target=insert_job, args=(i,q, table, args.mode))
		plist.append(p)
		p.start()
        if args.delete:
                debugprint( "starting delete job")
                p = multiprocessing.Process(target=delete_random_lines, args=(table, args.delete))
                p.start()
	for proc in plist:
		p.join()
        
        
	results = [q.get() for proc in plist]
	if args.drop:
		drop_table(table)
                drop_table("%slog" % table)
        print "average insertion time per job: %s seconds" % (sum(results) / args.parallel)
