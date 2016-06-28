# encoding: utf-8
__author__ ='zhangyuanxiang'

from __future__ import generators
from optparse import OptionParser
import sys   
import time
import datetime
import re
try:
     from collections import Ordere
except ImportError:
     from ordereddict import OrderedDict
from pymysql.cursors import DictCursorMixin, Cursor

def get_cli_options():
    parser = OptionParser(usage="usage: python %prog [options]",
                          description=""" Data Transfer """)

    parser.add_option("--from", "--from_dsn",
                      dest="from_dsn",
                      default="local2",
                      help="host:port:user:pass:db:tablename"
                     )
    
    parser.add_option("--to", "--to_dsn",
                      dest="to_dsn",
                      default="local2",
                      help="host:port:user:pass:db:tablename")

    parser.add_option("--where", "--sql_where",
                      dest="where",
                      default="1=1",
                      help="where")
    
    parser.add_option("--sid", "--sid",
                      dest="sid",
                      default="orcl",
                      help="Oracle SID")

    parser.add_option("--type", "--type",
                      dest="dbtype",
                      default="NULL",
                      help="o2mongo  m2mongo  s2mongo"
                      )
    (options, args) = parser.parse_args()

 
    return options



class  excutemysqlstr():

    def __init__(self,str1,str2,sid):
        self.host=str1.strip().split(':')[0]
        self.port=str1.strip().split(':')[1]
        self.user=str1.strip().split(':')[2]
        self.passwd=str1.strip().split(':')[3]
        self.dbname=str1.strip().split(':')[4]
        self.table= str1.strip().split(':')[5]
        self.oracle_sid=sid
        self.dbtype=str2

    def gen_cursor(self):
          if self.dbtype.lower()=="o2mongo" or self.dbtype.lower()=="m2mongo" \
             or self.dbtype.lower()=="s2mongo":
             import pymongo
             from pymongo import MongoClient
             from bson.objectid import ObjectId
             from optparse import OptionParser
             from pymongo import ReadPreference
             client  = MongoClient(self.host+':'+self.port,document_class=OrderedDict)
             client['admin'].authenticate(self.user,self.passwd)
             db=client[self.dbname]
             collection=db[self.table]
             return collection
          else:
             "error"
class query_data():

    def __init__(self,str1,str2,sid,where):

        self.host=str1.strip().split(':')[0]
        self.port=str1.strip().split(':')[1]
        self.user=str1.strip().split(':')[2]
        self.passwd=str1.strip().split(':')[3]
        self.dbname=str1.strip().split(':')[4]
        self.table= str1.strip().split(':')[5]
        self.oracle_sid=sid
        self.dbtype=str2
        self.where=where

    def gen_cursor(self):
         dbtype=self.dbtype.lower()  
         if dbtype=="m2mongo":
             import pymysql as dbapi
             import pymysql.cursors
             from pymysql.constants import FIELD_TYPE
             try:
               conn = dbapi.connect(host=self.host,port=int(self.port),user=self.user,passwd=self.passwd,db=self.dbname,charset='UTF8',\
                                     cursorclass=dbapi.cursors.SSCursor)
             except dbapi.Error, e:
               print "Error connecting %d: %s" % (e.args[0], e.args[1])
             return conn 
         elif dbtype=="o2mongo":
             import cx_Oracle as dbapi
             try:
               dsn_tns = dbapi.makedsn(self.host,self.port,self.oracle_sid)
               conn = dbapi.connect(self.user,self.passwd,dsn_tns)
             except dbapi.Error, e:
               print "Error connecting %d: %s" % (e.args[0], e.args[1])
             return conn
         elif dbtype=="s2mongo":
              import pymssql as dbapi
              try:
                conn = dbapi.connect(server=self.host,port=self.port,user=self.user,password=self.passwd,database=self.dbname,charset="utf8")
              except dbapi.Error, e:
                print "Error connecting %d: %s" % (e.args[0], e.args[1])
              return conn
         else:
           print "input source database type"
     
    def gen_cnt(self):
        dbtype=self.dbtype.lower()
        start=0
        end=0
        sqlstr="select * from " 
        if dbtype=="s2mongo" or dbtype=="m2mmongo":
           query_str=sqlstr+" "+self.table +" "+"where"+" "+self.where
        else:
           query_str=sqlstr+" "+self.dbname+'.'+self.table +" "+"where"+" "+self.where
        #print query_str
        return query_str

    def query_db(self):
        mysql_cursor=self.gen_cursor().cursor()
        sqlstr=self.gen_cnt()
        return  sqlstr
    def Getdbtype(self):
        dbtype=self.dbtype.lower()
        return dbtype
def import_data():
    options = get_cli_options()
    excute_str=excutemysqlstr(options.to_dsn,options.dbtype,options.sid) 
    query_str=query_data(options.from_dsn,options.dbtype,options.sid,options.where)
    result= []   
    count=0
    batch=10000
    cnt=5
    print "start................"
    selectsql=query_str.query_db()
    tocursor =excute_str.gen_cursor()
    fromcursor = query_str.gen_cursor().cursor()
    fromcursor.execute(selectsql)
    result = fromcursor.fetchmany(batch)
    columns = [i[0] for i in fromcursor.description]
    result =[OrderedDict(zip(columns, row)) for row in result]

    while result: 
          lg=len(result)
          try:
             tocursor.insert_many(result,ordered=False)
             count+=1
             dt=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
             if (lg==batch and (count%cnt)==0):
                print "%s commited %s" %(count*batch,dt)
             elif(lg != batch):
                print "%s commited %s" %((count-1)*batch+lg,dt)
             else:
                pass
          except (KeyboardInterrupt, SystemExit):
                 raise
          except Exception,e:
               print Exception,":",e
          result = fromcursor.fetchmany(batch)
          columns = [i[0] for i in fromcursor.description]
          result =[OrderedDict(zip(columns, row)) for row in result]
def main():
    import_data()

if __name__ == '__main__':
       main()
