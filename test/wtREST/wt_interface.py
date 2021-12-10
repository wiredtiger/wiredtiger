#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function
from wt_lib_setup import setupLib
from web import wtREST
import glob, json, os, random, re, sys

# This function sets up the WiredTiger table and open a cursor to be used by the 
# REST server.
def runREST():

    # Setup the paths required for importing wiredtiger python interface
    setupLib()
    import wiredtiger
    
    conn_config = "cache_size=1GB,create=true,in_memory=true"
    table_config = "key_format=S,value_format=S"
    table_name = 'cache'
    uri='table:' + table_name
    pid = os.getpid()
    dir_name = 'TEST_DIR_' + str(pid)
    curdir = os.getcwd()
    dir = curdir + '/' + dir_name
    if not os.path.exists(dir):
        os.mkdir(dir)
    conn = wiredtiger.wiredtiger_open(dir, conn_config)
    session = conn.open_session()
    session.create(uri, table_config)
    cursor = session.open_cursor(uri)

    # Start the REST server. Pass the WT cursor, port and the table name.
    server = wtREST(cursor, 5000, table_name)
    server.run()

if __name__ == '__main__':
    runREST()
