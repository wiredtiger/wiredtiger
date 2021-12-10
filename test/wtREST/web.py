#!/usr/bin/env python
# encoding: utf-8
import json
from flask import Flask, request, jsonify
from wt_lib_setup import setupLib

# I have made the WT cursor a global variable because for some reason I haven't been
# able to pass variable to function that reposond to end node requests.
wtCursor = None

class wtREST:
    
    listenPort = None
    wtTableName = None
    flaskApp = None

    def __init__(self, _wtCursor, _listenPort, _wtTableName):
        setupLib()
        import wiredtiger
        self.listenPort = _listenPort
        global wtCursor
        wtCursor = _wtCursor
        self.wtTableName = _wtTableName
        
        # Setup Flask REST server
        self._setupFlask()
    
    def _setupFlask(self):
        self.flaskApp = Flask(__name__)
        @self.flaskApp.route('/' + self.wtTableName, methods=['GET'])
        def read():
            try:
                key = str(request.args.get('key'))
                wtCursor.set_key(key)
            except:
                return 'Bad request\n', 400 
            ret = wtCursor.search()
            if (ret == 0):
                val = wtCursor.get_value()
                return val + '\n'
            return 'Key not found\n',404

        @self.flaskApp.route('/' + self.wtTableName, methods=['POST'])
        def write():
            try:
                key = str(request.args.get('key'))
                val = str(request.args.get('val'))
            except:
                return 'Bad request\n', 400 
            wtCursor.set_key(key)
            ret = wtCursor.search()
            wtCursor.set_value(val)
            if (ret == 0):
                wtCursor.insert()
            else:
                wtCursor.update()
            return 'Done\n', 200
        
        @self.flaskApp.route('/' + self.wtTableName, methods=['DELETE'])
        def delete():
            try:
                key = str(request.args.get('key'))
                wtCursor.set_key(key)
            except:
                return 'Bad request\n', 400 
            ret = wtCursor.search()
            if (ret == 0):
                wtCursor.remove();
                return 'Done\n', 200
            return 'Key not found\n',404

    def run(self):
        print ("Starting WT REST server @ Port: " + str(self.listenPort))
        self.flaskApp.run(port=self.listenPort)
