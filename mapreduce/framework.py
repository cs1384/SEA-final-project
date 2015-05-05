# 
import tornado.ioloop
import tornado.web
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
# 
import subprocess
import time
import json
import os
import yaml
#
from fs import DisList, DisTable

class MapReduceFramework:
  workers = []
  def getWorkerInfo(self, jsonPath='address.json'):
    self.workers = json.load(open(jsonPath, "r"))['mapreduce']
    #self.workers = json.load(open(jsonPath, "r"))
    self.nMachines = len(self.workers)
    print self.workers

  def formMapQuery(self, jobTableName, address, mapperPath, inputPath, numReducers):
		return 'http://'+address+'/map?mapperPath='+mapperPath+'&inputFile='+inputPath+'&numReducers='+numReducers+'&jobTableName='+jobTableName

  def taskIDtoString(self, taskIDs):
		return ",".join(taskIDs)

  def formReduceQuery(self, jobTableName, address, reducerIx, reducerPath, mapTaskIDs, outputDir):
		return 'http://'+address+'/reduce?reducerIx='+str(reducerIx)+'&reducerPath='+reducerPath+'&mapTaskIDs='+mapTaskIDs+'&outputDir='+outputDir+'&jobTableName='+jobTableName
  
  @gen.coroutine
  def mapReduce(self, inputDir, mapperPath, nReducers, reducerPath, outputDir):
    if len(self.workers)==0:
      print 'workers infomation has not been set!'
      # tornado.ioloop.IOLoop.current().stop()


		# get all input files
    inputs = []
    for f in os.listdir(inputDir):
      if '.in' in f:
        inputs.append(inputDir+'/'+f)
    print "========= input files =========="
    print inputs
		
		# do mapper's job
    taskIDs = []
    http_client = AsyncHTTPClient()
    idx = 0
    while len(inputs) > 0:
      url = self.formMapQuery(self.workers[idx%self.nMachines], mapperPath, inputs[0], str(nReducers))
      print url
      response = yield http_client.fetch(url)
      temp = json.loads(response.body)
      if temp['status'] == 'success':
        taskIDs.append(temp['mapTaskID'])
        del inputs[0]
      idx += 1
    taskIDs = self.taskIDtoString(taskIDs)
    print "========= number of taskIDs =========="
    print len(taskIDs.split(','))

		# do reducer's job
    print "========= reduce starts =========="
    idx = 0
    num = 0
    future = []
    while num < nReducers:
      url = self.formReduceQuery(self.workers[idx%self.nMachines], num, reducerPath, taskIDs, outputDir)
      print url
      request = tornado.httpclient.HTTPRequest(url=url, connect_timeout=80.0, request_timeout=80.0)
      response = yield tornado.gen.Task(http_client.fetch, request)
      temp = yaml.load(response.body)
      print temp
      if temp['status'] == 'success':
        num += 1
      idx += 1

    print "========= FINISHED! =========="
    tornado.ioloop.IOLoop.current().stop()

  @gen.coroutine
  def mapReduceFS(self, jobTableName, inputDir, mapperPath, nReducers, reducerPath, outputDir):
    if len(self.workers)==0:
      print 'workers infomation has not been set!'
      tornado.ioloop.IOLoop.current().stop()

    # FS: create jobTable
    #test = DisTable({1: 'a', 2: 'b', 3: [1, 2, 3, 4, {5: 'f', 6: 'g', 7: 'h'}]}, tableName='TEST')
    jobTable = DisTable({'test': 'test'}, tableName=jobTableName)
    #jobTable = DisTable({}, tableName=jobTableName)
    print 'go'

    # get all input files
    inputs = []
    for f in os.listdir(inputDir):
      if '.in' in f:
        inputs.append(inputDir+'/'+f)
    print "========= input files =========="
    print inputs
    
    # do mapper's job
    taskIDs = []
    http_client = AsyncHTTPClient()
    idx = 0
    while len(inputs) > 0:
      # let mappers know the jobTableName
      url = self.formMapQuery(jobTableName, self.workers[idx%self.nMachines], mapperPath, inputs[0], str(nReducers))
      print url
      request = tornado.httpclient.HTTPRequest(url=url, connect_timeout=600.0, request_timeout=600.0)
      response = yield tornado.gen.Task(http_client.fetch, request)
      temp = json.loads(response.body)
      print temp
      if temp['status'] == 'success':
        taskIDs.append(temp['mapTaskID'])
        del inputs[0]
      idx += 1
    taskIDs = self.taskIDtoString(taskIDs)
    print "========= number of taskIDs =========="
    print len(taskIDs.split(','))

    # do reducer's job
    print "========= reduce starts =========="
    idx = 0
    num = 0
    future = []
    while num < nReducers:
      # let reducers know the jobTableName and their indices
      url = self.formReduceQuery(jobTableName, self.workers[idx%self.nMachines], num, reducerPath, taskIDs, outputDir)
      print url
      request = tornado.httpclient.HTTPRequest(url=url, connect_timeout=600.0, request_timeout=600.0)
      response = yield tornado.gen.Task(http_client.fetch, request)
      temp = yaml.load(response.body)
      print temp
      if temp['status'] == 'success':
        num += 1
      idx += 1

    print "========= FINISHED! =========="
    tornado.ioloop.IOLoop.current().stop()
