# tornado 
import tornado.ioloop
import tornado.web
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
# system
import json
import subprocess
import uuid
# 
from ..config import settings
from fs import DisList, DisTable

mapResult = {}

class MapHandler(tornado.web.RequestHandler):
  @gen.coroutine
  def get(self):
    # GET information
    mapperPath = self.get_arguments('mapperPath')[0]
    inputPath = str(self.get_arguments('inputFile')[0])
    numReducers = int(self.get_arguments('numReducers')[0])
    jobTableName = self.get_arguments('jobTableName')[0]
    # run mapper
    print 'mapper starts!!'
    file = open(inputPath, 'r')
    content = file.read()
    p = subprocess.Popen(["python", "-m", mapperPath], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    (out, err) = p.communicate(content)
    # if error
    if err:
      res = {"status": "failed"}
      self.write(json.dumps(res))
      return
		
    # get jobTable
    jobTable = DisTable(tableName=jobTableName)
    
    # get task ID
    taskID = str(uuid.uuid4()).replace('-','')
    print type(jobTable)  
    '''  
    newTable = jobTable.fetch_all()
    print type(newTable)
    while taskID in newTable:
      taskID = str(uuid.uuid4()).replace('-','')
    '''
    
    #jobTable[taskID] = { "test": "Calvin rocks." }
    #jobTable[taskID] = {}
    jobTable[taskID] = {}
    print type(jobTable[taskID])


    #jobTable[taskID]['1'] = []
    
    #newTable = jobTable[taskID].fetch_all()
    # create lists for reducers
    for idx in range(numReducers):
      jobTable[taskID][idx] = [] 
      #jobTable[taskID] = { "test": "Calvin rocks." }
      #print jobTable[taskID].length

    print '----------------------------'

    lines = out.split("\n")
    for line in lines:
      if line == '':continue
      temp = line.split(settings.delimiter)
      idx = hash(temp[0])%numReducers
      # update this certain reducer's list
      jobTable[taskID][idx].append(temp)
    
		# sort all lists in result
    print '==== sort'
    newTable = jobTable[taskID].fetch_all()
    for key in newTable.keys():
      lst = newTable[key]
      lst.sort(key=lambda x: x[0])
    jobTable[taskID] = newTable

		# write response 
    print taskID
    res = {"status": "success", "mapTaskID": taskID}
    self.write(json.dumps(res))
    print '----------------------------'

class RetrieveOutputHandler(tornado.web.RequestHandler):
  @gen.coroutine
  def get(self):
    # GET information
    reducerIx = int(self.get_arguments('reducerIx')[0])
    taskID = self.get_arguments('mapTaskID')[0]
    # check availability
    if taskID not in mapResult:
      res = []
      self.write(json.dumps(res))
      return
    # write response 
    res = mapResult[taskID][reducerIx]
    self.write(json.dumps(res))
    # delete retrieved list and even entire task result
    del mapResult[taskID][reducerIx]
    if len(mapResult[taskID]) == 0:
      del mapResult[taskID]


	
	
	
