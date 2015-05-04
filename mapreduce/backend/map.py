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

mapResult = {}

class MapHandler(tornado.web.RequestHandler):
  @gen.coroutine
  def get(self):
    # GET information
    mapperPath = self.get_arguments('mapperPath')[0]
    inputPath = str(self.get_arguments('inputFile')[0])
    numReducers = int(self.get_arguments('numReducers')[0])
    # run mapper
    file = open(inputPath, 'r')
    content = file.read()
    p = subprocess.Popen(["python", "-m", mapperPath], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    (out, err) = p.communicate(content)
    # if error
    if err:
      res = {"status": "failed"}
      self.write(json.dumps(res))
      return
		# constitute the task result
    result = {}
    for idx in range(numReducers):
      result[idx] = []
    lines = out.split("\n")
    for line in lines:
      if line == '':continue
      temp = line.split(settings.delimiter)
      idx = hash(temp[0])%numReducers
      result[idx].append(temp)
		# sort all lists in result
    for key in result.keys():
      lst = result[key]
      lst.sort(key=lambda x: x[0])
		# write the task result to gloabal memory 
    taskID = str(uuid.uuid4()).replace('-','')
    while taskID in mapResult:
      taskID = str(uuid.uuid4()).replace('-','')
    mapResult[taskID] = result
		# write response 
    print taskID
    res = {"status": "success", "mapTaskID": taskID}
    self.write(json.dumps(res))

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

	
	
	
