# tornado 
import tornado.ioloop
import tornado.web
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
# system
import json
import yaml
import subprocess
import os
import pickle
from heapq import merge
# 
from ..config import settings
from fs import DisList, DisTable

class ReduceHandler(tornado.web.RequestHandler):
  @gen.coroutine
  def get(self):
  		
    # GET information
    reducerIx = int(self.get_arguments('reducerIx')[0])
    reducerPath = self.get_arguments('reducerPath')[0]
    mapTaskIDs = self.get_arguments('mapTaskIDs')[0].split(',')
    outputDir = self.get_arguments('outputDir')[0]
    jobTableName = self.get_arguments('jobTableName')[0]

    # get jobTable
    jobTable = DisTable(tableName=jobTableName)
    print type(jobTable)

    '''
    # fetch data from mappers
    future = []
    http_client = AsyncHTTPClient()
    for address in self.application.inventory:
      for taskID in mapTaskIDs:
        url = self.formFetchQuery(address, reducerIx, taskID)
        #print url
        future.append(http_client.fetch(url))

    # merge results
    data = []
    response = yield future
    for r in response:
      data = list(merge(data, json.loads(r.body)))
    '''

    # get mapper results directly from the jobTable
    data = []
    for taskID in mapTaskIDs:
      print taskID
      lst = jobTable[taskID][reducerIx].fetch_all()
      data = list(merge(data, lst))

    # run reducers
    inputString = '\n'.join(settings.delimiter.join(s for s in pair) for pair in data)
    p = subprocess.Popen(["python", "-m", reducerPath], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    (out, err) = p.communicate(inputString.encode('utf-8'))
		
		# write to the jobTable
    if out:
      print type(out)
      something = pickle.loads(out)
      jobTable[reducerIx] = something

      '''
      if not os.path.exists(outputDir): os.mkdir(outputDir)
      path = outputDir + '/' + str(reducerIx) + '.out'
      file = open(path, 'w')
      file.write(str(out))
      file.close()
      #print 'here'
      '''
      res = {'status': 'success'}
      self.write(json.dumps(res))
      print 'reducer done'
      return 
    else:
      res = {"status": "failed"}
      self.write(json.dumps(res))
      return

  def formFetchQuery(self, address, ix, id):
    return 'http://'+address+'/retrieveMapOutput?'+'reducerIx='+str(ix)+'&mapTaskID='+str(id)

	
	
