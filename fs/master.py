from tornado.httpclient import AsyncHTTPClient, HTTPResponse, HTTPClient
from tornado import gen
from tornado.concurrent import TracebackFuture as Future
import tornado.web

import cStringIO, os

from base import TableName, DisTable

try:
  import cPickle as pickle
except:
  import pickle

'''
Definition of Master's file system handler
'''

def formatQuery(host, type, args):
  return '{0}/fs?type={1}&param={2}'.format(host, type, pickle.dumps(args))

class FSMaster(object):
  def __init__(self, host, workers):
    self.host = host
    self.workers = workers
    self.num_workers = len(workers)
    self.tables = {}
    self.client = AsyncHTTPClient()

  def create(self, param, req):
    tableName = param['tableName'].name
    initVal = param['initVal']
    if tableName in self.tables:
      raise KeyError, 'Table name already existed'

    #print 'MASTER CREATE:', initVal
    self.tables[tableName] = {'len': 0, }
    #Data Partition
    vals = {}
    for worker_id in xrange(self.num_workers):
      vals[worker_id] = {}
    for key in initVal:
      #Bookkeeping
      self.tables[tableName]['len'] += 1
      self.tables[tableName][key] = type(initVal[key])
      worker = hash(key) % self.num_workers
      vals[worker][key] = initVal[key]

    futures = []
    for worker_id in xrange(self.num_workers):
      args = {'tableName': tableName, 'initVal': vals[worker_id]}
      futures.append(self.client.fetch(formatQuery(self.workers[worker_id], 'create', args)))

    fu = Future()
    req.url = self.host
    fu.set_result(HTTPResponse(req, 200, buffer=cStringIO.StringIO('OK')))
    return fu

  def get(self, param, req):
    tableName = param['tableName'].name
    key = param['key']
    if not tableName in self.tables:
      raise KeyError, 'Table not found!'

    worker = self.workers[hash(key) % self.num_workers]
    args = {'tableName': tableName, 'key': key}

    #print 'MASTER -> {0} GET: {1} with key {2}'.format(worker, tableName, key)

    return self.client.fetch(formatQuery(worker, 'get', args))

  def set(self, param, req):
    tableName = param['tableName'].name
    key = param['key']
    val = param['val']
    if not tableName in self.tables:
      raise KeyError, 'Table not Found!'
    worker = self.workers[hash(key) % self.num_workers]
    args = {'tableName': tableName, 'key': key, 'val': val}

    print 'MASTER -> {0} SET: {1} with key {2} to {3}'.format(worker, tableName, key, val)

    futures = []
    futures.append(self.client.fetch(formatQuery(worker, 'set', args)))

    fu = Future()
    req.url = self.host
    fu.set_result(HTTPResponse(req, 200, buffer=cStringIO.StringIO('OK')))
    return fu

  def remove(self, param, req):
    tableName = param['tableName'].name
    key = param['key']
    if not tableName in self.tables:
      #Silently fails
      return

    args = {'tableName': tableName, 'key': key}
    futures = []
    for worker in self.workers:
      futures.append(self.client.fetch(formatQuery(worker, 'remove', args)))
    if key is None:
      del self.tables[tableName]

    fu = Future()
    req.url = self.host
    fu.set_result(HTTPResponse(req, 200, buffer=cStringIO.StringIO('OK')))
    return fu

  def append(self, param, req):
    tableName = param['tableName'].name
    val = param['val']
    if not tableName in self.tables:
      raise KeyError, 'Table Not Found!'

    args = {'tableName': tableName, 'key': self.tables[tableName]['len'], 'val': val}
    worker = self.workers[hash(self.tables[tableName]['len']) % self.num_workers]

    #print 'MASTER -> {0} Append: {1} to key {2}'.format(worker, val, self.tables[tableName]['len'])
    self.client.fetch(formatQuery(worker, 'set', args))

    fu = Future()
    req.url = self.host
    fu.set_result(HTTPResponse(req, 200, buffer=cStringIO.StringIO(str(self.tables[tableName]['len']))))
    self.tables[tableName]['len'] += 1
    return fu

  def len(self, param, req):
    tableName = param['tableName'].name
    if not tableName in self.tables:
      raise KeyError, 'Table Not Found'

    fu = Future()
    req.url = self.host
    fu.set_result(HTTPResponse(req, 200, buffer=cStringIO.StringIO(str(self.tables[tableName]['len']))))
    return fu

  def fetch_all(self, param, req):
    tableName = param['tableName'].name
    if not tableName in self.tables:
      raise KeyError, 'Table not Found!'

    args = {'tableName': tableName}
    futures = []
    for worker in self.workers:
      futures.append(self.client.fetch(formatQuery(worker, 'fetch_all', args)))

    return futures
