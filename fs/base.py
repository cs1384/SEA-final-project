import uuid
from tornado.httpclient import HTTPClient, AsyncHTTPClient
from tornado import gen

import fs

try:
  import cPickle as pickle
except:
  import pickle

def formatQuery(host, type, args):
  return '{0}/fs?type={1}&param={2}'.format(host, type, pickle.dumps(args))

def formatEntry(val, indent):
  return '\t' * indent + str(val) + '\n'

def prettyPrint(dic, indent):
  ret = ''
  for key, value in dic.iteritems():
    ret += formatEntry(key, indent)
    if isinstance(value, dict):
      ret += prettyPrint(value, indent+1)
    else:
      ret += formatEntry(value, indent+1)

  return ret

class TableName(object):
  def __init__(self, name):
    self.tableName = name

  @property
  def name(self):
    return self.tableName

class DisObj(object):
  pass

'''
Distributed list Object
'''
class DisList(DisObj):
  def __init__(self, initVal=[], tableName=None, ref=True):
    if tableName is None:
      self.name = TableName(uuid.uuid4().hex)
    else:
      self.name = TableName(tableName)
    self.master = fs.INVENTORY.getMaster()

    newInitVal = {}
    for idx in xrange(len(initVal)):
      if isinstance(initVal[idx], dict):
        newInitVal[idx] = DisTable(initVal[idx])
      elif isinstance(initVal[idx], list):
        newInitVal[idx] = DisList(initVal[idx])
      else:
        newInitVal[idx] = initVal[idx]

    #Create request to master
    if len(newInitVal.keys()) > 0 or ref == False:
      param = {'tableName': self.name, 'initVal': newInitVal}
      HTTPClient().fetch(formatQuery(self.master, 'create', param))

  '''
  Getter
  returns - any, data of this list given idx
  '''
  def __getitem__(self, idx):
    param = {'tableName': self.name, 'key': idx}
    res = HTTPClient().fetch(formatQuery(self.master, 'get', param))
    return pickle.loads(res.body)

  '''
  Setter
  param idx - int, position to set value
  param val - any, value to be updated
  '''
  def __setitem__(self, idx, val):
    if isinstance(val, dict):
      newVal = DisTable(initVal=val, ref=False)
    elif isinstance(val, list):
      newVal = DisList(initVal=val, ref=False)
    else:
      newVal = val

    param = {'tableName': self.name, 'key': idx, 'val': val}
    return HTTPClient().fetch(formatQuery(self.master, 'set', param)).body

  '''
  Append val into the last position of list
  param val - any, value to be appended
  returns - int, idx of appended position
  '''
  def append(self, val):
    if isinstance(val, list):
      newVal = DisList(val)
    elif isinstance(val, dict):
      newVal = DistTable(val)
    else:
      newVal = val

    param = {'tableName': self.name, 'val': newVal}
    return int(HTTPClient().fetch(formatQuery(self.master, 'append', param)).body)
  '''
  Extend the list with new val
  param val - any, value to be extended into list
  '''
  def extend(self, val):
    ret = 0
    if isinstance(val, list):
      for v in val:
        ret = self.append(v)
    else:
      ret = self.append(val)

    return ret

  @property
  def length(self):
    param = {'tableName': self.name}
    return int(HTTPClient().fetch(formatQuery(self.master, 'len', param)).body)

  def fetch_all(self):
    param = {'tableName': self.name}
    client = HTTPClient()
    responses = pickle.loads(client.fetch(formatQuery(self.master, 'fetch_all', param)).body)

    dic = {}
    for val in responses:
      dic.update(val)

    ret = [(k, dic[k]) for k in dic]
    sorted(ret, lambda x, y: x[0] > y[0])
    ret = [x[1] for x in ret]

    for idx in xrange(len(ret)):
      if isinstance(ret[idx], DisObj):
        ret[idx] = ret[idx].fetch_all()

    return ret

  '''
  Remove given value in the list
  param val - any, the value to be removed from list
  param globl - Boolean, Optional value indicating this 
                operation is a global remove or just remove first value in list
  '''
  def remove(self, val, globl=False):
    raise NotImplementedError

  '''
  def __str__(self):
    raise NotImplementedError
  '''

'''
Distributed table Object
'''
class DisTable(DisObj):
  def __init__(self, initVal={}, tableName=None, ref=True):
    if tableName is None:
      self.name = TableName(uuid.uuid4().hex)
    else:
      self.name = TableName(tableName)
    self.master = fs.INVENTORY.getMaster()

    newInitVal = {}
    for key in initVal:
      if isinstance(initVal[key], dict):
        newInitVal[key] = DisTable(initVal[key])
      elif isinstance(initVal[key], list):
        newInitVal[key] = DisList(initVal[key])
      else:
        newInitVal[key] = initVal[key]

    #Create request to master
    if len(newInitVal.keys()) > 0 or ref == False:
      param = {'tableName': self.name, 'initVal': newInitVal}
      HTTPClient().fetch(formatQuery(self.master, 'create', param))

  def getName(self):
    return self.name

  '''
  Getter
  returns - any, data of this table given key
  '''
  def __getitem__(self, key):
    param = {'tableName': self.name, 'key': key}
    res = HTTPClient().fetch(formatQuery(self.master, 'get', param))
    return pickle.loads(res.body)

  '''
  Setter
  param key - key to index updated value
  param val - value to be updated into table
  '''
  def __setitem__(self, key, val):
    if isinstance(val, dict):
      newVal = DisTable(initVal=val, ref=False)
    elif isinstance(val, list):
      newVal = DisList(initVal=val, ref=False)
    else:
      newVal = val

    param = {'tableName': self.name, 'key': key, 'val': newVal}
    return HTTPClient().fetch(formatQuery(self.master, 'set', param)).body

  @property
  def length(self):
    param = {'tableName': self.name}
    return int(HTTPClient().fetch(formatQuery(self.master, 'len', param)).body)

  def fetch_all(self):
    param = {'tableName': self.name}
    client = HTTPClient()
    responses = pickle.loads(client.fetch(formatQuery(self.master, 'fetch_all', param)).body)

    ret = {}
    for val in responses:
      ret.update(val)

    for key in ret:
      if isinstance(ret[key], DisObj):
        ret[key] = ret[key].fetch_all()

    return ret

  #TODO: Below needs to be implemented

  '''
  Pop data of this key
  param key - The key of data to be removed and returned
  returns - AnyType, whatever value stored with key, None if there's no such value
  '''
  def pop(self, key):
    raise NotImplementedError

  '''
  Determine if table already has this key
  param key - The key to check
  returns - Boolean, True if this key is already in the table, False otherwise
  '''
  def hasKey(self, key):
    raise NotImplementedError

  '''
  def __str__(self):
    raise NotImplementedError
  '''
