import tornado.web
from tornado import gen

try:
  import cPickle as pickle
except:
  import pickle

class FSHandler(tornado.web.RequestHandler):
  def initialize(self, processObj):
    self.processObj = processObj
    #Registered methods
    self.method = {
      'create': self.processObj.create,
      'get': self.processObj.get,
      'set': self.processObj.set,
      'remove': self.processObj.remove,
      'append': self.processObj.append,
      'fetch_all': self.processObj.fetch_all,
      'len': self.processObj.len
    }

  @gen.coroutine
  def get(self):
    type = self.get_argument('type')
    param = pickle.loads(str(self.get_argument('param')))
    response = yield self.method[type](param, self.request)

    if isinstance(response, list):
      ret = [pickle.loads(r.body) for r in response]
      #print ret[0]
      self.write(pickle.dumps(ret))
    else:
      self.write(response.body)
