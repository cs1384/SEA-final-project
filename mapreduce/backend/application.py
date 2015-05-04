import tornado.web
import json

class Application(tornado.web.Application):
  def setInventory(self, jsonPath):
    self.inventory = json.load(open(jsonPath, "r"))['mapreduce']