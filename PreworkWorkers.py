# tornado
from tornado.process import fork_processes
import tornado.ioloop
import tornado.web
# package
from classification.config import tracker
from classification.backend import offline
from mapreduce.backend import map, reduce, application
# 
import sys
import argparse
import socket


def getArguments(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument('--nCWorker', required=False, type=int, default=5, metavar='<PATH_TO_MAPPER>', help='Input file name')
  parser.add_argument('--nMWorker', required=False, type=int, default=5, metavar='<PATH_TO_REDUCER>', help='Input file name')
  parser.add_argument('--jsonPath', required=False, type=str, default='prework_workers.json',metavar='<PATH_TO_JOB_DIR>', help='Input file name')
  parser.add_argument('--startPort', required=False, type=int, default=10000,metavar='<PATH_TO_JOB_DIR>', help='Input file name')
  return parser.parse_args(argv)

if __name__ == "__main__":
  # config info
  args = getArguments(sys.argv[1:])
  print '=============== info =================='
  print 'num_of_mapreduce_workers: ' + str(args.nMWorker)
  print 'num_of_classification_workers: ' + str(args.nCWorker)
  print 'worker_address_json_path: ' + args.jsonPath
  print 'start_port: ' + str(args.startPort)
  print '======================================='
  total = args.nMWorker + args.nCWorker  
  # assign worker info
  i = 0
  while i < args.nCWorker:
    tracker.addWorker('classification', socket.gethostname()+':'+str(args.startPort+1+i))
    i += 1
  while i < total:
    tracker.addWorker('mapreduce', socket.gethostname()+':'+str(args.startPort+1+i))
    i += 1
  tracker.toJson(args.jsonPath)
  # spin up workers
  pid = fork_processes(total, max_restarts=0)
  if pid < args.nCWorker:
    app = tornado.web.Application(([(r"/train?", offline.TrainingHandler)]))
    port = args.startPort + 1 + pid
    app.listen(port)
    print 'A classification worker is working at ' + tracker.MACHINES['classification'][pid]
  else:
    app = application.Application(([(r"/map?", map.MapHandler),
                                    (r"/retrieveMapOutput?", map.RetrieveOutputHandler),
                                    (r"/reduce?", reduce.ReduceHandler),
                                  ]))
    port = args.startPort + 1 + pid
    app.setInventory(args.jsonPath)
    app.listen(port)
    print 'A mapreduce worker is working at ' + tracker.MACHINES['mapreduce'][pid-args.nCWorker]
  tornado.ioloop.IOLoop.instance().start()
	





	
