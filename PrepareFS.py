import mapreduce.framework as framework
import classification.trainer as trainer
import tornado.ioloop
from src import color


def main():

  C = color.bcolors()

  print C.HEADER + "=========== Instantiate MapReduceFramework ===========" + C.ENDC  
  mrf = framework.MapReduceFramework()
  mrf.getWorkerInfo('prework_workers.json')
  
  #print C.HEADER + "=========== Start Local Indexing ===========" + C.ENDC
  # localIndexer.ReviewIndexing()
  
  print C.HEADER + "=========== Start Indexing Movies ===========" + C.ENDC
  print C.OKBLUE + "Start idfBuilder" + C.ENDC
  mrf.mapReduceFS('idfBuilder', 'constants/input_movie', 'src.idfBuilder.mapper', 1, 'src.idfBuilder.reducer', 'constants/idf')
  tornado.ioloop.IOLoop.instance().start()
  print C.OKBLUE + "Start invertedIndexer" + C.ENDC
  mrf.mapReduceFS('invertedIndexer', 'constants/input_movie', 'src.invertedIndexer.mapper', 3, 'src.invertedIndexer.reducer', 'constants/invertedIndex')  
  tornado.ioloop.IOLoop.instance().start()
  print C.OKBLUE + "Start documentStore" + C.ENDC
  mrf.mapReduceFS('documentStore', 'constants/input_movie', 'src.documentStore.mapper', 3, 'src.documentStore.reducer', 'constants/documentStore')
  tornado.ioloop.IOLoop.instance().start()

  print C.HEADER + "=========== Start Indexing Genre ===========" + C.ENDC
  print C.OKBLUE + "Start genreIndexer" + C.ENDC
  mrf.mapReduceFS('genreIndexer', 'constants/input_movie', 'src.genreIndexer.mapper', 1, 'src.genreIndexer.reducer', 'constants/genreIndexer')
  tornado.ioloop.IOLoop.instance().start()
  
  print C.HEADER + "=========== Start Indexing Reviews ===========" + C.ENDC
  print C.OKBLUE + "Start movieIndexer" + C.ENDC
  mrf.mapReduceFS('movieIndexer', 'constants/input_review', 'src.movieIndexer.mapper', 3, 'src.movieIndexer.reducer', 'constants/movieIndexer')
  tornado.ioloop.IOLoop.instance().start()
  print C.OKBLUE + "Start reviewIndexer" + C.ENDC
  mrf.mapReduceFS('movieIndexer', 'constants/input_review', 'src.reviewIndexer.mapper', 3, 'src.reviewIndexer.reducer', 'constants/reviewIndexer')
  tornado.ioloop.IOLoop.instance().start()

  print C.HEADER + "=========== Start Classification Training ===========" + C.ENDC
  worker_address = 'prework_workers.json'
  #raw_data = 'constants/Genre_dict'
  raw_data = 'constants/Genre_dictII_9500'
  training_set = 'constants/training_set.p'
  weights_dir = 'constants/classification_weights'
  tn = trainer.Trainer()
  tn.setWorkerInfo(worker_address)
  genres = tn.processRawData(raw_data, training_set)
  tn.setTraningParameter(0.9, 500, 0.01)
  tn.train(training_set, genres, weights_dir)
  tornado.ioloop.IOLoop.instance().start()
  tn.generateWeightTable(weights_dir)

if __name__ == "__main__":
  main()
