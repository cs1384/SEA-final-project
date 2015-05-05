import sys
from mapreduce.config import settings
for line in sys.stdin:
  kvPair = line.split(settings.delimiter, 1)
  key = kvPair[0]
  value = eval(kvPair[1])
  for review in value['reviews']:
    try:
      reviewer = review['critic']
      quote = review['quote'].lower()
      rating = review['original_score']
      print key + settings.delimiter + str((reviewer, rating, quote))
    except:
      pass


