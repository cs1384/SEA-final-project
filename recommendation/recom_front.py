import tornado.httpserver
import tornado.ioloop
import tornado.web
import hashlib
import socket
import getpass

import json, os, pickle, urllib, re, sys
import operator #so can sort the dict by key or value
from numpy import * #so can calculate coefficient between vectors

from tornado.httpclient import AsyncHTTPClient
from tornado import gen
from tornado.options import define, options
sys.path.append('../')
from src import color
bcolors = color.bcolors()

'''
The purpose of movieHandler is to: 
fetch MovieServers --> get the users --> calculate similarity --> fetch ReviewServers
'''


ports=[]
ports_movie = []
ports_review = []
movie_dict = {}



class Application(tornado.web.Application):
  def __init__(self):
    handlers = [(r"/recom", recomHandler)]
    tornado.web.Application.__init__(self, handlers)


def Return_dict2Frequency_dict(mydict):
  #From the dict that movieID is the key, to freq dict that critic is the key
  Frequency_dict = {}
  for eachID in mydict.keys():
    for (cretics, rating) in mydict[eachID]:
      if cretics:
        try:
          Frequency_dict[cretics].append((eachID, rating))
        except:
          Frequency_dict[cretics] = [(eachID, rating)]  
  return Frequency_dict
def letter2val(letter,addition):
  val = 0
  if letter =="A":
    val = 95
  elif letter =="B":
    val = 85
  elif letter =="C":
    val = 75
  elif letter =="D":
    val = 65
  elif letter =="E":
    val = 55  

  if addition == "+":
    val +=5
  elif addition == "-":
    val -=5
  
  return val

def RatingConversion(rating):
  myNewVal = nan
  try:
    myNewVal = float(rating)
  except:
    devide_matching = re.match( r'([0-9]+\.*[0-9]*).{,1}([0-9]+\.*[0-9]*)', rating, re.M|re.I)    
    letter_matching = re.match( r'([A-E])(\+|\-){0,1}', rating, re.M|re.I)    
    
    if devide_matching:
      # print "Hi, original rating: %s" % rating
      # print "devide_matching.group(1): %s" % devide_matching.group(1)
      # print "devide_matching.group(2): %s" % devide_matching.group(2)
      myNewVal = float(devide_matching.group(1))/float(devide_matching.group(2)) *100
    
    if letter_matching:
      # print "Hi, original rating: %s" % rating
      myNewVal = letter2val(letter_matching.group(1), letter_matching.group(2))
  
  return myNewVal

def printMatrix(matrix):
  for i in xrange(len(matrix)):
    print matrix[i]    

def ToFormat(mylist):
  global movie_dict
  body = '<font size="5" color="blue">Top %s Recommended Movies</font><br>\n<ol>' % len(mylist)
  for i in xrange(len(mylist)):
    # print movie_dict[mylist[i][0]].keys()
    body += '<li>MovieID: %s<br>Title: %s<br>WeightedRating: %s<br>Poster: <img src=%s alt="HTML5 Icon" ></li>' % (mylist[i][0], movie_dict[mylist[i][0]]['title'], mylist[i][1], movie_dict[mylist[i][0]]['posters']['profile'])
    # body += '<li><a href=%s>%s</a><br>DocId: %s<br>%s</li>' % (n['url'], n['title'], n['docID'], n['snippet'])
  body += '</ol>'
  return body

def calCoefficientFromFrequency_dict(freqDict, userPreference):
  '''
  Goal: Get the best and second best matched cretics, and return the coefficient to the user

  Define "match": most overlapped number of movies between user and cretics
  (e.g. user may rated 20 movies, and all other cretics may have 5 overlapped at most. So here we collect cretics with 5 or 4 overlapped movies)
  '''

  userSuperTable = {} #userSuperTable[movie] = rating
  superTable = {} #superTable[critic][movie] = rating
  cretic_sortedList = []  
  top_cretics = []
  movieOverlap = {}
  combinedOverlappedMovingRating = []


  for (movie, rating) in userPreference:
    userSuperTable[movie] = RatingConversion(rating)

  for critic in sorted(freqDict, key=lambda k:len(freqDict[k]), reverse=True):
    cretic_sortedList.append(critic)
    superTable[critic] = {}
    for (movie, rating) in freqDict[critic]:
      superTable[critic][movie] = RatingConversion(rating)
        
  #collect best and second best cretics with the most numbers of movie reviewed
  MaxFreq = len(freqDict[cretic_sortedList[0]]) #the first critics in sortedList is the most frequent one
  for eachCritic in cretic_sortedList:
    curFre = len(freqDict[eachCritic])
    if (curFre>=MaxFreq-1):
      top_cretics.append(eachCritic)
      for (movie, rating) in freqDict[eachCritic]:
        try:
          movieOverlap[movie]+=1
        except:
          movieOverlap[movie]=1
  
  UserOverlapMovieRating = []
  for eachMovie in movieOverlap.keys():
    UserOverlapMovieRating.append(userSuperTable[eachMovie])
  # combinedOverlappedMovingRating.append(mytmpOverlapMovieRating)

  for eachCritic in top_cretics:
    mytmpOverlapMovieRating = []
    for eachMovie in movieOverlap.keys():
      try:
        mytmpOverlapMovieRating.append(superTable[eachCritic][eachMovie])
      except:
        # mytmpOverlapMovieRating.append(nan)
        mytmpOverlapMovieRating.append(0)
    combinedOverlappedMovingRating.append(mytmpOverlapMovieRating)


  print "top_cretics:\n%s" % top_cretics
  print "movieOverlap:\n%s" % movieOverlap
  print "UserOverlapMovieRating:\n%s" % UserOverlapMovieRating
  print "combinedOverlappedMovingRating:\n%s" % combinedOverlappedMovingRating
  
  corrcoef2User = []
  for eachVector in combinedOverlappedMovingRating:
    corrcoef2User.append(corrcoef(UserOverlapMovieRating, eachVector)[0][1])

  # corrcoef2User = corrcoef(combinedOverlappedMovingRating)
  # corrcoef2User = list(corrcoef(combinedOverlappedMovingRating)[0])
  # corrcoef2User.pop(0)
  return (top_cretics, corrcoef2User)
  # return corrcoef([[80,40,nan,100], [80, 50, 90.7, 100], [10,80, 20, 10]], [80,40,nan,100])
  

class recomHandler(tornado.web.RequestHandler):
  @gen.coroutine
  def get(self):
    global ports_movie, ports_review, UserBook
    userID = self.get_argument('user', None)    
    userID = str(userID)
    
    MovieHistory = [movie for (movie, score) in UserBook[userID]]
    ScoreHistory = [score for (movie, score) in UserBook[userID]]
    
    
    myAllReturn_dict = {}
    for eachServer in ports_movie:
      toFetch = '+'.join(MovieHistory)
      toFetch = '%s/movie?movieID=%s' % (eachServer,toFetch)
      print "Fetch MovieServer: %s" % toFetch
      http_client = AsyncHTTPClient()                                
      response = yield http_client.fetch(toFetch)            
      tmp_dict = json.loads(response.body)
      # here to merge all the returned json to myAllReturn_dict
      myAllReturn_dict = {key: value for (key, value) in (myAllReturn_dict.items() + tmp_dict.items())}
    
    '''
    myAllReturn_dict.keys()  --> all the moviedIDs that has been rated by this current user
    myAllReturn_dict[movieID] --> [(critics1,rating1), (critics2,rating2)] --> all the historical critics rating in our database

    myFrequency_dict.keys()  --> all the critics
    myFrequency_dict[critics] --> [(MovieID, Rating), (MovieID, Rating) ... ]
    '''
    
    myFrequency_dict = Return_dict2Frequency_dict (myAllReturn_dict)
    # print "myFrequency_dict:\n%s" % myFrequency_dict

    (top_cretics, coefficient) = calCoefficientFromFrequency_dict(myFrequency_dict, UserBook[userID])    
    print "top_cretics:\n%s" % top_cretics
    print "coefficient:\n%s" % coefficient


    myReviewReturn_dict = {}
    for eachServer in ports_review:
      toFetch = '+'.join(top_cretics).replace(" ", "_")      
      toFetch = '%s/review?critics=%s' % (eachServer,toFetch)
      print "Fetch ReviewServer: %s" % toFetch
      http_client = AsyncHTTPClient()                                
      response = yield http_client.fetch(toFetch)            
      tmp_dict = json.loads(response.body)
      myReviewReturn_dict = {str(key): value for (key, value) in (myReviewReturn_dict.items() + tmp_dict.items())}
    
    print "myReviewReturn_dict:\n%s" % myReviewReturn_dict
    FinalList = []
    for i in xrange(len(top_cretics)):
      weighting = coefficient[i]
      cur_cretics = str(top_cretics[i])
      # print "type: %s" % type(cur_cretics)
      # print myReviewReturn_dict[cur_cretics][0]
      print "Weighting:\t%s\tcur_cretics:\t%s" % (weighting, cur_cretics)
      myNewWeightedRating = [(movie, RatingConversion(rating)*weighting) for (movie, rating) in myReviewReturn_dict[cur_cretics]]
      print "Orginal Rating:\n%s\nWeighted Rating:\n%s" % (myReviewReturn_dict[cur_cretics][:10], myNewWeightedRating[:10])
      FinalList.extend(myNewWeightedRating)

    #Sort tuple list
    FinalList = sorted(FinalList, key=lambda tup: tup[1], reverse=True)    
    toprint = ToFormat(FinalList[:20])
    self.write(toprint)

    # self.write("Hi User: %s <br>%s" % (userID, UserBook[userID]))
    





class FrontEndApp(object):
  def __init__(self, MovieServers, ReviewServer):
    global ports_movie, ports_review, UserBook, movie_dict
    ports_movie = MovieServers
    ports_review = ReviewServer
    
    path = os.path.dirname(__file__) + '/../constants/Movie_dict'    
    movie_dict = pickle.load(open(path, 'r'))

    path = os.path.dirname(__file__) + '/../userLog/myUserBook'
    print bcolors.OKGREEN + "Recommendation Front Loding User Log\nFirst 10 User IDs" + bcolors.ENDC     
    UserBook = pickle.load(open(path, 'r'))


    for i in range(10):
      print "\t%s" % UserBook.keys()[i]      
    self.app = tornado.httpserver.HTTPServer(Application() )        



