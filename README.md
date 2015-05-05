# SEA-final-project
Building up movie seach engine plus customized recommendation system

#Constants files:
[google drive](https://drive.google.com/folderview?id=0BzG5zLRRrgKwfkFPVHE5ZUZ2WGVZM28wUXZqUzU5WmhuZ3ZFdURTMzNYNzJNeVN2T1dGWWM&usp=sharing)


# Working Procedure

## 1. Split data into many partitions
```
#Note, the num of partitions should corresping to the num of backend works
#Default: (NumSuperFront, NumMaster, NumMovie, NumReview, NumIdx, NumDoc)= (1, 3, 3, 3, 3, 3)
python -m src.reformatter <# of partitions for review> <# of partitions for movie>
python -m src.reformatter 4 5 
```
## 2. call prework workers
```
python -m PreworkWorkers
```
## 3. spin up fs system
```
python -m fsStart
```
## 4.prepare pickle files for all servers 
```
python -m PrepareFS
```
##5 Start All the works

Goal: 1. find ports, 2. fire up all servers
```
python ./StartAll.py
```
## 6. Fire up frontend (google app engine)
https://cloud.google.com/sdk/#Quick_Start
```
dev_appserver.py --host=localhost --port=8080 frontend
```

#Structure:
The structure of fired-uped HTTP servers are:
```
                        --> classifier_front(?)   --> ?
User --> SuperFront     --> searchEng_front       --> searchEng_worker (inclusing IndexServer*3, and DocumentServer*3)
                        --> recom_front           --> recom_worker (inclusing MovieServer*3, and ReviewServer*3)
```
#Recommendation System:
###Goal: getting the user ID --> check user log to get review history --> check MovieServer to get similar critics --> check ReviewServer to get movies sorted by weighted rating
###Stucture and Usage:
```
recom_front --> MovieServer*3
            --> ReviewServer*3

#recom_front api: 
#http://linserv2.cims.nyu.edu:46829/recom?user=UserID (e.g. http://linserv2.cims.nyu.edu:46829/recom?user=d0aa6e9b-676b-428f-9758-65e7c09b38a4)

#MovieServer api:
# http://linserv2.cims.nyu.edu:46831/movie?movieID=MovieIDs (e.g. http://linserv2.cims.nyu.edu:46831/movie?movieID=770802394+770882996+12900+13217+11705+770876740+770710325+771362322+533693794+348462568)

#ReviewServer api:
#http://linserv2.cims.nyu.edu:46834/review?critics=CRITICS (e.g. http://linserv2.cims.nyu.edu:46834/review?critics=Emanuel_Levy+Roger_Ebert)
```

Current UserLog is created by:
```
python ./src/createFakeUserLog.py

#So it will create 20 reviews per user with random scoring on random movie. Total for 50 users with unique ID created.  
#saved at ../userLog/myUserBook
```


#TomatoCrawler
##Goal: to fetch rotten tomato website and save the info properly
Now we have:
- 250 movie to search
- 1718 movieIDs returned
```python
#If you like tomatoCrawler to save Movie_fs, Review_fs, and IDs_fs to file system
from src import tomatoCrawler
tomatoCrawler.main2FS()

#Or! just ask tomatoCrawler to save Movie_dict, Review_fs, and IDs_fs to ./constants as pickle files
tomatoCrawler.main2NormalDict()
```


#File System module Usage
##Distributed dictionary object
```python
from fs import DisTable

#Creating an object
a = DisTable()
# or
b = DisTable({ 1: 'a', 2: 'b', 3: 'c'})

#Set a key-value pair
a[1] = 'a'
a[2] = 'b'
#Get a value with key
a[1]
#returns 'a'

#Pop operation
a.pop(1)
#returns 'a' and remove (1, 'a') from dictionary

#hasKey operation
a.hasKey(2)
#returns True
a.hasKey(1)
#returns False

#Length property
a.length
#returns 1

#Pretty print of dictionary
print a
#1
#   a
'''
key1
  value1
  value2
  ...
key2
  value1
  value2
  ...
'''
```

##Distributed List
```python
from fs import DisList

#Creating an object
a = DisList()
# or
b = DisList([1, 2, 3, 4])

#Append/Extend a value into list
a.append(1)
a.append(2)
a.extend(3)
a.extend(4)

#Get a value given position
a[0]
#returns 1
a[1]
#returns 2

#Update value to given position
a[1] = 3
print a
#[ 1 3 3 4 ]

#Remove value from list
a.remove(1)
print a
#[ 3 3 4 ]
a.remove(3, globl=True)
print a
#[ 4 ]

#Pop operation
a.pop(1)
#returns 'a' and remove (1, 'a') from dictionary

#Length property
a.length
#returns 1
```
