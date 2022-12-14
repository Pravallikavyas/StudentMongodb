#Importing pymongo package to connect python and mongodb
import pymongo
#Creating a variable client to assign MongoClient instance
client=pymongo.MongoClient()
#Printing the list of databases in mongodb
print(client.list_database_names( ))
#Creating a variable db to assign student database
db=client.students
#Creating a variable to store all the documents in a collection.students.json is imported into exam collection in mongodb
exam_data=db.exam
#Prints collections present in students database
print(db.list_collection_names())
#Checking whether 200 documents in students.json is imported or not
print(db.exam.count_documents({ }))
#A query to print the students who scored maximum marks in all 
print("1>Students who scored maximum marks in all")
max_marks=exam_data.aggregate([
    {"$unwind":"$scores"},
    {"$group":
     {
         "_id":"$_id",
        "name":{"$first":"$name"}
      ,
     "Total":{"$sum":"$scores.score"},
      }
     },
      {"$sort":{"Total":-1}},
     {"$limit":1}
])
for doc in max_marks:
    print(doc)
#A query to print the students who scored below average in exam and pass marks is 40%
print("2>Students who scored below average in exam and pass marks is 40%")
avg_marks=exam_data.aggregate([
    {"$unwind": "$scores"},
    {"$match": {'scores.type': 'exam', "scores.score": {"$gt": 40, "$lt": 60}}
     }
])
for i in avg_marks:
    print(i)
#A query to find out pass/fail students
print("3>Studens who are pass/fail")
x =exam_data.aggregate(
[ {"$set":
   {"scores":
     {"$arrayToObject":
       [{"$map":
           {"input": "$scores",
            "as": "s",
            "in": {"k": "$$s.type", "v": "$$s.score"}}}]}}},
 {"$project":
  {
     "_id":1,
     "name":1,
     "result":{
            "$cond":
                    {"if": {"$and" : [{"$gte": ["$scores.exam", 40]}, {"$gte": ["$scores.quiz", 40]}, {"$gte": [ "$scores.homework", 40]}]
                            },
                    "then" :"pass",
                    "else":"fail"
                    }
               }
  }
}
  ])
for i in x:
    print(i)
#A query to print the sum and average of all the students in all categories
print("4>Sum and average of all the students")
avgtotal = db.avgtotal_collection
agg =exam_data.aggregate([
    {"$unwind":"$scores"},
    {"$group":
     {
         "_id":"$_id",
        "name":{"$first":"$name"}
      ,
     "Total":{"$sum":"$scores.score"},
      "Average":{"$avg":"$scores.score"}
      }
     },
     {"$sort":{"_id":1}}
     ])
avgtt = []
for i in agg:
  avgtt.append(i)
  print(i)
avgtotal.insert_many(avgtt)
#A query to store documents of students who scored below average and above 40% in collection below average
print("5>Students who scored below average and above 40% ")
passandavg = db.belowaverage
p =exam_data.aggregate(
[{"$match":
   {"$expr":
     {"$and":
       [{"$gt": [{"$min": "$scores.score"}, 40]},
         {"$lt": [{"$max": "$scores.score"}, 70]}
        ]
      }
    }
  }])
pavg = []
for i in p:
  pavg.append(i)
  print(i)
passandavg.insert_many(pavg)
#A query to print the details of the student who scored below pass mark
print("6>Students who scored below pass marks")
fail = db.allfail_collection
agg = exam_data.aggregate(
[{"$match": 
   {"$expr": 
         {"$lt": [{"$max": "$scores.score"}, 40]
          }
        
      }
    }
  ])
failed = []
for i in agg:
  failed.append(i)
  print(i)
fail.insert_many(failed)
#A query to print the student details who scored above pass marks
print("7>Students who got above pass marks")
pas = db.allpass_collection
agg = exam_data.aggregate(
[{"$match": 
   {"$expr": 
         {"$gt": [{"$min": "$scores.score"}, 40]
          }
        }
    }
  ])
passed = []
for i in agg:
  passed.append(i)
  print(i)
pas.insert_many(passed)  
