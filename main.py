import pymongo
client=pymongo.MongoClient()
print(client.list_database_names( ))
db=client.students
exam_data=db.exam
print(db.list_collection_names())
print(db.exam.count_documents({ }))
#cursor=db.exam.find({ })
#for doc in cursor:
 #   print(doc)
#print(db.exam.find_one({"name":"aimee Zank"}))
print("1>max")
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
print("2>avg and pass marks")
avg_marks=exam_data.aggregate([
    {"$unwind": "$scores"},
    {"$match": {'scores.type': 'exam', "scores.score": {"$gt": 40, "$lt": 60}}
     }
])
for i in avg_marks:
    print(i)
print("3>pass/fail")
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

print("5")
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
