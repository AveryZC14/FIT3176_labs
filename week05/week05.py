import json
import time
from pymongo import MongoClient
from pprint import pprint

# Connect to the MongoDB server
client = MongoClient('mongodb://localhost:27017/')  # Adjust the connection string as needed
# Define database name and collection names
db = client['Week05']
collection = db['mental_illness']

def insert_records_from_json(collection_name, json_file):
    # Access the specified collection
    collection = db[collection_name]
    
    # Drop the collection if it already exists
    db.drop_collection(collection_name)

    # Load JSON data from the file
    with open(json_file, 'r') as file:
        data = json.load(file)

    # Ensure the data is a list of records
    if isinstance(data, dict):
        data = [data]

    # Insert records into the collection
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        raise ValueError("The JSON file must contain a list of records.")

    print(f"Inserted {len(data)} records into the '{collection_name}' collection.")

def recreate_collection():
    insert_records_from_json('mental_illness', 'mental_illness_data.json')

def print_all(results):
    for result in results:
        pprint(result)

def taskx(coll):
    print("\n\nTaskx\n")

    results = coll.aggregate([])
    print_all(results)

def task1(coll):
    # print_all(coll.find({},{"_id":0,"year":1}))
    stage1 = {
        "$match": {
            "$and": [
                {"year": {"$gte":2009}},
                {"year": {"$lte":2019}}]
            
        }
    }

    stage2 = {
        "$group": {
            "_id":"$year",
            "total_population":{"$sum":"$population"}
        }
    }

    stage3 = {
        "$sort":{
            "_id":1
        }
    }

    stage4 = {
        "$project": {
            "_id":0,
            "total_population":1,
            "year":"$_id"
        }
    }

    results = coll.aggregate([stage1,stage2,stage3,stage4])
    print_all(results)

def task2(coll):
    print("\n\nTask2\n")

    stage1 = {
        "$match": {
            "country": {"$in":["Australia", "New Zealand", "India", "China", "Singapore"]}
        }
    }

    stage2 = {
        "$group": {
            "_id": "$country",
            "avg_female_depression": {"$avg":"$depression_rate.females"},
            "avg_male_depression": {"$avg":"$depression_rate.males"},
        }
    }

    results = coll.aggregate([stage1,stage2])
    print_all(results)

def task3(coll):
    print("\n\nTask3\n")

    # stage1 = {
    #     "$match": {
    #         "country": {"$in":["Australia", "New Zealand", "India", "China", "Singapore"]}
    #     }
    # }

    stage2 = {
        "$group": {
            "_id": "$country",
            "avg_female_depression": {"$avg":"$depression_rate.females"},
            # "avg_male_depression": {"$avg":"$depression_rate.males"},
        }
    }

    stage3 = {
        "$sort": {
            "avg_female_depression": -1
        }
    }

    stage4 = {
        "$limit": 5
    }
    
    stage5 = {
        "$sort": {
            "avg_female_depression": 1
        }
    }

    results = coll.aggregate([stage2,stage3,stage4,stage5])
    print_all(results)

def task4(coll):
    print("\n\nTask4\n")

    stage1 = {
        "$match": {
            "depression_rate": {"$exists":True}
        }
    }

    stage2 = {
        "$group": {
            "_id": "$country",
            "avg_female_depression": {"$avg":"$depression_rate.females"},
            "avg_male_depression": {"$avg":"$depression_rate.males"},
        }
    }

    stage3 = {
        "$project": {
            "_id":0,
            "country": "$_id",
            "difference": { "$subtract": [
                "$avg_female_depression",
                "$avg_male_depression"
                ]}
        }
    }

    stage4 = {
        "$sort":{
            "difference":-1
        }
    }


    results = coll.aggregate([stage1,stage2,stage3,stage4])
    print_all(results)

def task5(coll):
    print("\n\nTask5\n")
    
    find = {
        "$match": {
            "country":{"$in":["Australia", "New Zealand"]},
            "year":2019
        }
    }

    project = {
        "$project": {
            "year":1,
            "total_female_anxiety":{"$toInt":{"$divide": [{"$multiply":["$population","$anxiety_rate.females"]},100]}},
            "total_male_anxiety":{"$toInt":{"$divide": [{"$multiply":["$population","$anxiety_rate.males"]},100]}}
        }
    }

    group = {
        "$group": {
            "_id": "$year",
            "total_female_anxiety": {"$sum":"$total_female_anxiety"},
            "total_male_anxiety": {"$sum":"$total_male_anxiety"},
        }
    }

    project2 = {
        "$project": {
            "_id":0,
            "total_female_anxiety": 1,
            "total_male_anxiety": 1,
        }
    }

    results = coll.aggregate([find,project,group,project2])
    print_all(results)

def task6(coll):
    print("\n\nTask6\n")

    mat = {
        "$match" : {
            "year": 2019
        }
    }

    group = {
        "$group" : {
            "_id":"$country",
            "male_bipolar_rate": {"$sum":"$bipolar_rate.males"},
            "female_bipolar_rate": {"$sum":"$bipolar_rate.females"}
        }
    }

    project = {
        "$project" : {
            "avg_bipolar_rate": {"$divide":[{"$sum":["$male_bipolar_rate","$female_bipolar_rate"]},2]}
        }
    }

    sort = {
        "$sort" : {
            "avg_bipolar_rate":-1
        }
    }

    limit = {
        "$limit" : 5
    }

    project2 = {
        "$project": {
            "_id":0,
            "average_bipolar_rate":"$avg_bipolar_rate",
            "country":"$_id"
        }
    }

    results = coll.aggregate([mat,group,project,sort,limit,project2])
    print_all(results)

def task7(coll):
    print("\n\nTask7\n")

    mat = {
        "$match" : {
            "year": 2019,
            "anxiety_rate": {"$exists":True}
        }
    }

    group = {
        "$group" : {
            "_id":"$country",
            "male_anxiety_rate": {"$sum":"$anxiety_rate.males"},
            "female_anxiety_rate": {"$sum":"$anxiety_rate.females"},
        }
    }

    project = {
        # "$project" : {
        #     "avg_anxiety_rate": {"$sum": [ "$male_anxiety_rate" , "$female_anxiety_rate" ]}
        # }
        "$project" : {
            "avg_anxiety_rate": {"$divide": [ {"$sum": [ "$male_anxiety_rate" , "$female_anxiety_rate" ]} , 2 ]}
        }
    }

    sort = {
        "$sort" : {
            "avg_anxiety_rate": 1
        }
    }

    limit = {
        "$limit" : 3
    }

    project2 = {
        "$project" : {
            "_id":0,
            "avg_anxiety_rate":1,
            "country": "$_id"
        }
    }


    results = coll.aggregate([mat, group, project, sort,limit,project2])
    print_all(results)

def task8(coll):
    print("\n\nTask8\n")

    mat = {
        "$match" : {
            "$and" : [
                {"year":{"$gte":1990}},
                {"year":{"$lte":2019}},
            ],
            "country":"Australia"
        }
    }

    group = {
        "$group" : {
            "_id":"$year",
            "male_anxiety_rate": {"$sum":"$anxiety_rate.males"},
            "female_anxiety_rate": {"$sum":"$anxiety_rate.females"}
        }
    }

    project = {
        "$project" : {
            "_id":0,
            "year":"$_id",
            "difference": {"$subtract" : ["$female_anxiety_rate","$male_anxiety_rate"]}
        }
    }

    sort = {
        "$sort" : {
            "difference": -1
        }
    }

    limit = {
        "$limit": 5
    }

    results = coll.aggregate([mat, group, project, sort, limit])
    print_all(results)


if __name__ == "__main__":
    # recreate_collection()
    task1(collection)
    task2(collection)
    task3(collection)
    task4(collection)
    task5(collection)
    task6(collection)
    task7(collection)
    task8(collection)