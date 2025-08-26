import json
from pymongo import MongoClient
from pprint import pprint

# Connect to the MongoDB server
client = MongoClient('mongodb://localhost:27017/')  # Adjust the connection string as needed
# Define database name and collection names
db = client['Week04']
violence_stats = db["violence_stats"]

def insert_records_from_json(collection_name, json_file):
    # Access the specified collection
    collection = db[collection_name]
    
    # Delete existing records if any
    collection.delete_many({})

    # Load JSON data from the file
    with open(json_file, 'r') as file:
        data = json.load(file)

    # Insert each restructured document into MongoDB
    for doc in data:
        restructured_doc = restructure_document(doc)
        collection.insert_one(restructured_doc)

    
    print(f"Inserted {len(data)} records into the '{collection_name}' collection.")

# Function to restructure the document
def restructure_document(doc):
    # Initialize an empty document, we will add fields and values to this document in the required format
    restructured_doc = {}  
    
    # Add 'name' to the restructured document if it exists in the original document
    if "name" in doc:
        restructured_doc["name"] = doc["name"]
    
    # Create a 'violence' nested document and add relevant fields if they exist in the original document
    violence = {}
    if "violence_total" in doc:
        violence["total"] = doc["violence_total"]
    if "violence_sexual" in doc:
        violence["sexual"] = doc["violence_sexual"]
    if "violence_physical" in doc:
        violence["physical"] = doc["violence_physical"]
    if violence:  # Add 'violence' nested document to the restructured document if it is not empty
        restructured_doc["violence"] = violence
    
    # Add 'partner_or_family_violence' to the restructured document if it exists in the original document
    if "partner_or_family_violence" in doc:
        restructured_doc["partner_or_family_violence"] = doc["partner_or_family_violence"]
    
    # Add 'intimate_partner_violence' to the restructured document if it exists in the original document
    if "intimate_partner_violence" in doc:
        restructured_doc["intimate_partner_violence"] = doc["intimate_partner_violence"]
    
    # Create a 'cohabiting_partner' nested document and add relevant fields if they exist in the original document
    cohabiting_partner = {}
    if "cohabiting_partner_total" in doc:
        cohabiting_partner["total"] = doc["cohabiting_partner_total"]
    if "cohabiting_partner_violence" in doc:
        cohabiting_partner["violence"] = doc["cohabiting_partner_violence"]
    if "cohabiting_partner_emotional_abuse" in doc:
        cohabiting_partner["emotional_abuse"] = doc["cohabiting_partner_emotional_abuse"]
    if "cohabiting_partner_economic_abuse" in doc:
        cohabiting_partner["economic_abuse"] = doc["cohabiting_partner_economic_abuse"]
    if cohabiting_partner:  # Add 'cohabiting_partner' nested document to the restructured document if it is not empty
        restructured_doc["cohabiting_partner"] = cohabiting_partner
    
    # Add 'sexualHarassment' to the restructured document if it exists in the original document
    if "sexualHarassment" in doc:
        restructured_doc["sexualHarassment"] = doc["sexualHarassment"]
    
    # Add 'stalking' to the restructured document if it exists in the original document
    if "stalking" in doc:
        restructured_doc["stalking"] = doc["stalking"]
    
    # Add 'totalWomen' to the restructured document if it exists in the original document
    if "totalWomen" in doc:
        restructured_doc["totalWomen"] = doc["totalWomen"]
    
    return restructured_doc  # Return the restructured document

def recreate_collection():
    insert_records_from_json('violence_stats', 'stats-by-state.json')

def print_all(results):
    for result in results:
        pprint(result)

# define your task functions below

def task1(collection):
    data = collection.find({},{"_id":0,"name":1,"violence.total":1,"violence.physical":1})
    for d in data:
        pprint(d)


def task2(coll):
    print("\ntask2")
    # ,{"_id":0,"name":1,"sexualHarassment":1}
    data = coll.find({},{"_id":0,"name":1,"sexualHarassment":1}).sort("sexualharassment",-1)
    for d in data:
        pprint(d)

def task3(coll):
    print("\ntask3")
    data = coll.find({"totalWomen":{"$gt":500000}},{"_id":0,"name":1,"totalWomen":1})
    for d in data:
        pprint(d)


def task4(coll):
    print("\ntask4")
    data = coll.find({},{"_id":0,"name":1,"violence.total":1}).sort("violence.total",-1).limit(3)
    for d in data:
        pprint(d)

def task5(coll):
    print("\ntask5")
    # data = coll.find({"$expr": {"$gt":[{"$multiply":["$violence.total",0.4]},"$totalWomen"]}},{"_id":0,"name":1,"violence.total":1})#.sort("violence.total",-1)
    data = coll.find({"$expr": {"$gt":[{"$divide":["$violence.total","$totalWomen"]},0.4]}},{"_id":0,"name":1,"violence.total":1,"totalWomen":1})#.sort("violence.total",-1)
    for d in data:
        pprint(d)

def task6(coll):
    print("\ntask6")
    # data = coll.find({"$expr": {"$gt":[{"$multiply":["$violence.total",0.4]},"$totalWomen"]}},{"_id":0,"name":1,"violence.total":1})#.sort("violence.total",-1)
    data = coll.find({},{"_id":0,"name":1,"violence.total":1,"totalWomen":1})#.sort("violence.total",-1)
    for d in data:
        pprint(d)

    print("\n")

    coll.update_many({"totalWomen":{"$gt":1000000}},{"$mul":{"violence.total":1.1}})

    data = coll.find({},{"_id":0,"name":1,"violence.total":1,"totalWomen":1})#.sort("violence.total",-1)
    for d in data:
        pprint(d)
    # recreate_collection()






























def task7(coll):
    print("\ntask7")
    # data = coll.find({"$expr": {"$gt":[{"$multiply":["$violence.total",0.4]},"$totalWomen"]}},{"_id":0,"name":1,"violence.total":1})#.sort("violence.total",-1)
    data = coll.find({},{"_id":0,"name":1,"intimate_partner_violence":1,"totalWomen":1}
    ).sort("intimate_partner_violence",-1
    ).skip(2).limit(1)
    for d in data:
        pprint(d)

def task8(coll):
    print("\ntask8")
    # data = coll.find({"$expr": {"$gt":[{"$multiply":["$violence.total",0.4]},"$totalWomen"]}},{"_id":0,"name":1,"violence.total":1})#.sort("violence.total",-1)
    # data = coll.find({"$expr":{"$gt":[{"$divide":["$cohabiting_partner.emotional_abuse","$totalWomen"]},0.25]}},{"_id":0,"name":1,"cohabiting_partner.emotional_abuse":1,"totalWomen":1})
    data = coll.find({"$expr":{"$gt":[{"$divide":["$cohabiting_partner.emotional_abuse","$totalWomen"]},0.25]}},
    {"_id":0,"name":1,"ratio":{"$divide":["$cohabiting_partner.emotional_abuse","$totalWomen"]}})
    for d in data:
        pprint(d)

# call the functions
if __name__ == "__main__":
    # this will populate the database
    recreate_collection()
    task1(violence_stats)
    task2(violence_stats)
    task3(violence_stats)
    task4(violence_stats)
    task5(violence_stats)
    task6(violence_stats)
    task7(violence_stats)
    task8(violence_stats)