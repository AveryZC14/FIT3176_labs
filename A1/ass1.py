#### DO NOT IMPORT ANY OTHER LIBRARIES
import json
from datetime import datetime, timedelta
from pymongo import MongoClient, GEOSPHERE
from pprint import pprint
#### DO NOT IMPORT ANY OTHER LIBRARIES

client = MongoClient('mongodb://localhost:27017/')
db = client['airbnb']
collection = db['listings']


'''
Task 1: Recreate collection: This function must delete the listings collection (if exists), including dropping any indexes. Then, it should repopulate it as described
in assignment requirements and create any indexes that are required.
'''
def recreate_collection():
    # Feel free to create new functions that this function calls to recreate the collection

    collection_name = "listings"
    json_file = "listings.json"

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


def simple_restructure(
            old_doc,
            new_doc, 
            name_in_data,
            new_name = "", 
            transformation_func = (lambda x:x)
        ):
        if (new_name == ""):
            new_name = name_in_data
        
        if (name_in_data in old_doc) and (old_doc[name_in_data] != None):
            new_doc[new_name] = transformation_func(old_doc[name_in_data])

def create_restructure_func(old_doc, new_doc):
    def restructure_func(
        name_in_data,
        new_name = "", 
        transformation_func = (lambda x:x)
        ):
            simple_restructure(old_doc,new_doc,name_in_data,new_name,transformation_func)
    return restructure_func

def restructure_document(doc):
    # Initialize an empty document, we will add fields and values to this document in the required format
    new_doc = {}
    
    # Add fields to the restructured document if it exists in the original document
    if "id" in doc:
        new_doc["listing_id"] = doc["id"]

    #restructure all of the simple fields
    simple_restructure(doc, new_doc, "id","listing_id")
    simple_restructure(doc, new_doc, "listing_url")
    simple_restructure(doc, new_doc, "name")
    simple_restructure(doc, new_doc, "description")
    simple_restructure(doc, new_doc, "neighbourhood")
    simple_restructure(doc, new_doc, "accomodates")
    simple_restructure(doc, new_doc, "price")
    #transform 't' values into true, and other values to false
    simple_restructure(doc, new_doc, "has_availability","has_availability",(lambda available: available == 't'))

    #if theres both a latitude and a longitude, add a location doc
    if (doc["latitude"] != None and doc["longitude"] != None):
        location_doc = {"type": "Point"}
        location_doc["coordinates"] = [ float(doc["longitude"]), float(doc["latitude"]) ]
        new_doc["location"] = location_doc
    
    #embedded document for the host
    host_doc = {}
    #format for striptime
    time_format = "%Y-%m-%dT%H:%M:%S.%fZ"

    simple_restructure(doc, host_doc, "host_id", "id")
    simple_restructure(doc, host_doc, "host_url", "url")
    simple_restructure(doc, host_doc, "host_name", "name")
    simple_restructure(doc, host_doc, "host_is_superhost", "is_superhost", (lambda available: available == 't'))

    #if no date is given, use this default date
    default_date = datetime.strptime("2025-08-01T00:00:00.000Z", time_format)
    host_doc["joined"] = default_date

    simple_restructure(doc, host_doc, "host_joined","joined",(lambda date: datetime.strptime(date+"T00:00:00.000Z", time_format)))

    new_doc["host"] = host_doc

    #embedded document for the review scores
    review_doc = {}
    simple_restructure(doc, review_doc, "number_of_reviews", "total_reviews")
    simple_restructure(doc, review_doc, "review_scores_accuracy", "accuracy")
    simple_restructure(doc, review_doc, "review_scores_cleanliness", "cleanliness")
    simple_restructure(doc, review_doc, "review_scores_checkin", "checkin")
    simple_restructure(doc, review_doc, "review_scores_communication", "communication")
    simple_restructure(doc, review_doc, "review_scores_location", "location")

    #add the review doc if it's not empty
    if (review_doc != {}):
        new_doc["review"] = review_doc
    
    return new_doc  # Return the restructured document
    

# Task 2: Delete Listings of Inactive Hosts
def task2(yr):
    
    # Find hosts that joined more than yr years ago
    # delete if they have no reviews.

    # we can't match first, because we need to ensure that we get every single instanec of the hosts that joined over yr years ago
    # if we match first, we might miss some instances of the host that have reviews

    # group by host id, then get the min joined date and sum of reviews
    group = {
        "$group": {
            "_id": "$host.id",
            "joined": {"$min": "$host.joined"},
            # if the review doc doesn't exist in a document, mongodb will treat it as 0
            "total_reviews": {"$sum": "$review.total_reviews"}
        }
    }

    # match hosts that joined more than yr years ago and have no reviews
    mat = {
        "$match": {
            #days is the largest time unit that timedelta takes, so we convert years to days
            "joined": {"$lt": datetime.now() - timedelta(days=yr*365)},
            "total_reviews": 0
        }
    }


    results = list(collection.aggregate([group, mat]))

    pprint(results)

    # delete listings of these hosts
    host_ids_to_delete = [result["_id"] for result in results]
    delete_result = collection.delete_many({"host.id": {"$in": host_ids_to_delete}})
    print("Number of deleted listings: " + str(delete_result.deleted_count))

    #identify hosts that joined more than floor(yr/2) years ago but not more than yr years ago
    #if they have no reviews, mark them as to_be_deleted 

    #match
    mat2 = {
        "$match": {
            "joined": {
                #we don't need to do a greater than or equal to, because we are already deleting hosts that joined more than yr years ago
                "$lt": datetime.now() - timedelta(days=(yr/2)*365)
            },
            "review.total_reviews": 0
        }
    }

    #reuse group stage from before
    results2 = list(collection.aggregate([group, mat2]))

    host_ids_to_mark = [result["_id"] for result in results2]
    update_result = collection.update_many(
        #get the listings with hosts that match the ids
        {"host.id": {"$in": host_ids_to_mark}},
        #set to_be_deleted to true
        {"$set": {"to_be_deleted": True}}
    )
    print("Number of to_be_deleted listings: " + str(update_result.modified_count))




# Task 3: Identify Top Hosts
def task3(n):
    # write your solution here
    pass
    

# Task 4: Find the Best Listing and Nearby Listings
def task4(city, x):
    # write your solution here
    pass
    
    

# Call tasks
if __name__ == "__main__":
    # recreate_collection()
    # task2(2)
    pprint(list(collection.find({
        # "host.joined" : {
        #     "$lt": datetime.now() - timedelta(days=11*365),
        # },
        "reviews.total_reviews": 0
    },{
        "host.id": 1,
        "host.joined": 1,
        "review.total_reviews": 1,
        "_id": 0
    }).limit(5)))
