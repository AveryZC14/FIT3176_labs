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

    new_doc["review"] = review_doc
    
    return new_doc  # Return the restructured document
    

# Task 2: Delete Listings of Inactive Hosts
def task2(yr):
    
    # Find hosts that joined more than yr years ago
    # delete if they have no reviews.

    # group by host id, then get the min joined date and sum of reviews
    group = {
        "$group": {
            "_id": "$host.id",
            "joined": {"$min": "$host.joined"},
            "total_reviews": {"$sum": "$review.total_reviews"}
        }
    }

    # match hosts that joined more than yr years ago and have no reviews
    mat = {
        "$match": {
            "joined": {"$lt": datetime.now() - timedelta(days=yr*365)},
            "total_reviews": 0
        }
    }

    


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
    recreate_collection()
    #task2()
