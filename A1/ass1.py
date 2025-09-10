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
    
    #create the index on location
    collection.create_index([("location","2dsphere")])

    
    print(f"Inserted {len(data)} records into the '{collection_name}' collection.")


#a simple, reusable document restructuring function 
def simple_restructure(
            old_doc,
            new_doc, 
            name_in_data,
            new_name = "", #optional: what to rename the field to in the new doc
            transformation_func = (lambda x:x) # optional, a transformation done to the data
        ):
        if (new_name == ""):
            new_name = name_in_data
        
        if (name_in_data in old_doc) and (old_doc[name_in_data] != None):
            new_doc[new_name] = transformation_func(old_doc[name_in_data])

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

    #override the default if it exists in the document
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
                "$lt": datetime.now() - timedelta(days=(yr//2)*365)
            },
            "total_reviews": 0
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
    # identify guest-favourite listings
    # guest-favourite listing conditions:
    # at least 100 reviews
    # aggregate review score > 20 (accuracy + cleanliness + checkin + communication + location)

    gf_condition = {
        #total reviews must be at least 100
        "review.total_reviews": {"$gte": 100},
        #sum of all types of review score must be greater than 20
        "$expr": {
            "$gt": [
                {
                    "$sum": [
                        "$review.accuracy",
                        "$review.cleanliness",
                        "$review.checkin",
                        "$review.communication",
                        "$review.location"
                    ]
                },
                20
            ]
        }
    }

    gf_update = collection.update_many(
        #for each guest favourite listing
        gf_condition,
        #set is_guest_favourite to true
        {"$set": {"is_guest_favourite": True}}
    )

    print("Number of Guest Favourite Listings: " + str(gf_update.modified_count) )

    # assign a score to each listing based on the following criteria:
    # default score: 1 point
    # is_guest_favourite: 2 points
    # has no reviews or has no sub reviews: -1 point

    #boolean condition for a bad listing
    bad_listing_cond = {
        #each condition for a bad cond
        "$or" : [
            #if the review doc doesnt exist
            { "$not" : "$review" },
            #if none of the sub documents exist
            { "$and" : [
                { "$not" : "$review.accuracy" },
                { "$not" : "$review.cleanliness" },
                { "$not" : "$review.checkin" },
                { "$not" : "$review.communication" },
                { "$not" : "$review.location" },
            ]},
            #if there are 0 total reviews
            { "$eq" : ["$review.total_reviews" , 0 ] }
        ]
    }

    #condition block to calculate the listing score for each listing
    listing_score_cond = {
        #first check if it's guest favourite
        "$cond": {
            #is_guest_favourite is already a boolean
            "if" : "$is_guest_favourite" ,
            "then" : 2 ,
            #if it's not, then check if it's a bad listing
            "else" : { "$cond": {
                    "if" : bad_listing_cond ,
                    "then" : -1 ,
                    "else" : 1
                }
            }
        }
    }

    #project to add a listing score to each document
    listing_score_project = {
        "$project" : {
            "host" : 1,
            "listing_score" : listing_score_cond
        }
    }

    #group the listings by their hosts, summing the listing scores
    host_group = {
        "$group" : {
            "_id" : "$host.id",
            "url" : { "$first" : "$host.url" },
            "name" : { "$first" : "$host.name" },
            "listing_score" : { "$sum" : "$listing_score" }
        }
    }

    #sort the hosts by their total listing scores, then name
    host_sort = {
        "$sort" : {
            "listing_score" : -1,
            "name" : 1
        }
    }

    #only get the top n hosts
    host_nth_limit = {
        "$limit" : n
    }

    #use this to skip each host that's not the nth host
    host_skip = {
        "$skip" : (n-1)
    }

    #get the listing_score for the nth host
    nth_score_doc = collection.aggregate([listing_score_project, host_group, host_sort, host_nth_limit, host_skip ])
    nth_score_list = list(nth_score_doc)
    #if done correctly, nth_score_list should only have one element, we should be able to get it's listing score

    nth_score = nth_score_list[0]["listing_score"]

    #match based on score
    host_match_score = {
        "$match" : {
            "listing_score" : { "$gte" : nth_score }
        }
    }

    #use host_match_score instead of host_nth_limit and host_skip, which ensures that all hosts that have the same score as the nth host are also included
    n_best_hosts = collection.aggregate([listing_score_project, host_group, host_sort, host_match_score ])
    best_hosts = list(n_best_hosts)

    for host in best_hosts:
        print("Name: " + host["name"] + ", Listing score: " + str(host["listing_score"]))
    
    #listing scores are no longer needed, unset
    dell = collection.update_many(
        {},
        {"$unset" : { "listing_score" : "" }}
    )




    

# Task 4: Find the Best Listing and Nearby Listings
def task4(city, x):
    
    #find the best listing in the city

    #match listings that are in the city and that have prices
    city_match = {
        "$match" : {
            "neighbourhood" : city,
            "price" : { "$exists" : True }
        }
    }

    #project to sum the sub review scores into the agg_review_score, also grab a bunch of other fields
    city_project = {
        "$project" : {    
            "listing_id" : 1 ,
            "agg_review_score" : { "$sum": [
                "$review.accuracy",
                "$review.cleanliness",
                "$review.checkin",
                "$review.communication",
                "$review.location"
            ]},
            "total_reviews": "$review.total_reviews",
            "price" : 1 ,
            "location" : 1,
            "name":1,
            "host_name": "$host.name",
            "neighbourhood": 1,
            "url" : "$listing_url"
        }
    }

    #sort by each field as required
    city_sort = {
        "$sort" : {
            "agg_review_score" : -1,
            "total_reviews" : -1,
            "price" : 1,
            "listing_id" : 1
        }
    }

    #just get one
    city_limit = {
        "$limit" : 1
    }

    #aggregate to find the best listing
    listings_ranked = list(collection.aggregate([ city_match, city_project, city_sort, city_limit]))

    #if no valid listing was found, end early
    if len(listings_ranked) == 0:
        print("No valid listing found")
        return

    best_listing = listings_ranked[0]
    print("Best Listing:")
    print( f"Listing ID: {best_listing["listing_id"]}, Host Name: {best_listing["host_name"]}, Price: {best_listing["price"]}, Neighbourhood: {best_listing["neighbourhood"]}, Listing URL: {best_listing["url"]}\n")

    #use x to calculate the price bounds
    price_upper = best_listing["price"] + x
    price_lower = best_listing["price"] - x
    
    #use geoNear to make a new field on each document, distance from the best
    near_best = {
        "$geoNear" : {
            "near" : best_listing["location"],
            "distanceField" : "dist_from_best",
            "spherical" : True
        }
    }

    #match price, but also make sure the best is filtered out
    match_price = {
        "$match" : {
            "price": {"$gte" : price_lower, "$lte" : price_upper},
            "listing_id": {"$ne" : best_listing["listing_id"]}
        }
    }

    #sort by the distance from the best host
    sort_dist = {
        "$sort" : {
            "dist_from_best" : 1,
            #the following sort parameters are only in the example and not the details, so idk if they should be here
            "reviews.total_reviews": -1,
            "price": 1,
            "listing_id": 1

        }
    }
    
    limit_five = {
        "$limit" : 5
    }

    #aggregate to get similar listings
    similar_listings = list(collection.aggregate([ near_best, match_price, sort_dist, limit_five]))

    print("Top-5 Listings:")
    for listing in similar_listings:
        print( f"Listing ID: {listing["listing_id"]}, Host Name: {listing["host"]["name"]}, Price: {listing["price"]}, Distance: {listing["dist_from_best"]}, Neighbourhood: {listing["neighbourhood"]}")


    
    

# Call tasks
if __name__ == "__main__":
    recreate_collection()
    # task2(4)
    # task3(5)
    # task4("Monash",10)
