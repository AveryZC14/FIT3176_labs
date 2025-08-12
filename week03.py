from pymongo import MongoClient
# The pprint module in Python is used for "pretty-printing" complex data structures such as mongodb documents in a way that is easier to read. 
from pprint import pprint

def show_databases():
    client = MongoClient('mongodb://localhost:27017/')  # Adjust as needed
    # List all databases
    databases = client.list_database_names()

    # Print the list of databases
    for db in databases:
        pprint(db)

show_databases()

def setup_database():
    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')  # Adjust as needed
    
    # create a database called user_profiles
    db = client.user_profiles

    # add a collection called profiles and insert one record
    profiles = db.profiles
    profiles.insert_one({"name": "Alpha"})
	

    # delete all existing data in profiles if it exists (useful if you are running the code again)
    print("deleting all profiles (if any)")
    profiles.delete_many({})

    return profiles

profiles = setup_database()


def Task1(profiles):
    # Insert initial profiles
    profiles.insert_many([
        {"name": "Alice", "sex": "female", "gender": "woman", "age": 25},
        {"name": "Bob", "sex": "male", "gender": "man", "age": 30},
        {"name": "Charlie", "sex": "intersex", "gender": "non-binary", "age": 22}
    ])

    # Add more user profiles
    profiles.insert_many([
        {"name": "David", "sex": "male", "gender": "man", "age": 35},
        {"name": "Eve", "sex": "female", "gender": "woman", "age": 28},
        {"name": "Frank", "sex": "male", "gender": "prefer not to state", "age": 17}
    ])

    # Check the inserted profiles in MongoDB Compass.
    

Task1(profiles)


# this function prints all documents in the profiles collection
def print_all(profiles):
    # Retrieve all profiles
    print("All Profiles:")
    all_profiles = profiles.find()
    
    # Print each profile. We are using 'pprint' for pretty print.
    for profile in all_profiles:
        pprint(profile)


def Task2(profiles):
    
    # print all profiles
    print_all(profiles)

    # Retrieve profiles with sex 'female'
    print("\nProfiles with sex 'female':")
    female_profiles = profiles.find({"sex": "female"})
    for profile in female_profiles:
        pprint(profile)

    # Retrieve profiles of users older than 25
    print("\nProfiles older than 25:")
    older_profiles = profiles.find({"age": {"$gt": 25}})
    for profile in older_profiles:
        pprint(profile)
Task2(profiles)



def Task3(profiles):
    # Update a user's gender
    profiles.update_one({"name": "Alice"}, {"$set": {"gender": 'non-binary'}})

    # Add new fields: pronouns and chosen_name
    profiles.update_many({}, {"$set": {"pronouns": "", "chosen_name": ""}})

    # Update pronouns and chosen_name for Alice to "they/them" and "Ally"
    profiles.update_one({"name": "Alice"}, {"$set": {"pronouns": "they/them", "chosen_name": "Ally"}})
   	
    # Update pronouns and chosen_name for Bob to "he/him" and "Bobby"
    profiles.update_one({"name": "Bob"}, {"$set": {"pronouns": "he/him", "chosen_name": "Bobby"}})
    
    print_all(profiles)
    # Also, check the updated profiles in MongoDB Compass.
    
    

Task3(profiles)


def Task4(profiles):
    # Delete Charlie's profile
    profiles.delete_one({"name": "Charlie"})

    # Delete all profiles with age under 18
    profiles.delete_many({"age": {"$lt": 18}})

    print_all(profiles)

Task4(profiles)



print("\nUnguided tasks\n")

def Task5(profs):
    print("\ntask5:")
    theyThemProfiles = profs.find({"pronouns":"they/them"})

    for profile in theyThemProfiles:
        pprint(profile)

Task5(profiles)

def Task6(profs):
    profs.insert_one({"name":"Ben","sex":"male","gender":"agender","pronouns":"they/them","age":22})

Task6(profiles)

Task5(profiles)

def Task7(profs):
    print("\nTask7")
    profs.insert_many([{"chosen_name":"","name": "Jack", "sex": "male", "gender": "non-binary", "age": 35},
	{"name": "Dorothy", "sex": "female", "gender": "woman", "age": 30},
	{"name": "Cathilda", "sex": "female", "gender": "woman", "age": 31,"chosen_name":"Cathy"},
    {"name": "Simon", "sex": "male", "gender": "agender", "age": 32}])

    profs.update_many({"pronouns":{"$exists":False}},{"$set":{"pronouns":"TBC"}})
    
    profs.update_many({
        "$or":[{ "chosen_name":
            {"$exists":False}
            },{ "chosen_name" :
                {"$eq":""}
            }]
        }
        ,{ "$set":{
            "chosen_name":"-"
        }
        })

    print_all(profs)


Task7(profiles)

def Task8(profs):
    print("\n task 8")

    # chosenWomen = profs.find({"$and":[{"chosen_name":{"$ne":"-"}},{"gender":"woman"}]})
    chosenWomen = profs.find({"chosen_name":{"$ne":"-"},"gender":"woman"})

    for prof in chosenWomen:
        pprint(prof)

    print ("\n")

    TBCnb = profs.find({"$and":[
        {"gender":{"$ne":"man"}},
        {"gender":{"$ne":"woman"}},
        {"pronouns":{"$eq":"TBC"}}
    ]})

    for prof in TBCnb:
        pprint(prof)

Task8(profiles)


def Task9(profs):
    print("\n task 9")
    midAgeNB = profs.find({"$and":[
        {"age":{"$lt":40}},
        {"age":{"$gt":30}},
        {"gender":"non-binary"}
    ]
    })

    for prof in midAgeNB:
        pprint(prof)

Task9(profiles)

def Task10(profs):
    print("\n task 10")
    print_all(profs)
    print("\n")
    pfs = profs.find({"$or":[{"chosen_name":{"$exists":False}},{"chosen_name":"-"}]})
    for i in pfs:
        pprint(i)

    profs.update_many({"$or":[{"chosen_name":{"$exists":False}},{"chosen_name":"-"}]},{"$unset":{"pronouns":None}})
    print("\nupdated")

    print_all(profs)

Task10(profiles)


def Task11(profs):
    profs.insert_one({"name":"Jack","chosen_name":"Jaxon","pronouns":"they/them","gender":"gender fluid","age":26,"sex":"male"})

Task11(profiles)


def Task12(profs):
    profs.update_one({"name":"Alice"},{"$set":{"gender":"transgender man","pronouns":"he/him"}})
Task12(profiles)

def Task13(profs):
    print("\nTask 13")
    ageSorted = profs.find().sort("age",-1)

    for i in ageSorted:
        pprint(i)
Task13(profiles)