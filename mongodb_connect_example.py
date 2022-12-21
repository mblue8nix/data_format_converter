'''mblue
This is a simple example for loading json to mongodb 
'''
import argparse
import pymongo
import json
import sys
from pymongo import MongoClient, InsertOne


parser = argparse.ArgumentParser()
parser.add_argument('--host', help='DB host name',\
     nargs='*', dest='host_name')
parser.add_argument('--upload', '-u', help='Upload text file',\
     nargs='*', dest='file_name')

args = (parser.parse_args())

if args.file_name is None:
    print(" Usage is --upload loadfile.json")
    sys.exit()

else:
    if args.file_name:
        FILE_NAME = ' '.join(args.file_name)

if args.host_name:
        HOST_NAME = ' '.join(args.host_name)
else:
    if args.host_name is None:
        HOST_NAME = 'localhost:27017'


client = pymongo.MongoClient(HOST_NAME)

db = client.cv
collection = db.mini_cv
requesting = []

def load_json():
    with open(FILE_NAME, "r") as f:
        for jsonObj in f:
            miniCv = json.loads(jsonObj)
            requesting.append(InsertOne(miniCv))
    print(miniCv)
    result = collection.bulk_write(requesting)
    client.close()

load_json()
