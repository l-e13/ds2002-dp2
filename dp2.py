from pymongo import MongoClient
import os
import json

# connecting to mongodb
MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
db = client.jww2fj
collection = db.dp2

# Counters for tracking doc status
complete_imports = 0
num_corrupted_documents = 0
failed_imports = 0

# looping through files and inserting into DB
for (root, dirs, files) in os.walk("data/"):
    for f in files:
        if f.endswith('.json'):
            file_path = os.path.join(root, f)
            try:
                with open(file_path) as file:
                    data = json.load(file)
                    if isinstance(data, list):
                        collection.insert_many(data)
                        complete_imports += len(data)
                    else:
                        collection.insert_one(data)
                        complete_imports += 1
            except json.JSONDecodeError as e:
                num_corrupted_documents += 1
            except Exception as e:
                if isinstance(files, list):
                    failed_imports += len(files)
                else:
                    failed_imports += 1

            #except Exception as e:
              # num_corrupted_documents += 1
              #  failed_imports += 1

# count the number of imported documents
print(f"Number of complete imports: {complete_imports}")

# count the number of corrupted documents
print(f"Number of corrupted documents: {num_corrupted_documents}")

# count the number of incomplete documents (not imported)
print(f"Number of failed imports: {failed_imports}")
