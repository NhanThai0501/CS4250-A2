#-------------------------------------------------------------------------
# AUTHOR: Nhan Thai
# FILENAME: index_mongo.py
# SPECIFICATION: Many functions to modify the database
# FOR: CS 4250 - Assignment #2
# TIME SPENT: 7 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient 
import datetime


def connectDataBase():

    # Create a database connection object using pymongo
    # --> add your Python code here
    DB_NAME = "CPP"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try:

        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]

        return db

    except:
        print("Database not connected successfully")

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary (document) to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    term_count = {}
    words = docText.lower().split()  # Tokenizing the document text

    for word in words:
        if word in term_count:
            term_count[word] += 1
        else:
            term_count[word] = 1

    # create a list of dictionaries (documents) with each entry including a term, its occurrences, and its num_chars. Ex: [{term, count, num_char}]
    # --> add your Python code here
    term_data = [{"term": word, "count": count, "num_char": len(word)} for word, count in term_count.items()]

    #Producing a final document as a dictionary including all the required fields
    # --> add your Python code here
    document = {
        "id": docId,
        "text": docText,
        "title": docTitle,
        "date": docDate,
        "category": docCat,
        "terms": term_data  # storing the term data
    }

    # Insert the document
    # --> add your Python code here
    col.insert_one(document)
    print(f"Document with ID {docId} has been created.")

def deleteDocument(col, docId):

    # Delete the document from the database
    # --> add your Python code here
    result = col.delete_one({"id": docId})
    if result.deleted_count > 0:
        print(f"Document with ID {docId} has been deleted.")
    else:
        print(f"Document with ID {docId} not found.")


def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    deleteDocument(col, docId)

    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3', ...}
    # We are simulating an inverted index here in memory.
    # --> add your Python code here
    all_documents = col.find()
    inverted_index = {}
    
    for doc in all_documents:
        words = doc['text'].lower().split()  # Tokenizing the document text
        title = doc['title']
        for word in words:
            if word in inverted_index:
                if title in inverted_index[word]:
                    inverted_index[word][title] += 1
                else:
                    inverted_index[word][title] = 1
            else:
                inverted_index[word] = {title: 1}
    
    # Format and sort the inverted index output
    sorted_index = {term: ', '.join([f"{title}:{count}" for title, count in sorted(docs.items())])
                    for term, docs in sorted(inverted_index.items())}
    return sorted_index