from pymongo import MongoClient
from typing import List, Optional
import os


class DBDefinitionError(Exception):
    """
    Exception raised for errors in defining the DB client.
    """

    def __init__(self, database: str = None, collection: str = None):
        if database is None and collection is None:
            self.message = "Database name and collection name are required for creating the DB client."
        elif collection is None:
            self.message = "Collection name is required for creating the DB client."
        elif database is None:
            self.message = "Database name is required for creating the DB client."
        else:
            self.message = "Initiallization error."
        super().__init__(self.message)


class Mongo:
    """
    Initializes a custom MongoClient class to use for DB operations

    Requirements:
        database -> the name of the database to connect to. \n
        collection -> the name of the collection to use.
    """

    def __init__(self, database: str = None, collection: str = None):
        """
        Initiates a "pymongo" instance and sets the "client" and "collection" attributes based on
        environmental variables and arguments (database and collection) passed.
        """
        if database is not None and collection is not None:
            self.client = MongoClient(
                f"mongodb+srv://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_URL']}/"
            )
            self.collection = self.client[database][collection]
        else:
            raise DBDefinitionError(database, collection)

    def find_one(self, query: dict = {}, fields: Optional[dict] = {}) -> dict:
        """
        Takes a "query" dictonary and searches the DB for such one document.\n
        If an optional "fields" dictionary is provided, then only those fields will be returned\n
        Returns a dictionary of the found document.
        """
        return self.collection.find_one(query, fields)

    def find_many(
        self, query: dict = {}, fields: Optional[dict] = {}, limit: Optional[int] = None
    ) -> List[dict]:
        """
        Takes a "query" dictonary and searches the DB for any such documents.\n
        If an optional "fields" dictionary is provided, then only those fields will be returned\n
        If an optional "limit" integer is provided, then only that number of documents will be returned\n
        Returns a list of dictionaries found.
        """
        if limit:
            return [entry for entry in self.collection.find(query, fields).limit(limit)]
        else:
            return [entry for entry in self.collection.find(query, fields)]

    def update_one(
        self, query: dict = {}, update: dict = {}, upsert: Optional[bool] = True
    ) -> dict:
        """
        Takes a "query" dictonary and searches the DB for any such documents.\n
        Takes an "update" dictionary and updates the found results with the provided data\n
        If no documents are found, it inserts a new document with the provided data,
        unless "upsert" is explicitly set to False.\n
        Returns a "results" dictionary of the update operation.
        """
        update_result = self.collection.update_one(
            query, {"$set": update}, upsert=upsert
        )
        if update_result.modified_count != 0:
            results = {
                "matches_found": update_result.matched_count,
                "modified_results": update_result.modified_count,
                "raw_results": update_result.raw_result,
            }
            return results
        else:
            return None

    def update_many(
        self, query: dict = {}, update: dict = {}, upsert: Optional[bool] = True
    ) -> dict:
        """
        Takes a "query" dictonary and searches the DB for any such documents.\n
        Takes an "update" dictionary and updates all found results with the provided data\n
        If no documents are found, it inserts a new document with the provided data,
        unless "upsert" is explicitly set to False.\n
        Returns a "results" dictionary of the update operation.
        """
        update_result = self.collection.update_many(
            query, {"$set": update}, upsert=upsert
        )
        if update_result.modified_count != 0:
            results = {
                "matches_found": update_result.matched_count,
                "modified_results": update_result.modified_count,
                "raw_results": update_result.raw_result,
            }
            return results
        else:
            return None

    def insert_one(self, document: dict = {}) -> dict:
        """
        Takes a "document" dictonary and inserts it to the DB
        Returns the "ObjectId" of the inserted document.
        """
        result = self.collection.insert_one(document)
        if result.inserted_id:
            return result.inserted_id
        else:
            return None

    def insert_many(self, documents: List[dict] = {}) -> List[dict]:
        """
        Takes a List of "documents" (dictonaries) and inserts them into the DB
        Returns the List of "ObjectId"'s of the inserted documents.
        """
        result = self.collection.insert_many(documents)
        if result.inserted_ids:
            return result.inserted_ids
        else:
            return None
