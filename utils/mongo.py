import os
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
import csv
import io
from typing import List, Dict, Any, Optional
import gridfs

MONGO_URI = ""

class MongoDBClient:
    def __init__(self, uri: str = MONGO_URI):
        self.uri = uri
        self.client = None
        self.db = None
        self.fs = None
        

    def connect_to_database(self):
        """Connect to MongoDB and return the database instance"""
        try:
            # Create client with server API version (similar to your JS version)
            self.client = MongoClient(
                self.uri,
                server_api=ServerApi('1', strict=True, deprecation_errors=True)
            )
            
            # Test the connection
            self.client.admin.command('ping')
            print("Successfully connected to MongoDB!")
            
            # Return the 'funnel' database
            self.db = self.client['funnel']
            
            # Initialize GridFS for file storage
            self.fs = gridfs.GridFS(self.db)
            
            return self.db
            
        except Exception as error:
            print(f"Failed to connect to MongoDB: {error}")
            raise error


    def save_csv_with_reference(self, csv_content: str, filename: str, collection_name: str = "csv_files") -> Dict[str, str]:
        """
        Save CSV content as file in GridFS AND create a reference document in a collection
        
        Args:
            csv_content: The CSV content as a string
            filename: Name of the original file
            collection_name: Name of the collection for references (defaults to "csv_files")
            
        Returns:
            Dict with both file_id and document_id
        """
        if self.fs is None or self.db is None:
            raise Exception("Database not connected. Call connect_to_database() first.")
        
        # Parse CSV content for metadata
        csv_data = []
        if csv_content.strip():
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            csv_data = list(csv_reader)
        
        # Create file metadata
        file_metadata = {
            "original_filename": filename,
            "upload_date": datetime.utcnow(),
            "content_type": "text/csv",
            "row_count": len(csv_data),
            "columns": list(csv_data[0].keys()) if csv_data else []
        }
        
        # Save CSV content as file in GridFS
        file_id = self.fs.put(
            csv_content.encode('utf-8'),
            filename=f"{filename}.csv",
            metadata=file_metadata
        )
        
        # Create reference document in regular collection
        reference_document = {
            "original_filename": filename,
            "csv_filename": f"{filename}.csv",
            "gridfs_file_id": file_id,
            "upload_date": datetime.utcnow(),
            "file_size": len(csv_content.encode('utf-8')),
            "row_count": len(csv_data),
            "columns": list(csv_data[0].keys()) if csv_data else [],
            "status": "completed"
        }
        
        # Insert reference into collection
        collection = self.db[collection_name]
        doc_result = collection.insert_one(reference_document)
        
        print(f"CSV file saved to GridFS with ID: {file_id}")
        print(f"Reference document saved to collection '{collection_name}' with ID: {doc_result.inserted_id}")
        
        return {
            "file_id": str(file_id),
            "document_id": str(doc_result.inserted_id)
        }


# Global instance for easy usage
mongo_client = MongoDBClient()

# Helper functions for easy usage
def connect_to_database():
    """Helper function to connect to database"""
    return mongo_client.connect_to_database()


def close_database_connection():
    """Helper function to close database connection"""
    mongo_client.close_database_connection()


def save_csv_file_to_mongodb(csv_content: str, filename: str) -> Dict[str, str]:
    """
    Helper function to save CSV as file to MongoDB GridFS with reference document
    
    Args:
        csv_content: The CSV content as a string
        filename: Name of the original file
        
    Returns:
        Dict with file_id and document_id
    """
    return mongo_client.save_csv_with_reference(csv_content, filename)


