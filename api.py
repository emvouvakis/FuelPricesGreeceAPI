try:
  import unzip_requirements
except ImportError:
  pass

import pandas as pd
import json
import logging
from io import BytesIO
import boto3
import duckdb

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client("s3")

# S3 bucket and folder configuration
BUCKET_NAME = "fuelprices-greece"
PARSED_FOLDER = "parsed/"

def load_s3_data(key):
    """
    Load data from S3 and return it as a pandas DataFrame.
    
    Args:
        key (str): The S3 key for the data file.
    
    Returns:
        pd.DataFrame: The loaded data as a DataFrame.
    """
    try:
        logger.info(f"Loading data from S3: {key}")
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        buffer = BytesIO(response["Body"].read())
        df = pd.read_parquet(buffer)
        df['DATE'] = pd.to_datetime(df["DATE"], format="%d-%m-%Y")
        logger.info(f"Successfully loaded {key}")
        logger.info(f"Number of rows loaded: {df.shape[0]}")

        return df
    except s3_client.exceptions.NoSuchKey:
        logger.warning(f"Key {key} does not exist in S3.")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading {key}: {e}")
        return pd.DataFrame()


def filter_df(df, filter_conditions, limit=10000, offset=0):
    """
    Filter the DataFrame based on the provided conditions and apply pagination.
    
    Args:
        df (pd.DataFrame): The DataFrame to filter.
        filter_conditions (dict): The conditions to filter the DataFrame.
        limit (int): The maximum number of rows to return.
        offset (int): The number of rows to skip before starting to return rows.
    
    Returns:
        pd.DataFrame: The filtered DataFrame.
    """
    try :
        # Connect to DuckDB and register the DataFrame
        con = duckdb.connect()
        con.register("df", df)
        
        # Build the SQL query based on filter conditions
        query = "SELECT * FROM df WHERE 1=1"
        
        # Apply filter for REGION if provided
        if "REGION" in filter_conditions:
            query += f" AND REGION = '{filter_conditions['REGION']}'"
        
        # Apply date range filter if both start_date and end_date are provided
        if "start_date" in filter_conditions and "end_date" in filter_conditions:
            query += f" AND DATE >= '{filter_conditions['start_date']}' AND DATE <= '{filter_conditions['end_date']}'"
        
        # Apply filter for start_date only if provided
        if "start_date" in filter_conditions and not "end_date" in filter_conditions:
            query += f" AND DATE >= '{filter_conditions['start_date']}'"

        # Apply filter for end_date only if provided
        if not "start_date" in filter_conditions and "end_date" in filter_conditions:
            query += f" AND DATE <= '{filter_conditions['end_date']}'"

        # Apply pagination
        query += f" LIMIT {limit} OFFSET {offset}"

        # Execute the query and get the filtered DataFrame
        filtered_df = con.execute(query).df()

        # Close the connection
        con.close()

        # Change the DATE column to string
        filtered_df['DATE'] = filtered_df['DATE'].dt.strftime('%Y-%m-%d')

        logger.info(f"Final query: {query}")
        logger.info(f"Number of rows after filtering: {filtered_df.shape[0]}")

        return filtered_df
    
    except Exception as e:
        logger.error(f"Error in filter_df: {e}")
        raise



def main(event, context):
    try:
        # Log the received event
        logger.info(f"Received event: {event}")

        # Get the path from the event
        path = event.get("path", "")
        
        # Handle the /help path
        if path == "/help":
            # Define the response structure for help
            response_structure = {
                "filter_conditions": {
                    "region": "string",
                    "start_date": "YYYY-MM-DD",
                    "end_date": "YYYY-MM-DD"
                },
                "pagination": {
                    "limit": "integer",
                    "offset": "integer"
                }
            }

            # Create the response for the /help path
            response = {
                "statusCode": 200,
                "body": json.dumps(response_structure)
            }

        # Handle the /data path
        elif path == "/data":
            # Get filter conditions and pagination parameters from the query string
            filter_conditions = event.get("queryStringParameters", {})
            limit = int(filter_conditions.get("limit", 10000))
            offset = int(filter_conditions.get("offset", 0))

            # Load the data from S3
            df = load_s3_data(PARSED_FOLDER + "cleaned_data.parquet")

            # Check that start_date and end_date are in the correct format
            if "start_date" in filter_conditions:
                filter_conditions["start_date"] = pd.to_datetime(filter_conditions["start_date"]).strftime("%Y-%m-%d")
            
            if "end_date" in filter_conditions:
                filter_conditions["end_date"] = pd.to_datetime(filter_conditions["end_date"]).strftime("%Y-%m-%d")
            
            # Check that start_date is before end_date
            if "start_date" in filter_conditions and "end_date" in filter_conditions:
                if filter_conditions["start_date"] > filter_conditions["end_date"]:
                    raise ValueError("start_date must be before end_date.")

            # Check that the REGION filter condition is valid
            if "REGION" in filter_conditions:
                if filter_conditions["REGION"] not in df["REGION"].unique():
                    raise ValueError(f"REGION {filter_conditions['REGION']} does not exist in the data.Available Regions: {df['REGION'].unique()}")

            # Filter the DataFrame based on the conditions and pagination
            filtered_df = filter_df(df, filter_conditions, limit, offset)
            
            # Create the response with the filtered data
            response = {
                "statusCode": 200,
                "body": filtered_df.to_json(orient="records")
            }

        # Handle the root path
        else:
            # Create the response for the root path
            response = {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": "Welcome to my Fuel Prices Greece API project.",
                        "paths": {
                            "/": "List of available paths",
                            "/help": "Get help on how to use the API",
                            "/data": "Get the data"
                        }
                    }
                )
            }

    except Exception as e:
        # Log the error and create an error response
        logger.error(f"Error: {e}")
        response = {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
    
    return response