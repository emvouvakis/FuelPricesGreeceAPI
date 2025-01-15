try:
  import unzip_requirements
except ImportError:
  pass

import io
import os
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import pickle
import logging
import pandas as pd
import camelot

class S3Handler:
    def __init__(self):
        
        # Get bucket name from the environment variable
        self.bucket_name = os.environ.get("S3_BUCKET")

        self.s3_client = boto3.client("s3")
    
   # Function to load historical data from S3
    def load_s3_data(self, key):
        """
        Load historical data from S3.
        
        :param key: S3 object key
        :return: Deserialized data or an empty dictionary on failure
        """
        try:
            logger.info(f"Loading data from S3: {key}")
            response = self.s3_client.get_object(Bucket = self.bucket_name, Key=key)
            file_extension = key.split('.')[-1].lower()

            if file_extension == 'pkl':
                data = pickle.loads(response["Body"].read())
            elif file_extension == 'parquet':
                buffer = io.BytesIO(response["Body"].read())
                data = pd.read_parquet(buffer)
            else:
                raise ValueError(f"Unsupported file extension: {file_extension}")

            logger.info(f"[INFO] Successfully loaded {key}")
            return data
        except self.s3_client.exceptions.NoSuchKey:
            logger.error(f"[ERROR] Key {key} does not exist in S3.")
            return {}
        except Exception as e:
            logger.error(f"[ERROR] Error loading {key}: {e}")
            return {}

    def save_to_s3(self, data, key):
        """
        Save processed data back to S3.
        
        :param data: Data to serialize and save
        :param key: S3 object key
        """
        try:
            logger.info(f"Saving data to S3: {key}")
            serialized_data = pickle.dumps(data)
            self.s3_client.put_object(
                Bucket = self.bucket_name,
                Key=key,
                Body=serialized_data,
                ContentType="application/octet-stream"
            )
            logger.info(f"[INFO] Successfully saved {key} to S3.")
        except Exception as e:
            logger.error(f"[ERROR] Error saving {key} to S3: {e}")

    def save_to_s3(self, data, key):
        try:
            
            # Extract file extension from the key
            file_extension = key.split('.')[-1].lower()
            
            # Log the key and detected file type
            logger.info(f"[INFO] Saving data to S3: {key} with file extension: {file_extension}")
            

            if file_extension == 'parquet':
                data.columns = data.columns.map(str)
                with io.BytesIO() as buffer:
                    # Write the DataFrame to Parquet format in the buffer
                    data.to_parquet(buffer, index=False)
                    buffer.seek(0)  # Move cursor to the start of the buffer

                    self.s3_client.put_object(
                        Bucket= self.bucket_name,
                        Key=key,
                        Body=buffer.getvalue(),
                        ContentType="application/octet-stream"
                    )

            # Check if the file is a Pickle file (.pkl)
            elif file_extension == 'pkl':
                serialized_data = pickle.dumps(data)
                self.s3_client.put_object(
                    Bucket = self.bucket_name,
                    Key=key,
                    Body=serialized_data,
                    ContentType="application/octet-stream"
                )
            
            # If the file extension is neither xlsx nor pkl, raise an error
            else:
                raise ValueError(f"Unsupported file extension: {file_extension}")
            
            logger.info(f"[INFO] Successfully saved {key} to S3.")
        
        except Exception as e:
            logger.error(f"[ERROR] Error saving {key} to S3: {e}")


# Function to download and upload a single PDF to S3
def download_file(link):
    file_name = link['href'].split('NOMO')[-1].replace("_", "")
    file_date_str = file_name.split(".")[0]

    # Skip unwanted files
    unwanted = ["l.pdf", "?.pdf", ").pdf"]
    if any(unwanted_item in file_name for unwanted_item in unwanted):
        return None
    
    # Download the file only if it's not already processed
    if file_date_str not in all_data.keys() and file_date_str not in all_errors:
        logger.info(f"Downloading file {file_name} from {link['href']}")
        response = requests.get(urljoin(url, link['href']))
        if response.status_code == 200:

            temp_file_path = f"/tmp/{file_date_str}.pdf"

            with open(temp_file_path, 'wb') as f:
                f.write(response.content)

            logger.info(f"[INFO] Successfully uploaded {file_name} to S3")
            return file_name
        else:
            logger.error(f"[ERROR] Failed to download {file_name} from {link['href']} - Status code: {response.status_code}")
    return None


def process_pdf(filename):
    logger.info(f"Starting processing:{filename}")
    name = filename.split(".")[0]

    if len(name) != 8:
        logger.info(f"File {filename} is not valid.")
        return name, None

    if name in all_data or name in all_errors:
        
        if name in all_data:
            logger.info(f"[INFO] File {filename} already processed and found in all_data.")
        elif name in all_errors:
            logger.info(f"[INFO] File {filename} already processed and found in all_errors.")

        return None, None  # Already processed

    try:
        # Read all pages of the PDF
        temp_table = camelot.read_pdf(filename, pages="all", flavor="stream")
    except Exception as e:
        logger.error(f"exception reading {filename}: {e}")
        return name, None  # Return None if reading fails

    try:
        # Process all detected parts dynamically
        dfs = [table.df for table in temp_table if len(table.df)>15]
        
        combined_df = pd.concat(dfs).reset_index(drop=True)
        logger.info(f"[INFO] Finished processing:{filename}")
        return name, combined_df
    except Exception as e:
        logger.error(f"[ERROR] Exception processing {filename}: {e}")
        return name, None  # Return None if processing fails


# Lambda main
def main(event, context):
    global url, all_data, all_errors, logger
    url = 'http://www.fuelprices.gr/deltia_dn.view'

    # Configure logging 
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    logger.info("[INFO] Starting web scraping process.")

    # Load historical data from S3
    handler = S3Handler()
    history = handler.load_s3_data( os.path.join(os.environ.get("PARSED_FOLDER"), os.environ.get("HISTORY_DATA")) )

    all_data = history.get("data", {})
    all_errors = history.get("errors", [])

    logger.info(f"[INFO] Historical data loaded: {len(all_data.keys())}")
    logger.info(f"[INFO] Historical errors loaded: {len(all_errors)}")
    
    # Scrape the website for PDF links
    response = requests.get(url)    
    soup = BeautifulSoup(response.text, "html.parser")
    pdf_links = soup.select("a[href$='.pdf']")

    logger.info(f"[INFO] Found {len(pdf_links)} PDF links.")

    # Download files in parallel
    file_count = 0
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(download_file, pdf_links))
        file_count = sum(1 for result in results if result is not None)

    logger.info(f"[INFO] Downloaded {file_count} new PDF files.")


    logger.info(f"[INFO] Starting parsing pdfs.")

    
    os.chdir('/tmp')
    filenames = [f for f in os.listdir(os.getcwd()) if f.endswith('.pdf')]
    logger.info(filenames)
    parsed_count = 0
    error_count = 0

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_pdf, filename): filename for filename in filenames}

        for i, future in enumerate(as_completed(futures)):
            filename = futures[future]
            try:
                name, df = future.result()
                if name and df is not None:
                    all_data[name] = df
                    parsed_count += 1
                elif name:
                    all_errors.append(name)
                    logging.error(f"[ERROR] Error processing {filename}")
                    error_count += 1
            except Exception as e:
                logging.error(f"[ERROR] Exception processing {filename}: {e}")
                all_errors.append(name)
                error_count += 1
            
            # Logging progress
            logging.info(f"[INFO] Processed {i + 1}/{len(futures)} files")

    if parsed_count > 0:
        logger.info(f"[INFO] Parsed {parsed_count} pdfs.")
        # Add this at the end of your handler function
        logger.info("[INFO] Saving processed data back to S3.")

        history = {'data': all_data, 'errors': all_errors}
        handler.save_to_s3(history, os.path.join(os.environ.get("PARSED_FOLDER"), os.environ.get("HISTORY_DATA")) )

        logger.info("[INFO] Data saving completed.")

    else:
        logger.info("[INFO] No new pdfs parsed.")

    logger.info(f"[INFO] Total successful parses: {parsed_count}")
    logger.info(f"[INFO] Total errors: {error_count}")


    return {
        "statusCode": 200,
        "downloaded": file_count,
        "parsed": parsed_count        
    }
