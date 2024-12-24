try:
  import unzip_requirements
except ImportError:
  pass

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
    def __init__(self, bucket_name, parsed_folder):
        
        # AWS S3 bucket and folder configurations (set as environment variables)
        self.bucket_name = bucket_name
        self.parsed_folder = parsed_folder

        self.s3_client = boto3.client("s3")
    
    def load_s3_data(self, key):
        """
        Load historical data from S3.
        
        :param key: S3 object key
        :return: Deserialized data or an empty dictionary on failure
        """
        try:
            logger.info(f"Loading data from S3: {key}")
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            data = pickle.loads(response["Body"].read())
            logger.info(f"Successfully loaded {key}")
            return data
        except self.s3_client.exceptions.NoSuchKey:
            logger.warning(f"Key {key} does not exist in S3.")
            return {}
        except Exception as e:
            logger.error(f"Error loading {key}: {e}")
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
                Bucket=self.bucket_name,
                Key=key,
                Body=serialized_data,
                ContentType="application/octet-stream"
            )
            logger.info(f"Successfully saved {key} to S3.")
        except Exception as e:
            logger.error(f"Error saving {key} to S3: {e}")


# Function to download and upload a single PDF to S3
def download_file(link):
    file_name = link['href'].split('NOMO')[-1].replace("_", "")
    file_date_str = file_name.split(".")[0]

    unwanted = ["l.pdf", "?.pdf", ").pdf"]
    if any(unwanted_item in file_name for unwanted_item in unwanted):
        return None

    if file_date_str not in all_data.keys() and file_date_str not in all_errors.keys():
        logger.info(f"Downloading file {file_name} from {link['href']}")
        response = requests.get(urljoin(url, link['href']))
        if response.status_code == 200:

            temp_file_path = f"/tmp/{file_date_str}.pdf"

            with open(temp_file_path, 'wb') as f:
                f.write(response.content)

            logger.info(f"Successfully uploaded {file_name} to S3")
            return file_name
        else:
            logger.error(f"Failed to download {file_name} from {link['href']} - Status code: {response.status_code}")
    return None


def process_pdf(filename):
    logger.info(f"Starting processing:{filename}")
    name = filename.split(".")[0]
    date = datetime.strptime(name, "%d%m%Y").date()

    if name in all_data or name in all_errors:
        
        if name in all_data:
            logger.info(f"File {filename} already processed and found in all_data.")
        elif name in all_errors:
            logger.info(f"File {filename} already processed and found in all_errors.")

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
        logger.info(f"Finished processing:{filename}")
        return name, combined_df
    except Exception as e:
        logger.error(f"Exception processing {filename}: {e}")
        return name, None  # Return None if processing fails


# Lambda main
def main(event, context):
    global url, all_data, all_errors, logger
    url = 'http://www.fuelprices.gr/deltia_dn.view'

    handler = S3Handler(bucket_name="fuelprices-greece",
                    parsed_folder="parsed/")

    # AWS S3 bucket and folder configurations (set as environment variables)
    PARSED_FOLDER = handler.parsed_folder

    # Configure logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    logger.info("Starting web scraping process.")

    # Load historical data from S3
    all_data = handler.load_s3_data(PARSED_FOLDER + "all_data.pkl")
    all_errors = handler.load_s3_data(PARSED_FOLDER + "all_errors.pkl")
    
    # Scrape the website for PDF links
    response = requests.get(url)
    if response.status_code != 200:
        logger.error(f"Failed to fetch the website content - Status code: {response.status_code}")
        return {
            "statusCode": 500,
            "body": "Failed to fetch the website content."
        }
    
    soup = BeautifulSoup(response.text, "html.parser")
    pdf_links = soup.select("a[href$='.pdf']")

    logger.info(f"Found {len(pdf_links)} PDF links.")

    # Download files in parallel
    file_counter = 0
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(download_file, pdf_links))
        file_counter = sum(1 for result in results if result is not None)

    logger.info(f"Downloaded {file_counter} new PDF files.")


    logger.info(f"Starting parsing pdfs.")

    
    os.chdir('/tmp')
    filenames = os.listdir(os.getcwd())
    logger.info(filenames)
    number = 0
    error_count = 0

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_pdf, filename): filename for filename in filenames}

        for i, future in enumerate(as_completed(futures)):
            filename = futures[future]
            try:
                name, df = future.result()
                if name and df is not None:
                    all_data[name] = df
                    number += 1
                elif name:
                    all_errors[name] = None
                    logging.error(f"Error processing {filename}")
                    error_count += 1
            except Exception as e:
                logging.error(f"Exception processing {filename}: {e}")
                all_errors[filename] = str(e)
                error_count += 1
            
            # Logging progress
            logging.info(f"Processed {i + 1}/{len(futures)} files")

    if number > 0:
        logger.info(f"Parsed {number} pdfs.")
        # Add this at the end of your handler function
        logger.info("Saving processed data back to S3.")

        handler.save_to_s3(all_data, PARSED_FOLDER + "all_data.pkl")
        handler.save_to_s3(all_errors, PARSED_FOLDER + "all_errors.pkl")

        logger.info("Data saving completed.")
    else:
        logger.info("No new pdfs parsed.")

    logger.info(f"Total successful parses: {number}")
    logger.info(f"Total errors: {error_count}")


    return {
        "statusCode": 200,
        "body": f"Downloaded {file_counter} new PDF files and parsed {number} files."
    }
