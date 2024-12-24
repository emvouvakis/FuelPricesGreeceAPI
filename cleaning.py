try:
  import unzip_requirements
except ImportError:
  pass

import logging
import pickle
import boto3
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import json
import io

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

import unicodedata

def clean_text(text):

    if not isinstance(text, str):
        return text

    # Normalize to NFC form (canonical decomposition and composition)
    text = unicodedata.normalize('NFKC', text)
    
    # Define the replacement dictionary
    replacements = {
        '΢':'Σ',
        'Τγραέριο': 'Υγραέριο',
        'Τγραζριο': 'Υγραέριο',
        'Θζρμανσης': 'Θέρμανσης',
        'Αμόλσβδη': 'Αμόλυβδη',
        'Θέρμανση ς': 'Θέρμανσης',
        'Θέρμανσ ης': 'Θέρμανσης',
        'Υγραζριο':'Υγραέριο',
        'ΝΟΜΟΣ ΑΤΤΙΚΗΣ': 'N. ATHINON',
        'ΝΟΜΟΣ ΑΣΣΙΚΗΣ': 'N. ATHINON',
        'ΝΟΜΟΣ ΑΣΣΗΚΖΣ': 'N. ATHINON',
        'ΝΟΜΟΣ ΥΑΝΗΩΝ': 'N. ATHINON',
        'ΝΟΜΟΣ ΑΙΤΩΛΙΑΣ ΚΑΙ ΑΚΑΡΝΑΝΙΑΣ': 'N. ETOLOAKARNANIAS',
        'ΝΟΜΟΣ ΑΙΣΩΛΙΑΣ ΚΑΙ ΑΚΑΡΝΑΝΙΑΣ': 'N. ETOLOAKARNANIAS',
        'ΝΟΜΟΣ ΑΗΣΩΛΗΑΣ ΚΑΗ ΑΚΑΡΝΑΝΗΑΣ': 'N. ETOLOAKARNANIAS',
        'ΑΚΑΡΝΑΝΙΑΣ': 'N. ETOLOAKARNANIAS',
        'ΑΚΑΡΝΑΝΗΑΣ': 'N. ETOLOAKARNANIAS',
        'ΝΟΜΟΣ ΑΡΓΟΛΙΔΟΣ': 'N. ARGOLIDAS',
        'ΝΟΜΟΣ ΑΡΓΟΛΗΓΟΣ': 'N. ARGOLIDAS',
        'ΝΟΜΟΣ ΑΡΚΑΔΙΑΣ': 'N. ARKADIAS',
        'ΝΟΜΟΣ ΑΡΚΑΓΗΑΣ': 'N. ARKADIAS',
        'ΝΟΜΟΣ ΑΡΤΗΣ': 'N. ARTAS',
        'ΝΟΜΟΣ ΑΡΣΗΣ': 'N. ARTAS',
        'ΝΟΜΟΣ ΑΡΣΖΣ': 'N. ARTAS',
        'ΝΟΜΟΣ ΑΥΑΪΑΣ': 'N. ACHAIAS',
        'ΝΟΜΟΣ ΑΧΑΪΑΣ': 'N. ACHAIAS',
        'ΝΟΜΟΣ ΒΟΙΩΤΙΑΣ': 'N. VIOTIAS',
        'ΝΟΜΟΣ ΒΟΙΩΣΙΑΣ': 'N. VIOTIAS',
        'ΝΟΜΟΣ ΒΟΗΩΣΗΑΣ': 'N. VIOTIAS',
        'ΝΟΜΟΣ ΓΡΕΒΕΝΩΝ': 'N. GREVENON',
        'ΝΟΜΟΣ ΓΡΔΒΔΝΩΝ': 'N. GREVENON',
        'ΝΟΜΟΣ ΓΡΕΒΕΝΩ Ν': 'N. GREVENON',
        'ΝΟΜΟΣ ΔΡΑΜΑΣ': 'N. DRAMAS',
        'ΝΟΜΟΣ ΓΡΑΜΑΣ': 'N. DRAMAS',
        'ΝΟΜΟΣ ΔΩΔΕΚΑΝΗΣΟΥ': 'N. DODEKANISON',
        'ΝΟΜΟΣ ΔΩΔΕΚΑΝΗΣΟΤ': 'N. DODEKANISON',
        'ΝΟΜΟΣ ΔΩ ΔΕΚΑΝΗΣΟΤ': 'N. DODEKANISON',
        'ΝΟΜΟΣ ΓΩΓΔΚΑΝΖΣΟΤ': 'N. DODEKANISON',
        'ΝΟΜΟΣ ΕΒΡΟΥ': 'N. EVROU',
        'ΝΟΜΟΣ ΕΒΡΟΤ': 'N. EVROU',
        'ΝΟΜΟΣ ΔΒΡΟΤ': 'N. EVROU',
        'ΝΟΜΟΣ ΕΥΒΟΙΑΣ': 'N. EVVIAS',
        'ΝΟΜΟΣ ΕΤΒΟΙΑΣ': 'N. EVVIAS',
        'ΝΟΜΟΣ ΔΤΒΟΗΑΣ': 'N. EVVIAS',
        'ΝΟΜΟΣ ΕΥΡΥΤΑΝΙΑΣ': 'N. EVRYTANIAS',
        'ΝΟΜΟΣ ΖΑΚΥΝΘΟΥ': 'N. ZAKYNTHOU',
        'ΝΟΜΟΣ ΖΑΚΤΝΘΟΤ': 'N. ZAKYNTHOU',
        'ΝΟΜΟΣ ΗΛΕΙΑΣ': 'N. ILIAS',
        'ΝΟΜΟΣ ΗΜΑΘΙΑΣ': 'N. IMATHIAS',
        'ΝΟΜΟΣ ΖΜΑΘΗΑΣ': 'N. IMATHIAS',
        'ΝΟΜΟΣ ΗΡΑΚΛΕΙΟΥ': 'N. IRAKLIOU',
        'ΝΟΜΟΣ ΗΡΑΚΛΕΙΟΤ': 'N. IRAKLIOU',
        'ΝΟΜΟΣ ΗΡΑΚΛΕΙΟΤ': 'N. IRAKLIOU',
        'ΝΟΜΟΣ ΖΡΑΚΛΔΗΟΤ': 'N. IRAKLIOU',
        'ΝΟΜΟΣ ΘΕΣΠΡΩΤΙΑΣ': 'N. THESPROTIAS',
        'ΝΟΜΟΣ ΘΔΣΠΡΩΣΗΑΣ': 'N. THESPROTIAS',
        'ΝΟΜΟΣ ΘΕΣΠΡΩΣΙΑΣ': 'N. THESPROTIAS',
        'ΝΟΜΟΣ ΘΕΣΣΑΛΟΝΙΚΗΣ': 'N. THESSALONIKIS',
        'ΝΟΜΟΣ ΘΔΣΣΑΛΟΝΗΚΖΣ': 'N. THESSALONIKIS',
        'ΝΟΜΟΣ ΙΩΑΝΝΙΝΩΝ': 'N. IOANNINON',
        'ΝΟΜΟΣ ΗΩΑΝΝΗΝΩΝ': 'N. IOANNINON',
        'ΝΟΜΟΣ ΚΑΒΑΛΑΣ': 'N. KAVALAS',
        'ΝΟΜΟΣ ΚΑΡΔΙΤΣΗΣ': 'N. KARDITSAS',
        'ΝΟΜΟΣ ΚΑΡΔΙΣΣΗΣ': 'N. KARDITSAS',
        'ΝΟΜΟΣ ΚΑΡΓΗΣΣΖΣ': 'N. KARDITSAS',
        'ΝΟΜΟΣ ΚΑΣΤΟΡΙΑΣ': 'N. KASTORIAS',
        'ΝΟΜΟΣ ΚΑΣΣΟΡΙΑΣ': 'N. KASTORIAS',
        'ΝΟΜΟΣ ΚΑΣΣΟΡΗΑΣ': 'N. KASTORIAS',
        'ΝΟΜΟΣ ΚΕΡΚΥΡΑΣ': 'N. KERKYRAS',
        'ΝΟΜΟΣ ΚΔΡΚΤΡΑΣ': 'N. KERKYRAS',
        'ΝΟΜΟΣ ΚΕΡΚΤΡΑΣ': 'N. KERKYRAS',
        'ΝΟΜΟΣ ΚΕΦΑΛΛΗΝΙΑΣ': 'N. KEFALLONIAS',
        'ΝΟΜΟΣ ΚΔΦΑΛΛΖΝΗΑΣ': 'N. KEFALLONIAS',
        'ΝΟΜΟΣ ΚΙΛΚΙΣ': 'N. KILKIS',
        'ΝΟΜΟΣ ΚΗΛΚΗΣ': 'N. KILKIS',
        'ΝΟΜΟΣ ΚΟΖΑΝΗΣ': 'N. KOZANIS',
        'ΝΟΜΟΣ ΚΟΕΑΝΖΣ': 'N. KOZANIS',
        'ΝΟΜΟΣ ΚΟΡΙΝΘΙΑΣ': 'N. KORINTHOU',
        'ΝΟΜΟΣ ΚΟΡΗΝΘΗΑΣ': 'N. KORINTHOU',
        'ΝΟΜΟΣ ΚΥΚΛΑΔΩΝ': 'N. KYKLADON',
        'ΝΟΜΟΣ ΚΤΚΛΑΔΩ Ν': 'N. KYKLADON',
        'ΝΟΜΟΣ ΚΤΚΛΑΓΩΝ': 'N. KYKLADON',
        'ΝΟΜΟΣ ΚΤΚΛΑΔΩΝ': 'N. KYKLADON',
        'ΝΟΜΟΣ ΛΑΚΩΝΙΑΣ': 'N. LAKONIAS',
        'ΝΟΜΟΣ ΛΑΚΩ ΝΙΑΣ': 'N. LAKONIAS',
        'ΝΟΜΟΣ ΛΑΚΩΝΗΑΣ':'N. LAKONIAS',
        'ΝΟΜΟΣ ΛΑΡΙΣΗΣ': 'N. LARISAS',
        'ΝΟΜΟΣ ΛΑΡΗΣΖΣ': 'N. LARISAS',
        'ΝΟΜΟΣ ΛΑΣΙΘΙΟΥ': 'N. LASITHIOU',
        'ΝΟΜΟΣ ΛΑΣΗΘΗΟΤ': 'N. LASITHIOU',
        'ΝΟΜΟΣ ΛΑΣΙΘΙΟΤ': 'N. LASITHIOU',
        'ΝΟΜΟΣ ΛΕΣΒΟΥ': 'N. LESVOU',
        'ΝΟΜΟΣ ΛΕΣΒΟΤ': 'N. LESVOU',
        'ΝΟΜΟΣ ΛΔΣΒΟΤ': 'N. LESVOU',
        'ΝΟΜΟΣ ΛΕΣΒΟ Τ': 'N. LESVOU',
        'ΝΟΜΟΣ ΛΕΥΚΑΔΟΣ': 'N. LEFKADAS',
        'ΝΟΜΟΣ ΛΕΤΚΑΔΟΣ': 'N. LEFKADAS',
        'ΝΟΜΟΣ ΛΔΤΚΑΓΟΣ': 'N. LEFKADAS',
        'ΝΟΜΟΣ ΜΑΓΝΗΣΙΑΣ': 'N. MAGNISIAS',
        'ΝΟΜΟΣ ΜΕΣΣΗΝΙΑΣ': 'N. MESSINIAS',
        'ΝΟΜΟΣ ΜΔΣΣΖΝΗΑΣ': 'N. MESSINIAS',
        'ΝΟΜΟΣ ΜΑΓΝΖΣΗΑΣ': 'N. MESSINIAS',
        'ΝΟΜΟΣ ΞΑΝΘΗΣ': 'N. XANTHIS',
        'ΝΟΜΟΣ ΞΑΝΘΖΣ': 'N. XANTHIS',
        'ΝΟΜΟΣ ΠΕΛΛΗΣ': 'N. PELLAS',
        'ΝΟΜΟΣ ΠΔΛΛΖΣ': 'N. PELLAS',
        'ΝΟΜΟΣ ΠΙΕΡΙΑΣ': 'N. PIERIAS',
        'ΝΟΜΟΣ ΠΗΔΡΗΑΣ': 'N. PIERIAS',
        'ΝΟΜΟΣ ΠΡΕΒΕΖΗΣ': 'N. PREVEZAS',
        'ΝΟΜΟΣ ΠΡΔΒΔΕΖΣ': 'N. PREVEZAS',
        'ΝΟΜΟΣ ΡΕΘΥΜΝΗΣ': 'N. RETHYMNOU',
        'ΝΟΜΟΣ ΡΕΘΤΜΝΗΣ': 'N. RETHYMNOU',
        'ΝΟΜΟΣ ΡΕΘ ΤΜΝΗΣ': 'N. RETHYMNOU',
        'ΝΟΜΟΣ ΡΔΘΤΜΝΖΣ': 'N. RETHYMNOU',
        'ΝΟΜΟΣ ΡΟΔΟΠΗΣ': 'N. RODOPIS',
        'ΝΟΜΟΣ ΡΟΓΟΠΖΣ':'N. RODOPIS',
        'ΝΟΜΟΣ ΣΑΜΟΥ': 'N. SAMOU',
        'ΝΟΜΟΣ ΣΑΜΟΤ': 'N. SAMOU',
        'ΝΟΜΟΣ ΣΕΡΡΩΝ': 'N. SERRON',
        'ΝΟΜΟΣ ΣΔΡΡΩΝ': 'N. SERRON',
        'ΝΟΜΟΣ ΣΕΡΡΩ Ν': 'N. SERRON',
        'ΝΟΜΟΣ ΤΡΙΚΑΛΩΝ': 'N. TRIKALON',
        'ΝΟΜΟΣ ΣΡΙΚΑΛΩΝ': 'N. TRIKALON',
        'ΝΟΜΟΣ ΣΡΗΚΑΛΩΝ': 'N. TRIKALON',
        'ΝΟΜΟΣ ΦΘΙΩΤΙΔΟΣ': 'N. FTHIOTIDAS',
        'ΝΟΜΟΣ ΦΘΙΩΣΙΔΟΣ': 'N. FTHIOTIDAS',
        'ΝΟΜΟΣ ΦΘΗΩΣΗΓΟΣ': 'N. FTHIOTIDAS',
        'ΝΟΜΟΣ ΦΛΩΡΙΝΗΣ': 'N. FLORINAS',
        'ΝΟΜΟΣ ΦΛΩΡΗΝΖΣ': 'N. FLORINAS',
        'ΝΟΜΟΣ ΦΩΚΙΔΟΣ': 'N. FOKIDAS',
        'ΝΟΜΟΣ ΧΑΛΚΙΔΙΚΗΣ': 'N. CHALKIDIKIS',
        'ΝΟΜΟΣ ΧΑΝΙΩΝ': 'N. CHANION',
        'ΝΟΜΟΣ ΧΙΟΥ': 'N. CHIOU',
        'ΝΟΜΟΣ ΧΙΟΤ': 'N. CHIOU',
        'ΝΟΜΟΣ ΥΗΟΤ': 'N. CHIOU'
    }

        # Apply replacements directly
    for old, new in replacements.items():
        text = text.replace(old, new)

    return text

s3_client = boto3.client("s3")
# AWS S3 bucket and folder configurations (set as environment variables)
BUCKET_NAME = "fuelprices-greece"
PARSED_FOLDER = "parsed/"

# Function to load historical data from S3
def load_s3_data(key):
    try:
        logger.info(f"Loading data from S3: {key}")
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        file_extension = key.split('.')[-1].lower()

        if file_extension == 'pkl':
            data = pickle.loads(response["Body"].read())
        elif file_extension == 'parquet':
            buffer = io.BytesIO(response["Body"].read())
            data = pd.read_parquet(buffer)
        else:
            raise ValueError(f"Unsupported file extension: {file_extension}")

        logger.info(f"Successfully loaded {key}")
        return data
    except s3_client.exceptions.NoSuchKey:
        logger.warning(f"Key {key} does not exist in S3.")
        return {}
    except Exception as e:
        logger.error(f"Error loading {key}: {e}")
        return {}
    
def save_to_s3(data, key):
    try:
        
        # Extract file extension from the key
        file_extension = key.split('.')[-1].lower()
        
        # Log the key and detected file type
        logger.info(f"Saving data to S3: {key} with file extension: {file_extension}")
        
        # Check if the file is an Excel file (.xlsx)
        if file_extension == 'xlsx':
            with io.BytesIO() as buffer:
                # Use openpyxl engine (this is the default for pd.ExcelWriter)
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    data.to_excel(writer, index=False)
                buffer.seek(0)  # Move cursor to the start of the buffer

                s3_client.put_object(
                    Bucket=BUCKET_NAME,
                    Key=key,
                    Body=buffer,
                    ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        elif file_extension == 'parquet':
            data.columns = data.columns.map(str)
            with io.BytesIO() as buffer:
                # Write the DataFrame to Parquet format in the buffer
                data.to_parquet(buffer, index=False)
                buffer.seek(0)  # Move cursor to the start of the buffer

                s3_client.put_object(
                    Bucket=BUCKET_NAME,
                    Key=key,
                    Body=buffer.getvalue(),
                    ContentType="application/octet-stream"
                )

        # Check if the file is a Pickle file (.pkl)
        elif file_extension == 'pkl':
            serialized_data = pickle.dumps(data)
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=key,
                Body=serialized_data,
                ContentType="application/octet-stream"
            )
        
        # If the file extension is neither xlsx nor pkl, raise an error
        else:
            raise ValueError(f"Unsupported file extension: {file_extension}")
        
        logger.info(f"Successfully saved {key} to S3.")
    
    except Exception as e:
        logger.error(f"Error saving {key} to S3: {e}")
    

def main(event, context):
    logger.info(f"addition works")

    # Load historical data from S3
    all_data = load_s3_data(PARSED_FOLDER + "all_data.pkl")
    all_errors = load_s3_data(PARSED_FOLDER + "all_errors.pkl")

    # Load previously cleaned data from S3
    # This is used to avoid re-cleaning data that has already been cleaned
    try:

        cleaned_data = load_s3_data(PARSED_FOLDER + "cleaned_data.parquet")
        cleaned_data_keys = pd.to_datetime(cleaned_data["DATE"], format="%d-%m-%Y").dt.strftime("%d%m%Y").unique().tolist()
        logger.info(f"Loaded cleaned data with {cleaned_data.shape[0]} rows.")

    except Exception as e:
        
        cleaned_data = None
        cleaned_data_keys=[]
        logger.info(f"No previous cleaned data found. Error: {e}")

    dfs_transformed_dict = dict()

    # Cleaning data only for dates that have not been cleaned in the past or have not encountered errors
    to_be_cleaned = [(date, df) for date, df in all_data.items() if date not in all_errors and date not in cleaned_data_keys]
    logger.info(f"Number of DataFrames to be cleaned: {len(to_be_cleaned)}, Dates: {[date for date, _ in to_be_cleaned]}")

    # Use ThreadPoolExecutor to clean data concurrently
    with ThreadPoolExecutor() as executor:
        results = executor.map(cleaning, to_be_cleaned)

    # Collect results into dfs_transformed
    for date, df in results:
        if df is not None:
            dfs_transformed_dict[date] = df

        else:
            all_errors[date]= df
            logger.warning(f"Skipping transformation for {date} due to errors.")


    problematic_dfs = [
        date
        for date, df in dfs_transformed_dict.items()
        if not  df.columns[df.columns == ''].empty or not df.columns[df.columns.duplicated()].empty
    ]

    for date in problematic_dfs:
        if date in dfs_transformed_dict:
            all_errors[date] = dfs_transformed_dict[date]
            del dfs_transformed_dict[date]
            


    dfs_transformed_list = [df for _, df in dfs_transformed_dict.items()]

    result_df = pd.concat(dfs_transformed_list, ignore_index=True, sort=True)

    result_df[result_df.columns.difference(['ΝΟΜΟΣ','DATE'])] = result_df[result_df.columns.difference(['ΝΟΜΟΣ','DATE'])].apply(lambda x: x.str.replace(',','.')).apply(pd.to_numeric, errors='coerce')  

    # Apply final renaming of columns
    result_df.columns = [
            'AUTOGAS' if col == 'Υγραέριο κίνησης (Autogas)' else
            'HOME_HEATING_DIESEL' if col == 'Diesel Θέρμανσης Κατ ́οίκον' else
            'AUTOMOTIVE_DIESEL' if col == 'Diesel Κίνησης' else
            'UNLEADED_100_OCTANE' if col == 'Αμόλυβδη 100 οκτ.' else
            'UNLEADED_95_Octane' if col == 'Αμόλυβδη 95 οκτ.' else
            'REGION' if col == 'ΝΟΜΟΣ' else
            col  # Leave other columns unchanged
            for col in result_df.columns
        ]
    
    # If cleaned_data are found, append the new data to the existing cleaned data
    if cleaned_data is not None:
        logger.info(f"Appending new data to existing cleaned data.")
        result_df = pd.concat([cleaned_data, result_df], ignore_index=True, sort=True)

    save_to_s3(all_errors, PARSED_FOLDER + "all_errors.pkl")
    save_to_s3(result_df, PARSED_FOLDER + "cleaned_data.parquet")


def cleaning(date_df):

    date, df = date_df
    logger.info(f"cleaning stage 1 : {date}")
    try:
        
        # Apply clean_text to the first row of each DataFrame
        df = df.map(clean_text)

        if df.iloc[:, 1].str.contains('για την', na=False).any():
            # Find the index where column 2 contains 'για την'
            start_index = df[df.iloc[:, 1].str.contains('για την', na=False)].index[0] + 1

            # Check if 'ΝN. CHIOU' exists in column 0
            if "N. CHIOU" in df[0].values:
                # Find the index where column 1 contains 'N. CHIOU'
                end_index = df[df[0] == "N. CHIOU"].index[0] + 1

                # Slice the DataFrame from start_index to end_index
                df = df.loc[start_index:end_index]
            else:
                df = df.loc[start_index:]

        df = df.apply(lambda x: x.astype(str).str.strip())
        df.replace({'-':'', 'nan':'', '0,0':'', 'None':'' }, regex=True, inplace=True)
        df = df.reset_index(drop=True)

    except Exception as e:
        logger.info({'STAGE':'CLEANING_1', 'MSG':str(e), 'DF':df.head()})
        return date, None


    logger.info(f"cleaning stage 2 : {date}")

    # Values to search
    value_to_find = '(τιμές σε €/λτ, συμπ. ΦΠΑ)'
    substring_to_find = 'ανά Νομό | ΦΠΑ' 
    
    try:
      
        # Check for exact match in column 1 or 2
        indices = -1
        for col in [0,1, 2]:
            if value_to_find in df[col].values:

                temp = df.index[df[col] == value_to_find][0] +1
                if temp > indices:
                    indices = temp
                    
        if indices>0:   
            df = df[indices:]
        

        # Check if any value in either column 0 or column 1 contains the substring
        if df.iloc[:, [0, 1]].astype(str).apply(lambda x: x.str.contains(substring_to_find, na=False)).any().any():
            # Find the index of the first occurrence of the substring in either column 0 or 1
            index = df.iloc[:, [0, 1]].astype(str).apply(lambda x: x.str.contains(substring_to_find, na=False)).stack().idxmax()[0]
            # Slice the DataFrame from the found index
            df = df.iloc[index + 1:]

        df = df.reset_index(drop=True)
    
    except Exception as e:
        logger.info({'STAGE':'CLEANING_2', 'MSG':str(e), 'DF':df})
        return date, None


    logger.info(f"cleaning stage 3 : {date}")

    try:
        # Ensure we are working with the first column for markers
        if 'N. ATHINON' in df.iloc[:, 0].values and 'N. CHIOU' in df.iloc[:, 0].values:
            # Find the indices of 'N. ATHINON' and 'N. CHIOU'
            start_index = df.index[df.iloc[:, 0] == 'N. ATHINON'][0]
            end_index = df.index[df.iloc[:, 0] == 'N. CHIOU'][0] +1
            
            # Separate header rows (before 'N. ATHINON')
            header_rows = df.iloc[:start_index].values.tolist()
            data_rows = df.iloc[start_index:end_index].values.tolist()  # Data rows between 'N. ATHINON' and 'N. CHIOU'

            # Combine header rows into a single header
            header = []
            for col in zip(*header_rows):  # Transpose to iterate column-wise
                combined_header = " ".join(str(item) for item in col if item).strip()
                header.append(combined_header)

            # Create DataFrame with new header
            df = pd.DataFrame(data_rows, columns=header)

            # Drop the row containing 
            df = df[~df.iloc[:, 0].isin(['ΝΟΜΟΣ ΑΗΣΩΛΗΑΣ ΚΑΗ', 'ΝΟΜΟΣ ΑΙΤΩΛΙΑΣ ΚΑΙ', ''])].reset_index(drop=True)

        else:
            raise ValueError("Missing 'N. ATHINON' or 'N. CHIOU' in the DataFrame.")

    except Exception as e:

        logger.info(json.dumps({'STAGE': 'CLEANING_3', 'MSG': str(e)}))
        return date, None


    logger.info(f"cleaning stage 4 : {date}")

    try:
        # Rename the columns to uniform names
        df.columns = [
            'Υγραέριο κίνησης (Autogas)' if col in ['ο κίνησης (Autogas','ο κίνησης (Autoga','ο κίνησης (Autoga s)','κίνησης (Autogas)', '(Autogas)', 'ιο κίνησης (Autoga s)','Υγραέριο κίνησης (Autogas )','Υγραέριο κίνηςησ (Autogas)','Τγραέρι ο κίνησης (Autoga s)','ο κίνησης (Autogas )'] else
            'Diesel Θέρμανσης Κατ ́οίκον' if col in ['Θέρμαν σης Κατ ́οίκο','Diesel Θέρμανσ ης Κατ ́οίκο','Diesel Θέρμανση','Diesel Θέρμανσ ης Κατ ́οίκο','Diesel Θζρμανση ς','Diesel Θζρμανση ς','Θέρμα νσης Κατ ́οί κον','Diesel Θέρμανσ ης Κατ ́οίκο ν','Diesel Θέρμανση ς Κατ ́οίκον','Diesel Θζρμανση ς Κατ ́οίκον', 'Θέρμανσης Κατ ́οίκον','Diesel Θέρμανσ ης Κατ ́οίκον', 'Θέρμαν σης Κατ ́οίκο ν'] else
            'Diesel Κίνησης' if col in ['Diesel Κίνηςησ'] else
            'Αμόλυβδη 100 οκτ.' if col in ['Αμόλυβδ η 100','Αμόλσβ δη 100','Αμόλσβ δη 100 οκτ.','Αμόλσβδ η 100 οκτ.','Αμόλυβδ η 100 οκτ.','100 οκτ.'] else
            'Αμόλυβδη 95 οκτ.' if col in ['Αμόλσβ δη 95','Αμόλυβ δη 95','Αμόλσβ δη 95 οκτ.','Αμόλσβδ η 95 οκτ.','Αμόλυβ δη 95 οκτ.','Αμόλυβδ η 95 οκτ.','95 οκτ.'] else
            'Super' if col in ['Υγραζρ Super'] else
            col  # Leave other columns unchanged
            for col in df.columns
        ]

        # Add the date as a new column in the DataFrame
        df["DATE"] = pd.to_datetime(date, format="%d%m%Y").strftime("%d-%m-%Y")
        
        return date, df

    except Exception as e:
        # all_errors[date] = {'STAGE':'CLEANING_4', 'MSG':str(e), 'DF':df}
        logger.info({'STAGE':'CLEANING_4', 'MSG':str(e)})
        return date, None
    
   


