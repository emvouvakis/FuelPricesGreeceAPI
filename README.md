# ⛽ Fuel Prices Greece API

This project demonstrates how to develop and deploy a serverless API on AWS Lambda using the Serverless Framework. The API provides daily fuel price data for each prefecture of Greece. The tools utilized include AWS Lambda, API Gateway, SNS and Step Functions.

## 🛠️ Step Functions

AWS Step Functions orchestrate the execution of the web scraping and data cleaning Lambda functions sending email via SNS on failure. 

### Schedule

The Step Functions state machine is triggered daily at 12:00 PM (UTC). This setup ensures that the data is scraped and cleaned automatically every day, providing up-to-date fuel price information.

### State Machine Definition

The state machine consists of 3 tasks:
1. **WebScraping**: Invokes the `webscraping-lambda` function to scrape the web for PDF links and download them.
2. **CheckStatus**: Check if new data were downloaded from `webscraping-lambda`. If nothing was downloaded then do not invoke `cleaning-lambda`. 
3. **Cleaning**: Invokes the `cleaning-lambda` function to process and clean the downloaded data.
4. **NotifyFailure**: Sends an email notification via SNS if any of the previous steps fail.

<br>

<p align="center">
    <img src="https://github.com/emvouvakis/FuelPricesGreeceAPI/blob/main/imgs/step_function.png?raw=true" alt="definition">
</p>

## 🧪 API Usage

To use the Fuel Prices Greece API, you can make HTTP GET requests to the `/data` endpoint. Below is an example of how to fetch data using Python and the `requests` library.

## 🔑 API Key

The API requires the provided `x-api-key` header for all requests. This is used to keep track of API calls and set limits.

### Example Code

```python
import requests
import pandas as pd

result = []
offset = 0
limit = 10000
request_count = 0  # Initialize a counter for the number of requests

while True:
    url = "https://5fcbs3i0z4.execute-api.eu-west-3.amazonaws.com/v2/data"
    params = {
        "start_date": "2023-01-01",     # Optional parameter
        "end_date": "2024-12-15",       # Optional parameter
        'REGION': 'N. ATHINON',         # Optional parameter
        "limit": limit,                 # Default is 10000
        "offset": offset                # Mandatory parameter to be incremented
    }
    headers = {
        "x-api-key": "VH5AaWqgBchJw3a8yOkq5i5nVJ0hNMl5mwzkPMm1"
    }

    response = requests.get(url, params=params, headers=headers)
    request_count += 1  # Increment the request counter
    data = response.json()
    
    if response.status_code != 200:
        print(f"Error: {response.status_code}. Breaking the loop.")
        break

    if not data:
        print("No more data available. Breaking the loop.")
        break
    
    result.append(pd.DataFrame(data))
    offset += limit  # Increment the offset for the next request

print(f"Total number of requests sent: {request_count}")

df = pd.concat(result, ignore_index=True)
df['DATE'] = pd.to_datetime(df["DATE"], format="%Y-%m-%d")
df.to_parquet('data.parquet')
df.head()
```
### Architecture Diagram

<p align="center">
    <img src="https://github.com/emvouvakis/FuelPricesGreeceAPI/blob/main/imgs/architecture_diagram.png?raw=true" alt="definition">
</p>