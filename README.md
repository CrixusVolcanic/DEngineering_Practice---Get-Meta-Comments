# Social Media Comments Extraction

This project is designed to extract comments from various social media platforms, including Facebook Ads, Facebook Feed Posts, and Instagram Media, for specific countries. The data is processed, cleaned, and stored for further analysis.

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)

## Requirements

- Python 3.7+
- Google BigQuery (for saving data)
- Access to Facebook and Instagram APIs

## Installation

1. Clone this repository:
   `git clone https://github.com/CrixusVolcanic/DEngineering_Practice---Get-Meta-Comments.git`

2.	Install the required Python packages:
   	`pip install -r requirements.txt`


## Configuration

1. Environment Variables:

   	The project relies on several environment variables to access APIs and BigQuery. You need to set the following variables:

   	- META_COUNTRY_CONFIG: Base64-encoded JSON string containing API credentials for different countries. Example:

	      `{
	         "CO": {
	            "access_token": "your_access_token",
	            "ig_business_account_id": "your_ig_business_account_id",
	            "page_id": "your_page_id"
	            },
	         "MX": {
	            "access_token": "your_access_token",
	            "ig_business_account_id": "your_ig_business_account_id",
	            "page_id": "your_page_id"
	            }
	      }`

   		Encode this JSON into Base64 and set it as the META_COUNTRY_CONFIG environment variable.

   	- BQ_PROJECT: The Google Cloud project ID for BigQuery.

3.	BigQuery:
Ensure that you have access to Google BigQuery and the appropriate credentials are set up in your environment. The data will be saved in tables under the meta_comments dataset.
4.	API Access:
Ensure you have access to the Facebook Graph API and Instagram API, with the appropriate permissions to extract media and comments.

## Usage
To run the full data extraction process for Colombia (CO) and Mexico (MX), execute the main script:
   
   `python main.py`


The script will extract comments from Facebook Ads, Facebook Feed Posts, and Instagram Media for both countries, clean the data, and save it to BigQuery.

## Project Structure
``` bash
.
├── ads/
│   ├── ads_class.py
│   └── ...
├── feedPost/
│   ├── feedpost_class.py
│   └── ...
├── instagram_media/
│   ├── media.py
│   └── ...
├── libraries/
│   ├── utils.py
│   └── bq_utils.py
├── main.py
├── requirements.txt
└── README.md
```

- ads/: Contains the logic for extracting comments from Facebook Ads.
- feedPost/: Contains the logic for extracting comments from Facebook Feed Posts.
- instagram_media/: Contains the logic for extracting comments from Instagram Media.
- libraries/: Utility functions and BigQuery saving logic.
- main.py: The main script to run the extraction process.
- requirements.txt: Lists the dependencies required for the project.
