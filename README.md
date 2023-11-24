# Multinational Retail Data Centralisation Project #

## Table of Contents ##
* Description
* What I Gained
* Installation
* Usage
* File structure of the project
* License

## Description ##
Imagine you're working on a project for a forward-thinking organization that envisions a centralized data management system for efficient handling of various datasets. While the scenario is fictional, the goal remains to streamline data processes, making it easy to access and analyse effectively. 

## What I Gained ##

## Installation ##
1. Create a database on pgadmin4 to store future cleaned tables of data
  
2. Clone this repository to your computer:

git clone https://github.com/5hivani0/multinational-retail-data-centralisation442.git

3. Navigate to the project directory:

cd multinational-retail-data-centralisation442

4. Make sure you are logged into AWS on your CLI, with permission to access and read s3 buckets

## Usage ##
1. Edit the parameters in main_script.py to specify the paths, URLs, and other details relevant to your data sources and database.
2. Run main_script.py to execute the entire data pipeline.

## File Structure ##
* README.md : This documentation file
* main_script : brings everything together, calling the classes and methods from the other scripts to orchestrate the process of data extraction, cleaning, and database upload.
* data_utils.py : contains DatabaseConnector class, to connect with and upload data to the database.
* data_extraction.py : contains DataExtractor class, used to extract data from different data sources, , these sources will include CSV files, a PDF, an API, and an S3 bucket.
* data_cleaning.py : contains DataCleaning class, used to clean data from each data source.


## License ##
