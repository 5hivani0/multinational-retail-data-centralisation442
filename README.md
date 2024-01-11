# Multinational Retail Data Centralisation Project #

## Table of Contents ##
* Description
* What I Gained
* Installation
* Usage
* File structure of the project
* Entity Relationship Diagram (ERD)
* License

## Description ##
This project is focused on developing infrastructure designed to manage a wide range of datasets. The objective is to improve accessibility and streamline the analysis of data. The process involves extracting information from diverse sources, cleaning the data, and uploading it to a database. This database is then optimized to enhance storage efficiency and enable practical analysis of the data. The overarching aim is to establish an efficient system to derive meaningful insights from the collected data.

## What I Gained ##
Through this project, I gained hands-on experience in data centralization, extraction, cleaning, and effective database management. I improved my skills with tools like pandas, boto3, and SQLAlchemy. Additionally, I learned to create STAR-based database schemas for optimized data storage and access, and executed complex SQL queries to extract valuable insights. Overall, this project has provided practical insights into building a comprehensive data pipeline for real-world scenarios.  

## Installation ##
  
1. Clone this repository to your computer:

git clone https://github.com/5hivani0/multinational-retail-data-centralisation442.git

2. Navigate to the project directory:

cd multinational-retail-data-centralisation442  
  
**Make sure you already have the folllowing tools installed before running files:**
* **pandas**:  
pip install pandas
  
* **re**  
  
* **boto3**  
pip install boto3
  
* **request**  
pip install requests
  
* **tabula**:  
Before using tabula, ensure that Java is installed on your machine, refer to this [tabula-py document for details](https://tabula-py.readthedocs.io/en/latest/getting_started.html) for troubleshooting and additional instructions. Follow these instructions [here](https://www.java.com/en/download/manual.jsp) to install Java. After installing Java, you can run:  
pip install tabula-py

* **sqlalchemy**:  
pip install sqlalchemy
  
* **yaml**:  
pip install pyYAML
  
## Usage ##
1. Create a database on pgAdmin to store future cleaned dataframes.  
2. Fill in the required information in the db_details section of the def upload_to_db method, found in the database_utils.py file, so that you can upload the cleaned dataframes to the databse you created in step 1. 
3. Make sure you are logged into AWS on your CLI, with permission to access and read s3 buckets. 
4. Run main_script.py to execute the entire data pipeline.  
5. Open pgAdmin and run the MRDC_database_schema.sql file on your database to convert your table columns to the appropriate data type.  
6. Use the MRDC_querying_the_data.sql file to run some queries to analyse the data.  
  
* If you want to access other data sources using the framework of the python scripts, edit the parameters in main_script.py to specify the paths, URLs, and other details relevant to your data sources and database. You can also edit the API in the api_key.txt file.Change cleaning methods and SQL table alterations and queries accordingly.

## File Structure ##
* README.md : This documentation file  
* main_script : brings everything together, calling the classes and methods from the other scripts to orchestrate the process of data extraction, cleaning, and database upload.  
* data_utils.py : contains DatabaseConnector class, to connect with and upload data to the database.  
* data_extraction.py : contains DataExtractor class, used to extract data from different data sources, , these sources will include CSV files, a PDF, an API, and an S3 bucket.  
* data_cleaning.py : contains DataCleaning class, used to clean data from each data source.  
* MRDC_database_schema.sql : contains SQL code to alter table columns to the appropriate data type, and create a schema with primary and foreign keys ready for analysis.  
* MRDC_querying_the_data : contains queries that will be used to analyse and extract data.
* .gitignore : a file specifying untracked files that Git should ignore.
* db_creds.yaml : YAML file containing database credentials
* api_key.txt : text file containing the api key.

## Entity Relationship Diagram (ERD) ##  
![MRDC ERD](https://github.com/5hivani0/multinational-retail-data-centralisation442/assets/149093767/489b6ca1-03c4-4fe6-93ee-f2d094c67def)


## License ##
MIT
