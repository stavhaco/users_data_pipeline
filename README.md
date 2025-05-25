# User data processing pipeline 

Modular project for data processing. Consists 3 main components - 
Readers - supports spark json reader.
Transformets - supports signin and user transformers.
Writers - supports spart to json writer.
The code is designed to be flexible, easy to extend and robust. 

## What does it do?
- Reads user data from one or more JSON files (using Apache Spark for scalability).
- Transforms the data into two outputs:
  - User data
  - Sign-in activity
- Handles edge cases like missing emails, empty fields, and more.
- Outputs the results as JSON files, ready for database import or further analysis.

## Why this structure?
- Separation of concerns: Each part of the pipeline (reading, transforming, writing) is its own module. You can swap out Spark for Pandas, or JSON for Parquet, with minimal changes.
- Extensible: Can add a new class for every component (like new transformation logic).
- Testable: The codebase includes unit tests for the core transformation logic, including some edge cases.

## Requirements
- Python 3.8+
- virtualenv


## How to run it
```bash
virtualenv venv_spark
source venv_spark/bin/activate
pip3 install -r requirements.txt
python3 pipeline.py
```

## Testing
To run the unit tests:
```bash
python3 -m pytest
```

## Design choices
In terms of data model - I chose to seprate the user data (name, email, etc) and sign-in data. Since sign-in activity changes much more frequently, separating it allowing to update and process this entity without having to rewrite or reload the entire user dataset. This leads to more efficient storage and processing. With this design for example the database can load the user data with merge on ID to a DIM user table and avoid writing on existing unchanged users - this is also allowing to store the user json data distinct if we want. Another FACT table in the database can hold only the login data. The tables can be joined based on the user ID field.   