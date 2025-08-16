# delta-flight-assignment


## Overview
This repository contains solutions for the Delta Data Engineering Assignment.  
The project is divided into two tasks:

- **Task 1** – SQL query to retrieve the most recent status for each flight from a database table.
- **Task 2** – Python script to process a CSV file of flight data and output the most recent status per flight.(Here given xlsx sheet is converted into csv and done some manual cleaning)

The code is tested with **Python 3.13+** and **pytest**.

---

## How to Run Locally
## Task1
1. Open DBeaver and connect to your local PostgreSQL instance.
2. Ensure the `flight_leg` table is already loaded with the provided data. 
3. Copy the SQL query and execute it in DBeaver.
4. The query will return:
        One row per flightkey
        The most recent flightstatus for each flight, determined by lastupdt

## Task2 (Windows)
1. Create a virtual environment myenv
- python -m venv myenv
2. Activate 
- myenv/Scripts/activate
3.Install required packages
- pip install -r requirements.txt

## Running task2.py
1. When output to be just printed
- python src/task2.py --input "filepath"
2. When output to be a file
- python src/task2.py --input "filepath" --output "filename.csv"
3. Specify handling for missing/unparseable time-only values: 
- python src/task2.py --input sample_data.csv --on-missing midnight
4. Tested with sample_data.csv and dummy_data

## Testing task2
1. pytest tests/


