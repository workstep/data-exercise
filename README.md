# data-exercise
### Overview
Here we've implemented a simple hypothetical (and partially problematic) spatial indexing data pipeline in Python.
It could conceivably be employed as part of a larger orchestration for generating a model training dataset.

The pipeline accepts a raw data file as an argument, processes it, and then "stores" the results to a "data warehouse,"
which in this example is simply a local `sqlite3` database, but you could just as easily imagine a Snowflake Connector
in the `db` module.

### Data format
The pipeline is expecting a raw data file that represents the wages associated with new job requisitions posted to job boards.

|time_posted|title|address|wage|
|--|--|--|--|
|2021-11-30 18:36:00|Warehouse Associate|137 Woodridge Dr, Twin Falls, ID 83301|17.25|

### Functional requirements
The pipeline processes addresses in the raw data to convert them to geospacial indices.

In this highly contrived example, the pipeline is leveraging a "dimension table" for Location Indices to avoid storing raw
coordinates, ostensibly for some downstream optimization or other business purpose.

The scope of the pipeline implementation is to store normalized data entries in the warehouse "fact table" for wage events.

### Getting started
1. Clone this repo locally
```
git clone https://github.com/workstep/data-exercise.git
```
2. Hop into the directory holding the repo
```
cd data-exercise
```
3. Create a python3 virtualenv:
```
python3 -m venv env
```
4. Install dependencies:
```
env/bin/pip3 install -r requirements.txt
```
5. Setup environment for geocoding API access (this will require a password from WorkStep):
```
env/bin/python3 setup.py
```
6. Execute the pipeline on a data filepath:
```
env/bin/python3 main.py data.csv
```

### Working with the "warehouse"
You can explore the `sqlite` data warehouse like so:
```
sqlite3 db/database.sqlite
```

You can see the tables stored like so:
```
sqlite> .tables
```

And explore the data with SQL queries like so:
```
sqlite> select * from location_index;
```
