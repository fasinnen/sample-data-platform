# Sample Data Platform 🗃️

The `sample-data-platform` is a local-based, easy-to-use data platform to ingest, transform and analyze public datasets in a reproducible way. It's a portfolio project that I've created to showcase my Data Engineering skills, but it can also be used by students and data enthusiasts to learn more about the field.

## Architecture

This sample platform is composed of 2 main components:

- **Generic Consumer**: It's a Python class that consumes data from CSV/JSON files and sends it to the `bronze` schema of a local DuckDB instance. The contents of the DuckDB instance are then persisted into a file called `duckdb/local_wh.db`. In order to facilitate the parametrization and management of these ingestions, there is also the `catalog/sources.yml` that's read by the `GenericConsumer` class to know which files to consume.
- **Transformation Engine**: In this piece we didn't reinvent the wheel and used `dbt` to transform the data from the `bronze` schema into the `main_silver` and `main_gold` schemas. The `dbt` models are located in the `transformation_engine/` folder and the `dbt` project is configured to use the `duckdb/local_wh.db` as the source of data.

## A Practical Example

In order to better illustrate the capabilities of this sample platform, I've created a practical example on this repo using the [San Francisco Fire Incidents Dataset](https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric/about_data). Once the entrypoint of the application runs, the following tables are created:

- `bronze.san_francisco_fire_incidents`: This table presents the "raw-like" data from the CSV file.
- `main_silver.san_francisco_fire_incidents`: This table presents the cleaned version of the dataset, transformed using the `dbt` models.
- `main_gold.san_francisco_fire_incidents`: This table presents a "fact-like" aggregated version of the dataset, transformed using the `dbt` models, helping analysts-alike to perform their analysis on top of it in an efficient manner. 

## How to Run

It's quite simple to run this project. First, you should have a Python environment set up. I personally recommend using `conda` or `miniconda` to manage your Python versions and virtual environments. Once you have that set up, you can run the following commands:

```bash
# Install the dependencies
pip install -r requirements.txt

# Run the entrypoint
python main.py
```
