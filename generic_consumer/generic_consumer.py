import yaml
import httpx
import duckdb
from pathlib import Path
from datetime import datetime


class GenericConsumer:
    def __init__(self, db_file='duckdb/local_wh.db'):
        self.db_file = Path(db_file)
        self.db_file.parent.mkdir(parents=True, exist_ok=True)
        self.execution_date = datetime.utcnow().strftime("%Y-%m-%d")

        self.conn = duckdb.connect(db_file)

    def ingest(self, catalog_path='catalog/sources.yml'):
        with open(catalog_path, 'r') as file:
            sources = yaml.safe_load(file)['source_catalog']

        self._create_bronze_schema()
        for source in sources:
            self._ingest_source(source)
            self._ingest_data_into_bronze(source)

    def _create_bronze_schema(self):
        self.conn.execute('CREATE SCHEMA IF NOT EXISTS bronze;')

    def _ingest_source(self, source):
        try:
            self._download_source_data(source)
            print(f'Successfully downloaded data for source: {source["name"]}')
        except Exception as e:
            print(f'Error downloading data for source: {source["name"]}. Error message: {str(e)}')

    def _download_source_data(self, source):
        data_dir = Path(f'landing/{source["name"]}/day={self.execution_date}')
        data_dir.mkdir(parents=True, exist_ok=True)
        data_path = data_dir / f'data.{source["format"]}'

        if data_path.exists():
            print(f'Data for source: {source["name"]} already exists. Skipping download.')
            return

        response = httpx.get(source['url'])
        if response.status_code != 200:
            raise Exception(f'Failed to download data. HTTP status code: {response.status_code}')

        with open(data_path, 'wb') as file:
            file.write(response.content)

    def table_exists(self, table_name):
        result = self.conn.execute(f"SELECT * FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = '{table_name}'").fetchall()
        return len(result) > 0

    def _ingest_data_into_bronze(self, source):
        table_name = f'bronze.{source["name"]}'

        try:
            if not self.table_exists(table_name):
                recursive_path = Path(f'landing/{source["name"]}/**/data.{source["format"]}')
                full_df = self.conn.sql(f"select * from '{recursive_path}'").df()
                full_df.drop_duplicates(subset=source["upsert_keys"], inplace=True)

                self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM full_df")
            else:
                incremental_path = Path(f'landing/{source["name"]}/day={self.execution_date}/data.{source["format"]}')
                incremental_df = self.conn.sql(f"select * from '{incremental_path}'").df()

                self.conn.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM incremental_df")
            print(f'Successfully ingest data for source: {source["name"]} into bronze table: {table_name}')
        except Exception as e:
            print(f'Error ingesting data for source: {source["name"]} into bronze table: {table_name}. Error message: {str(e)}')


    