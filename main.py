import os
from generic_consumer import GenericConsumer
from dbt.cli.main import dbtRunner, dbtRunnerResult

def run_dbt():
    os.chdir('transformation_engine')
    dbt = dbtRunner()

    cli_args = ["run"]
    res: dbtRunnerResult = dbt.invoke(cli_args)
    
    for r in res.result:
        print(f"{r.node.name}: {r.status}")

if __name__ == '__main__':
    consumer = GenericConsumer()

    consumer.ingest()
    run_dbt()