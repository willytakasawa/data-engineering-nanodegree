from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id='',
                 table='',
                 sql='',
                 truncate= True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info(f'Cleaning table {self.table}')
            postgres.run(f'TRUNCATE {self.table}')
            
        self.log.info(f'Loading dimension table {self.table}')
        postgres.run(f"INSERT INTO {self.table} " + self.sql)
        self.log.info(f"Successfully completed insert into {self.table}")
