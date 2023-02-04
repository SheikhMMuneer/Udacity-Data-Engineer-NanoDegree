from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {}
        {};
    """
    
    truncate_sql = """
        DELETE FROM {};
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 insert_sql_query='',
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.insert_sql_query=insert_sql_query
        self.truncate_table=truncate_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info('Clearing data from dimension Redshift table {}'.format(self.table))
            trunc_formatted_sql = LoadDimensionOperator.truncate_sql.format(self.table)
            redshift.run(trunc_formatted_sql)
        
        formatted_sql = LoadDimensionOperator.insert_sql.format(self.table, self.insert_sql_query)
        
        redshift.run(formatted_sql)
        self.log.info('LoadDimensionOperator for {} completed'.format(self.table))