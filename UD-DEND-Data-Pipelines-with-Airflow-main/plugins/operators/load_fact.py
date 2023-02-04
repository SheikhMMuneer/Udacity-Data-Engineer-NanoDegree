from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {}
        {};
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 insert_sql_query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.insert_sql_query=insert_sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        formatted_sql = LoadFactOperator.insert_sql.format(self.table, self.insert_sql_query)
        
        redshift.run(formatted_sql)
        self.log.info('LoadFactOperator for {} completed'.format(self.table))
