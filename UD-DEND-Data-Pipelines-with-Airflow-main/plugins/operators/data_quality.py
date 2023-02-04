from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 data_quality_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.data_quality_checks=data_quality_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if len(self.data_quality_checks) <= 0:
            self.log.info('No data quality checks were specified. Data quality checks canceled.')
            return
        
        for check in self.data_quality_checks:
            sql_query = check.get('sql_query')
            expected_result = check.get('expected_result')
            
            try:
                self.log.info('Starting SQL query for data check - {}'.format(sql_query))
                records = redshift.get_records(sql_query)
                num_records = len(records[0][0])
                
                if num_records != expected_result:
                    raise ValueError('Data quality check failed. {} entries excpected. {} given'.format(expected_result, num_records))
                    
            except ValueError as v:
                self.log.info(v.args)
                raise
            except Exception as e:
                self.log.info('SQL query for data check failed - {}. Exception: {}'.format(sql_query, e))
                continue
        