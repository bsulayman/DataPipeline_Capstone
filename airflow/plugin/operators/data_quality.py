from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests=[],
                 *args, **kwargs):

        """
        Initiation of DataQualityOperator

        Arguments: 
            redshift_conn_id   --  Redshift connection Id
            tests              --  An array of tests that will be performed. 
                                   The format of test should be something like the following:
                                   [
                                        {"table":"staging_events",
                                         "sql":"SELECT COUNT(*) FROM staging_events WHERE sessionid = null",
                                         "expected_result":0},
                                        {"table":"staging_songs",
                                         "sql":"SELECT COUNT(*) FROM staging_songs WHERE song_id = null",
                                         "expected_result":0}
                                   ]
        Returns: 
            None
        """
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests
        
    def execute(self, context):
        """
        Initiation of DataQualityOperator

        Arguments: 
            redshift_conn_id   --  Redshift connection Id
            tests              --  An array of tests that will be performed. 
                                   The format of test should be something like the following:
                                   [
                                        {"table":"staging_events",
                                         "sql":"SELECT COUNT(*) FROM staging_events WHERE sessionid = null",
                                         "expected_result":0},
                                        {"table":"staging_songs",
                                         "sql":"SELECT COUNT(*) FROM staging_songs WHERE song_id = null",
                                         "expected_result":0}
                                   ]
        Returns: 
            None
        """
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        error_count = 0
        
        self.log.info('Run data quality check')
        for test in self.tests:
            sql = test.get('sql')
            expected_result = test.get('expected_result')
            result = redshift_hook.get_records(sql)
            if expected_result != result[0][0]:
                error_count += 1
                self.log.info("This test {} failed. Result is {} and expected result is {}".format(sql, result, expected_result))

        if error_count >= 1:
            raise ValueError("Data quality checks failed")
        else:
            self.log.info("Data quality checks completed successfully")