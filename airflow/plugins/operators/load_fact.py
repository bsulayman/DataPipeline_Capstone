from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 sql_query="",
                 *args, **kwargs):

        """
        Initiation of LoadFactOperator

        Arguments: 
            redshift_conn_id   --  Redshift connection Id
            destination_table  --  Redshift table that need to be created
            sql_query          --  Fact insert SQL Query
            
        Returns: 
            None
        """
        
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_query = sql_query

    def execute(self, context):
        """
        Execution of LoadFactOperator

        Arguments: 
            redshift_conn_id   --  Redshift connection Id
            destination_table  --  Redshift table that need to be created
            sql_query          --  Fact insert SQL Query

        Returns: 
            None
        """

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.destination_table))
        
        self.log.info("Insert data to Fact table")
        formatted_sql = self.sql_query.format(self.destination_table)
        redshift.run(formatted_sql)