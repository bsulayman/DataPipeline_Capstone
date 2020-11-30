from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 append_data="",
                 sql_query="",
                 *args, **kwargs):

        """
        Initiation of LoadDimensionOperator

        Arguments: 
            redshift_conn_id   --  Redshift connection Id
            destination_table  --  Redshift table that need to be created
            append_data        --  Allow user to switch between append and truncate-insert. 
                                   If it's true, it will append data at the end of the table. Otherwise, it will truncate-insert
            sql_query          --  Dimension insert sql query

        Returns: 
            None
        """
            
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.append_data = append_data
        self.sql_query = sql_query

    def execute(self, context):
        """
        Execution of LoadDimensionOperator

        Arguments: 
            redshift_conn_id   --  Redshift connection Id
            destination_table  --  Redshift table that need to be created
            append_data        --  Allow user to switch between append and truncate-insert. 
                                   If it's true, it will append data at the end of the table. Otherwise, it will truncate-insert
            sql_query          --  Dimension insert sql query

        Returns: 
            None
        """

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_data == False:
            self.log.info("Clearing data from destination {} table".format(self.destination_table))
            redshift.run("DELETE FROM {}".format(self.destination_table))
            
        self.log.info("Append data to {} table".format(self.destination_table))
        formatted_sql = self.sql_query.format(self.destination_table)
        redshift.run(formatted_sql)