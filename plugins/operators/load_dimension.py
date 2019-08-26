from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    insert_sql = """
        INSERT INTO {}
        {}
        ;
    """
    
    delete_sql = """
        DELETE FROM {}
        {}
        ;
    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 query="",
                 mode="", #fill in "delete" for delete_load mode or leave it blank for append_only mode
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.mode = mode


    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.mode == "delete": 
            dim_sql = LoadDimensionOperator.delete_sql.format(
                self.table,
                self.query,
            )
            self.log.info(f"Deleting data from {dim_sql} ...")
            redshift.run(dim_sql)
        
        else:
            dim_sql = LoadDimensionOperator.insert_sql.format(
                self.table,
                self.query
            )
            self.log.info(f"Loading data to {dim_sql} ...")
            redshift.run(dim_sql)
