import snowflake.connector
from snowflake.connector import DictCursor

class SnowflakeConnection:
    def __init__(self, config):
        """
        Initialize the SnowflakeConnection with connection details.

        Parameters:
        config (dict): Dictionary containing Snowflake connection parameters.
                       Requires keys: 'user', 'password', 'account', 'warehouse', 
                                      'database', 'schema', 'role'.
        """
        self.user = config['user']
        self.password = config['password']
        self.account = config['account']
        self.warehouse = config['warehouse']
        self.database = config['database']
        self.schema = config['schema']
        self.role = config['role']
        self.connection, self.cursor = self.connect()

    def connect(self):
        """
        Create the SQLAlchemy engine for connecting to Snowflake.
        
        Returns:
        engine: SQLAlchemy engine for Snowflake connection.
        """
        conn = snowflake.connector.connect(
            account = self.account,
            password = self.password,
            user = self.user,
            database = self.database,
            schema = self.schema,
            warehouse = self.warehouse,
            role = self.role
        )
        cs = conn.cursor(DictCursor)

        return conn, cs
    
    def getCursor(self):
        """
        Get the cursor for executing queries.

        Returns:
        cursor: Snowflake cursor object.
        """
        return self.cursor

    def getConnection(self):
        """
        Get the connection object.

        Returns:
        connection: Snowflake connection object.
        """
        return self.connection
