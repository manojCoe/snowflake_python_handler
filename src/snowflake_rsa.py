import snowflake.connector
from snowflake.connector import DictCursor
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa, dsa
from cryptography.hazmat.primitives import serialization

class SnowflakeRSAConnection:
    def __init__(self, config):
        """
        Initialize the SnowflakeConnection with connection details.

        Parameters:
        config (dict): Dictionary containing Snowflake connection parameters.
                       Requires keys: 'user', 'password', 'account', 'warehouse', 
                                      'database', 'schema', 'role'.
        """

        with open("C:/Users/coe16/rsa_key.p8", "rb") as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password=None,
                backend=default_backend()
            )
        
        private_key = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        self.user = config['user']
        self.private_key = private_key
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
            private_key = self.private_key,
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
