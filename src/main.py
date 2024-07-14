import os
from dotenv import load_dotenv
import pandas as pd
from snowflake_ import SnowflakeConnection
from snowflake_rsa import SnowflakeRSAConnection
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

# Access environment variables
user = os.getenv('USER')
account = os.getenv('ACCOUNT')
warehouse = os.getenv('WAREHOUSE')
database = os.getenv('DATABASE')
schema = os.getenv('SCHEMA')
role = os.getenv('ROLE')
csv = os.getenv("CSV")
# class Handler(SnowflakeConnection):
#     # def __init__(self):
#     #     super.__init__()
#     def getData(self, tableName):
#         query = f"select* from {tableName} limit 5;"
#         cursor = self.getCursor()
#         res = self.cursor.execute(query)
#         df = pd.DataFrame(res.fetchall())
#         print(df)

class Handler(SnowflakeRSAConnection):
    # def __init__(self):
    #     super.__init__()
    def getData(self, tableName):
        query = f"select* from {tableName} limit 5;"
        cursor = self.getCursor()
        if cursor is None:
            print("No connection established. Call connect() first.")
            return
        try:
            res = self.cursor.execute(query)
            df = pd.DataFrame(res.fetchall())
            print(df)
        except Exception as e:
            print(e)
        finally:
            self.cursor.close()

    # def createTable(self, query, df, table_name):
    #     cursor = self.getCursor()
    #     connection = self.getConnection()
    #     if cursor is None:
    #         print("No connection established. Call connect() first.")
    #         return
    #     try:
    #         self.cursor.execute(query)
    #         write_pandas(connection, df, table_name = table_name)
    #     except Exception as e:
    #         print(e)
    #     finally:
    #         self.cursor.close()

    def createTable(self, query, df, table_name):
        cursor = self.getCursor()
        connection = self.getConnection()
        if cursor is None:
            print("No connection established. Call connect() first.")
            return
        try:
            cursor.execute(query)
            # Ensure write_pandas completes before closing the cursor
            success, nchunks, nrows, _ = write_pandas(connection, df, table_name)
            if success:
                print(f"Successfully wrote {nrows} rows to {table_name}")
            else:
                print(f"Failed to write data to {table_name}")
        except Exception as e:
            print(e)
        finally:
            cursor.close()


if __name__ == "__main__":
    params = {
            "user" : user,
            "account" : account,
            "warehouse" : warehouse,
            "database" : database,
            "schema": schema,
            "role": role
         }
    obj = Handler(params)
    df = pd.read_csv(csv)
    print(df.info())
    print(df.head())
    query = 'create or replace table "DIABETES" ("Pregnancies" int, "Glucose" int, "BloodPressure" int, "SkinThickness" int, "Insulin" int, "BMI" double, "DiabetesPedigreeFunction" float, "Age" int, "Outcome" int)'
    obj.createTable(query, df, "DIABETES")
    obj.getData("DIABETES")



