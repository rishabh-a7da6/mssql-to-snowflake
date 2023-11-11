# Necessary Imports
import pytz
import logging
import datetime
import pandas as pd
import sqlalchemy as sa
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from typing import List

# Log filename
log_file = f"logs/{datetime.datetime.now().strftime('%Y-%m-%d')}.log"

# Create a custom formatter without milliseconds
date_format = '%Y-%m-%d %H:%M:%S'

# Logging config
logging.basicConfig(filename=log_file, 
                    filemode='a',
                    level=logging.INFO,
                    format='%(levelname)s : %(asctime)s : %(message)s',
                    datefmt=date_format)

def getSnowflakeSession(credentials:dict) -> snowpark.Session:
    """
    Create a Snowflake session using the provided credentials and return the session object.

    Parameters:
    - credentials (dict): A dictionary containing Snowflake connection credentials, including
      server, user, password, warehouse, database, schema, and role.

    Returns:
    - snowpark.Session: A Snowflake session object.

    This function configures and creates a Snowflake session using the specified credentials.
    If the session is created successfully, an informational message is logged, and the session
    object is returned. In case of an error during session creation, a critical error is logged,
    and an exception is raised.

    Example:
        # Define Snowflake credentials
        snowflake_credentials = {
            'server': 'your_server',
            'user': 'your_user',
            'password': 'your_password',
            'warehouse': 'your_warehouse',
            'database': 'your_database',
            'schema': 'your_schema',
            'role': 'your_role'
        }

        # Create a Snowflake session\n
        snowflake_session = getSnowflakeSession(snowflake_credentials)
    """
    try:
        s = Session.builder.configs(credentials).create()
        logging.info('Snowflake Session Created Successfully.')
        return s
    
    except Exception as e:
        logging.critical(e)
        raise Exception('Snowflake Connection error : Aborting.')
    
def getMsSqlSession(credentials:dict) -> sa.engine.base.Connection:
    """
    Create a Microsoft SQL Server connection using the provided credentials and return a connection object.

    Parameters:
    - credentials (dict): A dictionary containing MS SQL Server connection credentials, including server,
      database, username, and password.

    Returns:
    - sqlalchemy.engine.Connection: A SQLAlchemy connection object for the Microsoft SQL Server.

    This function configures and creates a connection to a Microsoft SQL Server database using the specified credentials.
    It uses SQLAlchemy and PyODBC to establish the connection. If the connection is created successfully, an informational
    message is logged, and the connection object is returned. In case of an error during connection setup, a critical error
    is logged, and an exception is raised.

    Example:
        # Define MS SQL Server credentials
        mssql_credentials = {
            'server': 'your_server',
            'database': 'your_database',
            'username': 'your_username',
            'password': 'your_password'
        }

        # Create a Microsoft SQL Server connection\n
        mssql_connection = getMsSqlSession(mssql_credentials)
    """
    try:
        
        connString = f"mssql+pyodbc://{credentials['username']}:{credentials['password']}@{credentials['server']}/{credentials['database']}?driver=ODBC+Driver+17+for+SQL+Server"
        engine = sa.create_engine(connString)
        connection = engine.connect()

        logging.info(f"MS SQL Connection created Successfully for {credentials['database']} database.")
        return connection
    
    except Exception as e:
        logging.critical(e)
        raise Exception('MS SQL Connection Error : Aborting.')
    
def getMsSqlSessionsForDatabases(mapping:dict, credentials:dict) -> dict:
    """
    Create Microsoft SQL Server sessions for databases based on table mappings and credentials.

    Parameters:
    - mapping (dict): A dictionary containing table mappings, associating source tables with their respective target tables.
    - credentials (dict): A dictionary containing MS SQL Server connection credentials, including server, username, and password.

    Returns:
    - dict: A dictionary mapping database names to their corresponding SQL Alchemy session objects.

    This function takes a table mapping and MS SQL Server credentials as input and creates SQL Alchemy sessions for each
    unique database found in the source tables of the mapping. It establishes connections to the databases using the
    provided credentials and returns a dictionary with the database names as keys and the corresponding session objects
    as values.

    Example:
        # Define table mappings
        table_mappings = {
            'db1.table1': 'db_target1.table_target1',
            'db2.table2': 'db_target2.table_target2'
        }

        # Define MS SQL Server credentials
        mssql_credentials = {
            'server': 'your_server',
            'username': 'your_username',
            'password': 'your_password'
        }

        # Create MS SQL Server sessions for databases\n
        mssql_sessions = createMsSqlSessions(table_mappings, mssql_credentials)
    """
    # Empty Dictionary for session to database mapping
    databaseSessions = {}

    try :

        for sourceTable, targetTable in mapping.items():

            # Getting Database
            sourceDatabase = sourceTable.split('.')[0]

            # Create a session for the database if it doesn't exist
            if sourceDatabase not in databaseSessions:
                databaseCredentials = credentials.copy()

                # Adding database to credentials
                databaseCredentials['database'] = sourceDatabase

                # Getting Session for that database
                databaseSession = getMsSqlSession(credentials=databaseCredentials)

                # Adding session and database to final dictionary
                databaseSessions[sourceDatabase] = databaseSession

    except Exception as e:
        logging.critical(e)
        raise Exception("Something wrong while creating database connections. Aborting.")

    return databaseSessions

def getMsSqlTableData(session:sa.engine.base.Connection, database:str, table:str, schema:str='dbo', chunks:int=10000) -> pd.DataFrame:
    """
    Retrieve data from a Microsoft SQL Server table in chunks using an established session.

    Parameters:
        session (sqlalchemy.engine.base.Connection): An active SQL Alchemy database connection.
        database (str): The name of the database in which the table is located.
        table (str): The name of the table from which data is to be extracted.
        schema (str, optional): The name of the Schema in which table is present (default is 'dbo').
        chunks (int, optional): The number of rows to retrieve in each chunk (default is 10,000).

    Returns:
        pd.DataFrame: A pandas DataFrame containing the table data, returned in chunks.

    The function connects to the specified MS SQL database and table using the provided session,
    and retrieves data from the table in chunks.

    Usage:
        - Establish a SQL Alchemy session (session) and specify the database and table.
        - Retrieve data from the 'my_table' table in chunks of 10,000 rows.\n
        data = getMsSqlTableData(session, database='my_database', table='my_table', chunks=10000)
    """
    try:

        query = f"SELECT * FROM {database}.{schema}.{table}"
        return pd.read_sql(sql=query, con=session, chunksize=chunks, dtype_backend='pyarrow')
    
    
    except Exception as e:
        logging.error(e)
        raise Exception('MS SQL Table data error : Aborting.')
    
def deleteSfTable(session:snowpark.Session, table:str) -> str:
    """
    Drop a Snowflake table using a Snowpark session and return the operation's success status.

    Parameters:
    - session (snowpark.Session): A Snowpark session used to interact with Snowflake.
    - table (str): The name of the table to be dropped.

    Returns:
    - str: 'Success' if the table was dropped successfully, 'Fail' if the operation encountered an error.

    This function takes a Snowpark session and the name of a Snowflake table as input. It attempts to drop the specified
    table using SQL, and then collects the operation result. The function logs the result and determines the success status
    based on whether the operation was successful or encountered an error.

    Example:
        - Create a Snowpark session (snow_session) and specify the table name (table_name) to be deleted.
        - Delete the table and check the operation's success status.\n
        deletion_status = deleteSfTable(snow_session, table_name)
    """
    try:
        result = session.sql(f'DROP TABLE IF EXISTS {table}').collect()
        logging.info(result)
        result = result[0].asDict()['status']

        logging.info(result)

        status = 'Success' if 'successfully' in result else 'Fail'
        return status

    except Exception as e:
        logging.error(e)
        raise Exception('Snowflake table deletion Error : Aborting.')
    
def getMsSqlTableDataTypes(session:sa.engine.base.Connection, database:str, table:str, schema:str='dbo') -> dict:
    """
    Retrieve column data types for a Microsoft SQL Server table using a session.

    Parameters:
    - session (sqlalchemy.engine.base.Connection): The Microsoft SQL session for querying the database.
    - database (str): The name of the database containing the table.
    - table (str): The name of the table for which data types are to be retrieved.
    - schema (str, optional): The name of the schema in which table is present. Default is 'dbo'.

    Returns:
    - dict: A dictionary with column names as keys and their corresponding data types as values.

    This function takes a Microsoft SQL Server session, a database name, and a table name as input and queries the database's
    INFORMATION_SCHEMA.COLUMNS to retrieve the data types of each column in the specified table. The data is returned as a
    dictionary with column names as keys and their data types as values.

    Example:
        - Create a Microsoft SQL Server session (sql_session) and specify the database and table name.
        - Retrieve the data types of columns in the specified table.\n
        data_types = getMsSqlTableDataTypes(sql_session, database_name, table_name)
    """
    try:

        query = f"""
        SELECT
            TABLE_NAME AS TableName,
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{schema}'
        """
        df = pd.read_sql(sql=query, con=session, dtype_backend='pyarrow')

        if df.empty:
            logging.warning('No data fetched for data types.')
            raise Exception('Datatype Dataframe is empty. Aborting')
        
        else:
            # Only extracting column_name and its data type but more information is also there in dataframe
            return dict(zip(df['COLUMN_NAME'], df['DATA_TYPE']))
    
    except Exception as e:
        logging.error(e)
        raise Exception(f'Something wrong while fetching datatypes of MsSql table {table}: Aborting.')
    
def convertDatatypesFromMssqlToSf(mssqlDataTypeMappingDict:dict) -> dict:
    """
    Convert Microsoft SQL Server data types to Snowflake-compatible data types for a given dictionary.

    Parameters:
    - mssqlDataTypeMappingDict (dict): A dictionary with column names as keys and their corresponding MS SQL Server data types as values.

    Returns:
    - dict: A dictionary with column names as keys and their corresponding Snowflake-compatible data types as values.

    This function takes a dictionary with column names and their MS SQL Server data types as input and converts
    the data types to Snowflake-compatible data types. It returns a new dictionary with the same column names and
    Snowflake-compatible data types. If a data type is not recognized, it defaults to 'STRING'.

    Example:
        - Define a dictionary with MS SQL Server data types for columns
        mssql_data_types = {
            'column1': 'int',
            'column2': 'varchar',
            'column3': 'date',
        }

        - Convert the data types to Snowflake-compatible data types\n
        snowflake_data_types = convertDatatypesFromMssqlToSf(mssql_data_types)
    """
    sql_server_to_snowflake_mapping = {
                'int': 'NUMBER',
                'bigint': 'NUMBER',
                'smallint': 'NUMBER',
                'tinyint': 'NUMBER',
                'numeric': 'FLOAT',
                'decimal': 'FLOAT',
                'float': 'FLOAT',
                'real': 'FLOAT',
                'money': 'NUMBER',
                'smallmoney': 'NUMBER',
                'bit': 'BOOLEAN',
                'char': 'STRING',
                'varchar': 'STRING',
                'text': 'STRING',
                'nchar': 'STRING',
                'nvarchar': 'STRING',
                'ntext': 'STRING',
                'date': 'DATE',
                'time': 'TIME',
                'datetime': 'TIMESTAMP',
                'datetime2': 'TIMESTAMP',
                'timestamp': 'TIMESTAMP',
                # Add more mappings as needed
    }

    return {column.upper() : sql_server_to_snowflake_mapping.get(dataType, 'STRING') for column, dataType in mssqlDataTypeMappingDict.items()}

def sendEmailNotif(session:snowpark.Session, notifIntegrationName:str, sendTo:List[str], subject:str, body:str):
    """
    Send email notifications using Snowflake's Email Notification Object through a Snowpark session.

    Parameters:
    - session (snowpark.Session): The Snowpark session for executing Snowflake queries.
    - notifIntegrationName (str): The name of the Snowflake Notification Integration to use.
    - sendTo (List[str]): A list of email addresses to send the email notifications to.
    - subject (str): The subject of the email notification.
    - body (str): The body of the email notification.

    This function takes a Snowpark session, Notification Integration name, a list of email recipients, a subject, and a message body
    as input. It constructs a Snowflake SQL query to send an email using the specified Notification Integration and provided details.
    The email is sent to the recipients with the given subject and body.

    Example:
        - Define Snowpark session (snow_session), Notification Integration name, recipient list (recipients),
        - email subject (email_subject), and email body (email_body).
        - Send an email notification using Snowflake's Email Notification Object.\n
        sendEmailNotif(snow_session, 'notification_integration_name', recipients, email_subject, email_body)
    """

    subject = subject.replace("'", "")
    body = body.replace("'", "")
    query = f"""
            CALL SYSTEM$SEND_EMAIL(
            '{notifIntegrationName}',
            '{", ".join(sendTo)}',
            '{subject}',
            '{body}'
            )
            """
    
    logging.info(session.sql(query).collect())

