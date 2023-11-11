# Necessary Imports
import json

# Local Imports
from mssql import *

# Parameters for Snowflake Notification Integration
notificationName = ''
participantsList = ['']

# Reading MS-SQL credentials
with open('mssql.json') as f:
    mssql_creds = json.load(f)

# Reading Snowflake credentials
with open('snowflake.json') as d:
    snowflake_creds = json.load(d)

# MS-SQL to Snowflake Table mapping example
# mapping = {
#     "SQL_DB.DBO.USER_DATA" : "SNOWFLAKE_DB.RAW_SCHEMA.USER_DATA",
# }

mapping = {
    "" : "",
}

try:
    # Snowflake Session
    snowflake_session = getSnowflakeSession(credentials=snowflake_creds)

    # MS SQL Sessions (mapping of sessions and databases)
    mssql_sessions = getMsSqlSessionsForDatabases(mapping=mapping, credentials=mssql_creds)

    for msSqlTable, sfTable in mapping.items():
            
        # Getting database for current table
        sourceDb = msSqlTable.split('.')[0]

        # Getting session for current table from sessions dictionary
        session = mssql_sessions[sourceDb]

        # Getting table name
        sourceTable = msSqlTable.split('.')[2]

        # Getting Schema Name
        sourceSchema = msSqlTable.split('.')[1]

        # Getting Datatypes of MSSQL table
        mssqlDataTypes = getMsSqlTableDataTypes(session=session,
                                                database=sourceDb,
                                                table=sourceTable,
                                                schema=sourceSchema)
        
        # Getting Snowflake Datatypes corresponding to mssql datatypes
        sfDatatypes = convertDatatypesFromMssqlToSf(mssqlDataTypeMappingDict=mssqlDataTypes)

        # Getting data in chunks
        chunks = getMsSqlTableData(session=session, 
                                database=sourceDb,
                                table=sourceTable,
                                schema=sourceSchema,
                                chunks=500
                                )
        
        if chunks is None:
            logging.warning(f"Something wrong with getting Data from SQL Server for {sourceTable} from {sourceDb} database.")
            sendEmailNotif(session=snowflake_session,
                            notifIntegrationName=notificationName,
                            sendTo=participantsList,
                            subject='Snowpark Job MSSQL to SF : Failed',
                            body=f"""Error Occured while fetching table {sourceTable} from Database {sourceDb}.\nPlease Check Logs.
                                    \n{datetime.datetime.now(pytz.timezone('UTC'))}"""
                            )
        else:
            # Deleting SF table data
            status = deleteSfTable(session=snowflake_session, table=sfTable)

            # Flag for re-ordering column names for different chunks
            flag = 0

            if status == 'Success':

                # Loading chunkwise data in to snowflake
                for df in chunks:

                    # Changing columns to uppercase 
                    df.rename(columns={col: col.upper() for col in df.columns}, inplace=True)

                    # Converting Pandas DF to snowpark DF
                    snow_df = snowflake_session.createDataFrame(df)

                    # Typecasting columns in snowpark df
                    for column, dataType in sfDatatypes.items():
                        # Typecasting Timestamp columns to string first (IMPORTANT STEP)
                        if dataType == 'TIMESTAMP':
                            snow_df = snow_df.withColumn(column, snow_df[column].cast('string'))

                        snow_df = snow_df.withColumn(column, snow_df[column].cast(dataType))

                    # Empty list to get column names for re-ordering
                    newColumns = []

                    if flag > 0:
                        temp_df = snowflake_session.table(sfTable)

                        # Getting columns in list
                        newColumns = temp_df.columns

                    if len(newColumns) > 0:
                        snow_df = snow_df.select(newColumns)

                    # Copying snowpark dataframe to permanent table
                    snow_df.write.mode('append').save_as_table(sfTable)
                    flag += 1
            
            elif status == 'Fail':
                logging.warning(f"Failed to delete {sfTable} data.")

                sendEmailNotif(session=snowflake_session,
                                notifIntegrationName=notificationName,
                                sendTo=participantsList,
                                subject='Snowpark Job MSSQL to SF : Failed',
                                body=f"Error Occured while deleting table {sfTable}. Please Check Logs.\n{datetime.datetime.now(pytz.timezone('UTC'))}"
                            )
    sendEmailNotif(session=snowflake_session,
                        notifIntegrationName=notificationName,
                        sendTo=participantsList,
                        subject='Snowpark Job MSSQL to SF : Success',
                        body=f"Successfully loaded {len(mapping)} tables from MS-SQL Server to Snowflake. \n{datetime.datetime.now(pytz.timezone('UTC'))}"
                    )

except Exception as e:
    logging.error(e)
    e = str(e).replace("'", "")

    sendEmailNotif(session=snowflake_session,
                notifIntegrationName=notificationName,
                sendTo=participantsList,
                subject='Snowpark Job MSSQL to SF : Failed',
                body=f"Job has failed due to reason : {e}\nPlease check logs.\n{datetime.datetime.now(pytz.timezone('UTC'))}"
                )