# MS-SQL to Snowflake Data Transfer

This repository contains Python code and scripts for transferring data from Microsoft SQL Server to Snowflake. It provides a step-by-step guide and example scripts to facilitate the data migration process.

## Table of Contents

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Contributing](#contributing)
- [Troubleshooting](#Troubleshooting)
- [License](#license)

## Introduction

Data migration is a common task in many organizations. This repository aims to provide a simple and effective way to transfer data from a Microsoft SQL Server database to Snowflake, a cloud data warehousing platform. The Python code in this repository can be customized to meet your specific data transfer needs.

## Prerequisites

Before you begin, ensure you have the following:

1. Access to the source Microsoft SQL Server and the target Snowflake instance and properly configured ODBC driver for SQL Server.

2. **Snowflake Credentials**: Prepare a JSON file (`snowflake.json`) containing the Snowflake credentials required to establish a connection. JSON file should present in same directory as main python file. The JSON file should include the following fields:
   - `user`: The Snowflake username
   - `password`: The Snowflake password
   - `account`: The Snowflake account name
   - `role`: The Snowflake role name
   - `warehouse`: The Snowflake warehouse name
   - `database`: The Snowflake database name
   - `schema`: The Snowflake schema name

3. **Snowflake Email Notificaiton Object**: Need to create a Snowflake's Email Notification Object which will be used to send success and failure email message.
    - Creating Email Notification
      ```sql
      CREATE NOTIFICATION INTEGRATION my_email_int
      TYPE=EMAIL
      ENABLED=TRUE
      ALLOWED_RECIPIENTS=('first.last@example.com','first2.last2@example.com');
      ```
    - More information can be found on Snowflake Website [here](https://docs.snowflake.com/en/user-guide/email-stored-procedures)

3. **Microsoft SQL Server Credentials**: Prepare a JSON file (`mssql.json`) containing the MS-SQL credentials required to establish a connection. JSON file should present in same directory as main python file. the JSON file should include the following fields:
    - `server` : MS-SQL Server Name/IP address
    - `username` : SQL Server username
    - `password` : SQL Server password

3. **Python Dependencies**: Either create a new virtual environment using `conda` or use standard `pip` to install dependencies:

   - **Pip**: Install the dependencies listed in the `requirements.txt` file using the command:

     ```bash
     pip install -r requirements.txt
     ```

   - **Conda**: Create a new virtual environment using the provided `conda-env.yaml` file. Run the following command:

     ```bash
     conda env create -f conda-env.yaml
     conda activate mssql-env
     ```

   The above steps will create a virtual environment named `mssql-env` with the necessary dependencies installed.

   NOTE: Barebones of all prerequisites can be found in code repository itself.


## Usage
1. After activating conda environment, we can edit `main.py` file to set up name of Email integration, recipient's list and mapping which defines one to one mapping of MS-SQL server to Snowflake table.

2. After populating `snowflake.json` and `mssql.json` files with necessary credentials script is ready.
    ```bash
    python main.py
    ```

## Contributing
Contributions to this project are welcome. Feel free to submit bug reports, feature requests, or even pull requests to help improve the data transfer process.

## Troubleshooting
- If you encounter any issues or errors during the process, please ensure that you have correctly set up the prerequisites and provided valid credentials.
- If you need assistance, please open an issue on the [GitHub repository](https://github.com/rishabh-a7da6/mssql-to-snowflake) for this project.

## License
This project is licensed under the [MIT License](LICENSE). Feel free to use, modify, and distribute this code as needed, but please provide appropriate attribution and consider contributing back to the open-source community.
