Environment Variable Setup
This project uses environment variables to store Snowflake credentials so that sensitive information is not hard-coded in the source code.

Steps
Create a file named .env in the root directory of the project.

Add the following Snowflake configuration details to the .env file:

SNOWFLAKE_ACCOUNT=your_account_identifier SNOWFLAKE_USER=your_username SNOWFLAKE_PASSWORD=your_password SNOWFLAKE_ROLE=SYSADMIN SNOWFLAKE_WAREHOUSE=MEDIUM_WH SNOWFLAKE_DATABASE=ETL_DB SNOWFLAKE_SCHEMA=PUBLIC

Update the values with your own Snowflake account details.

The project uses the python-dotenv library to load these variables at runtime, so no additional setup is required once the .env file is created.

Note

The .env file should not be pushed to GitHub.
Make sure .env is listed in .gitignore
