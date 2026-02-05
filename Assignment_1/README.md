
## Environment Variable Setup

This project uses environment variables to store Snowflake credentials so that sensitive information is not hard-coded in the source code.

### Steps

1. Create a file named .env in the root directory of the project.
2. Add the following Snowflake configuration details to the `.env` file:

   SNOWFLAKE_ACCOUNT=your_account_identifier
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_ROLE=SYSADMIN
   SNOWFLAKE_WAREHOUSE=MEDIUM_WH
   SNOWFLAKE_DATABASE=ETL_DB
   SNOWFLAKE_SCHEMA=PUBLIC
3. Update the values with your own Snowflake account details.
4. The project uses the `python-dotenv` library to load these variables at runtime, so no additional setup is required once the `.env` file is created.

**Note**

* The .env file should not be pushed to GitHub.
* Make sure .env is listed in .gitignore

<pre class="overflow-visible! px-0!" data-start="526" data-end="741"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-[calc(var(--sticky-padding-top)+9*var(--spacing))]"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"></div></div></pre>


<pre class="overflow-visible! px-0!" data-start="526" data-end="741"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-[calc(var(--sticky-padding-top)+9*var(--spacing))]"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"></div></div></pre>
