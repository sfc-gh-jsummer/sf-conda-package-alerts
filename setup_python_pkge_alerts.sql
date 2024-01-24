/* 
PYTHON PACKAGE NOTIFICATIONS

Script will create a set of objects to send email alerts to notify of newly available Python package versions in Snowflake.

Python packages of interest should be marked with TRACKING = 1 in table EMAILS.MANAGE.PYTHON_PACKAGE_TRACKER.
This table will capture the latest version and runtime version supported for packages. A task will 
check the SNOWFLAKE.INFORMATION_SCHEMA.PACKAGES for newer versions and runtimes and transmit email around 2AM UTC
if updates were found.

To start tracking an existing package with alerts, simply set TRACKING = 1 for the corresponding row (example 1). 
To track recently availalbe packages (example 2),
INSERT a row in EMAILS.MANAGE.PYTHON_PACKAGE_TRACKER(package_name, version, runtime_version, tracking)
from SNOWFLAKE.INFORMATION_SCHEMA.PACKAGES corresponding to the package of interest. See Example 2. 
A similar method can be used to create an alert for packages that aren't yet supported natively in Snowflake.

Note that the stream should be reset after making manual alterations.

Examples:
1) Existing Package: You're using pandas and want to be alerted when a new pandas version is supported in Snowflake directly.
run 
```
ALTER PYTHON_PACKAGE_TRACKER SET TRACKING = 1 WHERE PACKAGE_NAME = 'pandas';
```
An alert will be sent once a new version of pandas becomes available.

2) You already have pacckage alerts setup but now want to track the recently made available package, pycaret.
run 
```
-- Confirm pycaret isn't currently tracked for alerting
SELECT * FROM PYTHON_PACKAGE_TRACKER WHERE PACKAGE_NAME = 'pycaret';

-- Add pycaret and update to track for alerting
INSERT INTO PYTHON_PACKAGE_TRACKER VALUES (
    SELECT 
       PACKAGE_NAME
       , MAX(VERSION) AS VERSION
       , MAX(RUNTIME_VERSION) AS RUNTIME_VERSION
       , 1 AS TRACKING
    FROM SNOWFLAKE.INFORMATION_SCHEMA.PACKAGES;
    WHERE PACKAGE_NAME = 'pycaret'
    GROUP BY PACKAGE_NAME
    );
```
An alert will be sent once a new version of pycaret becomes available.

Created by Jason Summer
 */

-- SET STARTER VARIABLES
SET EMAILS = '["troy.barnes@greendale.edu", "abed.nadir@greendale.edu"]';-- UPDATE ME
SET PACKAGES = '["pandas","snowflake-snowpark-python"]'; -- UPDATE ME
SET WAREHOUSE = 'DATA_SCIENCE'; -- UPDATE ME
SET EMAIL_SUBJECT = 'New Python Packages in Snowflake'; -- Can change
SET EMAILS_PARANS = (SELECT REPLACE(REPLACE($EMAILS,'[','('), ']',')')); -- Don't change


-- CREATE DATABASE AND SCHEMA
USE ROLE SYSADMIN;
CREATE OR REPLACE DATABASE EMAILS;
CREATE OR REPLACE SCHEMA EMAILS.MANAGE;

-- SETUP RBAC
USE ROLE SECURITYADMIN;
CREATE OR REPLACE ROLE EMAIL_ADMIN;
GRANT ALL ON DATABASE EMAILS TO ROLE EMAIL_ADMIN;
GRANT ALL ON SCHEMA EMAILS.MANAGE TO ROLE EMAIL_ADMIN;
GRANT USAGE ON WAREHOUSE IDENTIFIER($WAREHOUSE) TO ROLE EMAIL_ADMIN;
GRANT ROLE EMAIL_ADMIN TO ROLE SYSADMIN;

USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE EMAIL_ADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE EMAIL_ADMIN;
USE WAREHOUSE IDENTIFIER($WAREHOUSE);
USE DATABASE EMAILS;
USE SCHEMA EMAILS.MANAGE;

USE ROLE EMAIL_ADMIN;

-- STEP 1) CREATE LOOKUP TABLE FOR EMAILS
CREATE OR REPLACE TABLE PYTHON_EMAILS AS 
(select DISTINCT REPLACE(VALUE,'"') AS EMAILS from table(flatten(input => parse_json($EMAILS))));

-- STEP 2) CREATE NOTIFICATION INTEGRATION
USE ROLE ACCOUNTADMIN;
BEGIN 
  let emails varchar := $EMAILS_PARANS;  
  let ddl varchar := 'CREATE OR REPLACE NOTIFICATION INTEGRATION PYTHON_PACKAGE_ALERTS
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = '  || :emails ;   
  let rs resultset := (EXECUTE IMMEDIATE :ddl); 
  RETURN 'Notification integration created';
END;  
GRANT OWNERSHIP ON INTEGRATION PYTHON_PACKAGE_ALERTS TO EMAIL_ADMIN;

-- STEP 3) CREATE CURRENT BASE TABLE OF PACKAGES
USE ROLE EMAIL_ADMIN;
CREATE OR REPLACE TABLE PYTHON_PACKAGE_TRACKER
AS
SELECT 
    PACKAGE_NAME
    , MAX(VERSION) AS VERSION
    , MAX(RUNTIME_VERSION) AS RUNTIME_VERSION
    , 0 AS TRACKING
FROM SNOWFLAKE.INFORMATION_SCHEMA.PACKAGES
GROUP BY PACKAGE_NAME, TRACKING;

-- SET INITIAL PACKAGES TO BE TRACKED
UPDATE PYTHON_PACKAGE_TRACKER
SET TRACKING = 1
WHERE PACKAGE_NAME IN (select value from table(flatten(input => parse_json($packages))));

-- STEP 4) CREATE STREAM ON BASE TABLE
ALTER TABLE PYTHON_PACKAGE_TRACKER SET CHANGE_TRACKING = TRUE;
CREATE STREAM PYTHON_PACKAGE_TRACKER_STREAM ON TABLE PYTHON_PACKAGE_TRACKER;

-- STEP 5) TASK to UPDATE BASE TABLE
CREATE OR REPLACE TASK PYTHON_PACKAGE_TRACKER_UPDATER
  SCHEDULE = 'USING CRON 0 2 * * * UTC' -- Every day at 2AM
AS
MERGE INTO PYTHON_PACKAGE_TRACKER my_pkges
  USING (SELECT 
            PACKAGE_NAME
            ,MAX(VERSION) AS VERSION
            ,MAX(RUNTIME_VERSION) AS RUNTIME_VERSION
        FROM INFORMATION_SCHEMA.PACKAGES
        GROUP BY PACKAGE_NAME) pkges ON my_pkges.PACKAGE_NAME = pkges.PACKAGE_NAME
  WHEN MATCHED AND pkges.VERSION > my_pkges.VERSION AND my_pkges.TRACKING = 1 AND pkges.VERSION IS NOT NULL THEN UPDATE SET my_pkges.VERSION = pkges.VERSION
  WHEN MATCHED AND pkges.RUNTIME_VERSION > my_pkges.RUNTIME_VERSION AND my_pkges.TRACKING = 1 AND pkges.RUNTIME_VERSION IS NOT NULL THEN UPDATE SET my_pkges.RUNTIME_VERSION = pkges.RUNTIME_VERSION;

ALTER TASK PYTHON_PACKAGE_TRACKER_UPDATER SUSPEND;

-- STEP 6) CREATE STORED PROCEDURE TO CHECK STREAM AND SEND EMAIL
CREATE OR REPLACE PROCEDURE EMAIL_PYTHON_PACKAGE_UPDATES() 
returns string
language python
runtime_version=3.8
packages = ('snowflake-snowpark-python', 'tabulate')
handler = 'create_email_body'
execute as owner
as
$$
import snowflake

def create_email_body(session):
    header = '<h2>New Python Packages</h2>'
    paragraph = '<p>The below Python package versions are now available directly in Snowflake. See INFORMATION_SCHEMA.PACKAGES for the full list.</p>'
    try: 
        body = session.sql(
          "SELECT PACKAGE_NAME, VERSION, RUNTIME_VERSION FROM PYTHON_PACKAGE_TRACKER_STREAM WHERE METADATA$ACTION = 'INSERT' LIMIT 100"
        ).to_pandas()
        if body.size > 0:
          body = header + '\n' + paragraph + '\n' + body.to_html(index=False, justify='left')
          emails = session.sql(
          "SELECT EMAILS FROM PYTHON_EMAILS LIMIT 50"
            ).to_pandas()['EMAILS'].values.tolist()
          for s in emails:
            try:
              session.call('SYSTEM$SEND_EMAIL',
                          'PYTHON_PACKAGE_ALERTS',
                          s,
                          'New Python Packages in Snowflake',
                          body,
                          'text/html'
                          )
            except snowflake.snowpark.exceptions.SnowparkSQLException:
              # Email not valid, continue
              continue
          return 'email(s) sent'
        else:
          return 'no package updates to transmit'
    except snowflake.snowpark.exceptions.SnowparkSQLException as e:
      # Table select not valid
      body = '%s\n%s' % (type(e), e)
      return 'Table PYTHON_PACKAGE_TRACKER_STREAM invalid'
$$;


-- STEP 7) SCHEDULE EMAIL PROCEDURE IN TASK
CREATE OR REPLACE TASK PROCESS_PYTHON_PACKAGE_ALERT
warehouse = $WAREHOUSE
AFTER PYTHON_PACKAGE_TRACKER_UPDATER
WHEN SYSTEM$STREAM_HAS_DATA('PYTHON_PACKAGE_TRACKER_STREAM')
AS 
CALL EMAIL_PYTHON_PACKAGE_UPDATES();

-- STEP 8) CREATE TASK TO RESET STREAM
CREATE OR REPLACE TASK RESET_PYTHON_PACKAGE_TRACKER_STREAM
warehouse = $WAREHOUSE
AFTER PROCESS_PYTHON_PACKAGE_ALERT
WHEN SYSTEM$STREAM_HAS_DATA('PYTHON_PACKAGE_TRACKER_STREAM')
AS 
CREATE OR REPLACE TEMP TABLE RESET_TBL AS
SELECT * FROM PYTHON_PACKAGE_TRACKER_STREAM limit 1;

-- AUTOMATE - RESUME TASKS
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('PYTHON_PACKAGE_TRACKER_UPDATER');

