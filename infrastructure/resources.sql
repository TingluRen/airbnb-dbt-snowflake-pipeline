CREATE FILE FORMAT IF NOT EXISTS csv_format
  TYPE = 'CSV' 
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

SHOW FILE FORMATS;

CREATE OR REPLACE STAGE snowstage
FILE_FORMAT = csv_format
URL='s3://tinglu-airbnb-data-snowbucket/source/';

SHOW STAGES;

-- Credentials have been removed from the codebase for security reasons.
COPY INTO BOOKINGS
FROM @snowstage
FILES=('bookings.csv')

COPY INTO LISTINGS
FROM @snowstage
FILES=('listings.csv')

COPY INTO HOSTS
FROM @snowstage
FILES=('hosts.csv')

SELECT * FROM LISTINGS; 