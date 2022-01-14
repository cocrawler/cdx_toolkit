# Using cdx_toolkit's columnar index with Athena

## Installing

```
$ pip install cdx_toolkit[athena]
```

## Credentials and Configuration

In addition to having AWS credentials, a few more configuration items are needed.

credentials: can be done multiple ways, here is one: ~/.aws/config and [profile cdx_athena]

aws_access_key_id
aws_secret_access_key

s3_staging_dir=, needs to be writeable, need to explain how to clear this bucket
schema_name= will default to 'ccindex', this is the database name, not the table name

region=us-east-1  # this is the default, and this is where CC's data is stored
# "When specifying a Region inline during client initialization, this property is named region_name."
s3_staging_dir=s3://dshfjhfkjhdfshjgdghj/staging


## Initializing the database

asetup
asummary
get_all_crawls

## Arbitrary queries

asql
explain the partitions
explain how to override the safety-belt LIMIT 

## Iterating similar to the CDX index

## Generating subset WARCs from an sql query or iteration

## Clearing the staging directory

configure rclone
