import boto3
import time
import logging
client = boto3.client('athena')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
client = boto3.client('athena', region_name='us-east-1')
session = boto3.Session()
s3 = session.resource('s3')


def lambda_handler(event, context):

    # Destination Bucket to dump CSV's
    athena_result_bucket = f"s3://aws-athena-query-s3-access-results"
    # Name of S3 Buckets we are scanning to retrieve user access logs
    s3_buckets = ['ndar_confluence_backup', 'ndar_jenkins_backup']

    # Loop through buckets
    for bucket in s3_buckets:
        # Check if bucket actually exists
        if s3.Bucket(bucket).creation_date is None:
            logger.info((f"The following bucket does not exist: {bucket}"))
        else:
            query = f'SELECT regexp_extract_all(requestdatetime,\'([a-zA-Z]+)\')[1] months , \
                remoteip , requester , key , objectsize, bytessent, useragent, httpstatus \
                    FROM s3_accesslogsdb.data_access_logs WHERE operation= \'REST.GET.OBJECT\' \
                        AND (httpstatus= \'206\' OR httpstatus=\'200\') AND CAST(objectsize AS varchar(40)) <> \'-\' \
                            AND month IN (\'09\',\'08\',\'07\',\'06\',\'05\',\'04\') and year = \'2021\' AND bucketname=\'{bucket}\''

            database = "s3_accesslogsdb"

            # Run query
            queryStart = client.start_query_execution(
                # PUT_YOUR_QUERY_HERE
                QueryString=query,
                QueryExecutionContext={
                    # YOUR_ATHENA_DATABASE_NAME
                    'Database': database
                },
                ResultConfiguration={
                    # query result output location you mentioned in AWS Athena
                    "OutputLocation": f"{athena_result_bucket}/{bucket}"
                }
            )
