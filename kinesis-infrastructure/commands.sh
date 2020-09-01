# package data stream
aws cloudformation package --s3-bucket cowculate \
    --template-file kinesis_datastream.yaml \
    --output-template-file gen/kinesis_datastream_generated.yaml

# deploy data stream
aws cloudformation deploy --template-file gen/kinesis_datastream_generated.yaml \
    --stack-name CowculateStream

# package firehose
aws cloudformation package --s3-bucket cowculate \
    --template-file kinesis_firehose.yaml \
    --output-template-file gen/kinesis_firehose_generated.yaml

# deploy firehose
aws cloudformation deploy --template-file gen/kinesis_firehose_generated.yaml \
    --stack-name CowculateFirehose

# delete firehose
aws cloudformation delete-stack --stack-name CowculateFirehose

# delete data stream
aws cloudformation delete-stack --stack-name CowculateStream