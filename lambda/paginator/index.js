const AWS = require('aws-sdk');
const s3 = new AWS.S3();

exports.lambda_handler = async (event) => {
  console.log(event);
  const bucket = event.bucket;
  const allKeys = await getAllKeys(bucket, []);
  return {
    keys: allKeys,
    bucket: bucket
  };
};

const getAllKeys = async (bucket, keys, continuationToken) => {
  const params = {
    Bucket: bucket,
    ContinuationToken: continuationToken? continuationToken : null
  };
  const response = await s3.listObjectsV2(params).promise();
  console.log(response.Contents);
  keys.push(...response.Contents.map(object => object.Key));
  console.log(keys);
  if (!response.NextContinuationToken)
    return keys;
  
  return await getAllKeys(bucket, keys, response.NextContinuationToken);
  
};