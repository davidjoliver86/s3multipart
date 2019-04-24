# s3multipart

Simple wrapper around the Boto3 S3 client to handle multipart uploads. Written mainly to satisfy a one-time use case.

Assumes :
* You want server-side-encryption
* You've done the file splitting beforehand for the parts
* Each part ends with a `.[0-9]+` suffix

The `multipart.json` file has the `CreateMultipartUpload` response that tracks the all-important `UploadId` token.

## TODO
* Tests
* Have this script deal with the splitting
* CLI magic for special S3 API parameters
