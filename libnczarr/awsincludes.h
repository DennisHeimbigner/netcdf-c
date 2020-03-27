/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */

/**
 * @author Dennis Heimbigner
 */

#include <aws/core/Aws.h> /* Needed for InitAPI, SDKOptions, Logging,
                             and ShutdownAPI */
#include <aws/s3/S3Client.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <iostream>
#include <fstream>
#include <sys/stat.h>
