/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */

/**
 * @author Dennis Heimbigner
 */

#include <aws/core/Aws.h> /* Needed for InitAPI, SDKOptions, Logging,
                             and ShutdownAPI */
#if 0
#include <aws/core/utils/memory/stl/AwsStringStream.h> 
#endif
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <iostream>
#include <fstream>
#include <sys/stat.h>
