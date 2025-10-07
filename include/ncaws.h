/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#ifndef NCAWS_H
#define NCAWS_H 1

#include "ncexternl.h"

#define AWSHOST ".amazonaws.com"
#define GOOGLEHOST "storage.googleapis.com"

/* Define the "global" default region to be used if no other region is specified */
#define AWS_GLOBAL_DEFAULT_REGION "us-east-1"

#define AWS_DEFAULT_CONFIG_FILE ".aws/config"
#define AWS_DEFAULT_CREDS_FILE ".aws/credential"


/* Provide macros for the keys for the possible sources of
   AWS values: environment variables, .aws profiles, .ncrc keys, URL fragment keys, and URL notes
*/

/* Use this enum when searching generically (as argument to NC_aws_lookup) */
typedef enum AWS_KEYSORT {
AWS_SORT_UNKNOWN=0,
AWS_SORT_CONFIG_FILE=1,
AWS_SORT_CREDS_FILE=2,
AWS_SORT_PROFILE=3,
AWS_SORT_BUCKET=4,
AWS_SORT_DEFAULT_REGION=5,
AWS_SORT_REGION=6,
AWS_SORT_ACCESS_KEY_ID=7,
AWS_SORT_SECRET_ACCESS_KEY=8,
} AWS_KEYSORT;

/* For testing, track the possible sources;
   these can act as indices into aws_keys.
*/
enum AWS_SOURCE {
    AWS_SRC_SORT=0,
    AWS_SRC_ENV=1,
    AWS_SRC_RC=2,
    AWS_SRC_PROF=3,
    AWS_SRC_FRAG=4,
    AWS_SRC_NOTE=5
};


/* Generic names */
#define AWS_CONFIG_FILE "AWS_CONFIG_FILE"
#define AWS_CREDS_FILE "AWS_SHARED_CREDENTIALS_FILE"
#define AWS_PROFILE "AWS_PROFILE"
#define AWS_BUCKET "AWS_BUCKET"
#define AWS_DEFAULT_REGION "AWS_DEFAULT_REGION"
#define AWS_REGION "AWS_REGION"
#define AWS_ACCESS_KEY_ID "AWS_ACCESS_KEY_ID"
#define AWS_SECRET_ACCESS_KEY "AWS_SECRET_ACCESS_KEY"

/* AWS environment variable */
#define AWS_ENV_CONFIG_FILE AWS_CONFIG_FILE
#define AWS_ENV_CREDS_FILE AWS_SHARED_CREDENTIALS_FILE
#define AWS_ENV_PROFILE AWS_PROFILE
#define AWS_ENV_DEFAULT_REGION AWS_DEFAULT_REGION
#define AWS_ENV_REGION AWS_REGION
#define AWS_ENV_ACCESS_KEY_ID AWS_ACCESS_KEY_ID
#define AWS_ENV_SECRET_ACCESS_KEY AWS_SECRET_ACCESS_KEY

/* AWS .rc keys */
#define AWS_RC_CONFIG_FILE "AWS.CONFIG_FILE"
#define AWS_RC_CREDS_FILE "AWS.CREDENTIALS_FILE"
#define AWS_RC_PROFILE "AWS.PROFILE"
#define AWS_RC_DEFAULT_REGION "AWS.DEFAULT_REGION"
#define AWS_RC_REGION "AWS.REGION"
#define AWS_RC_ACCESS_KEY_ID "AWS.ACCESS_KEY_ID"
#define AWS_RC_SECRET_ACCESS_KEY "AWS.SECRET_ACCESS_KEY"

/* Known .aws profile keys (lowercase) */
#define AWS_PROF_REGION "region"
#define AWS_PROF_ACCESS_KEY_ID "aws_access_key_id"
#define AWS_PROF_SECRET_ACCESS_KEY "aws_secret_access_key"

/* AWS URI fragment keys */
#define AWS_FRAG_PROFILE "aws.profile"
#define AWS_FRAG_REGION "aws.region"
#define AWS_FRAG_ACCESS_KEY_ID "aws.access_key_id"
#define AWS_FRAG_SECRET_ACCESS_KEY "aws.secret_access_key"

/* AWS URI notes keys */
#define AWS_NOTES_TYPE "aws.uri_type" /* Notes only: Track the kind of URI after rebuild (see AWSURI below) */
#define AWS_NOTES_BUCKET "aws.bucket" /* Notes only: Track the inferred bucket */
#define AWS_NOTES_REGION "aws.region" /* Notes only: Track the inferred region */

/* Track the URI type, if known */
/* Note cannot use enum because C standard does not allow forward declarations of enums. */ 
typedef enum AWSURITYPE {
AWS_TYPE_UNK=0, /* unknown */
AWS_TYPE_S3=1,  /* s3.amazon.aws */
AWS_TYPE_GS=2,  /* storage.googleapis.com */
AWS_TYPE_APP=3, /* Arbitrary appliance url*/
} AWSURITYPE;

/* Opaque */
struct NCURI;
struct NClist;
struct NCglobalstate;

/**
The process of looking-up an AWS parameter is unfortunately complex
(see [AWS API Document](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)).

For now, the lookup process is implemented procedurally.  The primary lookup
function is ````NC_aws_lookup(enum AWS_KEY_SORT key, NCURI* uri)````, where uri may be NULL.
Internally it uses a number of other functions to do lookups of .ncrc (key,value) pairs,
environment variables, and profile pairs.

The search algorithm looks for keys in the following sources.
The value associated with the first occurrence of a key is the one returned.

The search order is basically as follows (no. 1 is highest precedence).
1. URI notes pairs (if path is URI).
2. URI fragment pairs (if path is URI).
3. environment variables
4. active profile fields
5. .rc file

Profiles are loaded and parsed from the following files, in order:
1. config_file -- defaults to ~/.aws/config
2. creds_file  -- defaults to ~/.aws/credentials

*/

struct AWSprofile {
    char* profilename;
    struct NClist* pairs; /* key,value pairs */
};

#ifdef __cplusplus
extern "C" {
#endif

/* Lookup procedure for finding aws (key,value) pairs. */
DECLSPEC const char* NC_aws_lookup(AWS_KEYSORT, struct NCURI* uri);

/* Testing only: Lookup an aws_keys source name for given sort */
DECLSPEC const char* NC_aws_source(AWS_KEYSORT sort, enum AWS_SOURCE src);

#if 0
/* Extract AWS values from various sources */
DECLSPEC void NC_awsglobal(void);
DECLSPEC void NC_awsnczfile(NCawsconfig* fileaws, struct NCURI* uri);
DECLSPEC void NC_awsenvironment(struct NCawsconfig* aws);
DECLSPEC void NC_awsrc(struct NCawsconfig* aws, struct NCURI* uri);
DECLSPEC void NC_awsfrag(struct NCawsconfig* aws, struct NCURI* uri);
DECLSPEC void NC_awsprofile(const char* profile, struct NCawsconfig* aws);
#endif

/* Parse and load profile */
DECLSPEC int NC_profiles_load(void);  /* Use NCglobalstate to get the config_dir and the creds_dir */
DECLSPEC void NC_profiles_free(struct NClist*); /* Free profiles in NCglobalstate */
DECLSPEC int NC_profiles_lookup(const char* profile, struct AWSprofile** profilep);
DECLSPEC void NC_profiles_insert(struct AWSprofile* profile); /* Overwrite existing occurrence */
DECLSPEC int NC_profiles_findpair(const char* profile, const char* key, const char** valuep);
DECLSPEC struct AWSprofile NC_profiles_empty(void); /* To initialize a profile object */

/* Compute current active profile */
DECLSPEC const char* NC_getactiveawsprofile(struct NCURI*);
/* Compute current active region */
DECLSPEC const char* NC_getactiveawsregion(struct NCURI*);
/* Compute current active bucket */
DECLSPEC const char* NC_getactiveawsbucket(struct NCURI*);

#ifdef __cplusplus
}
#endif

#endif /*NCAWS_H*/
