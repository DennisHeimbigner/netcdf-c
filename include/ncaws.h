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

/* Provide macros for the keys for the possible sources of
   AWS values: getenv(), .aws profiles, .ncrc keys, and URL fragment keys
*/

AWS_SHARED_CREDENTIALS_FILE
AWS_CONFIG_FILE

/* AWS getenv() keys */
#define AWS_ENV_CONFIG_DIR "AWS_CONFIG_DIR"
#define AWS_ENV_PROFILE "AWS_PROFILE"
#define AWS_ENV_DEFAULT_REGION "AWS_DEFAULT_REGION"
#define AWS_ENV_REGION "AWS_REGION"
#define AWS_ENV_ACCESS_KEY_ID "AWS_ACCESS_KEY_ID"
#define AWS_ENV_SECRET_ACCESS_KEY "AWS_SECRET_ACCESS_KEY"

/* AWS .rc keys */
#define AWS_RC_CONFIG_DIR "AWS.CONFIG_DIR"
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
#define AWS_FRAG_PROFILE AWS_RC_PROFILE
#define AWS_FRAG_REGION AWS_RC_REGION
#define AWS_FRAG_ACCESS_KEY_ID AWS_RC_ACCESS_KEY_ID
#define AWS_FRAG_SECRET_ACCESS_KEY AWS_RC_SECRET_ACCESS_KEY

/* AWS URI notes keys */
#define AWS_NOTES_TYPE "aws.uri_type" /* Notes only: Track the kind of URI after rebuild (see AWSURI below) */
#define AWS_NOTES_BUCKET "aws.bucket" /* Notes only: Track the inferred bucket */

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
NCawsconfig is a unified profile object containing an extended set of
all the relevant AWS profile information.
There are two "instances" of this profile object.
1. NCglobalstate holds the values defined globally and independent of any file.
2. NC_FILE_INFO_T.format_file_info contains a per-file profile set of values.

Each profile is constructed as the union of values from various
sources.  The sources are loaded in a specific order with later loads
over-writing earlier loads.

When loading NCglobalstate, load -- in order -- from these sources:
1. .rc file entries that have no associated URI.
2. environment variables
3, profile, if defined.

When loading NC_FILE_INFO_T, load -- in order -- from these sources:
1.  NCglobalstate values (see above)
2. .rc file with URI patterns matching the file path for NC_FILE_INfO_T
3. URI notes keys (if path is URI) inferred from URI
4. URI fragment keys (if path is URI).
5. profile, if defined

If the default_region is undefined, then attempt to define it using (in order):
1, the global default region -- us-east-1.

If the region is undefined, then attempt to define it using (in order):
1. default_region

If the profile is undefined, then try the following alternatives are used (in order):
1. "default" profile, if defined
2. "no" profile as last resort.

Note that profiles are loaded from a directory specified as follows.
1. If config_dir is defined, then use that absolute path.
2. If config_dir is undefined then set it to $HOME/.aws

In either case, parse and load the following files:
1.<config_dir>/config
2.<config_dir>/credentials.

The set of values loaded from the above sources
include the following:
1. config_dir -- used to find profiles
2. profile -- the currently active profile
3. default_region -- in case no region is defined

typedef struct NCawsconfig {
    char* config_dir;
    char* profile;
    char* default_region;
    char* region;
    char* access_key_id;
    char* secret_access_key; 
} NCawsconfig;;

struct AWSentry {
    char* key;
    char* value;
};

struct AWSprofile {
    char* profilename;
    struct NClist* pairs; /* key,value pairs */
};

#ifdef __cplusplus
extern "C" {
#endif

/* Extract AWS values from various sources */
DECLSPEC void NC_awsglobal(void);
DECLSPEC void NC_awsnczfile(NCawsconfig* fileaws, struct NCURI* uri);
DECLSPEC void NC_awsenvironment(struct NCawsconfig* aws);
DECLSPEC void NC_awsrc(struct NCawsconfig* aws, struct NCURI* uri);
DECLSPEC void NC_awsfrag(struct NCawsconfig* aws, struct NCURI* uri);
DECLSPEC void NC_awsprofile(const char* profile, struct NCawsconfig* aws);

DECLSPEC void NC_clearawsconfig(struct NCawsconfig* aws);
DECLSPEC NCawsconfig NC_awsprofile_empty(void);

/* Parse and load profile */
DECLSPEC int NC_profiles_load(struct NCglobalstate* gstate);
DECLSPEC void NC_profiles_free(struct NClist* config);
DECLSPEC int NC_profiles_lookup(const char* profile, struct AWSprofile** profilep);
DECLSPEC void NC_profiles_insert(struct AWSprofile* profile);
DECLSPEC int NC_profiles_findpair(const char* profile, const char* key, const char** valuep);

#ifdef __cplusplus
}
#endif

#endif /*NCAWS_H*/
