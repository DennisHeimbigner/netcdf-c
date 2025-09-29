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

/* AWS getenv() keys */
#define AWS_ENV_CONFIG_FILE "AWS_CONFIG_FILE"
#define AWS_ENV_PROFILE "AWS_PROFILE"
#define AWS_ENV_REGION "AWS_REGION"
#define AWS_ENV_DEFAULT_REGION "AWS_DEFAULT_REGION"
#define AWS_ENV_ACCESS_KEY_ID "AWS_ACCESS_KEY_ID"
#define AWS_ENV_SECRET_ACCESS_KEY "AWS_SECRET_ACCESS_KEY"
#define AWS_ENV_SESSION_TOKEN "AWS_SESSION_TOKEN"

/* AWS .rc keys */
#define AWS_RC_CONFIG_FILE "AWS.CONFIG_FILE"
#define AWS_RC_PROFILE "AWS.PROFILE"
#define AWS_RC_REGION "AWS.REGION"
#define AWS_RC_DEFAULT_REGION "AWS.DEFAULT_REGION"
#define AWS_RC_ACCESS_KEY_ID "AWS.ACCESS_KEY_ID"
#define AWS_RC_SECRET_ACCESS_KEY "AWS.SECRET_ACCESS_KEY"
#define AWS_RC_SESSION_TOKEN "AWS.SESSION_TOKEN"

/* Known .aws profile keys (lowercase) */
#define AWS_PROF_REGION "region"
#define AWS_PROF_ACCESS_KEY_ID "aws_access_key_id"
#define AWS_PROF_SECRET_ACCESS_KEY "aws_secret_access_key"
#define AWS_PROF_SESSION_TOKEN "aws_session_token"

/* AWS URI fragment keys */
#define AWS_FRAG_CONFIG_FILE AWS_RC_CONFIG_FILE
#define AWS_FRAG_PROFILE AWS_RC_PROFILE
#define AWS_FRAG_REGION AWS_RC_REGION
#define AWS_FRAG_DEFAULT_REGION AWS_RC_DEFAULT_REGION
#define AWS_FRAG_ACCESS_KEY_ID AWS_RC_ACCESS_KEY_ID
#define AWS_FRAG_SECRET_ACCESS_KEY AWS_RC_SECRET_ACCESS_KEY
#define AWS_FRAG_SESSION_TOKEN AWS_RC_SESSION_TOKEN

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

Each profile is constructed as the union of values from various sources, namely:
1. .rc file entries that have no associated URI.
2. environment variables
3. .rc file entries that have an associated URI.
4. fragment keys of the path if the path is a URI.
5. profile taken from fragment keys

When loading the NCglobalstate profile, load from the following sources:
1. .rc file without URI patterns.
2. environment variables
Notes:
* precedence order: 2 over 1
* region field set from defaults

When loading the NC_FILE_INFO_T.format_file_info profile, load from the following sources:
1. existing NCglobalstate profile values
2. .rc file with URI patterns.
3. environment variables 
4. URI fragment keys (only if path is URI).
5. profile if defined
Notes:
* precedence order: 5 over 4 over 3 over 2 over 1
* region field set from defaults
*/

typedef struct NCawsconfig {
    char* config_file;
    char* profile;
    char* region;
    char* default_region;
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
