/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See LICENSE.txt for license information.
*/

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "netcdf.h"
#include "ncglobal.h"
#include "ncaws.h"
#include "ncuri.h"
#include "ncrc.h"

/**************************************************/
/* Local Macros */

#ifndef REPLACE
#define REPLACE(dst,src) do{nullfree(dst); dst = nulldup(src);}while(0)
#endif

/**************************************************/
/* Forward */

static void NC_awsprofilemerge(NCawsprofile* baseaws, NCawsprofile* newaws);

/**************************************************/
/* Aws Param management */

void
NC_clearawsprofile(NCawsprofile* aws)
{
    nullfree(aws->config_file);
    nullfree(aws->profile);
    nullfree(aws->region);
    nullfree(aws->default_region);
    nullfree(aws->access_key_id);
    nullfree(aws->secret_access_key);
    memset(aws,0,sizeof(NCawsprofile));
}

#if 0
void
NC_cloneawsprofile(NCawsprofile* clone, NCawsprofile* aws)
{
    NC_clearawsprofile(clone);
    clone->config_file = nulldup(aws->config_file);
    clone->profile = nulldup(aws->profile);
    clone->region = nulldup(aws->region);
    clone->default_region = nulldup(aws->default_region);
    clone->access_key_id = nulldup(aws->access_key_id);
    clone->secret_access_key = nulldup(aws->secret_access_key);
}
#endif

NCawsprofile
NC_awsprofile_empty(void)
{
    NCawsprofile aws;
    memset(&aws,0,sizeof(NCawsprofile));
    return aws;
}

/**************************************************/
/* Capture environmental Info */

/*
When loading the globalstate AWS key values, load in the following order:
1. .rc file without URI patterns.
2. environment variables
Note: precedence order: 2 over 1.
*/

/* Load the globalstate.aws fields */
void
NC_awsglobal(void)
{
    NCglobalstate* gs = NC_getglobalstate();
    NCawsprofile aws;

    aws = NC_awsprofile_empty();
    NC_clearawsprofile(gs->aws);
    
    /* Get .rc information */
    NC_clearawsprofile(&aws);
    NC_awsrc(&aws,NULL);
    NC_awsprofilemerge(gs->aws,&aws);

    /* Get environment information; overrides .rc file*/
    NC_clearawsprofile(&aws);
    NC_awsenvironment(&aws);
    NC_awsprofilemerge(gs->aws,&aws);
    NC_clearawsprofile(&aws);   

    /* Load from specified profile, if defined */
    if(gs->aws->profile != NULL) {
	????	
    }

    /* Do some defaulting */
    if(gs->aws->default_region == NULL) gs->aws->default_region = nulldup(AWS_GLOBAL_DEFAULT_REGION);
    if(gs->aws->region == NULL) gs->aws->region = nulldup(gs->aws->default_region);
}

/*
When loading the NC_FILE_INFO_T AWS key values, load in the following order:
1. existing globalstate values
2. .rc file with URI patterns.
3. environment variables 
4. URI fragment keys (only if path is URI).
Note: precedence order: 4 over 3 over 2 over 1.
*/

/*
Load the NC_FILE_INFO_T->format_file_info aws fields.
@param fileaws profile to fill per-file
@param uri to control which .rc entries to load (may be NULL)
*/
void
NC_awsnczfile(NCawsprofile* fileaws, NCURI* uri)
{
    NCawsprofile aws;
    NCglobalstate* gs = NC_getglobalstate();
    
    aws = NC_awsprofile_empty();
    NC_clearawsprofile(fileaws);

    /* Initialize the aws from gs->aws */
    NC_awsprofilemerge(fileaws,gs->aws);

    /* Get .rc information with uri from the open/create path */
    NC_clearawsprofile(&aws);
    NC_awsrc(&aws,uri);
    NC_awsprofilemerge(fileaws,&aws);

    /* Get environment information; overrides .rc file*/
    NC_clearawsprofile(&aws);
    NC_awsenvironment(&aws);
    NC_awsprofilemerge(fileaws,&aws);

    /* Get URI fragment information */
    NC_clearawsprofile(&aws);
    NC_awsfrag(&aws,uri);
    NC_awsprofilemerge(fileaws,&aws);
    NC_clearawsprofile(&aws);

    /* Do some defaulting */
    if(fileaws->default_region == NULL) fileaws->default_region = nulldup(AWS_GLOBAL_DEFAULT_REGION);
    if(fileaws->region == NULL) fileaws->region = nulldup(fileaws->default_region);
}

static void
NC_awsprofilemerge(NCawsprofile* baseaws, NCawsprofile* newaws)
{
    assert(baseaws != NULL && newaws != NULL);
    if(newaws->config_file != NULL)       REPLACE(baseaws->config_file,newaws->config_file);
    if(newaws->profile != NULL)           REPLACE(baseaws->profile,newaws->profile);
    if(newaws->region != NULL)            REPLACE(baseaws->region,newaws->region);
    if(newaws->default_region != NULL)    REPLACE(baseaws->default_region,newaws->default_region);
    if(newaws->access_key_id != NULL)     REPLACE(baseaws->access_key_id,newaws->access_key_id);
    if(newaws->secret_access_key != NULL) REPLACE(baseaws->secret_access_key,newaws->secret_access_key);
}

/**************************************************/
/* Lookup functors */

/* Collect aws profile from env variables */
void
NC_awsenvironment(NCawsprofile* aws)
{
    NC_clearawsprofile(aws);
    aws->profile = nulldup(getenv(AWS_ENV_PROFILE));
    aws->config_file = nulldup(getenv(AWS_ENV_CONFIG_FILE));
    aws->region = nulldup(getenv(AWS_ENV_REGION));
    aws->default_region = nulldup(getenv(AWS_ENV_DEFAULT_REGION));
    aws->access_key_id = nulldup(getenv(AWS_ENV_ACCESS_KEY_ID));
    aws->secret_access_key = nulldup(getenv(AWS_ENV_SECRET_ACCESS_KEY));
}

/* Setup aws profile from .rc file */
void
NC_awsrc(NCawsprofile* aws, NCURI* uri)
{
    NC_clearawsprofile(aws);
    aws->profile = nulldup(NC_rclookupx(uri,AWS_RC_PROFILE));
    aws->config_file = nulldup(NC_rclookupx(uri,AWS_RC_CONFIG_FILE));
    aws->region = nulldup(NC_rclookupx(uri,AWS_RC_REGION));
    aws->default_region = nulldup(NC_rclookupx(uri,AWS_RC_DEFAULT_REGION));
    aws->access_key_id = nulldup(NC_rclookupx(uri,AWS_RC_ACCESS_KEY_ID));
    aws->secret_access_key = nulldup(NC_rclookupx(uri,AWS_RC_SECRET_ACCESS_KEY));
}

/* Setup aws profile from URI fragment */
void
NC_awsfrag(NCawsprofile* aws, NCURI* uri)
{
    NC_clearawsprofile(aws);
    aws->profile = nulldup(ncurifragmentlookup(uri,AWS_FRAG_PROFILE));
    aws->config_file = nulldup(ncurifragmentlookup(uri,AWS_FRAG_CONFIG_FILE));
    aws->region = nulldup(ncurifragmentlookup(uri,AWS_FRAG_REGION));
    aws->default_region = nulldup(ncurifragmentlookup(uri,AWS_FRAG_DEFAULT_REGION));
    aws->access_key_id = nulldup(ncurifragmentlookup(uri,AWS_FRAG_ACCESS_KEY_ID));
    aws->secret_access_key = nulldup(ncurifragmentlookup(uri,AWS_FRAG_SECRET_ACCESS_KEY));
}

/* Setup aws profile object from specified profile */
void
NC_awsprofile(const char* profile, NCawsprofile* aws)
{
    const char* value;
    NC_clearawsprofile(aws); /* clear return value */
    value = NULL;
    (void)NC_s3profilelookup(profile,AWS_PROF_PROFILE,&value);
    aws->profile = nulldup(value); value = NULL;
    (void)NC_s3profilelookup(profile,AWS_PROF_CONFIG_FILE,&value);
    aws->config_file = nulldup(value); value = NULL;
    (void)NC_s3profilelookup(profile,AWS_PROF_REGION,&value);
    aws->region = nulldup(value); value = NULL;
    (void)NC_s3profilelookup(profile,AWS_PROF_DEFAULT_REGION,&value);
    aws->default_region = nulldup(value); value = NULL;
    (void)NC_s3profilelookup(profile,AWS_PROF_ACCESS_KEY_ID,&value);
    aws->access_key_id = nulldup(value); value = NULL;
    (void)NC_s3profilelookup(profile,AWS_PROF_SECRET_ACCESS_KEY,&value);
    aws->secret_access_key = nulldup(value); value = NULL;
}

