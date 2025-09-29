/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "config.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef _MSC_VER
#include <io.h>
#endif

#include "netcdf.h"
#include "nc4internal.h"
#include "ncuri.h"
#include "nclist.h"
#include "ncbytes.h"
#include "ncrc.h"
#include "nclog.h"
#include "ncs3sdk.h"
#include "ncutil.h"
#include "ncaws.h"

#undef AWSDEBUG

/**************************************************/
/* Local Macros */

#ifndef REPLACE
#define REPLACE(dst,src) do{nullfree(dst); dst = nulldup(src);}while(0)
#endif

/**************************************************/

enum URLFORMAT {UF_NONE=0, UF_VIRTUAL=1, UF_PATH=2, UF_S3=3, UF_OTHER=4};

/**************************************************/
/* Forward */

static int endswith(const char* s, const char* suffix);
static void freeprofile(struct AWSprofile* profile);
static void clearprofile(struct AWSprofile* profile);
static void freeentry(struct AWSentry* e);
static int awsparse(const char* text, NClist* profiles);
static void NC_awsconfigmerge(NCawsconfig* baseaws, NCawsconfig* newaws);
static void NC_awsprofilemerge(NCawsconfig* aws, const char* profilename);

NCS3NOTES
NC_s3notes_empty(void)
{
    NCS3NOTES notes;
    memset(&notes,0,sizeof(NCS3NOTES));
    return notes;
}

/**************************************************/
/* Capture environmental Info */

EXTERNL void
NC_s3sdkenvironment(void)
{
    /* Get various environment variables as defined by the AWS sdk */
    NCglobalstate* gs = NC_getglobalstate();
    if(getenv(AWS_ENV_REGION)!=NULL)
	REPLACE(gs->aws->default_region,getenv(AWS_ENV_REGION));
    else if(getenv(AWS_ENV_DEFAULT_REGION)!=NULL)
	REPLACE(gs->aws->default_region,getenv(AWS_ENV_DEFAULT_REGION));
    else if(gs->aws->default_region == NULL)
	REPLACE(gs->aws->default_region,AWS_GLOBAL_DEFAULT_REGION);
    REPLACE(gs->aws->access_key_id,getenv(AWS_ENV_ACCESS_KEY_ID));
    REPLACE(gs->aws->config_file,getenv(AWS_ENV_CONFIG_FILE));
    REPLACE(gs->aws->profile,getenv(AWS_ENV_PROFILE));
    REPLACE(gs->aws->secret_access_key,getenv(AWS_ENV_SECRET_ACCESS_KEY));
}

/**************************************************/
/* Generic S3 Utilities */

/*
Rebuild an S3 url into a canonical path-style url.
Additionally:
* collect inferred info from url into notes arg
* collect and store information from url into aws arg.
@param url     (in) the current url
@param notes   (in/out) 
@param aws     (in/out) collect info from the rebuild
@param newurlp (out) rebuilt url
*/

int
NC_s3urlrebuild(NCURI* url, NCS3NOTES* notes, NCawsconfig* aws, NCURI** newurlp)
{
    size_t i;
    int stat = NC_NOERR;
    NClist* hostsegments = NULL;
    NClist* pathsegments = NULL;
    NCbytes* buf = ncbytesnew();
    NCURI* newurl = NULL;
    char* bucket = NULL;
    char* host = NULL;
    char* path = NULL;
    char* region = NULL;
    NCS3SVC svc = NCS3UNK;
    
    if(url == NULL)
        {stat = NC_EURL; goto done;}

    /* Parse the hostname */
    hostsegments = nclistnew();
    /* split the hostname by "." */
    if((stat = NC_split_delim(url->host,'.',hostsegments))) goto done;

    /* Parse the path*/
    pathsegments = nclistnew();
    /* split the path by "/" */
    if((stat = NC_split_delim(url->path,'/',pathsegments))) goto done;

	/* Distinguish path-style from virtual-host style from s3: and from other.
	Virtual:
		(1) https://<bucket-name>.s3.<region>.amazonaws.com/<path>
		(2) https://<bucket-name>.s3.amazonaws.com/<path> -- region defaults (to us-east-1)
	Path:
		(3) https://s3.<region>.amazonaws.com/<bucket-name>/<path>
		(4) https://s3.amazonaws.com/<bucket-name>/<path> -- region defaults to us-east-1
	S3:
		(5) s3://<bucket-name>/<path>
	Google:
		(6) https://storage.googleapis.com/<bucket-name>/<path>
		(7) gs3://<bucket-name>/<path>
	Other:
		(8) https://<host>/<bucket-name>/<path>
		(9) https://<bucket-name>.s3.<region>.domain.example.com/<path>
		(10)https://s3.<region>.example.com/<bucket>/<path>
	*/
	if(url->host == NULL || strlen(url->host) == 0)
        {stat = NC_EURL; goto done;}

    /* Reduce the host to standard form such as s3.amazonaws.com by pulling out the
       region and bucket from the host */
    if(strcmp(url->protocol,"s3")==0 && nclistlength(hostsegments)==1) { /* Format (5) */
	bucket = nclistremove(hostsegments,0);
	/* region unknown at this point */
	/* Host will be set to canonical form later */
	svc = NCS3;
    } else if(strcmp(url->protocol,"gs3")==0 && nclistlength(hostsegments)==1) { /* Format (7) */
	bucket = nclistremove(hostsegments,0);
	/* region unknown at this point */
	/* Host will be set to canonical form later */
	svc = NCS3GS;
    } else if(endswith(url->host,AWSHOST)) { /* Virtual or path */
	svc = NCS3;
	/* If we find a bucket as part of the host, then remove it */
	switch (nclistlength(hostsegments)) {
	default: stat = NC_EURL; goto done;
	case 3: /* Format (4) */ 
	    /* region unknown at this point */
    	    /* bucket unknown at this point */
	    break;
	case 4: /* Format (2) or (3) */
            if(strcasecmp(nclistget(hostsegments,0),"s3")!=0) { /* Presume format (2) */
	        /* region unknown at this point */
	        bucket = nclistremove(hostsegments,0); /* Make canonical */
            } else if(strcasecmp(nclistget(hostsegments,0),"s3")==0) { /* Format (3) */
	        region = nclistremove(hostsegments,1); /* Make canonical */
	        /* bucket unknown at this point */
	    } else /* ! Format (2) and ! Format (3) => error */
	        {stat = NC_EURL; goto done;}
	    break;
	case 5: /* Format (1) */
            if(strcasecmp(nclistget(hostsegments,1),"s3")!=0)
	        {stat = NC_EURL; goto done;}
	    /* Make canonical */
	    region = nclistremove(hostsegments,2);
    	    bucket = nclistremove(hostsegments,0);
	    break;
	}
    } else if(strcasecmp(url->host,GOOGLEHOST)==0) { /* Google (6) */
        if((host = strdup(url->host))==NULL)
	    {stat = NC_ENOMEM; goto done;}
        /* region is unknown */
	/* bucket is unknown at this point */
	svc = NCS3GS;
    } else { /* Presume Formats (8),(9),(10) */
		if (nclistlength(hostsegments) > 3 && strcasecmp(nclistget(hostsegments, 1), "s3") == 0){
			bucket = nclistremove(hostsegments, 0);
			region = nclistremove(hostsegments, 2);
			host = strdup(url->host + sizeof(bucket) + 1);
		}else{
			if (nclistlength(hostsegments) > 2 && strcasecmp(nclistget(hostsegments, 0), "s3") == 0){
				region = nclistremove(hostsegments, 1);
			}
			if ((host = strdup(url->host)) == NULL){
				stat = NC_ENOMEM;
				goto done;
			}
		}
	}

    /* Attempt to compute a region */
    if(region == NULL && notes != NULL && notes->aws != NULL)
	region = nulldup(notes->aws->region);
    if(region == NULL) { /* Get default region */
	region = (char*)nulldup(notes->aws->default_region);
    }
    if(region == NULL)
        region = AWS_GLOBAL_DEFAULT_REGION;

    /* bucket = (1) from url */
    if(bucket == NULL && nclistlength(pathsegments) > 0) {
	bucket = nclistremove(pathsegments,0); /* Get from the URL path; will reinsert below */
    }
    if(bucket == NULL && notes != NULL)
	bucket = nulldup(notes->bucket);
    if(bucket == NULL) {stat = NC_ES3; goto done;}

    if(svc == NCS3) {
        /* Construct the revised host */
	ncbytesclear(buf);
        ncbytescat(buf,"s3");
	assert(region != NULL);
        ncbytescat(buf,".");
	ncbytescat(buf,region);
        ncbytescat(buf,AWSHOST);
	nullfree(host);
        host = ncbytesextract(buf);
    } else if(svc == NCS3GS) {
	nullfree(host);
	host = strdup(GOOGLEHOST);
    }

    ncbytesclear(buf);

    /* Construct the revised path */
    if(bucket != NULL) {
        ncbytescat(buf,"/");
        ncbytescat(buf,bucket);
    }
    for(i=0;i<nclistlength(pathsegments);i++) {
	ncbytescat(buf,"/");
	ncbytescat(buf,nclistget(pathsegments,i));
    }
    path = ncbytesextract(buf);

    /* clone the url so we can modify it*/
    if((newurl=ncuriclone(url))==NULL) {stat = NC_ENOMEM; goto done;}

    /* Modify the URL to canonical form */
    ncurisetprotocol(newurl,"https");
    assert(host != NULL);
    ncurisethost(newurl,host);
    assert(path != NULL);
    ncurisetpath(newurl,path);

    /* Add "s3" to the mode list */
    NC_addmodetag(newurl,"s3");

    /* Rebuild the url->url */
    ncurirebuild(newurl);
    /* return various items */
#ifdef AWSDEBUG
    fprintf(stderr,">>> NC_s3urlrebuild: final=%s bucket=|%s| region=|%s|\n",newurl->uri,bucket,region);
#endif
    if(newurlp) {*newurlp = newurl; newurl = NULL;}
    if(notes != NULL) {
        notes->svc = svc;
        notes->bucket = bucket; bucket = NULL;
        notes->region = region; region = NULL;
    }
done:
    nullfree(region);
    nullfree(bucket)
    nullfree(host)
    nullfree(path)
    ncurifree(newurl);
    ncbytesfree(buf);
    nclistfreeall(hostsegments);
    nclistfreeall(pathsegments);
    return stat;
}

static int
endswith(const char* s, const char* suffix)
{
    if(s == NULL || suffix == NULL) return 0;
    size_t ls = strlen(s);
    size_t lsf = strlen(suffix);
    ssize_t delta = (ssize_t)(ls - lsf);
    if(delta < 0) return 0;
    if(memcmp(s+delta,suffix,lsf)!=0) return 0;
    return 1;
}

/**************************************************/
/* S3 utilities */

/**
Process the url to get info for NCS3NOTES object.
Also rebuild the url to proper path format.
@param url to process
@param newurlp the rebuilt url.
@param s3 store infot from url
@return NC_NOERR | NC_EXXX
*/
EXTERNL int
NC_s3buildnotes(NCURI* url, NCawsconfig* aws, NCURI** newurlp, NCS3NOTES** notesp)
{
    int stat = NC_NOERR;
    NCURI* url2 = NULL;
    const char* profile0 = NULL;
    NCS3NOTES* notes = NULL;

    if(url == NULL)
        {stat = NC_EURL; goto done;}

    if((notes=(NCS3NOTES*)calloc(1,sizeof(NCS3NOTES)))==NULL)
	{stat = NC_ENOMEM; goto done;}
    assert(aws->profile != NULL);
    notes->aws = aws;

    /* Rebuild the URL to path format and get a usable region and optional bucket*/
    if((stat = NC_s3urlrebuild(url,notes,&url2))) goto done;
#if 0
    /* construct the rootkey minus the leading bucket */
    pathsegments = nclistnew();
    if((stat = NC_split_delim(url2->path,'/',pathsegments))) goto done;
    if(nclistlength(pathsegments) > 0) {
	char* seg = nclistremove(pathsegments,0);
        nullfree(seg);
    }
    if((stat = NC_join(pathsegments,&s3->rootkey))) goto done;
#endif
    if(newurlp) {*newurlp = url2; url2 = NULL;}

done:
    ncurifree(url2);
    return stat;
}

#if 0
int
NC_s3notesclone(NCS3NOTES* s3, NCS3NOTES** clonep)
{
    NCS3NOTES* clone = NULL;
    if(s3 && clonep) {
	if((clone = (NCS3NOTES*)calloc(1,sizeof(NCS3NOTES)))==NULL)
           return NC_ENOMEM;
	clone->svc = s3->svc;
	if((clone->region = nulldup(s3->region))==NULL) return NC_ENOMEM;
	if((clone->bucket = nulldup(s3->bucket))==NULL) return NC_ENOMEM;
	clone->aws = s3->aws;
    }
    if(clonep) {*clonep = clone; clone = NULL;}
    else {NC_s3notesclear(clone); nullfree(clone);}
    return NC_NOERR;
}
#endif

int
NC_s3notesclear(NCS3NOTES* s3)
{
    if(s3) {
	nullfree(s3->region); s3->region = NULL;
	nullfree(s3->bucket); s3->bucket = NULL;
	s3->svc = NCS3UNK;
	s3->aws = NULL;
    }
    return NC_NOERR;
}

/*
Check if a url has indicators that signal an S3 or Google S3 url.
The rules are as follows:
1. If the protocol is "s3" or "gs3" , then return (true,s3|gs3).
2. If the mode contains "s3" or "gs3", then return (true,s3|gs3).
3. Check the host name:
3.1 If the host ends with ".amazonaws.com", then return (true,s3).
3.1 If the host is "storage.googleapis.com", then return (true,gs3).
4. Otherwise return (false,unknown).
*/

int
NC_iss3(NCURI* uri, NCS3SVC* svcp)
{
    int iss3 = 0;
    NCS3SVC svc = NCS3UNK;

    if(uri == NULL) goto done; /* not a uri */
    /* is the protocol "s3" or "gs3" ? */
    if(strcasecmp(uri->protocol,"s3")==0) {iss3 = 1; svc = NCS3; goto done;}
    if(strcasecmp(uri->protocol,"gs3")==0) {iss3 = 1; svc = NCS3GS; goto done;}
    /* Is "s3" or "gs3" in the mode list? */
    if(NC_testmode(uri,"s3")) {iss3 = 1; svc = NCS3; goto done;}
    if(NC_testmode(uri,"gs3")) {iss3 = 1; svc = NCS3GS; goto done;}    
    /* Last chance; see if host looks s3'y */
    if(uri->host != NULL) {
        if(endswith(uri->host,AWSHOST)) {iss3 = 1; svc = NCS3; goto done;}
        if(strcasecmp(uri->host,GOOGLEHOST)==0) {iss3 = 1; svc = NCS3GS; goto done;}
    }    
    if(svcp) *svcp = svc;
done:
    return iss3;
}

/**************************************************/

/*
@param text of the aws credentials file
@param profiles list of form struct AWSprofile (see ncauth.h)
*/

#if 0
static void
freeprofile(struct AWSentry* profile)
{
    if(profile) {
#ifdef AWSDEBUG
fprintf(stderr,">>> freeprofile: %s\n",profile->name);
#endif
	for(size_t i=0;i<nclistlength(profile->entries);i++) {
	    struct AWSentry* e = (struct AWSentry*)nclistget(profile->entries,i);
	    freeentry(e);
	}
        nclistfree(profile->entries);
	nullfree(profile->name);
	nullfree(profile);
    }
}
#endif

const char*
s3svcname(NCS3SVC svc)
{
    switch (svc) {
    case NCS3: return "NCS3";
    case NCS3GS: return "NCS3GS";
    case NCS3APP: return "NCS3APP";
    default: break;
    }
    return "NCs3UNK";
}

#if 0
const char*
NC_s3dumps3notes(NCS3NOTES* info)
{
    static char text[8192];
    snprintf(text,sizeof(text),"svc=%s region=%s bucket=%s",
		info->svc,
		(info->region?info->region:"null"),
		(info->bucket?info->bucket:"null")
		);
    return text;
}
#endif

/**
 * Get the credentials
 @param notes
 @param region return region from profile or env
 @param accessid return accessid from progile or env
 @param accesskey return accesskey from profile or env
 */
void
NC_s3getcredentials(NCS3NOTES* notes, const char** regionp, const char** accessidp, const char** accesskeyp)
{
    assert(notes != NULL && notes->aws != NULL);
    if(notes->region != NULL) {
	if(regionp) *regionp = notes->region;
    } else {
        if(regionp) *regionp = notes->aws->region;
    }
    if(accessidp) *accessidp = notes->aws->access_key_id;
    if(accesskeyp) *accesskeyp = notes->aws->secret_access_key;
}

/**************************************************/
/*
Get the current active profile. The priority order is as follows:
1. aws->profile
2. "default"
3. "no" -- meaning do not use any profile => no secret key

@param notes
@param profilep return profile name here or NULL if none found
@return NC_NOERR if no error.
@return NC_EINVAL if something else went wrong.
*/

int
NC_getactives3profile(NCS3NOTES* notes, const char** profilep)
{
    int stat = NC_NOERR;
    const char* profile = NULL;
    struct AWSprofile* ap = NULL;

    assert(notes != NULL && notes->aws != NULL);
    profile = notes->aws->profile;
    if(profile == NULL) {
	if((stat = NC_profiles_lookup("default",&ap))) goto done;
	if(ap) profile = "default";
    }
    if(profile == NULL) {
	if((stat = NC_profiles_lookup("no",&ap))) goto done;
	if(ap) profile = "no";
    }
    if(profilep) {*profilep = profile; profile = NULL;}
done:
    return stat;
}

#if 0
/*
Get the current default region. The search order is as follows:
1. aws.region key in mode flags
2. aws.region in .rc entries
3. aws_region key in current profile (only if profiles are being used)
4. NCglobalstate.aws.default_region

@param uri uri with mode flags, may be NULL
@param regionp return region name here or NULL if none found
@return NC_NOERR if no error.
@return NC_EINVAL if something else went wrong.
*/

int
NC_getdefaults3region(NCURI* uri, const char** regionp)
{
    int stat = NC_NOERR;
    const char* region = NULL;
    const char* profile = NULL;

    region = ncurifragmentlookup(uri,AWS_FRAG_REGION);
    if(region == NULL)
        region = NC_rclookupx(uri,AWS_RC_REGION);
    if(region == NULL) {/* See if we can find a profile */
        if(NC_getactives3profile(uri,&profile)==NC_NOERR) {
	    if(profile)
	        (void)NC_profiles_findpair(profile,AWS_PROF_REGION,&region);
	}
    }
    if(region == NULL)
	region = (NC_getglobalstate()->aws->default_region ? NC_getglobalstate()->aws->default_region : "us-east-1"); /* Force use of the Amazon default */
#ifdef AWSDEBUG
    fprintf(stderr,">>> activeregion = |%s|\n",region);
#endif
    if(regionp) *regionp = region;
    return stat;
}
#endif

