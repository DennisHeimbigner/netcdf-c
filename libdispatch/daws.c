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
#include "ncutil.h"
#include "nclog.h"

/**************************************************/
/* Local Macros */

#ifndef REPLACE
#define REPLACE(dst,src) do{nullfree(dst); dst = nulldup(src);}while(0)
#endif

/**************************************************/

/* Define precedence order */
#define A_NOTE 0
#define A_FRAG 1
#define A_ENV  2
#define A_PROF 3
#define A_RC   4
#define NSRCS  5

/* Warning: keep in sorted order by sort value */
/* This holds the translation table for looking up various keys in various sources */
const struct AWS_KEY {
    AWS_KEYSORT sort;
    const char* env;  /* corresponding environment variable key */
    const char* rc;   /* corresponding .ncrc key */
    const char* prof; /* corresponding profile key */
    const char* frag; /* corresponding fragment key */
    const char* note; /* corresponding annotation key */
} aws_keys[] = {
    { AWS_SORT_UNKNOWN, // sort
        NULL,           // env
        NULL,           // .rc
        NULL,           // profile
        NULL,           // fragment
        NULL,           // notes
    },
    { AWS_SORT_CONFIG_FILE, // sort
        "AWS_CONFIG_FILE",  // env
        "AWS.CONFIG_FILE",  // .rc
        NULL,               // profile
        NULL,               // fragment
        NULL,               // notes
    },
    { AWS_SORT_CREDS_FILE,             // sort
        "AWS_SHARED_CREDENTIALS_FILE", // env
        "AWS.SHARED_CREDENTIALS_FILE", // .rc
        NULL,                          // profile
        NULL,                          // fragment
        NULL,                          // notes
    },
    { AWS_SORT_PROFILE, // sort
        "AWS_PROFILE",  // env
        "AWS.PROFILE",  // .rc
        NULL,           // profile
        "aws.profile",  // fragment
        NULL,           // notes
    },
    { AWS_SORT_BUCKET, // sort
        "AWS_BUCKET",  // env
        NULL,          // .rc
        NULL,          // profile
        NULL,          // fragment
        "aws.bucket",  // notes
    },
    { AWS_SORT_DEFAULT_REGION, // sort
        "AWS_DEFAULT_REGION",  // env
        "AWS.DEFAULT_REGION",  // .rc
        "aws_default_region",  // profile
        "aws.default_region",  // fragment
        NULL,                  // notes
    },
    { AWS_SORT_REGION, // sort
        "AWS_REGION",  // env
        "AWS.REGION",  // .rc
        "aws_region",  // profile
        "aws.region",  // fragment
        NULL,          // notes
    },
    { AWS_SORT_ACCESS_KEY_ID, // sort
        "AWS_ACCESS_KEY_ID",  // env
        "AWS.ACCESS_KEY_ID",  // .rc
        "aws_access_key_Id",  // profile
        "aws.access_key_id",  // fragment
        NULL,                 // notes
    },
    { AWS_SORT_SECRET_ACCESS_KEY, // sort
        "AWS_SECRET_ACCESS_KEY",  // env
        "AWS.SECRET_ACCESS_KEY",  // .rc
        "aws_secret_access_key",  // profile
        "aws.secret_access_key",  // fragment
        NULL,  // notes
    },
};

/**************************************************/
/**
The .aws/config and .aws/credentials files
are in INI format (https://en.wikipedia.org/wiki/INI_file).
This format is not well defined, so the grammar used
here is restrictive. Here, the term "profile" is the same
as the INI term "section".

The grammar used is as follows:

Grammar:

inifile: profilelist ;
profilelist: profile | profilelist profile ;
profile: '[' profilename ']' EOL entries ;
entries: empty | entries entry ;
entry:  WORD = WORD EOL ;
profilename: WORD ;
Lexical:
WORD    sequence of printable characters - [ \[\]=]+
EOL	'\n' | ';'

Note:
1. The semicolon at beginning of a line signals a comment.
2. # comments are not allowed
3. Duplicate profiles or keys are ignored.
4. Escape characters are not supported.
*/

#define AWS_EOF (-1)
#define AWS_ERR (0)
#define AWS_WORD (0x10001)
#define AWS_EOL (0x10002)
#define LBR '['
#define RBR ']'

typedef struct AWSparser {
    char* text;
    char* pos;
    size_t yylen; /* |yytext| */
    NCbytes* yytext;
    int token; /* last token found */
    int pushback; /* allow 1-token pushback */
} AWSparser;

/**************************************************/

/* Read these files in order and later overriding earlier */
static const char* AWSCONFIGFILES[] = {"config","credentials",NULL};

/* The .aws directory name */
static const char* AWSCONFIGDIR = ".aws";

/**************************************************/
/* Forward */

static void clearprofile(struct AWSprofile* p);
static void freeprofile(struct AWSprofile* p);
static const char* tokenname(int token);
static int awslex(AWSparser* parser);
static int awsparse(const char* text, NClist* profiles);
static const char* awsdumpprofiles(NClist* profiles);

/**************************************************/

#if 0
/* Aws Param management */
void
NC_clearawsconfig(NCawsconfig* aws)
{
    nullfree(aws->config_file);
    nullfree(aws->profile);
    nullfree(aws->region);
    nullfree(aws->default_region);
    nullfree(aws->access_key_id);
    nullfree(aws->secret_access_key);
    memset(aws,0,sizeof(NCawsconfig));
}

void
NC_cloneawsconfig(NCawsconfig* clone, NCawsconfig* aws)
{
    NC_clearawsconfig(clone);
    clone->config_file = nulldup(aws->config_file);
    clone->profile = nulldup(aws->profile);
    clone->region = nulldup(aws->region);
    clone->default_region = nulldup(aws->default_region);
    clone->access_key_id = nulldup(aws->access_key_id);
    clone->secret_access_key = nulldup(aws->secret_access_key);
}

NCawsconfig
NC_awsconfig_empty(void)
{
    NCawsconfig aws;
    memset(&aws,0,sizeof(NCawsconfig));
    return aws;
}
#endif

/**************************************************/
/*
See the comment in ncaws.h describing the loading of AWS values. 
*/

/* Lookup an AWS key by searching sources in precedence order */

const char*
NC_aws_lookup(AWS_KEYSORT sort, NCURI* uri)
{
    int stat = NC_NOERR;
    int i;
    const char* value = NULL;
    const char* key = NULL;
    const char* profile = NULL;
    const struct AWS_KEY* keyset = NULL;
    NCglobalstate* gs = NC_getglobalstate();

    assert(gs->rcinfo != NULL);
    assert(gs->profiles != NULL);

    keyset = &aws_keys[(int)sort];

    for(i=0;i<NSRCS && value == NULL;i++) {
	/* get key */
	switch (i) {
	case A_NOTE:
	    if(keyset->note == NULL) continue; /* ignore */
	    if(uri != NULL) {value = ncurinoteslookup(uri,keyset->note);}
	    break;
	case A_FRAG:
	    if(keyset->frag == NULL) continue; /* ignore */
	    if(uri != NULL) {value = ncurifragmentlookup(uri,keyset->frag);}
	    break;
	case A_ENV:
	    if(keyset->env == NULL) continue; /* ignore */
	    value = getenv(keyset->env);
	    break;
	case A_PROF:
	    if(keyset->prof == NULL) continue; /* ignore */
	    profile = NC_getactiveawsprofile(uri);
	    assert(profile != NULL);
	    if((stat = NC_profiles_findpair(profile,keyset->prof,&value))) goto done;
	    break;
	case A_RC: {
	    char* hostport = NULL;
   	    char* path = NULL;
	    if(keyset->rc == NULL) continue; /* ignore */
	    if(uri != NULL) {
		hostport = NC_combinehostport(uri->host,uri->port);
		path = uri->path;
	    }
	    value = NC_rclookup(keyset->rc,hostport,path);
	    nullfree(hostport);
	    } break;
	}
    }
done:
    if(stat) {nullfree(value); value = NULL;}
    return value;
}

/* Testing only: Lookup an aws_keys source name for given sort */
const char*
NC_aws_source(AWS_KEYSORT sort, enum AWS_SOURCE src)
{
    int stat = NC_NOERR;
    const struct AWS_KEY* keyset = NULL;
    const char* srcname = NULL;

    keyset = &aws_keys[(int)sort];
    switch(src) {
    case AWS_SRC_SORT:
        switch (keyset->sort) {
	case AWS_SORT_UNKNOWN:           srcname = "AWS_SORT_UNKNOWN";           break;
	case AWS_SORT_CONFIG_FILE:       srcname = "AWS_SORT_CONFIG_FILE";       break;
	case AWS_SORT_CREDS_FILE:        srcname = "AWS_SORT_CREDS_FILE";        break;
	case AWS_SORT_PROFILE:           srcname = "AWS_SORT_PROFILE";           break;
	case AWS_SORT_BUCKET:            srcname = "AWS_SORT_BUCKET";            break;
	case AWS_SORT_DEFAULT_REGION:    srcname = "AWS_SORT_DEFAULT_REGION";    break;
	case AWS_SORT_REGION:            srcname = "AWS_SORT_REGION";            break;
	case AWS_SORT_ACCESS_KEY_ID:     srcname = "AWS_SORT_ACCESS_KEY_ID";     break;
	case AWS_SORT_SECRET_ACCESS_KEY: srcname = "AWS_SORT_SECRET_ACCESS_KEY"; break;
	}; break;
    case AWS_SRC_ENV:  srcname = keyset->env;  break;
    case AWS_SRC_RC:   srcname = keyset->rc;   break;
    case AWS_SRC_PROF: srcname = keyset->prof; break;
    case AWS_SRC_FRAG: srcname = keyset->frag; break;
    case AWS_SRC_NOTE: srcname = keyset->note; break;
    };
    return srcname;
}


#if 0
/* Load the globalstate.aws fields */
void
NC_awsglobal(void)
{
    NCglobalstate* gs = NC_getglobalstate();
    NCawsconfig aws;

    aws = NC_awsconfig_empty();
    NC_clearawsconfig(gs->aws);
    
    /* Get .rc information */
    NC_clearawsconfig(&aws);
    NC_awsrc(&aws,NULL);
    NC_awsconfigmerge(gs->aws,&aws);

    /* Get environment information; overrides .rc file*/
    NC_clearawsconfig(&aws);
    NC_awsenvironment(&aws);
    NC_awsconfigmerge(gs->aws,&aws);
    NC_clearawsconfig(&aws);   

    /* Load the profiles */
    if(aws->config_dir
	
    }

    /* Load from specified profile, if defined */
    if(gs->aws->profile != NULL)
	NC_awsprofilemerge(gs->aws,gs->aws->profile);

    /* Do defaulting  */
    if(gs->aws->default_region == NULL) gs->aws->default_region = nulldup(AWS_GLOBAL_DEFAULT_REGION);
    if(gs->aws->region == NULL) gs->aws->region = nulldup(gs->aws->default_region);
}

/*
Load the NC_FILE_INFO_T->format_file_info aws fields.
@param fileaws profile to fill per-file
@param uri to control which .rc entries to load (may be NULL)
*/
void
NC_awsnczfile(NCawsconfig* fileaws, NCURI* uri)
{
    NCawsconfig aws;
    NCglobalstate* gs = NC_getglobalstate();
    
    aws = NC_awsconfig_empty();
    NC_clearawsconfig(fileaws);

    /* Initialize the aws from gs->aws */
    NC_awsconfigmerge(fileaws,gs->aws);

    /* Get .rc information with uri from the open/create path */
    NC_clearawsconfig(&aws);
    NC_awsrc(&aws,uri);
    NC_awsconfigmerge(fileaws,&aws);

    /* Get environment information; overrides .rc file*/
    NC_clearawsconfig(&aws);
    NC_awsenvironment(&aws);
    NC_awsconfigmerge(fileaws,&aws);

    /* Get URI fragment information */
    NC_clearawsconfig(&aws);
    NC_awsfrag(&aws,uri);
    NC_awsconfigmerge(fileaws,&aws);
    NC_clearawsconfig(&aws);

    /* Do some defaulting */
    if(fileaws->default_region == NULL) fileaws->default_region = nulldup(AWS_GLOBAL_DEFAULT_REGION);
    if(fileaws->region == NULL) fileaws->region = nulldup(fileaws->default_region);
}

static void
NC_awsconfigmerge(NCawsconfig* baseaws, NCawsconfig* newaws)
{
    assert(baseaws != NULL && newaws != NULL);
    if(newaws->config_file != NULL)       REPLACE(baseaws->config_file,newaws->config_file);
    if(newaws->profile != NULL)           REPLACE(baseaws->profile,newaws->profile);
    if(newaws->region != NULL)            REPLACE(baseaws->region,newaws->region);
    if(newaws->default_region != NULL)    REPLACE(baseaws->default_region,newaws->default_region);
    if(newaws->access_key_id != NULL)     REPLACE(baseaws->access_key_id,newaws->access_key_id);
    if(newaws->secret_access_key != NULL) REPLACE(baseaws->secret_access_key,newaws->secret_access_key);
}

static void
NC_awsprofilemerge(NCawsconfig* aws, const char* profilename)
{
    int stat = NC_NOERR;
    struct AWSprofile* ap = NULL;
    assert(aws != NULL && profilename != NULL);
    if((stat = NC_profiles_lookup(profilename,&ap))) goto done;
    if(ap != NULL) {
	const char* value;
	value = NULL;
	if((stat = NC_profiles_findpair(aws->profile,AWS_PROF_REGION,&value))) goto done;
	if(value) REPLACE(aws->region,value);
	if((stat = NC_profiles_findpair(aws->profile,AWS_PROF_ACCESS_KEY_ID,&value))) goto done;
	if(value) REPLACE(aws->access_key_id,value);
	if((stat = NC_profiles_findpair(aws->profile,AWS_PROF_SECRET_ACCESS_KEY,&value))) goto done;
	if(value) REPLACE(aws->secret_access_key,value);
    }
done:
    return;
}
#endif

/**************************************************/
/* Lookup functors */

#if 0
/* Collect aws profile from env variables */
void
NC_awsenvironment(NCawsconfig* aws)
{
    NC_clearawsconfig(aws);
    aws->profile = nulldup(getenv(AWS_ENV_PROFILE));
    aws->config_file = nulldup(getenv(AWS_ENV_CONFIG_FILE));
    aws->region = nulldup(getenv(AWS_ENV_REGION));
    aws->default_region = nulldup(getenv(AWS_ENV_DEFAULT_REGION));
    aws->access_key_id = nulldup(getenv(AWS_ENV_ACCESS_KEY_ID));
    aws->secret_access_key = nulldup(getenv(AWS_ENV_SECRET_ACCESS_KEY));
}

/* Setup aws profile from .rc file */
void
NC_awsrc(NCawsconfig* aws, NCURI* uri)
{
    NC_clearawsconfig(aws);
    aws->profile = nulldup(NC_rclookupx(uri,AWS_RC_PROFILE));
    aws->config_file = nulldup(NC_rclookupx(uri,AWS_RC_CONFIG_FILE));
    aws->region = nulldup(NC_rclookupx(uri,AWS_RC_REGION));
    aws->default_region = nulldup(NC_rclookupx(uri,AWS_RC_DEFAULT_REGION));
    aws->access_key_id = nulldup(NC_rclookupx(uri,AWS_RC_ACCESS_KEY_ID));
    aws->secret_access_key = nulldup(NC_rclookupx(uri,AWS_RC_SECRET_ACCESS_KEY));
}

/* Setup aws profile from URI fragment */
void
NC_awsfrag(NCawsconfig* aws, NCURI* uri)
{
    NC_clearawsconfig(aws);
    aws->profile = nulldup(ncurifragmentlookup(uri,AWS_FRAG_PROFILE));
    aws->region = nulldup(ncurifragmentlookup(uri,AWS_FRAG_REGION));
    aws->access_key_id = nulldup(ncurifragmentlookup(uri,AWS_FRAG_ACCESS_KEY_ID));
    aws->secret_access_key = nulldup(ncurifragmentlookup(uri,AWS_FRAG_SECRET_ACCESS_KEY));
}

/* Setup aws profile object from specified profile */
void
NC_awsconfig(const char* profile, NCawsconfig* aws)
{
    const char* value;
    NC_clearawsconfig(aws); /* clear return value */
    value = NULL;
    (void)NC_profiles_findpair(profile,AWS_PROF_REGION,&value);
    aws->region = nulldup(value); value = NULL;
    (void)NC_profiles_findpair(profile,AWS_PROF_ACCESS_KEY_ID,&value);
    aws->access_key_id = nulldup(value); value = NULL;
    (void)NC_profiles_findpair(profile,AWS_PROF_SECRET_ACCESS_KEY,&value);
    aws->secret_access_key = nulldup(value); value = NULL;
}
#endif

/**************************************************/
/* Manage .aws profiles */

void
NC_profiles_free(NClist* profiles)
{
    size_t i,j;
    if(profiles) {
	for(i=0;i<nclistlength(profiles);i++) {
	    struct AWSprofile* p = (struct AWSprofile*)nclistget(profiles,i);
	    assert(p != NULL);
	    freeprofile(p);
	}
	nclistfree(profiles);
    }
}

static void
clearprofile(struct AWSprofile* p)
{
    if(p) {
	nullfree(p->profilename);
	nclistfreeall(p->pairs);
	memset(p,0,sizeof(struct AWSprofile));
    }
}

#if 0
static struct AWSprofile
cloneprofile(struct AWSprofile* p)
{
    struct AWSprofile clone;
    memset(&clone,0,sizeof(struct AWSprofile));
    clone.profilename = nulldup(p->profilename);
    clone.pairs = nclistclone(p->pairs,AWSDEEP);
    return clone;    
}
#endif

static void
freeprofile(struct AWSprofile* p)
{
    clearprofile(p);
    nullfree(p);
}

/* Find, load, and parse the aws config &/or credentials file */
int
NC_profiles_load(void)
{
    int stat = NC_NOERR;
    size_t i,j;
    NClist* awscfgfiles = NULL;
    NCbytes* buf = ncbytesnew();
    const char* aws_config_file;
    const char* aws_creds_file;
    NClist* awscfg = NULL;
    struct AWSprofile* dfalt = NULL;
    char path[4096];
    NCglobalstate* gs = NC_getglobalstate();

    if(gs->profiles == NULL) gs->profiles = nclistnew();

    /* add a "no" credentials */
    {
	struct AWSprofile* noprof = (struct AWSprofile*)calloc(1,sizeof(struct AWSprofile));
	noprof->profilename = strdup("no");
	noprof->pairs = nclistnew();
	NC_profiles_insert(noprof); noprof = NULL;
    }

    /* See if default ~/.aws is overridden */
    aws_config_file = NC_aws_lookup(AWS_SORT_CONFIG_FILE,NULL);
    aws_creds_file = NC_aws_lookup(AWS_SORT_CREDS_FILE,NULL);
    aws_config_file = nullify(aws_config_file);
    aws_creds_file = nullify(aws_creds_file);
    
    awscfgfiles = nclistnew();
    /* compute where to look for profile files */
    if(aws_config_file == NULL) {
        snprintf(path,sizeof(path),"%s/%s",gs->home,AWS_DEFAULT_CONFIG_FILE);
	aws_config_file = path;
    } 
    nclistpush(awscfgfiles,strdup(aws_config_file));

    if(aws_creds_file == NULL) {
        snprintf(path,sizeof(path),"%s/%s",gs->home,AWS_DEFAULT_CREDS_FILE);
	aws_creds_file = path;
    } 
    nclistpush(awscfgfiles,strdup(aws_creds_file));

    /* Load and parse in order */
    for(i=0;i<nclistlength(awscfgfiles);i++) {
	const char* cfgpath = nclistget(awscfgfiles,i);
	ncbytesclear(buf);
	if((stat=NC_readfile(cfgpath,buf))) {
	    nclog(NCLOGWARN, "Could not open file: %s",cfgpath);
	} else {
	    /* Parse the file and insert profiles (with override) */
	    const char* text = ncbytescontents(buf);
	    if((stat = awsparse(text,gs->profiles))) goto done;
	}
    }
    /* Search for "default" credentials */
    if((stat=NC_profiles_lookup("default",&dfalt))) goto done;

#if 0
    /* If there is no default credentials, then try to synthesize one
       from various environment variables */

    if(dfalt == NULL) {
	/* Verify that we can build a default */
	if(gs->aws->access_key_id != NULL && gs->aws->secret_access_key != NULL) {
	    /* Build new default profile */
	    if((dfalt = (struct AWSprofile*)calloc(1,sizeof(struct AWSprofile)))==NULL) {stat = NC_ENOMEM; goto done;}
	    dfalt->profilename = strdup("default");
	    dfalt->pairs = nclistnew();
	    /* Create the entries for default */

	    entry->key = strdup(AWS_PROF_ACCESS_KEY_ID);
	    entry->value = strdup(gs->aws->access_key_id);
	    nclistpush(dfalt->pairs,key); entry = NULL;
	    if((entry = (struct AWSentry*)calloc(1,sizeof(struct AWSentry)))==NULL) {stat = NC_ENOMEM; goto done;}
	    entry->key = strdup(AWS_PROF_SECRET_ACCESS_KEY);
	    entry->value = strdup(gs->aws->secret_access_key);
	    nclistpush(dfalt->pairs,entry); entry = NULL;
	    /* Save the new default profile */
	    nclistpush(gs->awsprofiles,dfalt); dfalt = NULL;
	}
    }
#endif

done:
    nclistfreeall(awscfgfiles);
    ncbytesfree(buf);
    return stat;
}

/* Lookup a profile by name;
@param profilename to lookup
@param profilep return the matching profile; null if not found
@return NC_NOERR | NC_EXXX
*/

int
NC_profiles_lookup(const char* profilename, struct AWSprofile** profilep)
{
    int stat = NC_NOERR;
    size_t i;
    NCglobalstate* gs = NC_getglobalstate();

    if(profilep) *profilep = NULL; /* initialize */
    for(i=0;i<nclistlength(gs->profiles);i++) {
	struct AWSprofile* profile = (struct AWSprofile*)nclistget(gs->profiles,i);
	if(strcmp(profilename,profile->profilename)==0)
	    {if(profilep) {*profilep = profile; goto done;}}
    }
done:
    return stat;
}

/* Insert a profile by name; overwrite if already exists.
@param entry to insert; this code takes control of memory.
@return NC_NOERR | NC_EXXX
*/
void
NC_profiles_insert(struct AWSprofile* prof)
{
    int stat = NC_NOERR;
    NCglobalstate* gs = NC_getglobalstate();
    struct AWSprofile* p = NULL;

    if((stat = NC_profiles_lookup(prof->profilename,&p))) goto done;
    if(p) { /* overwrite */
	clearprofile(p);
	*p = *prof;
	memset(prof,0,sizeof(struct AWSprofile)); /* Avoid memory leak */
    } else {
	nclistpush(gs->profiles,prof);
        prof = NULL;
    }
done:
    nullfree(prof);
    return;
}

/**
Lookup a value in WRT the keys in a specified profile.
@param profile name of profile
@param key key to search for in profile
@param value place to store the value if key is found; NULL if not found
@return NC_NOERR | NC_EXXX
*/

int
NC_profiles_findpair(const char* profile, const char* key, const char** valuep)
{
    int stat = NC_NOERR;
    size_t i;
    struct AWSprofile* awsprof = NULL;
    const char* value = NULL;

    if(profile == NULL) {stat = NC_ES3; goto done;}
    if((stat = NC_profiles_lookup(profile,&awsprof))) goto done;
    if(awsprof == NULL) {stat = NC_ES3; goto done;}
    for(i=0;i<nclistlength(awsprof->pairs);i+=2) {
	const char* pairkey = nclistget(awsprof->pairs,i);
	if(strcasecmp(pairkey,key)==0) {
	    value = nclistget(awsprof->pairs,i+1);
	    break;
	}
    }
    if(valuep) *valuep = value;
done:
    return stat;
}


/**************************************************/
/* .aws config files parser */

/**
The .aws/config and .aws/credentials files
are in INI format (https://en.wikipedia.org/wiki/INI_file).
This format is not well defined, so the grammar used
here is restrictive. Here, the term "profile" is the same
as the INI term "section".

The grammar used is as follows:

Grammar:

inifile: profilelist ;
profilelist: profile | profilelist profile ;
profile: '[' profilename ']' EOL entries ;
entries: empty | entries entry ;
entry:	WORD = WORD EOL ;
profilename: WORD ;
Lexical:
WORD	sequence of printable characters - [ \[\]=]+
EOL	'\n' | ';'

Note:
1. The semicolon at beginning of a line signals a comment.
2. # comments are not allowed
3. Duplicate profiles or keys are ignored.
4. Escape characters are not supported.
*/

#define AWS_EOF (-1)
#define AWS_ERR (0)
#define AWS_WORD (0x10001)
#define AWS_EOL (0x10002)

//#ifdef LEXDEBUG
static const char*
tokenname(int token)
{
    static char ch[32];
    switch(token) {
    case AWS_EOF: return "EOF"; break;
    case AWS_ERR: return "ERR"; break;
    case AWS_WORD: return "WORD"; break;
    case AWS_EOL: return "'\\n'"; break;
    default:
	if(token >= ' ' && token <= '~')
	     snprintf(ch,sizeof(ch),"'%c'",(char)token);
        else snprintf(ch,sizeof(ch),"0x%x",(unsigned)token);
	return ch;
	break;
    }
    return "UNKNOWN";
}
//#endif

static int
awslex(AWSparser* parser)
{
    int token = 0;
    char* start;
    size_t count;

    parser->token = AWS_ERR;
    ncbytesclear(parser->yytext);
    ncbytesnull(parser->yytext);

    if(parser->pushback != AWS_ERR) {
	token = parser->pushback;
	parser->pushback = AWS_ERR;
	goto done;
    }

    while(token == 0) { /* avoid need to goto when retrying */
	char c = *parser->pos;
	if(c == '\0') {
	    token = AWS_EOF;
	} else if(c == '\n') {
	    parser->pos++;
	    token = AWS_EOL;
	} else if(c <= ' ' || c == '\177') {
	    parser->pos++;
	    continue; /* ignore whitespace */
	} else if(c == ';') {
	    char* p = parser->pos - 1;
	    if(*p == '\n') {
		/* Skip comment */
		do {p++;} while(*p != '\n' && *p != '\0');
		parser->pos = p;
		token = (*p == '\n'?AWS_EOL:AWS_EOF);
	    } else {
		token = ';';
		ncbytesappend(parser->yytext,';');
		parser->pos++;
	    }
	} else if(c == '[' || c == ']' || c == '=') {
	    ncbytesappend(parser->yytext,c);
	    token = c;
	    parser->pos++;
	} else { /*Assume a word*/
	    start = parser->pos;
	    for(;;) {
		c = *parser->pos++;
		if(c <= ' ' || c == '\177' || c == '[' || c == ']' || c == '=') break; /* end of word */
	    }
	    /* Pushback last char */
	    parser->pos--;
	    count = (size_t)(parser->pos - start);
	    ncbytesappendn(parser->yytext,start,count);
	    token = AWS_WORD;
	}
#ifdef LEXDEBUG
fprintf(stderr,">>> lex: token=%s: text=|%s|\n",tokenname(token),ncbytescontents(parser->yytext));
#endif
    } /*for(;;)*/

done:
    parser->token = token;
    return token;
}

/*
@param text of the aws credentials file
@param profiles list of form struct AWSprofile (see ncauth.h)
*/

#define LBR '['
#define RBR ']'

static int
awsparse(const char* text, NClist* profiles)
{
    int stat = NC_NOERR;
    size_t len,i;
    AWSparser* parser = NULL;
    struct AWSprofile* profile = NULL;
    int token;
    char* key = NULL;
    char* value = NULL;
    int profileexists = 0;

    if(text == NULL) text = "";

    parser = calloc(1,sizeof(AWSparser));
    if(parser == NULL)
	{stat = (NC_ENOMEM); goto done;}
    len = strlen(text);
    parser->text = (char*)malloc(len+1+1+1); /* double nul term plus leading EOL */
    if(parser->text == NULL)
	{stat = (NCTHROW(NC_EINVAL)); goto done;}
    parser->pos = parser->text;
    parser->pos[0] = '\n'; /* So we can test for comment unconditionally */
    parser->pos++;
    strcpy(parser->text+1,text);
    parser->pos += len;
    /* Double nul terminate */
    parser->pos[0] = '\0';
    parser->pos[1] = '\0';
    parser->pos = &parser->text[0]; /* reset */
    parser->yytext = ncbytesnew();
    parser->pushback = AWS_ERR;

    /* Do not need recursion, use simple loops */
    for(;;) {
	char* profilename = NULL;
	token = awslex(parser); /* make token always be defined */
	if(token ==  AWS_EOF) break; /* finished */
	if(token ==  AWS_EOL) {continue;} /* blank line */
	if(token != LBR) {stat = NCTHROW(NC_EINVAL); goto done;}
	/* parse [profile name] or [name] */
	token = awslex(parser);
	if(token != AWS_WORD) {stat = NCTHROW(NC_EINVAL); goto done;}
	profilename = ncbytesextract(parser->yytext);
	if(strncmp("profile", profilename, sizeof("profile")) == 0 ) {
	    nullfree(profilename); profilename = NULL;
	    /* next word is real profile name */
	    token = awslex(parser);
	    if(token != AWS_WORD) {stat = NCTHROW(NC_EINVAL); goto done;}
	    profilename = ncbytesextract(parser->yytext);
	}
	/* See if this profile already exists */
	for(profileexists=0,i=0;i<nclistlength(profiles);i++) {
	    struct AWSprofile* oldp = (struct AWSprofile*)nclistget(profiles,i);
	    if(strcasecmp(oldp->profilename,profilename)==0) {profileexists = 1; profile = oldp; break;}
	}
	if(!profileexists) {
	    if((profile = (struct AWSprofile*)calloc(1,sizeof(struct AWSprofile)))==NULL)
	        {stat = NC_ENOMEM; goto done;}
	    profile->profilename = profilename; profilename = NULL;
	    profile->pairs = nclistnew();
	} else {
	    nullfree(profilename);
	    profilename = NULL;
	}
	token = awslex(parser);
	if(token != RBR) {stat = NCTHROW(NC_EINVAL); goto done;}
#ifdef PARSEDEBUG
fprintf(stderr,">>> parse: profile=%s\n",profile->profilename);
#endif
	/* The fields can be in any order */
	for(;;) {
	    size_t j,k;
	    nullfree(key); key = NULL;
	    nullfree(value); value = NULL;
	    token = awslex(parser);
	    if(token == AWS_EOL) {
		continue; /* ignore empty lines */
	    } else if(token == AWS_EOF) {
		break;
	    } else if(token == LBR) {/* start of next profile */
		parser->pushback = token;
		break;
	    } else if(token ==	AWS_WORD) {
		int match = 0;
		key = ncbytesextract(parser->yytext);
		token = awslex(parser);
		if(token != '=') {stat = NCTHROW(NC_EINVAL); goto done;}
		token = awslex(parser);
		if(token != AWS_EOL && token != AWS_WORD) {stat = NCTHROW(NC_EINVAL); goto done;}
		value = ncbytesextract(parser->yytext);
#ifdef PARSEDEBUG
fprintf(stderr,">>> parse: pair=(%s,%s)\n",key,value);
#endif
		/* merge pair into profile */
		for(match=0,j=0;j<nclistlength(profile->pairs);j+=2){ /* walk existing pairs */
		    const char* oldkey = nclistget(profile->pairs,j);
    		    const char* oldvalue = nclistget(profile->pairs,j+1);
		    if(strcasecmp(key,oldkey)==0) { /* pair exists */
			/* Overwrite */
			nclistset(profile->pairs,j,key); key = NULL;
			nclistset(profile->pairs,j+1,value); value = NULL;
			nullfree(oldkey); nullfree(oldvalue);
			match = 1;
			break;
		    }
		}
		if(!match) { /* add new pair to profile */
		    nclistpush(profile->pairs,key);
		    nclistpush(profile->pairs,value);
		    key = NULL; value = NULL;
		}
		if(token == AWS_WORD) token = awslex(parser); /* finish the line */
	    } else
		{stat = NCTHROW(NC_EINVAL); goto done;}
	}
	if(!profileexists) {nclistpush(profiles,profile);}
	profile = NULL;
	profileexists = 0;
    }

done:
    if(profile) freeprofile(profile);
    nullfree(key);
    nullfree(value);
    if(parser != NULL) {
	nullfree(parser->text);
	ncbytesfree(parser->yytext);
	free(parser);
    }
    return (stat);
}

/* Provide profile-related  dumper(s) */
const char*
awsdumpprofile(struct AWSprofile* p)
{
    static char sp[8192];
    size_t j;
    strcpy(sp,"profile{");
    if(p == NULL) {
	strcat(sp,"NULL");
	goto done;
    }
    strcat(sp,"name=|");
    strcat(sp,p->profilename);
    strcat(sp,"| pairs=<");
    if(p->pairs == NULL) {
	fprintf(stderr,"NULL>");
	goto done;
    }
    for(j=0;j<nclistlength(p->pairs);j+=2) {
	const char* key = nclistget(p->pairs,j);
	const char* value = nclistget(p->pairs,j+1);
	if(j > 0) strcat(sp," ");
	strcat(sp,key); strcat(sp,"="); strcat(sp,value);
    }
    strcat(sp,">");
done:
    strcat(sp,"}");
    return sp;
}

const char*
awsdumpprofiles(NClist* profiles)
{
    static char ch[1<<14];
    size_t i;
    ch[0] = '\0';
    for(i=0;i<nclistlength(profiles);i++) {
	struct AWSprofile* p = (struct AWSprofile*)nclistget(profiles,i);
	strcat(ch,"	");
	strcat(ch,awsdumpprofile(p));
	strcat(ch,"\n");
    }
    return ch;
}
