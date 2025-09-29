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

/* Alternate .aws directory path */
#define NC_TEST_AWS_DIR "NC_TEST_AWS_DIR"

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

static void NC_awsconfigmerge(NCawsconfig* baseaws, NCawsconfig* newaws);
static void NC_awsprofilemerge(NCawsconfig* aws, const char* profilename);
static void clearprofile(struct AWSprofile* p);
static void freeprofile(struct AWSprofile* p);
static void freeentry(struct AWSentry* e);
static const char* tokenname(int token);
static int awslex(AWSparser* parser);
static int awsparse(const char* text, NClist* profiles);

/**************************************************/
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

#if 0
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
#endif

NCawsconfig
NC_awsconfig_empty(void)
{
    NCawsconfig aws;
    memset(&aws,0,sizeof(NCawsconfig));
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

    /* Load from specified profile, if defined */
    if(gs->aws->profile != NULL)
	NC_awsprofilemerge(gs->aws,gs->aws->profile);

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

/**************************************************/
/* Lookup functors */

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
    aws->config_file = nulldup(ncurifragmentlookup(uri,AWS_FRAG_CONFIG_FILE));
    aws->region = nulldup(ncurifragmentlookup(uri,AWS_FRAG_REGION));
    aws->default_region = nulldup(ncurifragmentlookup(uri,AWS_FRAG_DEFAULT_REGION));
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

/**************************************************/
/* Manage .aws profiles */

void
NC_profiles_free(NClist* profiles)
{
    size_t i,j;
    if(profiles) {
	for(i=0;i<nclistlength(profiles);i++) {
	    struct AWSprofile* p = (struct AWSprofile*)nclistget(profiles,i);
	}
	nclistfree(profiles);
    }
}

static void
clearprofile(struct AWSprofile* p)
{
    size_t i;
    if(p) {
	nullfree(p->profilename);
	for(i=0;i<nclistlength(p->pairs);i++) {
	    struct AWSentry* e = (struct AWSentry*)nclistget(p->pairs,i);
	    freeentry(e);
	}
	nclistfree(p->pairs);
	memset(p,0,sizeof(struct AWSprofile));
    }
}

static void
freeprofile(struct AWSprofile* p)
{
    clearprofile(p);
    nullfree(p);
}

static void
freeentry(struct AWSentry* e)
{
    nullfree(e->key);
    nullfree(e->value);
    nullfree(e);
}

/* Find, load, and parse the aws config &/or credentials file */
int
NC_aws_profiles_load(NCglobalstate* gs)
{
    int stat = NC_NOERR;
    size_t i,j;
    NClist* awscfgfiles = NULL;
    NCbytes* buf = ncbytesnew();
    char cfgdir[8192];
    char cfgpath[8192];
    const char* aws_test_root = getenv(NC_TEST_AWS_DIR);
    const NCawsconfig* aws = gs->aws;
    NClist* awscfg = NULL;
    struct AWSentry* entry = NULL;
    struct AWSprofile* dfalt = NULL;

    if(gs->awsprofiles == NULL) gs->awsprofiles = nclistnew();

    /* add a "no" credentials */
    {
	struct AWSprofile* noprof = (struct AWSprofile*)calloc(1,sizeof(struct AWSprofile));
	noprof->profilename = strdup("no");
	noprof->pairs = nclistnew();
	NC_profiles_insert(noprof); noprof = NULL;
    }

    /* Test dir takes precedence */
    if(aws_test_root != NULL)
	strncpy(cfgdir,aws_test_root,sizeof(cfgdir));
    else {
	snprintf(cfgdir,sizeof(cfgdir),"%s/%s",gs->home,AWSCONFIGDIR);
    }

    /* Collect, in precedence order, the profile paths */
    if(aws->config_file != NULL) /* Highest precedence; used as-is */
	nclistpush(awscfgfiles,strdup(aws->config_file));
    else for(i=0;;i++) {
        const char* file;
	if((file = AWSCONFIGFILES[i])==NULL) break;
	if(snprintf(cfgpath,sizeof(cfgpath),"%s/%s",cfgdir,file) >= sizeof(cfgpath)) /* create an absolute path */
	    {stat = NC_ENOMEM; goto done;}
        nclistpush(awscfgfiles,strdup(cfgpath)); /* save */
    }

   for(i=0;i<nclistlength(awscfgfiles);i++) {
	const char* cfg = nclistget(awscfgfiles,i);
	ncbytesclear(buf);
	if((stat=NC_readfile(cfgpath,buf))) {
	    nclog(NCLOGWARN, "Could not open file: %s",cfgpath);
	} else {
	    /* Parse the file and insert profiles (with override) */
	    const char* text = ncbytescontents(buf);
	    if((stat = awsparse(text,gs->awsprofiles))) goto done;
	}
    }
  
    /* Search for "default" credentials */
    if((stat=NC_profiles_lookup("default",&dfalt))) goto done;

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
	    if((entry = (struct AWSentry*)calloc(1,sizeof(struct AWSentry)))==NULL) {stat = NC_ENOMEM; goto done;}
	    entry->key = strdup(AWS_PROF_ACCESS_KEY_ID);
	    entry->value = strdup(gs->aws->access_key_id);
	    nclistpush(dfalt->pairs,entry); entry = NULL;
	    if((entry = (struct AWSentry*)calloc(1,sizeof(struct AWSentry)))==NULL) {stat = NC_ENOMEM; goto done;}
	    entry->key = strdup(AWS_PROF_SECRET_ACCESS_KEY);
	    entry->value = strdup(gs->aws->secret_access_key);
	    nclistpush(dfalt->pairs,entry); entry = NULL;
	    /* Save the new default profile */
	    nclistpush(gs->awsprofiles,dfalt); dfalt = NULL;
	}
    }

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
    for(i=0;i<nclistlength(gs->awsprofiles);i++) {
	struct AWSprofile* profile = (struct AWSprofile*)nclistget(gs->awsprofiles,i);
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
	nclistpush(gs->awsprofiles,p);
	p = NULL;
    }
done:
    freeprofile(prof);
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
    for(i=0;i<nclistlength(awsprof->pairs);i++) {
	struct AWSentry* entry = (struct AWSentry*)nclistget(awsprof->pairs,i);
	if(strcasecmp(entry->key,key)==0) {
	    value = entry->value;
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

#ifdef LEXDEBUG
static const char*
tokenname(int token)
{
    static char num[32];
    switch(token) {
    case AWS_EOF: return "EOF";
    case AWS_ERR: return "ERR";
    case AWS_WORD: return "WORD";
    default: snprintf(num,sizeof(num),"%d",token); return num;
    }
    return "UNKNOWN";
}
#endif

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
	    ncbytesnull(parser->yytext);
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
	    ncbytesnull(parser->yytext);
	    token = AWS_WORD;
	}
#ifdef LEXDEBUG
fprintf(stderr,"%s(%d): |%s|\n",tokenname(token),token,ncbytescontents(parser->yytext));
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
    size_t len;
    AWSparser* parser = NULL;
    struct AWSprofile* profile = NULL;
    int token;
    char* key = NULL;
    char* value = NULL;

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
	token = awslex(parser); /* make token always be defined */
	if(token ==  AWS_EOF) break; /* finished */
	if(token ==  AWS_EOL) {continue;} /* blank line */
	if(token != LBR) {stat = NCTHROW(NC_EINVAL); goto done;}
	/* parse [profile name] or [name] */
	token = awslex(parser);
	if(token != AWS_WORD) {stat = NCTHROW(NC_EINVAL); goto done;}
	assert(profile == NULL);
	if((profile = (struct AWSprofile*)calloc(1,sizeof(struct AWSprofile)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	profile->profilename = ncbytesextract(parser->yytext);
	if(strncmp("profile", profile->profilename, sizeof("profile")) == 0 ) {
		token =	 awslex(parser);
		if(token != AWS_WORD) {stat = NCTHROW(NC_EINVAL); goto done;}
		nullfree(profile->profilename);
		profile->profilename = ncbytesextract(parser->yytext);
	}
	profile->pairs = nclistnew();
	token = awslex(parser);
	if(token != RBR) {stat = NCTHROW(NC_EINVAL); goto done;}
#ifdef PARSEDEBUG
fprintf(stderr,">>> parse: profile=%s\n",profile->profilename);
#endif
	/* The fields can be in any order */
	for(;;) {
	    struct AWSentry* entry = NULL;
	    token = awslex(parser);
	    if(token == AWS_EOL) {
		continue; /* ignore empty lines */
	    } else if(token == AWS_EOF) {
		break;
	    } else if(token == LBR) {/* start of next profile */
		parser->pushback = token;
		break;
	    } else if(token ==	AWS_WORD) {
		key = ncbytesextract(parser->yytext);
		token = awslex(parser);
		if(token != '=') {stat = NCTHROW(NC_EINVAL); goto done;}
		token = awslex(parser);
		if(token != AWS_EOL && token != AWS_WORD) {stat = NCTHROW(NC_EINVAL); goto done;}
		value = ncbytesextract(parser->yytext);
		if((entry = (struct AWSentry*)calloc(1,sizeof(struct AWSentry)))==NULL)
		    {stat = NC_ENOMEM; goto done;}
		entry->key = key; key = NULL;
		entry->value = value; value = NULL;
#ifdef PARSEDEBUG
fprintf(stderr,">>> parse: entry=(%s,%s)\n",entry->key,entry->value);
#endif
		nclistpush(profile->pairs,entry); entry = NULL;
		if(token == AWS_WORD) token = awslex(parser); /* finish the line */
	    } else
		{stat = NCTHROW(NC_EINVAL); goto done;}
	}

	/* If this profile already exists, then overwrite old one */
	for(size_t i=0;i<nclistlength(profiles);i++) {
	    struct AWSprofile* p = (struct AWSprofile*)nclistget(profiles,i);
	    if(strcasecmp(p->profilename,profile->profilename)==0) {
		// Keep unique parameters from previous (incomplete!?) profile
		for (size_t j=0;j<nclistlength(p->pairs);j++){
			struct AWSentry* old = (struct AWSentry*)nclistget(p->pairs,j);
			int add = 1;
			for (size_t z=0;z<nclistlength(profile->pairs);z++){
				struct AWSentry* new = (struct AWSentry*)nclistget(profile->pairs,z);
				add &= (strcasecmp(old->key,new->key)!=0);
			}
			if(add){
			    nclistpush(profile->pairs, nclistremove(p->pairs,j--));
			}
		}
		nclistset(profiles,i,profile);
		profile = NULL;
		/* reclaim old one */
		freeprofile(p);
		break;
	    }
	}
	if(profile) nclistpush(profiles,profile);
	profile = NULL;
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
void
awsdumpprofile(struct AWSprofile* p)
{
    size_t j;
    if(p == NULL) {
	fprintf(stderr,"    <NULL>");
	goto done;
    }
    fprintf(stderr,"	[%s]",p->profilename);
    if(p->pairs == NULL) {
	fprintf(stderr,"<NULL>");
	goto done;
    }
    for(j=0;j<nclistlength(p->pairs);j++) {
	struct AWSentry* e = (struct AWSentry*)nclistget(p->pairs,j);
	fprintf(stderr," %s=%s",e->key,e->value);
    }
done:
    fprintf(stderr,"\n");
}

void
awsdumpprofiles(NClist* profiles)
{
    size_t i;
    NCglobalstate* gs = NC_getglobalstate();
    for(i=0;i<nclistlength(gs->awsprofiles);i++) {
	struct AWSprofile* p = (struct AWSprofile*)nclistget(profiles,i);
	awsdumpprofile(p);
    }
}
