/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/

#include "config.h"
#include <stddef.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_STDARG_H
#include <stdarg.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "netcdf.h"
#include "ncbytes.h"
#include "ncuri.h"
#include "ncrc.h"
#include "nclog.h"
#include "ncauth.h"
#include "ncpathmgr.h"
#include "nc4internal.h"
#include "ncs3sdk.h"
#include "ncdispatch.h"
#include "ncutil.h"
#include "ncglob.h"

#undef NOREAD

#undef DRCDEBUG
#undef LEXDEBUG
#undef PARSEDEBUG

#define RTAG ']'
#define LTAG '['

#undef MEMCHECK
#define MEMCHECK(x) if((x)==NULL) {goto nomem;} else {}

/* Forward */
static int NC_rcload(void);
static char* rcreadline(char** nextlinep);
static void rctrim(char* text);
static void rcorder(NClist* rc);
static int rccompile(const char* path);
static int rcequal(NCRCentry* rcentry, NCRCentry* candidate);
static int rclocatepos(NCRCentry* candidate);
static struct NCRCentry* rclocate(NCRCentry* candidate);
static int rcsearch(const char* prefix, const char* rcname, char** pathp);
static void rcfreeentries(NClist* rc);
static void rcfreeentry(NCRCentry* t);
static void rccpyentry(NCRCentry* dst, NCRCentry* src);
static void rctrimentry(NCRCentry* entry);
static void rcnullifyentry(NCRCentry* entry);
#ifdef DRCDEBUG
static void storedump(char* msg, NClist* entrys);
#endif

/* Define default rc files and aliases, also defines load order*/
static const char* rcfilenames[] = {".ncrc", ".daprc", ".dodsrc", NULL};

static int NCRCinitialized = 0;

static
NCRCentry NCRCentry_empty(void)
{
    static NCRCentry empty = {NULL,NULL,NULL};
    return empty;
}

/**************************************************/
/* User API */

/**
The most common case is to get the most general value for a key,
where most general means that the urlpath and hostport are null
So this function returns the value associated with the key
where the .rc entry has the simple form "key=value".
If that entry is not found, then return NULL.

@param key table entry key field
@param  table entry key field
@return value matching the key -- caller frees
@return NULL if no entry of the form key=value exists
*/
char*
nc_rc_get(const char* key)
{
    NCglobalstate* ncg = NULL;
    char* value = NULL;
    NCRCentry entry = NCRCentry_empty();

    if(!NC_initialized) nc_initialize();

    ncg = NC_getglobalstate();
    assert(ncg != NULL && ncg->rcinfo != NULL && ncg->rcinfo->entries != NULL);
    if(ncg->rcinfo->ignore) goto done;
    entry.key = (char*)key;
    value = NC_rclookupentry(&entry);
done:
    value = nulldup(value);   
    return value;
}

/**
Set simple key=value in .rc table.
Will overwrite any existing value.

@param key
@param value 
@return NC_NOERR if success
@return NC_EINVAL if fail
*/
int
nc_rc_set(const char* key, const char* value)
{
    int stat = NC_NOERR;
    NCglobalstate* ncg = NULL;
    NCRCentry entry = NCRCentry_empty();
    
    if(!NC_initialized) nc_initialize();

    ncg = NC_getglobalstate();
    assert(ncg != NULL && ncg->rcinfo != NULL && ncg->rcinfo->entries != NULL);
    if(ncg->rcinfo->ignore) goto done;;
    entry.key = (char*)key;
    entry.value = (char*)value;
    stat = NC_rcfile_insert(&entry);
done:
    return stat;
}

/**************************************************/
/* External Entry Points */

/*
Initialize defaults and load:
* .ncrc
* .dodsrc
* ${HOME}/.aws/config
* ${HOME}/.aws/credentials

For debugging support, it is possible
to change where the code looks for the .aws directory.
This is set by the environment variable NC_TEST_AWS_DIR.

*/

void
ncrc_initialize(void)
{
    if(NCRCinitialized) return;
    NCRCinitialized = 1; /* prevent recursion */

#ifndef NOREAD
    {
    int stat = NC_NOERR;
    NCglobalstate* ncg = NC_getglobalstate();
    /* Load entrys */
    if((stat = NC_rcload())) {
        nclog(NCLOGWARN,".rc loading failed");
    }
    /* Load .aws/config &/ credentials */
    if((stat = NC_aws_load_credentials(ncg))) {
        nclog(NCLOGWARN,"AWS config file not loaded");
    }
    }
#endif
}

static void
ncrc_setrchome(void)
{
    const char* tmp = NULL;
    NCglobalstate* ncg = NC_getglobalstate();
    assert(ncg && ncg->home);
    if(ncg->rcinfo->rchome) return;
    tmp = getenv(NCRCENVHOME);
    if(tmp == NULL || strlen(tmp) == 0)
	tmp = ncg->home;
    ncg->rcinfo->rchome = strdup(tmp);
#ifdef DRCDEBUG
    fprintf(stderr,"ncrc_setrchome: %s\n",ncg->rcinfo->rchome);
#endif
}

void
NC_rcclear(NCRCinfo* info)
{
    if(info == NULL) return;
    nullfree(info->rcfile);
    nullfree(info->rchome);
    rcfreeentries(info->entries);
    NC_s3freeprofilelist(info->s3profiles);
}

void
NC_rcclearentry(NCRCentry* t)
{
    ncurifree(t->uri);
    nullfree(t->key);
    nullfree(t->value);
    memset(t,0,sizeof(NCRCentry));
}

static void
rcfreeentry(NCRCentry* t)
{
    NC_rcclearentry(t);
    free(t);
}

static void
rcfreeentries(NClist* rc)
{
    size_t i;
    for(i=0;i<nclistlength(rc);i++) {
	NCRCentry* t = (NCRCentry*)nclistget(rc,i);
	rcfreeentry(t);
    }
    nclistfree(rc);
}

/* locate, read and compile the rc files, if any */
static int
NC_rcload(void)
{
    size_t i;
    int ret = NC_NOERR;
    char* path = NULL;
    NCglobalstate* globalstate = NULL;
    NClist* rcfileorder = nclistnew();

    if(!NCRCinitialized) ncrc_initialize();
    globalstate = NC_getglobalstate();

    if(globalstate->rcinfo->ignore) {
        nclog(NCLOGNOTE,".rc file loading suppressed");
	goto done;
    }
    if(globalstate->rcinfo->loaded) goto done;

    /* locate the configuration files in order of use:
       1. Specified by NCRCENV_RC environment variable.
       2. If NCRCENV_RC is not set then merge the set of rc files in this order:
	  1. $HOME/.ncrc
	  2. $HOME/.daprc
  	  3. $HOME/.dodsrc
	  3. $CWD/.ncrc
	  4. $CWD/.daprc
  	  5. $CWD/.dodsrc
	  Entries in later files override any of the earlier files
    */
    if(globalstate->rcinfo->rcfile != NULL) { /* always use this */
	nclistpush(rcfileorder,strdup(globalstate->rcinfo->rcfile));
    } else {
	const char** rcname;
	const char* dirnames[3];
	const char** dir;

        /* Make sure rcinfo.rchome is defined */
	ncrc_setrchome();
	dirnames[0] = globalstate->rcinfo->rchome;
	dirnames[1] = globalstate->cwd;
	dirnames[2] = NULL;

        for(dir=dirnames;*dir;dir++) {
	    for(rcname=rcfilenames;*rcname;rcname++) {
	        ret = rcsearch(*dir,*rcname,&path);
		if(ret == NC_NOERR && path != NULL)
		    nclistpush(rcfileorder,path);
		path = NULL;
	    }
	}
    }
#ifdef DRCDEBUG
    for(i=0;i<nclistlength(rcfileorder);i++) {
	path = (char*)nclistget(rcfileorder,i);
	fprintf(stderr,">>> path[%d]: %s\n",(int)i,path);
    }
#endif
    for(i=0;i<nclistlength(rcfileorder);i++) {
	path = (char*)nclistget(rcfileorder,i);
	if((ret=rccompile(path))) {
	    nclog(NCLOGWARN, "Error parsing %s\n",path);
	    ret = NC_NOERR; /* ignore it */
	    goto done;
	}
    }

done:
    globalstate->rcinfo->loaded = 1; /* even if not exists */
    nclistfreeall(rcfileorder);
    return (ret);
}

/**
 * Locate a entry by a full candidate entry
 * If duplicate keys, first takes precedence.
 * @param candidate to lookup
 * @return the value of the key or NULL if not found.
 */
char*
NC_rclookupentry(NCRCentry* candidate)
{
    struct NCRCentry* entry = NULL;
    if(!NCRCinitialized) ncrc_initialize();
    entry = rclocate(candidate);
    return (entry == NULL ? NULL : entry->value);
}

/**
 * Locate a entry by key+host+port+path
 * If duplicate keys, first takes precedence.
 * @return value of matching entry or NULL
 */
char*
NC_rclookup(const char* key, const char* host, const char* port, const char* path)
{
    char* value = NULL;
    NCURI uri;

    memset(&uri,0,sizeof(NCURI));
    
    uri.protocol = "https";
    uri.host = (char*)host;
    uri.port = (char*)port;
    uri.path = (char*)path;
    value = NC_rclookup_with_ncuri(key,&uri);
    return value;
}

/**
 * Locate a entry by URI string
 * If duplicate keys, first takes precedence.
 * @return value of matching entry or NULL
 */
char*
NC_rclookup_with_uri(const char* key, const char* uri)
{
    char* value = NULL;
    NCURI* ncuri = NULL;

    if(ncuriparse(uri,&ncuri) || ncuri == NULL) return NULL;
    value = NC_rclookup_with_ncuri(key,ncuri);
    ncurifree(ncuri);
    return value;
}

/**
 * Locate a entry by parsed URI
 * If duplicate keys, first takes precedence.
 * @return value of matching entry or NULL
 */
char*
NC_rclookup_with_ncuri(const char* key, NCURI* ncuri)
{
    char* value = NULL;
    NCRCentry candidate = NCRCentry_empty();

    candidate.key = (char*)key;
    candidate.uri = ncuri;
    value = NC_rclookupentry(&candidate);
    return value;
}

#if 0
/*!
Set the absolute path to use for the rc file.
WARNING: this MUST be called before any other
call in order for this to take effect.

\param[in] rcfile The path to use. If NULL then do not use any rcfile.

\retval OC_NOERR if the request succeeded.
\retval OC_ERCFILE if the file failed to load
*/

int
NC_set_rcfile(const char* rcfile)
{
    int stat = NC_NOERR;
    FILE* f = NULL;
    NCglobalstate* globalstate = NC_getglobalstate();

    if(rcfile != NULL && strlen(rcfile) == 0)
	rcfile = NULL;
    f = NCfopen(rcfile,"r");
    if(f == NULL) {
	stat = NC_ERCFILE;
        goto done;
    }
    fclose(f);
    NC_rcclear(globalstate->rcinfo);
    globalstate->rcinfo->rcfile = strdup(rcfile);
    /* Clear globalstate->rcinfo */
    NC_rcclear(&globalstate->rcinfo);
    /* (re) load the rcfile and esp the entriestore*/
    stat = NC_rcload();
done:
    return stat;
}
#endif

/**************************************************/
/* RC processing functions */

static char*
rcreadline(char** nextlinep)
{
    char* line;
    char* p;

    line = (p = *nextlinep);
    if(*p == '\0') return NULL; /*signal done*/
    for(;*p;p++) {
	if(*p == '\r' && p[1] == '\n') *p = '\0';
	else if(*p == '\n') break;
    }
    *p++ = '\0'; /* null terminate line; overwrite newline */
    *nextlinep = p;
    return line;
}

/* Trim TRIMCHARS from both ends of text; */
static void
rctrim(char* text)
{
    char* p;
    char* q;
    size_t len = 0;

    if(text == NULL || *text == '\0') return;

    len = strlen(text);

    /* elide upto first non-trimchar */
    for(q=text,p=text;*p;p++) {
	if(*p != ' ' && *p != '\t' && *p != '\r') {*q++ = *p;}
    }
    len = strlen(p);
    /* locate last non-trimchar */
    if(len > 0) {
        for(size_t i = len; i-->0;) {
	    p = &text[i];
	    if(*p != ' ' && *p != '\t' && *p != '\r') {break;}
	    *p = '\0'; /* elide trailing trimchars */
        }
    }
}

/* Order the entries: those with urls must be first,
   but otherwise relative order does not matter.
*/
static void
rcorder(NClist* rc)
{
    size_t i;
    size_t len = nclistlength(rc);
    NClist* tmprc = NULL;
    if(rc == NULL || len == 0) return;
    tmprc = nclistnew();
    /* Two passes: 1) pull entries with glob url */
    for(i=0;i<len;i++) {
        NCRCentry* ti = nclistget(rc,i);
	if(ti->uri == NULL) continue;
	nclistpush(tmprc,ti);
    }
    /* pass 2 pull entries without URI*/
    for(i=0;i<len;i++) {
        NCRCentry* ti = nclistget(rc,i);
	if(ti->uri != NULL) continue;
	nclistpush(tmprc,ti);
    }
    /* Move tmp to rc */
    nclistsetlength(rc,0);
    for(i=0;i<len;i++) {
        NCRCentry* ti = nclistget(tmprc,i);
	nclistpush(rc,ti);
    }
#ifdef DRCDEBUG
    storedump("reorder:",rc);
#endif
    nclistfree(tmprc);
}

/* Merge a entry store from a file*/
static int
rccompile(const char* filepath)
{
    int ret = NC_NOERR;
    NClist* rc = NULL;
    char* contents = NULL;
    NCbytes* tmp = ncbytesnew();
    NCURI* uri = NULL;
    char* nextline = NULL;
    NCglobalstate* globalstate = NC_getglobalstate();
    NCS3INFO s3;
    NCRCentry candidate = NCRCentry_empty();

    memset(&s3,0,sizeof(s3));

    if((ret=NC_readfile(filepath,tmp))) {
        nclog(NCLOGWARN, "Could not open configuration file: %s",filepath);
	goto done;
    }
    contents = ncbytesextract(tmp);
    if(contents == NULL) contents = strdup("");
    /* Either reuse or create new  */
    rc = globalstate->rcinfo->entries;
    if(rc == NULL) {
        rc = nclistnew();
        globalstate->rcinfo->entries = rc;
    }
    nextline = contents;
    for(;;) {
	char* line;
	size_t llen;
        NCRCentry* entry;
	char* value = NULL;
	char* canduri = NULL;

	line = rcreadline(&nextline);
	if(line == NULL) break; /* done */
        rctrim(line);  /* trim leading and trailing blanks */
        if(line[0] == '#') continue; /* comment */
	if((llen=strlen(line)) == 0) continue; /* empty line */
	if(line[0] == LTAG) {
	    canduri = ++line;
            char* rtag = strchr(line,RTAG);
            if(rtag == NULL) {
                nclog(NCLOGERR, "Malformed glob [url] in %s entry: %s",filepath,line);
		continue;
            }
	    line = rtag + 1;
            *rtag = '\0';
            /* compile the glob url */
 	    if(uri) {ncurifree(uri); uri = NULL;}
	    if(canduri != NULL && strlen(canduri) > 0) {
		if(ncuriparseglob(canduri,&uri) || uri == NULL) {
		    nclog(NCLOGERR, "Malformed [url] in %s entry: %s",filepath,line);
		    continue;
		}
		NC_rcfillfromuri(&candidate,uri);
	    }
	    ncurifree(uri); uri = NULL;
	}
        /* split off key and value */
        candidate.key=line;
        value = strchr(line, '=');
        if(value == NULL)
            value = line + strlen(line);
        else {
            *(value) = '\0';
            value++;
        }
	candidate.key = nulldup(candidate.key);
        if(candidate.key && strlen(candidate.key)==0) candidate.key = NULL;
        rctrim(candidate.key);
        if(value && strlen(value)==0) value = NULL;
	value = nulldup(value);
        rctrim(value);
	/* See if key already exists */
	entry = rclocate(&candidate);
	if(entry == NULL) {
	    entry = (NCRCentry*)calloc(1,sizeof(NCRCentry));
	    if(entry == NULL) {ret = NC_ENOMEM; goto done;}
	    nclistpush(rc,entry);
	    rccpyentry(entry,&candidate);
    	    entry->value = value; value = NULL;
#ifdef DRCDEBUG
	    fprintf(stderr,">>> new entry: %s=%s (%s)\n",
		(candidate.key != NULL ? candidate.key : "<null>"),
		(candidate.value != NULL ? candidate.value : "<null>"),
		filepath);
#endif
	} else {/* Overwrite value */
#ifdef DRCDEBUG
	    fprintf(stderr,">>> overwrite: %s=%s -> %s=%s (%s)\n",
		(entry->key != NULL ? entry->key : "<null>"),
		(entry->value != NULL ? entry->value : "<null>"),
		(candidate.key != NULL ? candidate.key : "<null>"),
		(candidate.value != NULL ? candidate.value : "<null>"),
		filepath);
	    fprintf(stderr,"rc: uri=%s key=%s value=%s\n",
		(entry->uri != NULL ? entry->uri : "<null>"),
		(entry->key != NULL ? entry->key : "<null>"),
		(entry->value != NULL ? entry->value : "<null>"));
#endif
	    rccpyentry(entry,&candidate); /* Overwrite everything */
    	    entry->value = value; value = NULL;
	}
	entry = NULL;
	NC_rcclearentry(&candidate);
    }
#ifdef DRCDEBUG
    fprintf(stderr,"reorder.path=%s\n",filepath);
#endif
    rcorder(rc);

done:
    NC_rcclearentry(&candidate);
    NC_s3clear(&s3);
    if(contents) free(contents);
    ncurifree(uri);
    ncbytesfree(tmp);
    return (ret);
}

/**
Encapsulate equality comparison: return 1|0
*/
static int
rcequal(NCRCentry* rcentry, NCRCentry* candidate)
{
    if(rcentry->key == NULL || candidate->key == NULL) return 0;
    if(strcmp(rcentry->key,candidate->key) != 0) return 0;
    
    /* Match URI's */
    if(rcentry->uri == NULL) return 1; /* no entry glob URI => test only key (above) */

    /* If entry has a URL and the candidate does not, then fail */
    if(candidate->uri == NULL) return 0;

    /* Match the two URI's */
    if(!glob_match_uri(candidate->uri,rcentry->uri,NULL)) return 0;
    
    return 1;
}

/**
 * (Internal) Locate a entry by property key and URI (may be null)
 * If duplicate keys, first takes precedence.
 */
static int
rclocatepos(NCRCentry* candidate)
{
    size_t i;
    NCglobalstate* globalstate = NC_getglobalstate();
    struct NCRCinfo* info = globalstate->rcinfo;
    NCRCentry* rcentry = NULL;
    NClist* rc = info->entries;

    if(info->ignore) return -1;

    for(i=0;i<nclistlength(rc);i++) {
      rcentry = (NCRCentry*)nclistget(rc,i);
      if(rcequal(rcentry,candidate)) return (int)i;
    }
    return -1;
}

/**
 * (Internal) Locate a entry by property key and URI (may be null).
 * If duplicate keys, first takes precedence.
 */
static struct NCRCentry*
rclocate(NCRCentry* candidate)
{
    int pos;
    NCglobalstate* globalstate = NC_getglobalstate();
    struct NCRCinfo* info = globalstate->rcinfo;

    if(globalstate->rcinfo->ignore) return NULL;
    if(candidate->key == NULL || info == NULL) return NULL;
    pos = rclocatepos(candidate);
    if(pos < 0) return NULL;
    return NC_rcfile_ith(info,(size_t)pos);
}

/**
 * Locate rc file by searching in directory prefix.
 */
static
int
rcsearch(const char* prefix, const char* rcname, char** pathp)
{
    char* path = NULL;
    FILE* f = NULL;
    size_t plen = (prefix?strlen(prefix):0);
    size_t rclen = strlen(rcname);
    int ret = NC_NOERR;

    size_t pathlen = plen+rclen+1+1; /*+1 for '/' +1 for nul */
    path = (char*)malloc(pathlen); /* +1 for nul*/
    if(path == NULL) {ret = NC_ENOMEM;	goto done;}
    snprintf(path, pathlen, "%s/%s", prefix, rcname);
    /* see if file is readable */
    f = NCfopen(path,"r");
    if(f != NULL)
        nclog(NCLOGNOTE, "Found rc file=%s",path);
done:
    if(f == NULL || ret != NC_NOERR) {
	nullfree(path);
	path = NULL;
    }
    if(f != NULL)
      fclose(f);
    if(pathp != NULL)
      *pathp = path;
    else {
      nullfree(path);
      path = NULL;
    }
    errno = 0; /* silently ignore errors */
    return (ret);
}

static void
rccpyentry(NCRCentry* dst, NCRCentry* src)
{
    if(dst == NULL || src == NULL) return;
    NC_rcclearentry(dst);
    dst->key = nulldup(src->key);
    dst->value = nulldup(src->value);
    if(src->uri)
	dst->uri = ncuriclone(src->uri);
    rcnullifyentry(dst);
    rctrimentry(dst);
}

static void
rcnullifyentry(NCRCentry* entry)
{
    if(entry->key && strlen(entry->key)==0) entry->key = NULL;
    if(entry->value && strlen(entry->value)==0) entry->value = NULL;
}

static void
rctrimentry(NCRCentry* entry)
{
    if(entry == NULL) return;
    rctrim(entry->key);
}

int
NC_rcfile_insert(NCRCentry* candidate)
{
    int ret = NC_NOERR;
    /* See if this key already defined */
    struct NCRCentry* entry = NULL;
    NCglobalstate* globalstate = NULL;
    NClist* rc = NULL;

    if(!NCRCinitialized) ncrc_initialize();

    if(candidate->key == NULL || candidate->value == NULL)
        {ret = NC_EINVAL; goto done;}

    globalstate = NC_getglobalstate();
    rc = globalstate->rcinfo->entries;

    if(rc == NULL) {
	rc = nclistnew();
        globalstate->rcinfo->entries = rc;
	if(rc == NULL) {ret = NC_ENOMEM; goto done;}
    }
    entry = rclocate(candidate);
    if(entry == NULL) {
	entry = (NCRCentry*)calloc(1,sizeof(NCRCentry));
	if(entry == NULL) {ret = NC_ENOMEM; goto done;}
	rccpyentry(entry,candidate);
    }
#ifdef DRCDEBUG
    storedump("NC_rcfile_insert",rc);
#endif    
done:
    return ret;
}

/* Obtain the count of number of entries */
size_t
NC_rcfile_length(NCRCinfo* info)
{
    return nclistlength(info->entries);
}

/* Obtain the ith entry; return NULL if out of range */
NCRCentry*
NC_rcfile_ith(NCRCinfo* info, size_t i)
{
    if(i >= nclistlength(info->entries))
	return NULL;
    return (NCRCentry*)nclistget(info->entries,i);
}

void
NC_rcfillfromuri(NCRCentry* dst, NCURI* src)
{
    if(dst == NULL || src == NULL) return;
    NC_rcclearentry(dst);
    memset(dst,0,sizeof(NCRCentry));
    dst->uri = ncuriclone(src);
    rcnullifyentry(dst);
    rctrimentry(dst);
}

#ifdef DRCDEBUG
static void
storedump(char* msg, NClist* entries)
{
    int i;

    if(msg != NULL) fprintf(stderr,"%s\n",msg);
    if(entries == NULL || nclistlength(entries)==0) {
        fprintf(stderr,"<EMPTY>\n");
        return;
    }
    for(i=0;i<nclistlength(entries);i++) {
	NCRCentry* t = (NCRCentry*)nclistget(entries,i);
	char* build = NULL;

	if(t->uri) {	
	    build = ncuribuild(t->uri,NCURIBASE);
            fprintf(stderr,"\t[%s] ",build);
	    nullfree(build); build = NULL;
	}				
        fprintf(stderr,"%s=%s\n",t->key,(t->value==NULL?"<null>":t->value));
    }
    fflush(stderr);
}
#endif
