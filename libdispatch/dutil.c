/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "config.h"
#include <stddef.h>
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
#include "ncuri.h"
#include "ncbytes.h"
#include "nclist.h"
#include "nclog.h"
#include "ncrc.h"
#include "ncpathmgr.h"
#include "ncutil.h"

#define NC_MAX_PATH 4096
#ifndef nulldup
 #define nulldup(x) ((x)?strdup(x):(x))
#endif


/* Forward */
static int lexical_compare(const void* arg1, const void* arg2);

/**************************************************/
/** \internal
 * Provide a hidden interface to allow utilities
 * to check if a given path name is really an ncdap4 url.
 * If no, return null, else return basename of the url
 * minus any extension.
 */

int
NC__testurl(const char* path, char** basenamep)
{
    NCURI* uri;
    int ok = NC_NOERR;
    if(ncuriparse(path,&uri))
	ok = NC_EURL;
    else {
	char* slash = (uri->path == NULL ? NULL : strrchr(uri->path, '/'));
	char* dot;
	if(slash == NULL) slash = (char*)path; else slash++;
        slash = nulldup(slash);
        if(slash == NULL)
            dot = NULL;
        else
            dot = strrchr(slash, '.');
        if(dot != NULL &&  dot != slash) *dot = '\0';
        if(basenamep)
            *basenamep=slash;
        else if(slash)
            free(slash);
    }
    ncurifree(uri);
    return ok;
}

/** \internal Return 1 if this machine is little endian */
int
NC_isLittleEndian(void)
{
    union {
        unsigned char bytes[SIZEOF_INT];
	int i;
    } u;
    u.i = 1;
    return (u.bytes[0] == 1 ? 1 : 0);
}

/** \internal */
char*
NC_backslashEscape(const char* s)
{
    const char* p;
    char* q;
    size_t len;
    char* escaped = NULL;

    len = strlen(s);
    escaped = (char*)malloc(1+(2*len)); /* max is everychar is escaped */
    if(escaped == NULL) return NULL;
    for(p=s,q=escaped;*p;p++) {
        char c = *p;
        switch (c) {
	case '\\':
	case '/':
	case '.':
	case '@':
	    *q++ = '\\'; *q++ = '\\';
	    break;
	default: *q++ = c; break;
        }
    }
    *q = '\0';
    return escaped;
}

/** \internal */
char*
NC_backslashUnescape(const char* esc)
{
    size_t len;
    char* s;
    const char* p;
    char* q;

    if(esc == NULL) return NULL;
    len = strlen(esc);
    s = (char*)malloc(len+1);
    if(s == NULL) return NULL;
    for(p=esc,q=s;*p;) {
	switch (*p) {
	case '\\':
	     p++;
	     /* fall thru */
	default: *q++ = *p++; break;
	}
    }
    *q = '\0';
    return s;
}

/** \internal */
char*
NC_entityescape(const char* s)
{
    const char* p;
    char* q;
    size_t len;
    char* escaped = NULL;
    const char* entity;

    len = strlen(s);
    escaped = (char*)malloc(1+(6*len)); /* 6 = |&apos;| */
    if(escaped == NULL) return NULL;
    for(p=s,q=escaped;*p;p++) {
	char c = *p;
	switch (c) {
	case '&':  entity = "&amp;"; break;
	case '<':  entity = "&lt;"; break;
	case '>':  entity = "&gt;"; break;
	case '"':  entity = "&quot;"; break;
	case '\'': entity = "&apos;"; break;
	default	 : entity = NULL; break;
	}
	if(entity == NULL)
	    *q++ = c;
	else {
	    len = strlen(entity);
	    memcpy(q,entity,len);
	    q+=len;
	}
    }
    *q = '\0';
    return escaped;
}

/** \internal
Depending on the platform, the shell will sometimes
pass an escaped octotherpe character without removing
the backslash. So this function is appropriate to be called
on possible url paths to unescape such cases. See e.g. ncgen.
*/
char*
NC_shellUnescape(const char* esc)
{
    size_t len;
    char* s;
    const char* p;
    char* q;

    if(esc == NULL) return NULL;
    len = strlen(esc);
    s = (char*)malloc(len+1);
    if(s == NULL) return NULL;
    for(p=esc,q=s;*p;) {
	switch (*p) {
	case '\\':
	     if(p[1] == '#')
	         p++;
	     /* fall thru */
	default: *q++ = *p++; break;
	}
    }
    *q = '\0';
    return s;
}

/** \internal
Wrap mktmp and return the generated path
or null if failed.
@param base is the base file path. XXXXX is appended to allow mktmp add its unique id.
@param tmpfile store the generated string in this.
@return NC_NOERR|NC_EXXX
*/
int
NC_mktmp(const char* base, char** tmpfile)
{
    int fd = -1;
    char tmp[8192];
#ifndef HAVE_MKSTEMP
    int tries;
#define MAXTRIES 4
#else
    mode_t mask;
#endif

#ifdef HAVE_MKSTEMP
    snprintf(tmp,sizeof(tmp),"%sXXXXXX",base);
    mask=umask(0077);
    fd = NCmkstemp(tmp);
    (void)umask(mask);
#else /* !HAVE_MKSTEMP */
    /* Need to simulate by using some kind of pseudo-random number */
    for(tries=0;tries<MAXTRIES;tries++) {
	int rno = rand();
	char spid[7];
	if(rno < 0) rno = -rno;
        snprintf(spid,sizeof(spid),"%06d",rno);
	snprintf(tmp,sizeof(tmp),"%s%s",base,spid);
        fd=NCopen3(tmp,O_RDWR|O_CREAT, _S_IREAD|_S_IWRITE);
	if(fd >= 0) break; /* sucess */
	fd = -1; /* try again */
    }
#endif /* !HAVE_MKSTEMP */
    if(fd < 0) {
        nclog(NCLOGERR, "Could not create temp file: %s",tmp);
	return NC_EINVAL;
    } else {
	if(fd >= 0) close(fd);
	if(tmpfile) {*tmpfile = strdup(tmp);}
	return NC_NOERR;
    }
}

/** \internal */
int
NC_readfile(const char* filename, NCbytes* content)
{
    int stat;
    stat = NC_readfilen(filename, content, -1);
    return stat;
}

int
NC_readfilen(const char* filename, NCbytes* content, long long amount)
{
    int ret = NC_NOERR;
    FILE* stream = NULL;

    stream = NCfopen(filename,"r");
    if(stream == NULL) {ret=errno; goto done;}
    ret = NC_readfileF(stream,content,amount);
    if (stream) fclose(stream);
done:
    return ret;
}

int
NC_readfileF(FILE* stream, NCbytes* content, long long amount)
{
#define READ_BLOCK_SIZE 4194304
    int ret = NC_NOERR;
    long long red = 0;
    char *part = (char*) malloc(READ_BLOCK_SIZE);

    while(amount < 0 || red < amount) {
	size_t count = fread(part, 1, READ_BLOCK_SIZE, stream);
	if(ferror(stream)) {ret = NC_EIO; goto done;}
	if(count > 0) ncbytesappendn(content,part,(unsigned long)count);
	red += (long long)count;
    if (feof(stream)) break;
    }
    /* Keep only amount */
    if(amount >= 0) {
	if(red > amount) ncbytessetlength(content, (unsigned long)amount); /* read too much */
	if(red < amount) ret = NC_ETRUNC; /* |file| < amount */
    }
    ncbytesnull(content);
done:
    free(part);
    return ret;
}

/** \internal */
int
NC_writefile(const char* filename, size_t size, void* content)
{
    int ret = NC_NOERR;
    FILE* stream = NULL;
    void* p;
    size_t remain;

    if(content == NULL) {content = ""; size = 0;}

    stream = NCfopen(filename,"w");
    if(stream == NULL) {ret=errno; goto done;}
    p = content;
    remain = size;
    while(remain > 0) {
	size_t written = fwrite(p, 1, remain, stream);
	if(ferror(stream)) {ret = NC_EIO; goto done;}
	remain -= written;
    if (feof(stream)) break;
    }
done:
    if(stream) fclose(stream);
    return ret;
}

/** \internal
Parse a path as a url and extract the modelist.
If the path is not a URL, then return a NULL list.
If a URL, but modelist is empty or does not exist,
then return empty list.
*/
int
NC_getmodelist(const char* modestr, NClist** modelistp)
{
    int stat=NC_NOERR;
    NClist* modelist = NULL;

    modelist = nclistnew();
    if(modestr == NULL || strlen(modestr) == 0) goto done;

    /* Parse the mode string at the commas or EOL */
    if((stat = NC_split_delim(modestr,',',modelist))) goto done;

done:
    if(stat == NC_NOERR) {
	if(modelistp) {*modelistp = modelist; modelist = NULL;}
    } else
        nclistfree(modelist);
    return stat;
}

/** \internal
Check "mode=" list for a path and return 1 if present, 0 otherwise.
*/
int
NC_testpathmode(const char* path, const char* tag)
{
    int found = 0;
    NCURI* uri = NULL;
    ncuriparse(path,&uri);
    if(uri != NULL) {
        found = NC_testmode(uri,tag);
        ncurifree(uri);
    }
    return found;
}

/** \internal
Check "mode=" list for a url and return 1 if present, 0 otherwise.
*/
int
NC_testmode(NCURI* uri, const char* tag)
{
    int stat = NC_NOERR;
    int found = 0;
    size_t i;
    const char* modestr = NULL;
    NClist* modelist = NULL;

    modestr = ncurifragmentlookup(uri,"mode");
    if(modestr == NULL) goto done;
    /* Parse mode str */
    if((stat = NC_getmodelist(modestr,&modelist))) goto done;
    /* Search for tag */
    for(i=0;i<nclistlength(modelist);i++) {
        const char* mode = (const char*)nclistget(modelist,i);
	if(strcasecmp(mode,tag)==0) {found = 1; break;}
    }
done:
    nclistfreeall(modelist);
    return found;
}

/** \internal
Add tag to fragment mode list unless already present.
*/
int
NC_addmodetag(NCURI* uri, const char* tag)
{
    int stat = NC_NOERR;
    int found = 0;
    const char* modestr = NULL;
    char* modevalue = NULL;
    NClist* modelist = NULL;

    modestr = ncurifragmentlookup(uri,"mode");
    if(modestr != NULL) {
        /* Parse mode str */
        if((stat = NC_getmodelist(modestr,&modelist))) goto done;
    } else
        modelist = nclistnew();
    /* Search for tag */
    for(size_t i=0;i<nclistlength(modelist);i++) {
        const char* mode = (const char*)nclistget(modelist,i);
	if(strcasecmp(mode,tag)==0) {found = 1; break;}
    }
    /* If not found, then add to modelist */
    if(!found) nclistpush(modelist,strdup(tag));
    /* Convert modelist back to string */
    if((stat=NC_joinwith(modelist,",",NULL,NULL,&modevalue))) goto done;
    /* modify the url */
    if((stat=ncurisetfragmentkey(uri,"mode",modevalue))) goto done;

done:
    nclistfreeall(modelist);
    nullfree(modevalue);
    return stat;
}

#if ! defined __INTEL_COMPILER
#if defined __APPLE__ 
/** \internal */

#if 0
#if ! defined HAVE_DECL_ISINF

int isinf(double x)
{
    union { unsigned long long u; double f; } ieee754;
    ieee754.f = x;
    return ( (unsigned)(ieee754.u >> 32) & 0x7fffffff ) == 0x7ff00000 &&
           ( (unsigned)ieee754.u == 0 );
}

#endif /* HAVE_DECL_ISINF */

#if ! defined HAVE_DECL_ISNAN
/** \internal */
int isnan(double x)
{
    union { unsigned long long u; double f; } ieee754;
    ieee754.f = x;
    return ( (unsigned)(ieee754.u >> 32) & 0x7fffffff ) +
           ( (unsigned)ieee754.u != 0 ) > 0x7ff00000;
}

#endif /* HAVE_DECL_ISNAN */
#endif
#endif /*APPLE*/
#endif /*!_INTEL_COMPILER*/

/** \internal */
int
NC_split_delim(const char* arg, char delim, NClist* segments)
{
    int stat = NC_NOERR;
    const char* p = NULL;
    const char* q = NULL;
    ptrdiff_t len = 0;
    char* seg = NULL;

    if(arg == NULL || strlen(arg)==0 || segments == NULL)
        goto done;
    p = arg;
    if(p[0] == delim) p++;
    for(;*p;) {
	q = strchr(p,delim);
	if(q==NULL)
	    q = p + strlen(p); /* point to trailing nul */
        len = (q - p);
	if(len == 0)
	    {stat = NC_EURL; goto done;}
	if((seg = malloc((size_t)len+1)) == NULL)
	    {stat = NC_ENOMEM; goto done;}
	memcpy(seg,p,(size_t)len);
	seg[len] = '\0';
	nclistpush(segments,seg);
	seg = NULL; /* avoid mem errors */
	if(*q) p = q+1; else p = q;
    }

done:
    nullfree(seg);
    return stat;
}

/** \internal concat the the segments with each segment preceded by '/' */
int
NC_join(NClist* segments, char** pathp)
{
    return NC_joinwith(segments,"/","/",NULL,pathp);
}

/** \internal
Concat the the segments with separator.
@param segments to join
@param sep to use between segments
@param prefix put at front of joined string: NULL => no prefix
@param suffix put at end of joined string: NULL => no suffix
@param pathp return the join in this
*/
int
NC_joinwith(NClist* segments, const char* sep, const char* prefix, const char* suffix, char** pathp)
{
    int stat = NC_NOERR;
    size_t i;
    NCbytes* buf = NULL;
    size_t seplen = nulllen(sep);

    if(segments == NULL)
	{stat = NC_EINVAL; goto done;}
    if((buf = ncbytesnew())==NULL)
	{stat = NC_ENOMEM; goto done;}
    if(prefix) ncbytescat(buf,prefix);
    for(i=0;i<nclistlength(segments);i++) {
	const char* seg = nclistget(segments,i);
	if(i>0 && strncmp(seg,sep,seplen)!=0)
	    ncbytescat(buf,sep);
	ncbytescat(buf,seg);
    }
    if(suffix) ncbytescat(buf,suffix);
    if(pathp) *pathp = ncbytesextract(buf);
done:
    ncbytesfree(buf);
    return stat;
}

static int
lexical_compare(const void* arg1, const void* arg2)
{
    char* s1 = *((char**)arg1);
    char* s2 = *((char**)arg2);
    int slen1 = (int)nulllen(s1);
    int slen2 = (int)nulllen(s2);
    if(slen1 != slen2) return (slen1 - slen2);
    return strcmp(s1,s2);
}

/**
Sort a vector of strings.
@param n Number of strings to sort
@param env vector of strings to sort
*/
void
NC_sortenvv(size_t n, char** envv)
{
    if(n <= 1) return;
    qsort(envv, n, sizeof(char*), lexical_compare);
}

/**
Sort a nclist of strings.
@param l NClist of strings
*/
void
NC_sortlist(NClist* l)
{
    if(l == NULL || nclistlength(l) == 0) return;
    NC_sortenvv(nclistlength(l),(char**)nclistcontents(l));
}

/* Free up a vector of strings */
void
NC_freeenvv(size_t nkeys, char** keys)
{
    size_t i;
    for(i=0;i<nkeys;i++)
	nullfree(keys[i]);
    nullfree(keys);
}

int
NC_swapatomicdata(size_t datalen, void* data, int typesize)
{
    int stat = NC_NOERR;
    size_t i;

    assert(datalen % (size_t)typesize == 0);

    if(typesize == 1) goto done;

    /*(typesize > 1)*/
    for(i=0;i<datalen;) {
	char* p = ((char*)data) + i;
        switch (typesize) {
        case 2: swapinline16(p); break;
        case 4: swapinline32(p); break;
        case 8: swapinline64(p); break;
        default: break;
	}
	i += (size_t)typesize;
    }
done:
    return stat;
}
