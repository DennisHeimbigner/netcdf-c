/*
 * Copyright 2018, University Corporation for Atmospheric Research
 * See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif
#ifdef _WIN32
#include <windows.h>
#include <io.h>
#include <wchar.h>
#endif

#include "netcdf.h"
#include "ncpathmgr.h"
#include "nclog.h"
#include "nclist.h"
#include "ncbytes.h"
#include "ncuri.h"
#include "ncutf8.h"

#undef PATHFORMAT

/*
Code to provide some path conversion code so that
cygwin and (some) mingw paths can be passed to open/fopen
for Windows. Other cases will be added as needed.
Rules:
1. a leading single alpha-character path element (e.g. /D/...)
   will be interpreted as a windows drive letter.
2. a leading '/cygdrive/X' will be converted to
   a drive letter X if X is alpha-char.
3. a leading D:/... is treated as a windows drive letter
5. If any of the above is encountered, then forward slashes
   will be converted to backslashes.
All other cases are passed thru unchanged
*/

/* Define legal windows drive letters */
static const char* windrive = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

static const size_t cdlen = 10; /* strlen("/cygdrive/") */

static int pathinitialized = 0;

static int pathdebug = -1;

static struct Path {
    int kind;
    int relative; /* 1=> relative path */
    int drive;
    char* path;
} wdpath = {NCPD_UNKNOWN,0,0,NULL};

struct Path empty = {NCPD_UNKNOWN,0,0,NULL};

static int parsepath(const char* inpath, struct Path* path);
static int unparsepath(struct Path* p, char** pathp);
static int getwdpath(struct Path* wd);
static char* printPATH(struct Path* p);
static int getlocalpathkind(void);
static void clearPath(struct Path* path);
static void pathinit(void);
#ifdef WINPATH
static int localtoutf8(const char* local, char** u8p);
static int localtowide(const char* local, wchar_t** u16p);
static int utf8towide(const char* utf8, wchar_t** u16p);
static int widetoutf8(const wchar_t* u16, char** u8p);
#endif

EXTERNL
char* /* caller frees */
NCpathcvt(const char* inpath)
{
    int stat = NC_NOERR;
    char* tmp1 = NULL;
    struct Path canon = empty;

    if(inpath == NULL) goto done; /* defensive driving */

    if(!pathinitialized) pathinit();

    if((stat = parsepath(inpath,&canon))) {goto done;}

    if(canon.kind != NCPD_REL && wdpath.kind != canon.kind) {
	nclog(NCLOGWARN,"NCpathcvt: path mismatch: platform=%d inpath=%d\n",
		wdpath.kind,canon.kind);
	canon.kind = wdpath.kind; /* override */
    }
    if((stat = unparsepath(&canon,&tmp1))) {goto done;}
done:
    if(pathdebug) {
        fprintf(stderr,"xxx: inpath=|%s| outpath=|%s|\n",
            inpath?inpath:"NULL",tmp1?tmp1:"NULL");
        fflush(stderr);
    }
    if(stat) {
        nullfree(tmp1); tmp1 = NULL;
	nclog(NCLOGERR,"NCpathcvt: stat=%d (%s)",
		stat,nc_strerror(stat));
    }
    clearPath(&canon);
    return tmp1;
}

EXTERNL
char* /* caller frees */
NCpathabsolute(const char* relpath)
{
    int stat = NC_NOERR;
    struct Path canon = empty;
    char* tmp1 = NULL;
    char* result = NULL;
    size_t len;

    if(relpath == NULL) goto done; /* defensive driving */

    if(!pathinitialized) pathinit();

    /* Canonicalize relpath */
    if((stat = parsepath(relpath,&canon))) {goto done;}
    
    /* See if relative */
    if(canon.kind == NCPD_REL) {
	/* prepend the wd path to the inpath */
	len = strlen(wdpath.path)+strlen(canon.path)+1+1;
	if((tmp1 = (char*)malloc(len))==NULL)
	    {stat = NC_ENOMEM; {goto done;}}
	tmp1[0] = '\0';
	strlcat(tmp1,wdpath.path,len);
	strlcat(tmp1,"/",len);
	strlcat(tmp1,canon.path,len);
       	nullfree(canon.path);
	canon.path = tmp1; tmp1 = NULL;
    }
    /* rebuild */
    if((stat=unparsepath(&canon,&result))) goto done;
done:
    if(pathdebug) {
        fprintf(stderr,"xxx: relpath=|%s| result=|%s|\n",
            relpath?relpath:"NULL",result?result:"NULL");
        fflush(stderr);
    }
    if(stat) {
        nullfree(tmp1); tmp1 = NULL;
	nclog(NCLOGERR,"NCpathcvt: stat=%d (%s)",
		stat,nc_strerror(stat));
    }
    clearPath(&canon);
    nullfree(tmp1);
    return result;
}

/* Fix up a path in case extra escapes were added by shell */
EXTERNL
char*
NCdeescape(const char* name)
{
    char* ename = NULL;
    const char* p;
    char* q;

    if(name == NULL) return NULL;
    ename = strdup(name);
    if(ename == NULL) return NULL;
    for(p=name,q=ename;*p;) {
	switch (*p) {
	case '\0': break;
	case '\\':
#if 0
	    if(p[1] == '#' || p[1] == 'x')
#endif
	    {
	        p++;
		break;
	    }
	    /* fall thru */
        default: *q++ = *p++; break;
	}
    }
    *q++ = '\0';
    return ename;
}

/* Testing support */
/* Force drive and wd before invoking NCpathcvt
   and then revert */
EXTERNL
char* /* caller frees */
NCpathcvt_test(const char* inpath, int ukind, int udrive)
{
    char* result = NULL;
    struct Path oldwd = wdpath;

    if(!pathinitialized) pathinit();
    /* Override */
    wdpath.kind = ukind;
    wdpath.drive = udrive;
    wdpath.path = strdup("/");
    if(pathdebug)
	fprintf(stderr,"xxx: wd=|%s|",printPATH(&wdpath));
    result = NCpathcvt(inpath);
    clearPath(&wdpath);
    wdpath = oldwd;
    return result;
}

static void
pathinit(void)
{
    if(pathinitialized) return;

    /* Check for path debug env vars */
    if(pathdebug < 0) {
	const char* s = getenv("NCPATHDEBUG");
        pathdebug = (s == NULL ? 0 : 1);
    }
    
    /* Get the local wd */
    if(wdpath.path == NULL) {
        getwdpath(&wdpath);
        if(pathdebug > 0) fprintf(stderr,"xxx: wdpath=%s\n",printPATH(&wdpath));
    }
    pathinitialized = 1;
}

static void
clearPath(struct Path* path)
{
    nullfree(path->path);
    path->path = NULL;    
}

#ifdef WINPATH

/*
Provide wrappers for open and fopen
*/

EXTERNL
FILE*
NCfopen(const char* path, const char* flags)
{
    int stat = NC_NOERR;
    FILE* f = NULL;
    char* cvtpath = NULL;
    wchar_t* wpath = NULL;
    wchar_t* wflags = NULL;
    cvtpath = NCpathcvt(path);
    if(cvtpath == NULL) return NULL;
    /* Convert from local to wide */
    if((stat = utf8towide(cvtpath,&wpath))) goto done;    
    if((stat = localtowide(flags,&wflags))) goto done;    
    f = _wfopen(wpath,wflags);
done:
    nullfree(cvtpath);    
    nullfree(wpath);    
    nullfree(wflags);    
    return f;
}

EXTERNL
int
NCopen3(const char* path, int flags, int mode)
{
    int stat = NC_NOERR;
    int fd = -1;
    char* cvtpath = NULL;
    wchar_t* wpath = NULL;
    cvtpath = NCpathcvt(path);
    if(cvtpath == NULL) goto done;
    /* Convert from utf8 to wide */
    if((stat = utf8towide(cvtpath,&wpath))) goto done;    
    fd = _wopen(wpath,flags,mode);
done:
    nullfree(cvtpath);    
    nullfree(wpath);    
    return fd;
}

EXTERNL
int
NCopen2(const char *path, int flags)
{
    return NCopen3(path,flags,0);
}

/*
Provide wrappers for other file system functions
*/

/* Return access applied to path+mode */
EXTERNL
int
NCaccess(const char* path, int mode)
{
    int status = 0;
    char* cvtpath = NULL;
    wchar_t* wpath = NULL;
    if((cvtpath = NCpathcvt(path)) == NULL) 
        {status = EINVAL; goto done;}
    if((status = utf8towide(cvtpath,&wpath))) {status = ENOENT; goto done;}
    if(_waccess(wpath,mode) < 0) {status = errno; goto done;}
done:
    free(cvtpath);    
    free(wpath);    
    errno = status;
    return (errno?-1:0);
}

EXTERNL
int
NCremove(const char* path)
{
    int status = 0;
    char* cvtpath = NULL;
    wchar_t* wpath = NULL;
    if((cvtpath = NCpathcvt(path)) == NULL) {status=ENOMEM; goto done;}
    if((status = utf8towide(cvtpath,&wpath))) {status = ENOENT; goto done;}
    if(_wremove(wpath) < 0) {status = errno; goto done;}
done:
    free(cvtpath);    
    free(wpath);    
    errno = status;
    return (errno?-1:0);
}

EXTERNL
int
NCmkdir(const char* path, int mode)
{
    int status = 0;
    char* cvtpath = NULL;
    wchar_t* wpath = NULL;
    if((cvtpath = NCpathcvt(path)) == NULL) {status=ENOMEM; goto done;}
    if((status = utf8towide(cvtpath,&wpath))) {status = ENOENT; goto done;}
    if(_wmkdir(wpath) < 0) {status = errno; goto done;}
done:
    free(cvtpath);    
    free(wpath);    
    errno = status;
    return (errno?-1:0);
}

EXTERNL
char*
NCcwd(char* cwdbuf, size_t cwdlen)
{
    int status = NC_NOERR;
    struct Path wd = empty;
    char* path = NULL;
    size_t len;

    errno = 0;
    if(cwdlen == 0) {status = ENAMETOOLONG; goto done;}
    if((status = getwdpath(&wd))) {status = ENOENT; goto done;}
    if((status = unparsepath(&wd,&path))) {status = EINVAL; goto done;}
    len = strlen(path);
    if(len >= cwdlen) {status = ENAMETOOLONG; goto done;}
    if(cwdbuf == NULL) {
	if((cwdbuf = malloc(cwdlen))==NULL) {status = ENOMEM; goto done;}
    }
    memcpy(cwdbuf,path,len+1);
done:
    clearPath(&wd);
    nullfree(path);
    errno = status;
    return cwdbuf;
}

int
NCstring2utf8(const char* s, char** u8p)
{
    return localtoutf8(s,u8p);
}
#else /*!WINPATH*/
int
NCstring2utf8(const char* path, char** u8p)
{
    int stat = NC_NOERR;
    char* u8 = NULL;
    if(path != NULL) {
        u8 = strdup(path);
	if(u8 == NULL) {stat =  NC_ENOMEM; goto done;}
    }
    if(u8p) {*u8p = u8; u8 = NULL;}
done:
    return stat;
}
#endif /*!WINPATH*/

EXTERNL int
NChasdriveletter(const char* path)
{
    int stat = NC_NOERR;
    int hasdl = 0;    
    struct Path canon = empty;
    /* Get the local wd */
    if(wdpath.path == NULL) {
        if((stat = getwdpath(&wdpath))) goto done;
    }
    if((stat = parsepath(path,&canon))) goto done;
    if(canon.drive == 0) canon.drive = wdpath.drive;
    hasdl = (canon.drive != 0);
done:
    clearPath(&canon);
    return hasdl;
}

/**************************************************/
/* Utilities */

/* Parse a path */
static int
parsepath(const char* inpath, struct Path* path)
{
    int stat = NC_NOERR;
    char* tmp1 = NULL;
    size_t len;
    char* p;
    
    assert(path);
    memset(path,0,sizeof(struct Path));

    if(inpath == NULL) goto done; /* defensive driving */

fprintf(stderr,"xxx: parsepath: inpath= %d |%s|\n",strlen(inpath),inpath);

    /* Convert to UTF8 */
    if((stat = NCstring2utf8(inpath,&tmp1))) goto done;
fprintf(stderr,"xxx: parsepath: tmp1= %d |%s|\n",strlen(tmp1),tmp1);
    /* Convert to forward slash */
    for(p=tmp1;*p;p++) {if(*p == '\\') *p = '/';}

    /* parse all paths to 2-parts:
	1. drive letter (optional)
	2. path after drive letter
    */

    len = strlen(tmp1);

    /* 1. look for MSYS path /D/... */
    if(len >= 2
	&& (tmp1[0] == '/')
	&& strchr(windrive,tmp1[1]) != NULL
	&& (tmp1[2] == '/' || tmp1[2] == '\0')) {
	/* Assume this is a mingw path */
	path->drive = tmp1[1];
	/* Remainder */
	if(tmp1[2] == '\0')
	    path->path = NULL;
	else
	    path->path = strdup(tmp1+2);
	if(path == NULL)
	    {stat = NC_ENOMEM; goto done;}
	path->kind = NCPD_MSYS;
    }
    /* 2. Look for leading /cygdrive/D where D is a single-char drive letter */
    else if(len >= (cdlen+1)
	&& memcmp(tmp1,"/cygdrive/",cdlen)==0
	&& strchr(windrive,tmp1[cdlen]) != NULL
	&& (tmp1[cdlen+1] == '/'
	    || tmp1[cdlen+1] == '\0')) {
	/* Assume this is a cygwin path */
	path->drive = tmp1[cdlen];
	/* Remainder */
	if(tmp1[cdlen+1] == '\0')
	    path->path = NULL;
	else
	    path->path = strdup(tmp1+cdlen+1);
	if(path == NULL)
	    {stat = NC_ENOMEM; goto done;}
	path->kind = NCPD_CYGWIN;
    }
    /* 3. Look for windows path:  D:/... where D is a single-char
          drive letter */
    else if(len >= 2
	&& strchr(windrive,tmp1[0]) != NULL
	&& tmp1[1] == ':'
	&& (tmp1[2] == '\0' || tmp1[2] == '/')) {
	/* Assume this is a windows path */
	path->drive = tmp1[0];
	/* Remainder */
	if(tmp1[2] == '\0')
	    path->path = NULL;
	else
	    path->path = strdup(tmp1+2);
	if(path == NULL)
	    {stat = NC_ENOMEM; goto done;}
	path->kind = NCPD_WIN;
    }
    /* look for *nix path */
    else if(len >= 1 && tmp1[0] == '/') {
	/* Assume this is a *nix path */
	path->drive = 0; /* no drive letter */
	/* Remainder */
	path->path = tmp1; tmp1 = NULL;
	path->kind = NCPD_NIX;	
    } else {/* Relative path of unknown type */
	path->kind = NCPD_REL;
	path->path = tmp1; tmp1 = NULL;
    }

done:
    nullfree(tmp1);
    if(stat) {clearPath(path);}
    return stat;
}

static int
unparsepath(struct Path* xp, char** pathp)
{
    int stat = NC_NOERR;
    size_t len;
    char* path = NULL;
    char sdrive[2] = {'\0','\0'};
    char* p = NULL;
    int kind = xp->kind;
    
fprintf(stderr,"xxx: unparsepath: xp->path= %d |%s|\n",strlen(xp->path),xp->path);
    switch (kind) {
    case NCPD_NIX:
	len = nulllen(xp->path);
	if(xp->drive != 0) {
	    len += 2;
	    sdrive[0] = xp->drive;
	}
	len++; /* nul terminate */
	if((path = (char*)malloc(len))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	path[0] = '\0';
	if(xp->drive != 0) {
	    strlcat(path,"/",len);
	    strlcat(path,sdrive,len);
	}	
	if(xp->path != NULL)
	    strlcat(path,xp->path,len);
        break;
    case NCPD_CYGWIN:
	if(xp->drive == 0) {xp->drive = wdpath.drive;} /*requires a drive */
	len = nulllen(xp->path)+cdlen+1+1;
	if((path = (char*)malloc(len))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	path[0] = '\0';
	strlcat(path,"/cygdrive/",len);
	sdrive[0] = xp->drive;
	strlcat(path,sdrive,len);
	if(xp->path)
	    strlcat(path,xp->path,len);
	break;
    case NCPD_WIN:
	if(xp->drive == 0) {xp->drive = wdpath.drive;} /*requires a drive */
	len = nulllen(xp->path)+2+1;
	if((path = (char*)malloc(len))==NULL)
	    {stat = NC_ENOMEM; goto done;}	
	path[0] = '\0';
	sdrive[0] = xp->drive;
	strlcat(path,sdrive,len);
	strlcat(path,":",len);
	if(xp->path)
	    strlcat(path,xp->path,len);
	/* Convert forward to back */ 
        for(p=path;*p;p++) {if(*p == '/') *p = '\\';}
	break;
    case NCPD_MSYS:
	if(xp->drive == 0) {xp->drive = wdpath.drive;} /*requires a drive */
	len = nulllen(xp->path)+2+1;
	if((path = (char*)malloc(len))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	path[0] = '\0';
	sdrive[0] = xp->drive;
	strlcat(path,"/",len);
	strlcat(path,sdrive,len);
	if(xp->path)
	    strlcat(path,xp->path,len);
	break;
    case NCPD_REL:
	path = strdup(xp->path);	
	/* Use wdpath to decide slashing */
	if(wdpath.kind == NCPD_WIN) {
	    /* Convert forward to back */ 
            for(p=path;*p;p++) {if(*p == '/') *p = '\\';}
	}
	break;
    default: stat = NC_EINTERNAL; goto done;
    }
fprintf(stderr,"xxx: unparsepath: path= %d |%s|\n",strlen(path),path);
    if(pathp) {*pathp = path; path = NULL;}
done:
    nullfree(path);
    return stat;
}

static int
getwdpath(struct Path* wd)
{
    int stat = NC_NOERR;
    char* path = NULL;
    if(wd->path != NULL) return stat;
    memset(wd,0,sizeof(struct Path));
    {
#ifdef _WIN32   
        wchar_t* wpath = NULL;
        wpath = _wgetcwd(NULL,8192);
        if((stat = widetoutf8(wpath,&path)))
            {nullfree(wpath); wpath = NULL; return stat;}
#else
        path = getcwd(NULL,8192);
#endif
    }
    stat = parsepath(path,wd);
    /* Force the kind */
    wd->kind = getlocalpathkind();
    nullfree(path); path = NULL;
    return stat;
}

static int
getlocalpathkind(void)
{
    int kind = NCPD_UNKNOWN;
#ifdef __CYGWIN__
	kind = NCPD_CYGWIN;
#elif __MSYS__
	kind = NCPD_MSYS;
#elif _MSC_VER /* not _WIN32 */
	kind = NCPD_WIN;
#else
	kind = NCPD_NIX;
#endif
    return kind;
}

#ifdef WINPATH
/**
 * Converts the filename from Locale character set (presumably some
 * ANSI character set like ISO-Latin-1 or UTF-8 to UTF-8
 * @param local Pointer to a nul-terminated string in locale char set.
 * @param u8p Pointer for returning the output utf8 string
 *
 * @return NC_NOERR return converted filename
 * @return NC_EINVAL if conversion fails
 * @return NC_ENOMEM if no memory available
 * 
 */
static int
localtoutf8(const char* local, char** u8p)
{
    int stat=NC_NOERR;
    char* u8 = NULL;
    int n;
    wchar_t* u16 = NULL;

#ifdef _WIN32
fprintf(stderr,"www: ACP=%d\n",GetACP());
#endif

    {
        /* Get length of the converted string */
        n = MultiByteToWideChar(CP_ACP, 0,  local, -1, NULL, 0);
        if (!n) {stat = NC_EINVAL; goto done;}
        if((u16 = malloc(sizeof(wchar_t) * n))==NULL)
	    {stat = NC_ENOMEM; goto done;}
        /* do the conversion */
        if (!MultiByteToWideChar(CP_ACP, 0, local, -1, u16, n))
            {stat = NC_EINVAL; goto done;}
fwprintf(stderr,L"www: u16=|%ls|\n",u16);
        /* Now reverse the process to produce utf8 */
        n = WideCharToMultiByte(CP_UTF8, 0, u16, -1, NULL, 0, NULL, NULL);
        if (!n) {stat = NC_EINVAL; goto done;}
        if((u8 = malloc(sizeof(char) * n))==NULL)
	    {stat = NC_ENOMEM; goto done;}
        if (!WideCharToMultiByte(CP_UTF8, 0, u16, -1, u8, n, NULL, NULL))
            {stat = NC_EINVAL; goto done;}
fprintf(stderr,"www: u8=%d |%s|\n",strlen(u8),u8);
#ifdef _WIN32
	{
	char* normal = NULL;
	stat = nc_utf8_normalize(u8,(unsigned char**)&normal);
fprintf(stderr,"www: normal=%d |%s|\n",strlen(normal),normal);
	nullfree(normal);
        }
#endif
    }
    if(u8p) {*u8p = u8; u8 = NULL;}
done:
    nullfree(u8);
    return stat;
}

static int
localtowide(const char* local, wchar_t** u16p)
{
    int stat=NC_NOERR;
    wchar_t* u16 = NULL;
    int n;

    /* Get length of the converted string */
    n = MultiByteToWideChar(CP_ACP, 0,  local, -1, NULL, 0);
    if (!n) {stat = NC_EINVAL; goto done;}
    if((u16 = malloc(sizeof(wchar_t) * n))==NULL)
	{stat = NC_ENOMEM; goto done;}
    /* do the conversion */
    if (!MultiByteToWideChar(CP_ACP, 0, local, -1, u16, n))
        {stat = NC_EINVAL; goto done;}
    if(u16p) {*u16p = u16; u16 = NULL;}
done:
    nullfree(u16);
    return stat;
}

static int
utf8towide(const char* utf8, wchar_t** u16p)
{
    int stat=NC_NOERR;
    wchar_t* u16 = NULL;
    int n;

    /* Get length of the converted string */
    n = MultiByteToWideChar(CP_UTF8, 0,  utf8, -1, NULL, 0);
    if (!n) {stat = NC_EINVAL; goto done;}
    if((u16 = malloc(sizeof(wchar_t) * n))==NULL)
	{stat = NC_ENOMEM; goto done;}
    /* do the conversion */
    if (!MultiByteToWideChar(CP_UTF8, 0, utf8, -1, u16, n))
        {stat = NC_EINVAL; goto done;}
    if(u16p) {*u16p = u16; u16 = NULL;}
done:
    nullfree(u16);
    return stat;
}

static int
widetoutf8(const wchar_t* u16, char** u8p)
{
    int stat=NC_NOERR;
    char* u8 = NULL;
    int n;

    /* Get length of the converted string */
    n = WideCharToMultiByte(CP_UTF8, 0,  u16, -1, NULL, 0, NULL, NULL);
    if (!n) {stat = NC_EINVAL; goto done;}
    if((u8 = malloc(sizeof(char) * n))==NULL)
	{stat = NC_ENOMEM; goto done;}
    /* do the conversion */
    if (!WideCharToMultiByte(CP_UTF8, 0, u16, -1, u8, n, NULL, NULL))
        {stat = NC_EINVAL; goto done;}
    if(u8p) {*u8p = u8; u8 = NULL;}
done:
    nullfree(u8);
    return stat;
}
#endif /*WINPATH*/

static char*
printPATH(struct Path* p)
{
    static char buf[4096];
    buf[0] = '\0';
    snprintf(buf,sizeof(buf),"Path{kind=%d drive='%c' path=|%s|}",
	p->kind,(p->drive > 0?p->drive:'0'),p->path);
    return buf;
}

/**************************************************/
#if 0
#ifdef HAVE_DIRENT_H
EXTERNL
DIR*
NCopendir(const char* path)
{
    DIR* ent = NULL;
    char* cvtpath = NCpathcvt(path);
    if(cvtpath == NULL) return -1;
    ent = opendir(cvtpath);
    free(cvtpath);    
    return ent;
}

EXTERNL
int
NCclosedir(DIR* ent)
{
    int stat = 0;
    char* cvtpath = NCpathcvt(path);
    if(cvtpath == NULL) return -1;
    stat = closedir(cvtpath);
    free(cvtpath);    
    return stat;
}
#endif
#endif /*0*/

