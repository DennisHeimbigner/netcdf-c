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
#endif

#include "netcdf.h"
#include "ncpathmgr.h"

extern char *realpath(const char *path, char *resolved_path);

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
4. a relative path will be converted to an absolute path.
5. If any of the above is encountered, then forward slashes
   will be converted to backslashes.
All other cases are passed thru unchanged
*/

/* Define legal windows drive letters */
static const char* windrive = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

static const size_t cdlen = 10; /* strlen("/cygdrive/") */

static int pathdebug = -1;

static char* makeabsolute(const char* relpath);
#ifdef WINPATH
static wchar_t* utf8towide(const char* utf8, wchar_t** u16p);
static char* localtoutf8(const char* local, char** u8p);
static char* localtowide(const char* local, char** u8p);
#endif

EXTERNL
char* /* caller frees */
NCpathcvt(const char* path)
{
    char* outpath = NULL; 
    char* p;
    char* q;
    size_t pathlen;

    if(path == NULL) goto done; /* defensive driving */

    /* Check for path debug env vars */
    if(pathdebug < 0) {
	const char* s = getenv("NCPATHDEBUG");
        pathdebug = (s == NULL ? 0 : 1);
    }

    pathlen = strlen(path);

    /* 1. look for MSYS path /D/... */
    if(pathlen >= 2
	&& (path[0] == '/' || path[0] == '\\')
	&& strchr(windrive,path[1]) != NULL
	&& (path[2] == '/' || path[2] == '\\' || path[2] == '\0')) {
	/* Assume this is a mingw path */
	outpath = (char*)malloc(pathlen+3); /* conservative */
	if(outpath == NULL) goto done;
	q = outpath;
	*q++ = path[1];
	*q++ = ':';
	strncpy(q,&path[2],pathlen);
	if(strlen(outpath) == 2)
	    strcat(outpath,"/");
	goto slashtrans;
    }

    /* 2. Look for leading /cygdrive/D where D is a single-char drive letter */
    if(pathlen >= (cdlen+1)
	&& memcmp(path,"/cygdrive/",cdlen)==0
	&& strchr(windrive,path[cdlen]) != NULL
	&& (path[cdlen+1] == '/'
	    || path[cdlen+1] == '\\'
	    || path[cdlen+1] == '\0')) {
	/* Assume this is a cygwin path */
	outpath = (char*)malloc(pathlen+1); /* conservative */
	if(outpath == NULL) goto done;
	outpath[0] = path[cdlen]; /* drive letter */
	outpath[1] = ':';
	strcpy(&outpath[2],&path[cdlen+1]);
	if(strlen(outpath) == 2)
	    strcat(outpath,"/");
	goto slashtrans;
    }

    /* 3. Look for leading D: where D is a single-char drive letter */
    if(pathlen >= 2
	&& strchr(windrive,path[0]) != NULL
	&& path[1] == ':'
	&& (path[2] == '\0' || path[2] == '/'  || path[2] == '\\')) {
	outpath = strdup(path);
	goto slashtrans;
    }

    /* 4. Look for relative path */
    if(pathlen > 1 && path[0] == '.') {
	outpath = makeabsolute(path);
	goto slashtrans;
    }

    /* Other: just pass thru */
    outpath = strdup(path);
    goto done;

slashtrans:
      /* In order to help debugging, and if not using MSC_VER or MINGW,
	 convert back slashes to forward, else convert forward to back
      */
    p = outpath;
    /* In all #1 or #2 cases, translate '/' -> '\\' */
    for(;*p;p++) {
	if(*p == '/') {*p = '\\';}
    }
#ifdef PATHFORMAT
#ifndef _WIN32
	p = outpath;
        /* Convert '\' back to '/' */
        for(;*p;p++) {
            if(*p == '\\') {*p = '/';}
	}
    }
#endif /*!_WIN32*/
#endif /*PATHFORMAT*/

done:
    if(pathdebug) {
        fprintf(stderr,"XXXX: inpath=|%s| outpath=|%s|\n",
            path?path:"NULL",outpath?outpath:"NULL");
        fflush(stderr);
    }
    return outpath;
}

static char*
makeabsolute(const char* relpath)
{
    char* path = NULL;
#ifdef _WIN32
    path = _fullpath(NULL,relpath,8192);
#else
    path = realpath(relpath, NULL);
#endif
    if(path == NULL)
	path = strdup(relpath);
    return path;    
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
	    if(p[1] == '#') {
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
    if((stat = localtowide(cvtpath,&wpath))) goto done;    
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
    if(cvtpath == NULL) return NULL;
    /* Convert from local to wide */
    if((stat = localtowide(cvtpath,&wpath))) goto done;    
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
        {status = NC_EINVAL; goto done;}
    if((status = localtowide(cvtpath,&wpath)) == NULL) goto done;
    status = _waccess(wpath,mode);
done:
    free(cvtpath);    
    free(wpath);    
    return status;
}

EXTERNL
int
NCremove(const char* path)
{
    int status = 0;
    char* cvtpath = NULL;
    wchar_t* wpath = NULL;
    if((cvtpath = NCpathcvt(path)) == NULL) {status=NC_ENOMEM: goto done;}
    if((wpath = localtowide(cvtpath,&wpath)) == NULL)
	{status=NC_EINVAL; goto done;}
    status = _wremove(wpath);
done:
    free(cvtpath);    
    free(wpath);    
    return status;
}

EXTERNL
int
NCmkdir(const char* path, int mode)
{
    int status = 0;
    char* cvtpath = NULL;
    wchar_t* wpath = NULL;
    if((cvtpath = NCpathcvt(path)) == NULL) {status=NC_ENOMEM: goto done;}
    if((status = localtowide(cvtpath,&wpath)) == NULL) goto done;
    status = _wmkdir(wpath);
done:
    free(cvtpath);    
    free(wpath);    
    return status;
}

EXTERNL
char*
NCcwd(char* cwdbuf, size_t len)
{
    int status = NC_NOERR;
    char* path8 = NULL;

    errno = 0;
    if(cwdbuf == NULL || len == 0) {status = ENAMETOOLONG; goto done;}
    if(_wgetcwd(cwdbuf,len) == NULL) {status = errno; goto done;}
    if((status=widetoutf8(cwdbuf,&path8)) == NULL) {goto done;}
done:
    errno = 0;
    if(status) {free(path8); return NULL;}
    return path8;
}

#if 0
EXTERNL int
NChasdriveletter(const char* path)
{
    FILE* f = NULL;
    char* cvtpath = NCpathcvt(path);
    int stat = NC_NOERR;
#ifdef _WIN32
    wchar_t* wpath = NULL;
    wchar_t* wflags = NULL;
#endif

    cvtpath = NCpathcvt(path);
    if(cvtpath == NULL) return NULL;

#ifdef _WIN32
    stat = localtowide(cvtpath,&wpath);
    if(stat) goto done;
    stat = localtowide(flags,wflags);
    if(stat) goto done;
    f = _wfopen(wpath,wflags);
#else
    f = fopen(cvtpath,flags);
#endif
done:
    free(cvtpath);    
#ifdef _WIN32
    nullfree(wpath);
    nullfree(wflags);
#endif
    return f;
}
#endif


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
localtoutf8(const char* local, char** u8p);
{
    int stat=NC_NOERR;
    char* u8 = NULL;
    wchar_t* u16 = NULL;
    DWORD dwFlags = MB_ERR_INVALID_CHARS;
    int n;

    /* Get length of the converted string */
    n = MultiByteToWideChar(CP_ACP, dwFlags,  local, -1, NULL, 0);
    if (!n) {stat = NC_EINAL; goto done;}
    if((u16 = malloc(sizeof(wchar_t) * n))==NULL)
	{stat = NC_ENOMEM; goto done;}
    /* do the conversion */
    if (!MultiByteToWideChar(CP_ACP, dwFlags, local, -1, u16, n))
        {stat = NC_EINAL; goto done;}
 
    /* Now reverse the process to produce utf8 */
    n = WideCharToMultiByte(CP_UTF8, dwFlags, u16, -1, NULL, 0, NULL, NULL);
    if (!n) {stat = NC_EINVAL; goto done;}
    if((u8 = malloc(sizeof(char) * n))==NULL)
	{stat = NC_ENOMEM; goto done;}
    if (!WideCharToMultiByte(CP_UTF8, 0, u16, -1, u8, n, NULL, NULL))
        {stat = NC_EINVAL; goto done;}
    if(u8p) {*u8p = u8; u8 = NULL;}
done:
    nullfree(u8);
    return stat;
}

static int
utf8towide(const char* utf8, wchar_t** u16p)
    int stat=NC_NOERR;
    wchar_t* u16 = NULL;
    DWORD dwFlags = MB_ERR_INVALID_CHARS;
    int n;

    /* Get length of the converted string */
    n = MultiByteToWideChar(CP_UTF8, dwFlags,  utf8, -1, NULL, 0);
    if (!n) {stat = NC_EINAL; goto done;}
    if((u16 = malloc(sizeof(wchar_t) * n))==NULL)
	{stat = NC_ENOMEM; goto done;}
    /* do the conversion */
    if (!MultiByteToWideChar(CP_UTF8, dwFlags, utf8, -1, u16, n)) {
        {stat = NC_EINAL; goto done;}
    if(u16p) {*u16p = u16; u16 = NULL;}
done:
    nullfree(u16);
    return stat;
}

static int
localtowide(const char* local, wchar_t** u16p)
{
    int stat=NC_NOERR;
    wchar_t* u16 = NULL;
    DWORD dwFlags = MB_ERR_INVALID_CHARS;
    int n;

    /* Get length of the converted string */
    n = MultiByteToWideChar(CP_ACP, dwFlags,  local, -1, NULL, 0);
    if (!n) {stat = NC_EINAL; goto done;}
    if((u16 = malloc(sizeof(wchar_t) * n))==NULL)
	{stat = NC_ENOMEM; goto done;}
    /* do the conversion */
    if (!MultiByteToWideChar(CP_ACP, dwFlags, local, -1, u16, n)) {
        {stat = NC_EINAL; goto done;}
    if(u16p) {*u16p = u16; u16 = NULL;}
done:
    nullfree(u16);
    return stat;
}

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

#endif /*WINPATH*/

#ifdef WINPATH
static int
NCtoutf8(const char* path, char** u8p);
{
    return localtoutf8(path,u8p);
}
#else /*!WINPATH*/
int
NCpath2utf8(const char* path, char** u8p)
{
    int stat = NC_NOERR;
    char* u8 = NULL;
    if(path != NULL) {
        u8 = strdup(path);
	if(u8 == NULL) {stat = NC_ENOMEM; goto done;}
    }
    if(u8p) {*u8p = u8; u8 = NULL;}
done:
    return stat;
}
#endif
