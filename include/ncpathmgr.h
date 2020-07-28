/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */
#ifndef _NCWINIO_H_
#define _NCWINIO_H_

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif
#include "ncexternl.h"

#ifndef WINPATH
#ifdef _WIN32
#define WINPATH 1
#endif
#ifdef __MINGW32__
#define WINPATH 1
#endif
#endif

/* Define wrapper constants for use with NCaccess */
#ifdef _WIN32
#define ACCESS_MODE_EXISTS 0
#define ACCESS_MODE_R 4
#define ACCESS_MODE_W 2
#define ACCESS_MODE_RW 6
#else
#define ACCESS_MODE_EXISTS (F_OK)
#define ACCESS_MODE_R (R_OK)
#define ACCESS_MODE_W (W_OK)
#define ACCESS_MODE_RW (R_OK|W_OK)
#endif

/* This function attempts to take an arbitrary path and convert
   it to a canonical form. It does several things:
1. converts the character set from platform character set to UTF-8
2. normalizes the incoming path to match the platform
   (e.g. cygwin, windows, mingw, linux). So for example
   using a cygwin path under visual studio will convert e.g.
   /cygdrive/d/x/y to d:\x\y. See ../unit_test/test_pathcvt.c
   for example conversions.
3. It converts doubly escaped characters to singly escaped.
   So for example it converts 'test\\xab' to 'test\xab'.
   This is because shell scripts typically attempt to handle
   escaped characters.

It returns the converted path.

Note that this function is intended to be Idempotent: f(f(x) == f(x).
This means it is ok to call it repeatedly with no harm.
*/
EXTERNL char* NCpathcvt(const char* path);

/* Canonicalize and make absolute */
EXTERNL char* NCpathabsolute(const char* name);

/* Provide a version for testing; DO NOT USE */
EXTERNL char* NCpathcvt_test(const char* path, int ukind, int udrive);

/* Wrap various stdio and unistd IO functions.
It is especially important to use for windows so that
NCpathcvt (above) is invoked on the path */
#ifdef WINPATH
/* path converter wrappers*/
EXTERNL FILE* NCfopen(const char* path, const char* flags);
EXTERNL int NCopen3(const char* path, int flags, int mode);
EXTERNL int NCopen2(const char* path, int flags);
EXTERNL int NCaccess(const char* path, int mode);
EXTERNL int NCremove(const char* path);
EXTERNL int NCmkdir(const char* path, int mode);
EXTERNL char* NCcwd(char* cwdbuf, size_t len);
#ifdef HAV_DIRENT_H
EXTERNL DIR* NCopendir(const char* path);
EXTERNL int NCclosedir(DIR* ent);
#endif
#else /*!WINPATH*/
#define NCfopen(path,flags) fopen((path),(flags))
#define NCopen3(path,flags,mode) open((path),(flags),(mode))
#define NCopen2(path,flags) open((path),(flags))
#define NCremove(path) remove(path)
#ifdef _WIN32
#define NCaccess(path,mode) _access(path,mode)
#else
#define NCaccess(path,mode) access(path,mode)
#endif
#define NCmkdir(path, mode) mkdir(path,mode)
#define NCcwd(buf, len) getcwd(buf,len)
#ifdef HAVE_DIRENT_H
#define NCopendir(path) opendir(path)
#define NCclosedir(ent) closedir(ent)
#endif
#endif /*WINPATH*/

/* Platform independent */
#define NCclose(fd) close(fd)

EXTERNL int NCstring2utf8(const char* path, char** u8p);

/* Possible Kinds Of Output */
#define NCPD_UNKNOWN 0
#define NCPD_NIX 1
#define NCPD_MSYS 2
#define NCPD_CYGWIN 3 
#define NCPD_WIN 4
#define NCPD_REL 5 /* actual kind is unknown */

#endif /* _NCWINIO_H_ */
