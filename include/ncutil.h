/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/

/**
API for libdispatch/dutil.c
*/

#ifndef NCUTIL_H
#define NCUTIL_H

/**************************************************/

/* signature: void swapinline16(void* ip) */
#define swapinline16(ip) \
{ \
    char b[2]; \
    char* src = (char*)(ip); \
    b[0] = src[1]; \
    b[1] = src[0]; \
    memcpy(ip, b, 2); \
}

/* signature: void swapinline32(void* ip) */
#define swapinline32(ip) \
{ \
    char b[4]; \
    char* src = (char*)(ip); \
    b[0] = src[3]; \
    b[1] = src[2]; \
    b[2] = src[1]; \
    b[3] = src[0]; \
    memcpy(ip, b, 4); \
}

/* signature: void swapinline64(void* ip) */
#define swapinline64(ip) \
{ \
    char b[8]; \
    char* src = (char*)(ip); \
    b[0] = src[7]; \
    b[1] = src[6]; \
    b[2] = src[5]; \
    b[3] = src[4]; \
    b[4] = src[3]; \
    b[5] = src[2]; \
    b[6] = src[1]; \
    b[7] = src[0]; \
    memcpy(ip, b, 8); \
}

/**************************************************/

#if defined(__cplusplus)
extern "C" {
#endif

EXTERNL int NC__testurl(const char* path, char** basenamep);
EXTERNL int NC_isLittleEndian(void);
EXTERNL char* NC_backslashEscape(const char* s);
EXTERNL char* NC_backslashUnescape(const char* esc);
EXTERNL char* NC_entityescape(const char* s);
EXTERNL char* NC_shellUnescape(const char* esc);
EXTERNL char* NC_mktmp(const char* base);
EXTERNL int NC_readfile(const char* filename, NCbytes* content);
EXTERNL int NC_readfilen(const char* filename, NCbytes* content, long long amount);
EXTERNL int NC_readfileF(FILE* stream, NCbytes* content, long long amount);
EXTERNL int NC_writefile(const char* filename, size_t size, void* content);
EXTERNL int NC_getmodelist(const char* modestr, NClist** modelistp);
EXTERNL int NC_testpathmode(const char* path, const char* tag);
EXTERNL int NC_testmode(NCURI* uri, const char* tag);
EXTERNL int NC_addmodetag(NCURI* uri, const char* tag);
EXTERNL int isinf(double x);
EXTERNL int isnan(double x);
EXTERNL int NC_split_delim(const char* arg, char delim, NClist* segments);
EXTERNL int NC_join(NClist* segments, char** pathp);
EXTERNL int NC_joinwith(NClist* segments, const char* sep, const char* prefix, const char* suffix, char** pathp);
EXTERNL void NC_sortenvv(size_t n, char** envv);
EXTERNL void NC_sortlist(NClist* l);
EXTERNL void NC_freeenvv(size_t nkeys, char** keys);
EXTERNL int NC_swapatomicdata(size_t datalen, void* data, int typesize);

#if defined(__cplusplus)
}
#endif

#endif /*NCUTIL_H*/
