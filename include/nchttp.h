/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * Header file for dhttp.c
 * @author Dennis Heimbigner
 */

#ifndef NCHTTP_H
#define NCHTTP_H

typedef enum HTTPMETHOD {
HTTPNONE=0, HTTPGET=1, HTTPPUT=2, HTTPPOST=3, HTTPHEAD=4
} HTTPMETHOD;

extern int nc_http_open(const char* objecturl, void** curlp, size64_t* filelenp);
extern int nc_http_size(void* curl, const char* objecturl, size64_t* sizep);
extern int nc_http_read(void* curl, const char* url, size64_t start, size64_t count, NCbytes* buf);
extern int nc_http_close(void* curl);

#endif /*NCHTTP_H*/
