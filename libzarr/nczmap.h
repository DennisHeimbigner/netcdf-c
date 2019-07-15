
/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file This header file contains types (and type-related macros)
 * for the libzarr code.
 *
 * @author Dennis Heimbigner
*/

#ifndef NCZMAP_H
#define NCZMAP_H

/* Forward */
struct NCZMAP_API;

/* Define the space of implemented (eventually) map implementations */
typedef enum NCZM_IMPL {
NCZM_UNDEF=0, /* In-memory implementation */
NCZM_FILE=1, /* File system directory-based implementation */
NCZM_S3=2, /* Amazon S3 implementation */
} NCZM_IMPL;

/* All instances have this as their first field
   so we can cast to this form; avoids need for
   a separate per-implementation malloc piece.
*/
typedef struct NCZMAP {
    struct NCZMAP_API* api;
    char* path;
    int mode;
    unsigned int flags;
} NCZMAP;

/* Forward */
typedef struct NCZMAP_API NCZMAP_API;

struct NCZMAP_API {

int format;

int (*create)(NCZMAP_API*,const char *path, int mode, unsigned flags, void* parameters, NCZMAP** mapp);
int (*open)(NCZMAP_API*, const char *path, int mode, unsigned flags, void* parameters, NCZMAP** mapp);
int (*close)(NCZMAP* map);
int (*clear)(NCZMAP* map);
int (*len)(NCZMAP* map, const char* rpath, fileoffset_t* lenp);
int (*read)(NCZMAP* map, const char* rpath, fileoffset_t start, fileoffset_t count, char** contentp);
int (*write)(NCZMAP* map, const char* rpath, fileoffset_t start, fileoffset_t count, const char* contentp);
int (*rename)(NCZMAP* map, const char* oldrpath, const char* newname);

};

/* convert implementation enum to corresponding API */
extern NCZMAP_API* nczmap_get_api(NCZM_IMPL);

/* API Wrappers */
extern int nczmap_create(const char *path, NCZM_IMPL impl, int mode, unsigned flags, void* parameters, NCZMAP** mapp);
extern int nczmap_open(const char *path, NCZM_IMPL impl, int mode, unsigned flags, void* parameters, NCZMAP** mapp);
extern int nczm_close(NCZMAP*);
extern int nczm_clear(NCZMAP*);
extern int nczm_len(NCZMAP*, const char*, fileoffset_t* lenp);
extern int nczm_read(NCZMAP*, const char*, fileoffset_t, fileoffset_t, char**);
extern int nczm_write(NCZMAP*, const char*, fileoffset_t, fileoffset_t, const char*);
extern int nczm_rename(NCZMAP*, const char*, const char*);

#endif /*NCZMAP_H*/




