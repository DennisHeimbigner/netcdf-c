/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file This header file contains types (and type-related macros)
 * for the libzarr code.
 *
 * @author Dennis Heimbigner
*/

#ifndef NCZTYPES_H
#define NCZTYPES_H

/* Define annotation data for NC_OBJ objects */

/* Annotation for NC dispatch data. */

typedef struct NCZINFO {
    NC* controller;
    char* rawurltext; /* as given to NCZ_open */
    char* uritext;    /* as modified by NCZ_open */
    NCURI* uri;      /* parse of rawuritext */
    NCZMAP* map; /* implementation */
    NClist* controls;
    NCauth auth;
    NC_FILE_INFO_T* dataset; /* root of the dataset tree */
} NCZINFO;

/* Annotation for NC_FILE_INFO_T */
typedef struct NCZ_FILE_INFO {
    NCZINFO* dispatch;
    struct nczarr {
	int zarr_version;
	int nczarr_version;
    } zarr;
} NCZ_FILE_INFO;

#endif /*NCZTYPES_H*/
