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

/*Forward*/
struct NCZMAP;

/* Define annotation data for NC_OBJ objects */

/* Annotation for NC_FILE_INFO_T */
typedef struct NCZ_FILE_INFO {
    NC_FILE_INFO_T* dataset; /* root of the dataset tree */
    struct NCZMAP* map; /* implementation */
    NClist* controls;
    NCauth auth;
    struct nczarr {
	int zarr_version;
	int nczarr_version;
    } zarr;
} NCZ_FILE_INFO;

/* Annotation for NC_TYPE_INFO_T */
typedef struct NCZ_TYPE_INFO {
} NCZ_TYPE_INFO;

/* Annotation for NC_GRP_INFO_T */
typedef struct NCZ_GRP_INFO {
} NCZ_GRP_INFO;

/* Annotation for NC_VAR_INFO_T */
typedef struct NCZ_VAR_INFO {
} NCZ_VAR_INFO;

/* Annotation for NC_DIM_INFO_T */
typedef struct NCZ_DIM_INFO {
} NCZ_DIM_INFO;

/* Annotation for NC_ATT_INFO_T */
typedef struct NCZ_ATT_INFO {
} NCZ_ATT_INFO;

#endif /*NCZTYPES_H*/
