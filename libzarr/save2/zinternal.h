/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file This header file contains macros, types, and prototypes for
 * the ZARR code in libzarr. This header should not be included in
 * code outside libzarr.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#ifndef ZINTERNAL_H
#define ZINTERNAL_H

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <malloc.h>

#include "netcdf.h"
#include "ncdispatch.h"
#include "nc4internal.h"
#include "ncuri.h"
#include "nclist.h"
#include "ncbytes.h"
#include "ncauth.h"
#include "nclog.h"

#include "zmap.h"
#include "zchunking.h"
#include "zjson.h"
#include "zdebug.h"

/* These have to do with creating chuncked datasets in ZARR. */
#define NCZ_CHUNKSIZE_FACTOR (10)
#define NCZ_MIN_CHUNK_SIZE (2)

/* An attribute in the ZARR root group of this name means that the
 * file must follow strict netCDF classic format rules. */
#define NCZ_NC3_STRICT_ATT_NAME "_nc3_strict"

/** Define Filter API Operations */
#define NCZ_FILTER_REG   1
#define NCZ_FILTER_UNREG 2
#define NCZ_FILTER_INQ   3

/**************************************************/
/* Constants */

#define RCFILEENV "DAPRCFILE"

/* Figure out a usable max path name max */
#ifdef PATH_MAX /* *nix* */
#define NC_MAX_PATH PATH_MAX
#else
#  ifdef MAX_PATH /*windows*/
#    define NC_MAX_PATH MAX_PATH
#  else
#    define NC_MAX_PATH 4096
#  endif
#endif

#define ZARRVERSION 2
/* NCZARRVERSION is ndependent of Zarr version,
   but NCZARRVERSION => ZARRVERSION */
#define NCZARRVERSION "1.1"

#define ZMETAROOT "/.zarr"
#define ZGROUP ".zgroup"
#define ZDIMS ".zdims"
#define ZATTRS ".zattrs"
#define ZVAR ".zarray"

/**************************************************/
/* Define annotation data for NCZ objects */

/* Common fields for all annotations */
typedef struct NCZcommon {
    int synced; /* Has this been inserted into the map? */
} NCZcommon;

/** Struct to hold ZARR-specific info for the file. */
typedef struct NCZ_FILE_INFO {
    NCZcommon common;
} NCZ_FILE_INFO;

/* This is a struct to handle the dim metadata. */
typedef struct NCZ_DIM_INFO {
    NCZcommon common;
} NCZ_DIM_INFO;

/** Strut to hold ZARR-specific info for attributes. */
typedef struct  NCZ_ATT_INFO {
    NCZcommon common;
} NCZ_ATT_INFO;

/* Struct to hold ZARR-specific info for a group. */
typedef struct NCZ_GRP_INFO {
    NCZcommon common;
} NCZ_GRP_INFO;

/* Struct to hold ZARR-specific info for a variable. */
typedef struct NCZ_VAR_INFO {
    NCZcommon common;
} NCZ_VAR_INFO;

/* Struct to hold ZARR-specific info for a field. */
typedef struct NCZ_FIELD_INFO {
    NCZcommon common;
} NCZ_FIELD_INFO;

/* Struct to hold ZARR-specific info for a type. */
typedef struct NCZ_TYPE_INFO {
    NCZcommon common;
} NCZ_TYPE_INFO;

/* Write metadata. */
int ncz_rec_write_metadata(NC_GRP_INFO_T* grp)

/* Adjust the cache. */
int ncz_adjust_var_cache(NC_GRP_INFO_T* grp, NC_VAR_INFO_T* var);

/* Enddef and closing files. */
int ncz_close_zarr_file(NC_FILE_INFO_T* file, int abort);
int ncz_rec_grp_del(NC_GRP_INFO_T* grp);
int ncz_enddef_file(NC_FILE_INFO_T* file);

/* Get the fill value for a var. */
int ncz_get_fill_value(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, void **fillp);

/* Find file, group, var, and att info, doing lazy reads if needed. */
int ncz_find_grp_var_att(int ncid, int varid, const char *name, int attnum,
                              int use_name, char *norm_name, NC_FILE_INFO_T** file,
                              NC_GRP_INFO_T** grp, NC_VAR_INFO_T** var,
                              NC_ATT_INFO_T** att);

/* Find var, doing lazy var metadata read if needed.* /
int ncz_find_grp_file_var(int ncid, int varid, NC_FILE_INFO_T** file,
                             NC_GRP_INFO_T** grp, NC_VAR_INFO_T** var);

/* Perform lazy read of the rest of the metadata for a var.* /
int ncz_get_var_meta(NC_VAR_INFO_T* var);

/* Define Filter API Function* /
int ncz_filter_action(int action, int formatx, int id, NC_FILTER_INFO* info);
/* Support functions for provenance info (defined in nc4hdf.c)* /
int ncz_get_libversion(unsigned*,unsigned*,unsigned*);/*libsrc4/nc4hdf.c*/
int ncz_get_superblock(struct NC_FILE_INFO*, int*);/*libsrc4/nc4hdf.c*/
int NCZ_isnetcdf4(struct NC_FILE_INFO*); /*libsrc4/nc4hdf.c*/

#endif /* ZINTERNAL_H* /

