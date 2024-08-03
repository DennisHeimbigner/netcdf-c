/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file
 *
 * @author Dennis Heimbigner
 */

#ifndef ZFORMAT_H
#define ZFORMAT_H

/*
Notes on internal architecture.

Zarr version 2 vs Zarr version 3 is handled by using
a dispatch table mechanism similar to the dispatch
mechanism used in netcdf_dispatch.h to choose the
netcdf file format.

The dispatcher is defined by the type NCZ_Formatter.
That dispatcher allows the Zarr format independent code
to be isolated from the Zarr format specific code.
The table has the following groups of entries:
1. open/create/close
2. reading metadata -- use the JSON metadata of a file to a fill in the tree of an instance of NC_FILE_INFO_T.
3. writing metadata -- use an NC_FILE_INFO_T tree to build and write the JSON metadata of a file.
4. misc. actions -- e.g. building chunk keys and converting between the Zarr codec and an HDF5 filter.
*/


/* This is the version of the formatter table. It should be changed
 * when new functions are added to the formatter table. */
#ifndef NCZ_FORMATTER_VERSION
#define NCZ_FORMATTER_VERSION 1
#endif /*NCZ_FORMATTER_VERSION*/

/* struct Fill Values */
#define NCZ_CODEC_ENV_EMPTY_V2 {NCZ_CODEC_ENV_VER, 2}
#define NCZ_CODEC_ENV_EMPTY_V3 {NCZ_CODEC_ENV_VER, 3}

/* Opaque */
struct NCZ_Plugin;

/* This is the dispatch table, with a pointer to each netCDF
 * function. */
typedef struct NCZ_Formatter {
    int nczarr_format;
    int zarr_format;
    int dispatch_version; /* Version of the dispatch table */
    int (*create)    (NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
    int (*open)      (NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
    int (*close)     (NC_FILE_INFO_T* file);

    /* Convert NetCDF4 Internal object to JSON */
    int (*build_group)(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jgroupp);
    int (*build_superblock)(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, NCjson** jsuperp);
    int (*build_grp_dims)(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCindex* dims, NCjson** jdimsp);
    int (*build_grp_vars)(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCindex* vars, NCjson** jvarsp);
    int (*build_grp_subgroups)(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCindex* subgrps, NCjson** jsubgrpsp);
    int (*build_nczarr_group)(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson* jdims, NCjson* jvars, NCjson* jsubgrps,  NCjson** jnczgrpp);
    int (*build_group_json)(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson* jsuper, NCjson* jatts, NCjson* jtypes, NCjson** jgrpp);
    int (*build_attributes_json)(NC_FILE_INFO_T* file, NC_OBJ* container, const NCindex* attlist, NCjson** jattsp, NCjson** jtypesp);
    int (*build_var_json)(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson* jatts, NCjson** jvarp);
    int (*build_nczarr_array)(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jnczarrayp);

    /* Write JSON to storage */
    int (*write_grp_json)(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jgroup, const NCjson* jatts);
    int (*write_var_json)(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const NCjson* jvar, const NCjson* jatts);

    /* Convert JSON to NetCDF4 Internal objects */
    int (*decode_group)(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jgroupp);
    int (*decode_superblock)(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, NCjson** jsuperp);
    int (*decode_grp_dims)(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NClist* dims, NCjson** jdimsp);
    int (*decode_grp_vars)(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NClist* vars, NCjson** jvarsp);
    int (*decode_grp_subgroups)(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NClist* subgrps, NCjson** jsubgrpsp);
    int (*decode_nczarr_group)(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* dims, NClist* vars, NClist* subgrps);
    int (*decode_attributes_json)(NC_FILE_INFO_T* file, NC_OBJ* container, NCjson** jattsp, NCjson** jtypesp);
    int (*decode_var_json)(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jvarp, NCjson** jattsp);
    int (*decode_nczarr_array)(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jnczarrayp);

    /* Read JSON to storage */
    int (*read_grp_json)(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jgroupp, NCjson** jatts, int* constattsp);
    int (*read_var_json)(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jvar, NCjson** jatts, int* constattsp);

    /* Type conversion actions */
    int (*dtype2nctype)(const NC_FILE_INFO_T* file, const char* dtype, nc_type typehint, nc_type* nctypep, int* endianp, size_t* typelenp);
    int (*nctype2dtype)(const NC_FILE_INFO_T* file, nc_type nctype, int endianness, size_t typesize, char** dtypep, char** daliasp);

    /* Misc. Actions */
    int (*hdf2codec) (const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, NCZ_Filter* filter); /* Codec converters */
    int (*build_chunkkey)(size_t rank, const size64_t* chunkindices, char dimsep, char** keyp);

    /* Special reclamation functions */
    int (*reclaim_atts_json)(const NC_FILE_INFO_T* file, const NCjson* jatts);

    /* Search functions */
    int (*searchvars)(const NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames);
    int (*searchsubgrps)(const NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* subgrpnames);

} NCZ_Formatter;

#if defined(__cplusplus)
extern "C" {
#endif

/* Called by nc_initialize and nc_finalize respectively */
extern int NCZF_initialize(void);
extern int NCZF_finalize(void);

/* Wrappers for the formatter functions */

extern int NCZF_create(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
extern int NCZF_open(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
extern int NCZF_close(NC_FILE_INFO_T* file);
    
/* Convert NetCDF4 Internal object to JSON */
extern int NCZF_build_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jgroupp);
extern int NCZF_build_superblock(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, NCjson** jsuperp);
extern int NCZF_build_grp_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCindex* dims, NCjson** jdimsp);
extern int NCZF_build_grp_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCindex* vars, NCjson** jvarsp);
extern int NCZF_build_grp_subgroups(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCindex* subgrps, NCjson** jsubgrpsp);
extern int NCZF_build_nczarr_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson* jdims, NCjson* jvars, NCjson* jsubgrps,  NCjson** jnczgrpp);
extern int NCZF_build_group_json(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson* jsuper, NCjson* jatts, NCjson* jtypes, NCjson** jgrpp);
extern int NCZF_build_attributes_json(NC_FILE_INFO_T* file, NC_OBJ* container, NCindex* attlist,NCjson** jattsp, NCjson** jtypesp);
extern int NCZF_build_var_json(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson* jatts, NCjson* jnczvar, NCjson** jvarp);
extern int NCZF_build_nczarr_array(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jnczarrayp);

/* Write JSON to storage */
extern int NCZF_write_grp_json(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jgroup, const NCjson* jatts);
extern int NCZF_write_var_json(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const NCjson* jvar, const NCjson* jatts);

/* Convert JSON to NetCDF4 Internal objects */
extern int NCZF_decode_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jgroupp);
extern int NCZF_decode_superblock(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, NCjson** jsuperp);
extern int NCZF_decode_grp_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NClist* dims, NCjson** jdimsp);
extern int NCZF_decode_grp_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NClist* vars, NCjson** jvarsp);
extern int NCZF_decode_grp_subgroups(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NClist* subgrps, NCjson** jsubgrpsp);
extern int NCZF_decode_nczarr_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* dims, NClist* vars, NClist* subgrps);
extern int NCZF_decode_attributes_json(NC_FILE_INFO_T* file, NC_OBJ* container, NCjson** jattsp, NCjson** jtypesp);
extern int NCZF_decode_var_json(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jvar, NCjson** jatts);
extern int NCZF_decode_nczarr_array(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jnczarrayp);

/* Read JSON to storage */
extern int NCZF_read_grp_json(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jgroupp, NCjson** jattsp, int* constattsp);
extern int NCZF_read_var_json(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jvarp, NCjson** jattsp, int* constattsp);

/* Misc. */
extern int NCZF_dtype2nctype(const NC_FILE_INFO_T* file, const char* dtype, nc_type typehint, nc_type* nctypep, int* endianp, size_t* typelenp);
extern int NCZF_nctype2dtype(const NC_FILE_INFO_T* file, nc_type nctype, int endianness, size_t typelen, char** dtypep, char** daliasp);
extern int NCZF_hdf2codec(const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, NCZ_Filter* filter);
extern int NCZF_buildchunkkey(const NC_FILE_INFO_T* file, size_t rank, const size64_t* chunkindices, char dimsep, char** keyp);

/* Special reclamation functions; These are necessary because V2 attributes are not part of a larger object, but they are for V3. */
extern int NCZF_reclaim_atts_json(const NC_FILE_INFO_T* file, const NCjson* jatts);

/* Search functions */
extern int NCZF_searchvars(const NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames);
extern int NCZF_searchsubgrps(const NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* subgrpnames);

/* Define known dispatch tables and initializers */
/* Each handles a specific NCZarr format + Pure Zarr */
/* WARNING: there is a lot of similar code in the dispatchers,
   so fixes to one may need to be propagated to the other dispatchers.
*/

extern const NCZ_Formatter* NCZ_formatter2; /* NCZarr V2 dispatch table => Zarr V2 */
extern const NCZ_Formatter* NCZ_formatter3; /* NCZarr V3 dispatch table => Zarr V3*/
/**************************************************/

/* Use inference to get map and the formatter */
extern int NCZ_get_map(NC_FILE_INFO_T* file, NCURI* url, mode_t mode, size64_t constraints, void* params, NCZMAP** mapp);
extern int NCZ_get_formatter(NC_FILE_INFO_T* file, const NCZ_Formatter** formatterp);

#if defined(__cplusplus)
}
#endif

#endif /* ZFORMAT_H */
