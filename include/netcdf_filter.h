/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

/*
 * In order to use any of the netcdf_XXX.h files, it is necessary
 * to include netcdf.h followed by any netcdf_XXX.h files.
 * Various things (like EXTERNL) are defined in netcdf.h
 * to make them available for use by the netcdf_XXX.h files.
*/

#ifndef NETCDF_FILTER_H
#define NETCDF_FILTER_H 1

/* API for libdispatch/dfilter.c */

/* Must match values in <H5Zpublic.h> */
#ifndef H5Z_FILTER_DEFLATE
#define H5Z_FILTER_DEFLATE 1
#endif
#ifndef H5Z_FILTER_SZIP
#define H5Z_FILTER_SZIP 4
#define H5_SZIP_ALLOW_K13_OPTION_MASK   1
#define H5_SZIP_CHIP_OPTION_MASK        2
#define H5_SZIP_EC_OPTION_MASK          4
#define H5_SZIP_NN_OPTION_MASK          32
#define H5_SZIP_MAX_PIXELS_PER_BLOCK    32

#define NC_SZIP_EC 4  /**< Selects entropy coding method for szip. */
#define NC_SZIP_NN 32 /**< Selects nearest neighbor coding method for szip. */
#endif

#define H5_SZIP_ALL_MASKS (H5_SZIP_CHIP_OPTION_MASK|H5_SZIP_EC_OPTION_MASK|H5_SZIP_NN_OPTION_MASK)

/** The maximum allowed setting for pixels_per_block when calling nc_def_var_szip(). */
#define NC_MAX_PIXELS_PER_BLOCK 32

#if defined(__cplusplus)
extern "C" {
#endif

/* Define the formats for NC_FILTER classes as aliases for NC_FORMATX_XXX*/
#define NC_FILTER_FORMAT_HDF5 (NC_FORMATX_NC_HDF5)
#define NCX_FILTER_FORMAT (NC_FORMATX_NCZARR)

/* Define a Header Object for all filter-related objects */

/* provide a common generic struct field */
/*
    format indicates e.g. HDF5|NCZARR
    sort indicates the "subclass" of the superclass
*/
typedef struct NC_Filterobject {int format;} NC_Filterobject;

/* Generic version of Filterspec */
typedef struct NC_Filterspec {
    NC_Filterobject  hdr;    /**< e.g. NC_FILTER_FORMAT_HDF5 */
} NC_Filterspec;

/**************************************************/
/* HDF5 Specific filter functions (Deprecated) */

/*Define a filter for a variable */
EXTERNL int
nc_def_var_filter(int ncid, int varid, unsigned int id, size_t nparams, const unsigned int* parms);

/* Learn about the first defined filter filter on a variable */
EXTERNL int
nc_inq_var_filter(int ncid, int varid, unsigned int* idp, size_t* nparams, unsigned int* params);

/* Support inquiry about all the filters associated with a variable */
/* As is usual, it is expected that this will be called twice: 
   once to get the number of filters, and then a second time to read the ids */
EXTERNL int nc_inq_var_filterids(int ncid, int varid, size_t* nfilters, unsigned int* filterids);

/* Learn about the filter with specified id wrt a variable */
EXTERNL int
nc_inq_var_filter_info(int ncid, int varid, unsigned int id, size_t* nparams, unsigned int* params);

/* Remove filter from variable*/
EXTERNL int nc_var_filter_remove(int ncid, int varid, unsigned int id);

/* HDF5 specific filter info */
typedef struct NC4_Filterspec {
    NC_Filterspec hdr;
    /* HDF5 specific extensions */
    unsigned int filterid; /**< ID for arbitrary filter. */
    size_t nparams;        /**< nparams for arbitrary filter. */
    unsigned int* params;  /**< Params for arbitrary filter. */
} NC4_Filterspec;

EXTERNL void NC4_filterfix8(unsigned char* mem, int decode);

EXTERNL int NC_parsefilterlist(const char* listspec, int* formatp, size_t* nfilters, NC_Filterspec*** filtersp);
EXTERNL int NC_parsefilterspec(const char* txt, int format, NC_Filterspec** specp);

/* Support direct user defined filters if enabled during configure;
   last arg is void*, but is actually H5Z_class2_t*.
   It is void* to avoid having to reference hdf.h.
*/
EXTERNL int nc_filter_client_register(unsigned int id, void*/*H5Z_class2_t* */);
EXTERNL int nc_filter_client_unregister(unsigned int id);
EXTERNL int nc_filter_client_inq(unsigned int id, void*/*H5Z_class2_t* */);

/* End HDF5 Specific Declarations */

/**************************************************/
/* X (String-based extension) Declarations */

/*Define a filter for a variable */
EXTERNL int
nc_def_var_filterx(int ncid, int varid, const char* id, const char* params);

/* Support inquiry about all the filters associated with a variable */
/* As is usual, it is expected that this will be called twice: 
   once to get the number of filters, and then a second time to read the ids */
EXTERNL int nc_inq_var_filteridsx(int ncid, int varid, size_t* nfilters, char*** filteridsp);

/* Learn about the filter with specified id wrt a variable */
EXTERNL int
nc_inq_var_filter_infox(int ncid, int varid, const char* id, char** paramsp);

/* Remove filter from variable*/
EXTERNL int nc_var_filter_removex(int ncid, int varid, const char* id);

/* String specific filter info */
typedef struct NCX_Filterspec {
    NC_Filterspec hdr;
    char* filterid; /**< ID for arbitrary filter. */
    char* params;   /**< Params for arbitrary filter. */
} NCX_Filterspec;

EXTERNL int NC_parsefilterlist(const char* listspec, int* formatp, size_t* nfilters, NC_Filterspec*** filtersp);
EXTERNL int NC_parsefilterspec(const char* txt, int format, NC_Filterspec** specp);

EXTERNL void NC4_filterfix8(unsigned char* mem, int decode);

/* Support direct user defined filters if enabled during configure;
   last arg is void*, but is actually H5Z_class2_t*.
   It is void* to avoid having to reference hdf.h.
*/
EXTERNL int nc_filter_client_registerx(const char* id, void*);
EXTERNL int nc_filter_client_unregisterx(const char* id);
EXTERNL int nc_filter_client_inqx(const char* id, void*);

/* End X (String-based extension) Declarations */

/**************************************************/

/* Set szip compression for a variable. */
EXTERNL int nc_def_var_szip(int ncid, int varid, int options_mask, int pixels_per_block);

#if defined(__cplusplus)
}
#endif

#endif /* NETCDF_FILTER_H */
