/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

/* This include file is used if one wished to build a filter plugin
   independent of HDF5. See examples in the plugins directory
*/

#include <netcdf_filter.h>

#ifndef NETCDF_FILTER_BUILD_H
#define NETCDF_FILTER_BUILD_H 1

/**************************************************/
/* WARNING: In order make NCZARR independent of HDF5,
   while still using HDF5-style filters, some HDF5
   declarations need to be duplicated here with
   different names. Watch out for changes in
   the underlying HDF5 declarations.

   See the file H5Zpublic.h for more detailed descriptions.

Note that these declarations are always enabled because
HDF5-style filters may have been created with these definitions
but for use by HDF5.
*/

/* H5Z_FILTER_RESERVED => NCZ_FILTER_RESERVED */
#define NC_FILTER_RESERVED 256 /*filter ids below this value are reserved for library use */

/* H5Z_FILTER_MAX => NCZ_FILTER_MAX */
#define NCZ_FILTER_MAX 65535 /*maximum filter id */

/* Only a limited set of definition and invocation flags are allowed */
/* Additional flags for filter invocation (not stored) */
#define NCZ_FILTER_REVERSE	0x0100	/*reverse direction; read	*/

/* htri_t (*H5Z_can_apply_func_t)(hid_t dcpl_id, hid_t type_id, hid_t space_id) => currently not supported; must be NULL. */
typedef int (*NCZ_filter_can_apply_func_t)(long long, long long, long long);

/* herr_t (*H5Z_set_local_func_t)(hid_t dcpl_id, hid_t type_id, hid_t space_id); => currently not supported; must be NULL. */
typedef int (*NCZ_filter_set_local_func_t)(long long, long long, long long);

/* H5Z_funct_t => NCZ_filter_func_t */
typedef size_t (*NCZ_filter_func_t)(unsigned int flags, size_t cd_nelmts,
			     const unsigned int cd_values[], size_t nbytes,
			     size_t *buf_size, void **buf);

/* H5Z_CLASS_T_VERS => NCZ_FILTER_CLASS_VERS */
#define NCZ_FILTER_CLASS_VER 1

/*
 * The filter table maps filter identification numbers to structs that
 * contain a pointers to the filter function and timing statistics.
 */
typedef struct NCZ_filter_class {
    int version;                    /* Version number of the struct; should be NCZ_FILTER_CLASS_VER */
    int id;		            /* Filter ID number                             */
    unsigned encoder_present;       /* Does this filter have an encoder?            */
    unsigned decoder_present;       /* Does this filter have a decoder?             */
    const char *name;               /* Comment for debugging                        */
    NCZ_filter_can_apply_func_t can_apply; /* The "can apply" callback for a filter        */
    NCZ_filter_set_local_func_t set_local; /* The "set local" callback for a filter        */
    NCZ_filter_func_t filter;              /* The actual filter function                   */
} NCZ_filter_class;

/* The HDF5/NCZarr dynamic loader looks for the following:*/

/* H5PL_type_t => NCZ_plugin_type_t */
/* Plugin type used by the plugin library */
typedef enum NCZ_plugin_type_t {
    NCZP_TYPE_ERROR         = -1,   /* Error                */
    NCZP_TYPE_FILTER        =  0,   /* Filter               */
    NCZP_TYPE_NONE          =  1    /* This must be last!   */
} NCZ_plugin_type_t;

/* Following External Discovery Functions should be present for the dynamic loading of filters */

/* returns specific constant NCZP_TYPE_FILTER */
typedef NCZ_plugin_type_t (*NCZ_get_plugin_type_proto)(void);

/* return <pointer to instance of NCZ_filter_class> */
typedef const void* (*NCZ_get_plugin_info_proto)(void);

#endif /*NETCDF_FILTER_BUILD_H*/

