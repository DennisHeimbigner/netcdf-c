/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

/* This include file is used if one wished to build a filter plugin
   independent of HDF5. See examples in the plugins directory
*/

#ifndef NETCDF_FILTER_BUILD_H
#define NETCDF_FILTER_BUILD_H 1

/**************************************************/
/* Build To the HDF5 C-API for Filters */


#ifdef H5_VERS_INFO

#include <hdf5.h>
/* Older versions of the hdf library may define H5PL_type_t here */
#include <H5PLextern.h>

#else /*!defined H5_VERS_INFO*/ /* Provide replacement definitions */

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

/* H5Z_FILTER_RESERVED => H5Z_FILTER_RESERVED */
#define H5Z_FILTER_RESERVED 256 /*filter ids below this value are reserved for library use */

/* H5Z_FILTER_MAX => H5Z_FILTER_MAX */
#define H5Z_FILTER_MAX 65535 /*maximum filter id */

/* Only a limited set of definition and invocation flags are allowed */
/* Additional flags for filter invocation (not stored) */
#define H5Z_FILTER_REVERSE	0x0100	/*reverse direction; read	*/

/* htri_t (*H5Z_can_apply_func_t)(hid_t dcpl_id, hid_t type_id, hid_t space_id) => currently not supported; must be NULL. */
typedef int (*H5Z_filter_can_apply_func_t)(long long, long long, long long);

/* herr_t (*H5Z_set_local_func_t)(hid_t dcpl_id, hid_t type_id, hid_t space_id); => currently not supported; must be NULL. */
typedef int (*H5Z_filter_set_local_func_t)(long long, long long, long long);

/* H5Z_funct_t => H5Z_filter_func_t */
typedef size_t (*H5Z_filter_func_t)(unsigned int flags, size_t cd_nelmts,
			     const unsigned int cd_values[], size_t nbytes,
			     size_t *buf_size, void **buf);

/* H5Z_CLASS_T_VERS => H5Z_FILTER_CLASS_VERS */
#define H5Z_FILTER_CLASS_VER 1

/*
 * The filter table maps filter identification numbers to structs that
 * contain a pointers to the filter function and timing statistics.
 */
typedef struct H5Z_filter_class {
    int version;                    /* Version number of the struct; should be H5Z_FILTER_CLASS_VER */
    int id;		            /* Filter ID number                             */
    unsigned encoder_present;       /* Does this filter have an encoder?            */
    unsigned decoder_present;       /* Does this filter have a decoder?             */
    const char *name;               /* Comment for debugging                        */
    H5Z_filter_can_apply_func_t can_apply; /* The "can apply" callback for a filter        */
    H5Z_filter_set_local_func_t set_local; /* The "set local" callback for a filter        */
    H5Z_filter_func_t filter;              /* The actual filter function                   */
} H5Z_filter_class;

/* The HDF5/H5Zarr dynamic loader looks for the following:*/

/* Plugin type used by the plugin library */
typedef enum H5PL_plugin_type_t {
    H5PL_TYPE_ERROR         = -1,   /* Error                */
    H5PL_TYPE_FILTER        =  0,   /* Filter               */
    H5PL_TYPE_NONE          =  1    /* This must be last!   */
} H5Z_plugin_type_t;

/* Following External Discovery Functions should be present for the dynamic loading of filters */

/* returns specific constant H5ZP_TYPE_FILTER */
typedef H5Z_plugin_type_t (*H5Z_get_plugin_type_proto)(void);

/* return <pointer to instance of H5Z_filter_class> */
typedef const void* (*H5Z_get_plugin_info_proto)(void);

#endif /*H5_VERS_INFO*/

/**************************************************/
/* Build To a NumCodecs-style C-API for Filters */

/* Version of the NCZ_codec_t structure */
#define NCZ_CODEC_CLASS_VER 1

/* List of the kinds of NCZ_codec_t formats */
#define NCZ_CODEC_HDF5 1 /* HDF5 <-> Codec converter */

/* Defined flags for filter invocation (not stored); powers of two */
#define NCZ_FILTER_DECODE 0x00000001

/* External Discovery Function */

/* NCZ_get_codec_info(void) --  returns pointer to instance of NCZ_codec_class_t or NULL.
				Can be recast based on version+sort to the plugin type specific info.
*/
typedef const void* (*NCZ_get_codec_info_proto)(void);

/* The current object returned by NCZ_get_codec_info is a
   pointer to an instance of NCZ_codec_t.

The key to this struct is the two function pointers that do the conversion between codec JSON and HDF5 parameters.

Convert a JSON representation to an HDF5 representation:
int (*NCZ_codec_to_hdf5)(const char* codec, int* nparamsp, unsigned** paramsp);

@param codec -- (in) ptr to JSON string representing the codec.
@param nparamsp -- (out) store the length of the converted HDF5 unsigned vector
@param paramsp -- (out) store a pointer to the converted HDF5 unsigned vector;
                  caller frees. Note the double indirection.
@return -- a netcdf-c error code.


Convert an HDF5 representation to a JSON representation
int (*NCZ_hdf5_to_codec)(int nparamsp, const unsigned* paramsp, char** codecp);
@param nparams -- (in) the length of the HDF5 unsigned vector
@param params -- (in) pointer to the HDF5 unsigned vector.
@param codecp -- (out) store the string representation of the codec; caller must free.
@return -- a netcdf-c error code.
*/

/*
The struct that provides the necessary filter info.
The combination of version + sort uniquely determines
the format of the remainder of the struct
*/
typedef struct NCZ_codec_t {
    int version; /* Version number of the struct */
    int sort; /* Format of remainder of the struct;
                 Currently always NCZ_CODEC_HDF5 */
    const char* codecid;            /* The name/id of the codec */
    const unsigned int hdf5id; /* corresponding hdf5 id */
    int (*NCZ_codec_to_hdf5)(const char* codec, int* nparamsp, unsigned* paramsp);
    int (*NCZ_hdf5_to_codec)(int nparams, unsigned* params, char** codecp);
} NCZ_codec_t;

#ifndef NC_UNUSED
#define NC_UNUSED(var) (void)var
#endif

#endif /*NETCDF_FILTER_BUILD_H*/

