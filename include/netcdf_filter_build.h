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
/* Build To a NumCodecs-stype C-API for Filters */

#define NCZ_PLUGIN_CLASS_VER 1

/* Plugin sorts */
#define NCZ_PLUGIN_CODEC 1	/* Codec */

#define NCZ_CODEC_CLASS_VER 1

/* Defined flags for filter invocation (not stored); powers of two */
#define H5Z_FILTER_DECODE 0x00000001

/* return pointer to instance of NCZ_plugin_class_t or NULL */
/* Can be recast based on sort to the plugin type specific info */
typedef const void* (*NCZ_get_plugin_info)(void);

/* Setup takes two arguments:
1. json_codec -- a string containing the JSON codec representation of id plus parameters (in)
2. statep -- a pointer into which is returned arbitrary state; state is allocated by this code; may be NULL if unneeded (out)
This function may be called multiple times.
*/
typedef int (*NCZ_codec_func_setup_t)(const char* json_codec, void** statep);

/* Shutdown takes two arguments:
1. state -- a pointer to final state; state must be reclaimed (in)
2. jsonp - a pointer into which a string containing the final JSON codec representation of id plus parameters;
   NULL result implies no change (out).
*/
typedef int (*NCZ_codec_func_shutdown_t)(void* state, char** jsonp);

/* Eval takes following arguments:
1. statep -- a pointer to pointer to arbitrary state: in/out; state may be modified by this function.
2. flags -- indicate if encoding or decoding; other flags not yet defined
3. allocp -- allocated size of the buffer (in/out)
4. usedp -- how much of buffer holds real data (in/out)
5. bufferp -- buffer of data (in/out)
Ideally the eval function will use the input buffer to store compressed/uncompressed data.
If necessary, the input buffer can be reclaimed and replaced with new output buffer,
with matching changes to allocp and usedp.
*/
typedef int (*NCZ_codec_func_eval_t)(void** state, unsigned flags, size_t* allocp, size_t* usedp, void** bufferp);

/*
In C, a form of pseudo subclassing is possible in that a struct can legally
be cast to the first field of the struct.
So all of the plugins return a pointer to an instance of NCZ_plugin_class_t
that can then be re-cast to be an instance of the containing struct.
*/
typedef struct NCZ_plugin_class_t {
    int version;	/* Version number of the struct; should be NCZ_PLUGIN_CLASS_VER */
    int sort;		/* What kind of plugin: see list above */
} NCZ_plugin_class_t;

typedef struct NCZ_codec_class_t {
    NCZ_plugin_class_t hdr;		/* All plugins begin with this for pseudo-subclassing */
    int version;			/* Version number of the struct; should be NCZ_CODEX_CLASS_VER */
    char* id;				/* The name/id of the codec */
    NCZ_codec_func_setup_t setup;	/* setup -- may be invoked multiple times with different parameters */
    NCZ_codec_func_shutdown_t shutdown;	/* shutdown -- optionally return final json for codec */
    NCZ_codec_func_eval_t codec;	/* The actual encode/decode function */
} NCZ_codec_class_t;

#endif /*NETCDF_FILTER_BUILD_H*/

