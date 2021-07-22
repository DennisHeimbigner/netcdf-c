/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/hdf5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* This include file is used if one wished to build a filter plugin
   independent of HDF5. See examples in the plugins directory
*/

#ifndef NETCDF_FILTER_BUILD_H
#define NETCDF_FILTER_BUILD_H 1

#include "netcdf_filter_hdf5_build.h"

/**************************************************/
/* Build To a NumCodecs-style C-API for Filters */

/* Version of the NCZ_codec_t structure */
#define NCZ_CODEC_CLASS_VER 1

/* List of the kinds of NCZ_codec_t formats */
#define NCZ_CODEC_HDF5 1 /* HDF5 <-> Codec converter */

/* Defined flags for filter invocation (not stored); powers of two */
#define NCZ_FILTER_DECODE 0x00000001

/* External Discovery Function */

/*
Obtain a pointer to an instance of NCZ_codec_class_t.

NCZ_get_codec_info(void) --  returns pointer to instance of NCZ_codec_class_t.
			      Instance an be recast based on version+sort to the plugin type specific info.
So the void* return value is typically actually of type NCZ_codec_class_t*.
*/
typedef const void* (*NCZ_get_codec_info_proto)(void);

/* The current object returned by NCZ_get_plugin_info is a
   pointer to an instance of NCZ_codec_t.

The key to this struct are the four function pointers that do setup/cleanup/finalize
and conversion between codec JSON and HDF5 parameters.

Setup context state for the codec converter
int (*NCZ_codec_setup)(int ncid, int varid, void** contextp);

@param ncid -- (in) ncid of the variable's group
@param varid -- (in) varid of the variable
@params contextp -- (out) context for this (var,codec) combination.
@return -- a netcdf-c error code.

Modify the set of HDF5 parameters; called after NCZ_codec_to_hdf5 and after NCZ_codec_setup
int (*NCZ_codec_modify)(void* context, size_t* nparamsp, unsigned** paramsp);

@params context -- (in/out) context from NCZ_codec_setup.
@params nparamsp -- (in/out) number of parameters
@params paramsp -- (in/out) allow setup to modify the number and values of the HDF5 parameters
@return -- a netcdf-c error code.

Reclaim any codec resources from setup. Not same as finalize.
int (*NCZ_codec_cleanup)(void* context);

@param context -- (in) context state

Finalize use of the plugin. Since HDF5 does not provide this functionality,
the codec may need to do it. See H5Zblosc.c for an example.
void (*NCZ_codec_finalize)(void);

@param context -- (in) context state

Convert a JSON representation to an HDF5 representation:
int (*NCZ_codec_to_hdf5)(void* context, const char* codec, size_t* nparamsp, unsigned** paramsp);

@param context -- (in) context state from setup.
@param codec   -- (in) ptr to JSON string representing the codec.
@param nparamsp -- (out) store the length of the converted HDF5 unsigned vector
@param paramsp -- (out) store a pointer to the converted HDF5 unsigned vector;
                  caller frees. Note the double indirection.
@return -- a netcdf-c error code.

Convert an HDF5 representation to a JSON representation
int (*NCZ_hdf5_to_codec)(void* context, size_t nparams, const unsigned* params, char** codecp);

@param context -- (in) context state from setup.
@param nparams -- (in) the length of the HDF5 unsigned vector
@param params -- (in) pointer to the HDF5 unsigned vector.
@param codecp -- (out) store the string representation of the codec; caller must free.
@return -- a netcdf-c error code.
*/

/* QUESTION? do we want to provide a netcdf-specific
  alternative to H5Z_set_local since NCZarr may not have HDF5 access?
  HDF5: herr_t set_local(hid_t dcpl, hid_t type, hid_t space);
  Proposed netcdf equivalent: int NCZ_set_local(int ncid, int varid, size_t* nparamsp, unsigned** paramsp);
  where ncid+varid is equivalent to the space.
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
    unsigned int hdf5id; /* corresponding hdf5 id */
    int (*NCZ_codec_to_hdf5)(void* context, const char* codec, size_t* nparamsp, unsigned** paramsp);
    int (*NCZ_hdf5_to_codec)(void* context, size_t nparams, const unsigned* params, char** codecp);
    int (*NCZ_codec_setup)(int ncid, int varid, void** contextp);
    int (*NCZ_codec_modify)(void* context, size_t* nparamsp, unsigned** paramsp);
    int (*NCZ_codec_cleanup)(void* context);
    void (*NCZ_codec_finalize)(void);
} NCZ_codec_t;

#ifndef NC_UNUSED
#define NC_UNUSED(var) (void)var
#endif

#ifndef DLLEXPORT
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif
#endif

#endif /*NETCDF_FILTER_BUILD_H*/
