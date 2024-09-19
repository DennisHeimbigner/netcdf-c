/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */

/**
 * @file This header file containsplugin related  macros, types, and prototypes for
 * the plugin code in libnczarr. This header should not be included in
 * code outside libnczarr.
 *
 * @author Dennis Heimbigner
 */

#ifndef ZPLUGIN_H
#define ZPLUGIN_H

/* zplugin.c */

/* Pluginlist management */

/* Opaque Handles */
struct H5Z_class2_t;
struct NCZ_codec_t;
struct NCPSharedLib;
struct NCproplist;

/* Hold the loaded filter plugin information */
typedef struct NCZ_Plugin {
    int incomplete;
    struct HDF5API {
        const struct H5Z_class2_t* filter;
        struct NCPSharedLib* hdf5lib; /* source of the filter */
    } hdf5;
    struct CodecAPI {
	int defaulted; /* codeclib was a defaulting library */
	int ishdf5raw; /* The codec is the hdf5raw codec */
	const struct NCZ_codec_t* codec;
	struct NCPSharedLib* codeclib; /* of the codec; null if same as hdf5 */
    } codec;
} NCZ_Plugin;

int NCZ_load_all_plugins(void);
int NCZ_plugin_loaded(size_t filterid, NCZ_Plugin** pp);
int NCZ_plugin_loaded_byname(const char* name, NCZ_Plugin** pp);
int NCZ_unload_plugin(NCZ_Plugin* plugin);

int NCZ_initialize(void** statep, struct NCproplist* plist);
int NCZ_finalize(void** statep);
int NCZ_setproperties(void* state, struct NCproplist*);

int NCZ_plugin_path_initialize(void* state, NCproplist* plist);
int NCZ_plugin_path_finalize(void* state);
int NCZ_plugin_path_get(void* statep,size_t*,char**);
int NCZ_plugin_path_set(void* statep,size_t,char** const);

#endif /*ZPLUGIN_H*/

