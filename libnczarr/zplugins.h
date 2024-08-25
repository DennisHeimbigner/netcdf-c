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

/* list of environment variables to check for plugin roots */
#define PLUGIN_ENV "HDF5_PLUGIN_PATH"
#define PLUGIN_DIR_UNIX "/usr/local/hdf5/plugin"
#define PLUGIN_DIR_WIN "%s/hdf5/lib/plugin"
#define WIN32_ROOT_ENV "ALLUSERSPROFILE"

/* zplugin.c */

/* Pluginlist management */

/* Opaque Handles */
struct H5Z_class2_t;
struct NCZ_codec_t;
struct NCPSharedLib;

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

int NCZ_plugin_path_initialize(void);
int NCZ_plugin_path_finalize(void);
int NCZ_plugin_path_list(int ncid, size_t* npathsp, char** pathlist);
int NCZ_plugin_path_append(int ncid, const char* path);
int NCZ_plugin_path_prepend(int ncid, const char* path);
int NCZ_plugin_path_remove(int ncid, const char* dir);
int NCZ_plugin_path_load(int ncid, const char* paths);
int NCZ_load_all_plugins(void);
int NCZ_plugin_loaded(size_t filterid, NCZ_Plugin** pp);
int NCZ_plugin_loaded_byname(const char* name, NCZ_Plugin** pp);
#if 0
int NCZ_load_plugin(const char* path, NCZ_Plugin** plugp);
int NCZ_unload_plugin(NCZ_Plugin* plugin);
int NCZ_addplugin(NC_FILE_INFO_T*, NC_VAR_INFO_T* var, unsigned int id, size_t nparams, const unsigned int* params);
int NCZ_plugin_setup(NC_VAR_INFO_T* var);
int NCZ_plugin_freelists(NC_VAR_INFO_T* var);
int NCZ_codec_freelist(NCZ_VAR_INFO_T* zvar);
int NCZ_applypluginchain(const NC_FILE_INFO_T*, NC_VAR_INFO_T*, NClist* chain, size_t insize, void* indata, size_t* outlen, void** outdata, int encode);
int NCZ_plugin_jsonize(const NC_FILE_INFO_T*, const NC_VAR_INFO_T*, NCZ_Plugin* plugin, struct NCjson**);
int NCZ_plugin_build(const NC_FILE_INFO_T*, NC_VAR_INFO_T* var, const NCjson* jplugin, int chainindex);
int NCZ_codec_attr(const NC_VAR_INFO_T* var, size_t* lenp, void* data);
#endif
#endif /*ZPLUGIN_H*/

