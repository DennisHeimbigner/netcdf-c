/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/

/*
Common functionality for plugin paths/
For internal use only.
*/

#ifndef NCPLUGINS_H
#define NCPLUGINS_H

/* Opaque */
struct NClist;

/* Define the plugin path management dispatch table */

typedef struct NC_PluginPathDispatch {
    int model; /* one of the NC_FORMATX #'s */
    int dispatch_version;
    int (*initialize)(void** statep, const struct NClist* initialpaths);
    int (*finalize)(void** statep);
    int (*getall)(void* state, size_t* npathsp, char** pathlist);
    int (*getith)(void* state, size_t index, char** entryp);
    int (*load)(void* state, const char* paths);
    int (*append)(void* state, const char* path);
    int (*prepend)(void* state, const char* path);
    int (*remove)(void* state, const char* dir);
} NC_PluginPathDispatch;

#if defined(__cplusplus)
extern "C" {
#endif

EXTERNL int NC_plugin_path_parse(const char* path0, NClist* list);
EXTERNL const char* NC_plugin_path_tostring(size_t npaths, char** paths);

#if defined(__cplusplus)
}
#endif

#endif /*NCPLUGINS_H*/
