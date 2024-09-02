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

#if defined(__cplusplus)
extern "C" {
#endif

EXTERNL int NCZ_plugin_path_initialize(void);
EXTERNL int NCZ_plugin_path_finalize(void);
EXTERNL int NCZ_plugin_path_sync(int formatx);

EXTERNL int NC4_hdf5_plugin_path_initialize(void);
EXTERNL int NC4_hdf5_plugin_path_finalize(void);
EXTERNL int NC4_hdf5_plugin_path_sync(int formatx);

EXTERNL int NCG_plugin_path_getall(size_t* npathsp, char** pathlist, NClist* pluginpaths);
EXTERNL int NCG_plugin_path_getith(size_t index, char** entryp, NClist* pluginpaths);
EXTERNL int NCG_plugin_path_load(const char* paths, NClist* pluginpaths);
EXTERNL int NCG_plugin_path_append(const char* path, NClist* pluginpaths);
EXTERNL int NCG_plugin_path_prepend(const char* path, NClist* pluginpaths);
EXTERNL int NCG_plugin_path_remove(const char* dir, NClist* pluginpaths);

EXTERNL int NC_plugin_path_parse(const char* path0, NClist* list);
EXTERNL const char* NC_plugin_path_stringify(size_t npaths, char** paths);
EXTERNL const char* NC_plugin_path_tostring(void);

#if defined(__cplusplus)
}
#endif

#endif /*NCPLUGINS_H*/
