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

EXTERNL int NCZ_plugin_path_get(size_t* ndirsp, char** dirs);
EXTERNL int NCZ_plugin_path_set(size_t ndirs, char** const dirs);

EXTERNL int NC4_hdf5_plugin_path_initialize(void);
EXTERNL int NC4_hdf5_plugin_path_finalize(void);

EXTERNL int NC4_hdf5_plugin_path_get(size_t* ndirsp, char** dirs);
EXTERNL int NC4_hdf5_plugin_path_set(size_t ndirs, char** const dirs);


#if defined(__cplusplus)
}
#endif

#endif /*NCPLUGINS_H*/
