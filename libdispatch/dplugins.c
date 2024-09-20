/**************************************************/
/* Global state plugin path implementation */

/*
 * Copyright 2018, University Corporation for Atmospheric Research
 * See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */
/**
 * @file
 * Functions for working with plugins. 
 */

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef _MSC_VER
#include <io.h>
#endif

#include "netcdf.h"
#include "netcdf_filter.h"
#include "ncdispatch.h"
#include "nc4internal.h"
#include "nclog.h"
#include "ncbytes.h"
#include "ncplugins.h"
#include "netcdf_aux.h"

/*
Unified plugin related code
*/
/**************************************************/
/* Plugin-path API */ 

/* list of environment variables to check for plugin roots */
#define PLUGIN_ENV "HDF5_PLUGIN_PATH"
#define PLUGIN_DIR_UNIX "/usr/local/hdf5/plugin"
#define PLUGIN_DIR_WIN "%s/hdf5/lib/plugin"
#define WIN32_ROOT_ENV "ALLUSERSPROFILE"

/* Control path verification */
#define PLUGINPATHVERIFY "NC_PLUGIN_PATH_VERIFY"

/* Initializers/Finalizers for the implementations */
#ifdef NETCDF_ENABLE_HDF5
extern int NC4_hdf5_plugin_path_finalize(void);
#endif
#ifdef NETCDF_ENABLE_NCZARR
extern int NCZ__plugin_path_finalize(void);
#endif

static int NC_plugin_path_initialized = 0;
static int NC_plugin_path_verify = 1;

/**
 * This function is called as part of nc_initialize.
 * Its purpose is to initialize the plugin paths state.
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_initialize(void)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NULL;
    char* defaultpluginpath = NULL;
    const char* pluginroots = NULL;
    NClist* dirs = NULL;
    size_t ndirs;

    if(!NC_initialized) nc_initialize();
    if(NC_plugin_path_initialized != 0) goto done;
    NC_plugin_path_initialized = 1;

    if(getenv(PLUGINPATHVERIFY) != NULL) NC_plugin_path_verify = 1;

    gs = NC_getglobalstate();
    dirs = nclistnew();

   /* Setup the plugin path default */
   {
#ifdef _WIN32
	const char* win32_root;
	char dfalt[4096];
	win32_root = getenv(WIN32_ROOT_ENV);
	if(win32_root != NULL && strlen(win32_root) > 0) {
	    snprintf(dfalt,sizeof(dfalt),PLUGIN_DIR_WIN,win32_root);
	    defaultpluginpath = strdup(dfalt);
	}
#else /*!_WIN32*/
	defaultpluginpath = strdup(PLUGIN_DIR_UNIX);
#endif
    }

    /* Find the plugin directory root(s) */
    pluginroots = getenv(PLUGIN_ENV); /* Usually HDF5_PLUGIN_PATH */
    if(pluginroots  != NULL && strlen(pluginroots) == 0) pluginroots = NULL;
    if((stat = ncaux_plugin_path_parse(pluginroots,'\0',&ndirs,NULL))) goto done;
    if(ndirs > 0) {
        nclistsetlength(dirs,ndirs);
        if((stat = ncaux_plugin_path_parse(pluginroots,'\0',&ndirs,(char**)nclistcontents(dirs)))) goto done;
    }
    /* Add the default to end of the dirs list if not already there */
    if(defaultpluginpath != NULL && !nclistmatch(dirs,defaultpluginpath,0)) {
        nclistpush(dirs,defaultpluginpath);
	defaultpluginpath = NULL;
    }

    /* Set the current plugin dirs sequence */
    assert(gs->pluginpaths == NULL);
    gs->pluginpaths = dirs; dirs = NULL;

    /* Sync to the actual implementations */
#ifdef NETCDF_ENABLE_NCZARR
    if((stat = NCZ_plugin_path_initialize())) goto done;    
#endif
#ifdef USE_HDF5
    if((stat = NC4_hdf5_plugin_path_initialize())) goto done;
#endif

done:
    nullfree(defaultpluginpath);
    nclistfreeall(dirs);
    return NCTHROW(stat);
}

/**
 * This function is called as part of nc_finalize()
 * Its purpose is to clean-up plugin path state.
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

int
nc_plugin_path_finalize(void)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    if(NC_plugin_path_initialized == 0) goto done;
    NC_plugin_path_initialized = 0;

    NC_plugin_path_verify = 0;

    /* Finalize the actual implementatios */
#ifdef NETCDF_ENABLE_NCZARR
    if((stat = NCZ_plugin_path_finalize())) goto done;    
#endif
#ifdef USE_HDF5
    if((stat = NC4_hdf5_plugin_path_finalize())) goto done;
#endif

    nclistfreeall(gs->pluginpaths); gs->pluginpaths = NULL;
done:
    return NCTHROW(stat);
}

/**
 * Return the current sequence of directories in the internal plugin path list.
 * Since this function does not modify the plugin path, it can be called at any time.
 * @param ndirsp return the number of dirs in the internal path list
 * @param dirs memory for storing the sequence of directies in the internal path list.
 * @return NC_NOERR
 * @author Dennis Heimbigner
 *
 * As a rule, this function needs to be called twice.
 * The first time with npaths not NULL and pathlist set to NULL
 *     to get the size of the path list.
 * The second time with pathlist not NULL to get the actual sequence of paths.
*/

int
nc_plugin_path_get(size_t* ndirsp, char** dirs)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();
    size_t ndirs;

    if(gs->pluginpaths == NULL) gs->pluginpaths = nclistnew(); /* suspenders and belt */
    ndirs = nclistlength(gs->pluginpaths);
    if(ndirsp) *ndirsp = ndirs;
    if(dirs != NULL && ndirs > 0) {
	size_t i;
	for(i=0;i<ndirs;i++) {
	    const char* dir = (const char*)nclistget(gs->pluginpaths,i);
	    dirs[i] = nulldup(dir);
	}
    }

    /* Verify that the implementation plugin paths are consistent */
    if(NC_plugin_path_verify) {
#ifdef NETCDF_ENABLE_HDF5
	{
	    size_t i,impl_ndirs;
	    char** impl_dirs = NULL;
	    NC4_hdf5_plugin_path_get(&impl_ndirs,NULL);
	    assert(impl_ndirs == ndirs);
	    if(dirs != NULL) {
		impl_dirs = (char**)calloc(ndirs,sizeof(char*)); /* Assume verify will succeed */
	        NC4_hdf5_plugin_path_get(&impl_ndirs,impl_dirs);
	        for(i=0;i<ndirs;i++) {
		    assert(strcmp(dirs[i],impl_dirs[i])==0);
		    nullfree(impl_dirs[i]);
		}
	    }
	    nullfree(impl_dirs);
	}
#endif /*NETCDF_ENABLE_HDF5*/
#ifdef NETCDF_ENABLE_HDF5
	{
	    size_t i,impl_ndirs;
	    char** impl_dirs = NULL;
	    NC4_hdf5_plugin_path_get(&impl_ndirs,NULL);
	    assert(impl_ndirs == ndirs);
	    if(dirs != NULL) {
		impl_dirs = (char**)calloc(ndirs,sizeof(char*)); /* Assume verify will succeed */
		NC4_hdf5_plugin_path_get(&impl_ndirs,impl_dirs);
		for(i=0;i<ndirs;i++) {
		    assert(strcmp(dirs[i],impl_dirs[i])==0);
		    nullfree(impl_dirs[i]);
		}
	    }
	    nullfree(impl_dirs);
        }
#endif /*NETCDF_ENABLE_HDF5*/
    }

    return NCTHROW(stat);
}

/**
 * Empty the current internal path sequence
 * and replace with the sequence of directories argument.
 *
 * Using a dirs argument of NULL or ndirs argument of 0 will clear the set of plugin dirs.
 * @param ndirs length of the dirs argument
 * @param dirs to overwrite the current internal dir list
 * @return NC_NOERR
 * @author Dennis Heimbigner
*/

int
nc_plugin_path_set(size_t ndirs, char** const dirs)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    /* Clear the current dir list */
    nclistfreeall(gs->pluginpaths);
    gs->pluginpaths = nclistnew();

    if(ndirs > 0) {
	size_t i;
        assert(gs->pluginpaths != NULL);
	for(i=0;i<ndirs;i++) {
	    nclistpush(gs->pluginpaths,nulldup(dirs[i]));
	}
    }

    /* Sync the global plugin path set to the individual implementations */
#ifdef NETCDF_ENABLE_HDF5
    if((stat = NC4_hdf5_plugin_path_set(ndirs,dirs))) goto done;
#endif
#ifdef NETCDF_ENABLE_NCZARR
    if((stat = NCZ_plugin_path_set(ndirs,dirs))) goto done;
#endif

done:
    return NCTHROW(stat);
}
