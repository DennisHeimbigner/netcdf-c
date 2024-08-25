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

/*
Unified plugin related code
*/

/**************************************************/
/* Plugin-path API */ 

/**
 * Return the current sequence of directories in the internal plugin path list.
 *
 * @param ncid ID ignored
 * @param npaths return the number of paths in the path list
 * @param pathlist return the sequence of directies in the path list
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
 *
 * As a rule, this function needs to be called twice.
 * The first time with npaths not NULL and pathlist set to NULL
 *     to get the size of the path list.
 * The second time with pathlist not NULL to get the actual sequence of paths.
*/

EXTERNL int
nc_plugin_path_list(int ncid, size_t* npaths, char** pathlist)
{
    int stat = NC_NOERR;
    NC* ncp;

    stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    if((stat = ncp->dispatch->plugin_path_list(ncid,npaths,pathlist))) goto done;
done:
    return stat;
}

/**
 * Append a directory to the end of the current internal path list.
 *
 * @param ncid ID ignored
 * @param dir directory to append.
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_append(int ncid, const char* path)
{
    int stat = NC_NOERR;
    NC* ncp;

    stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    if((stat = ncp->dispatch->plugin_path_append(ncid,path))) goto done;
done:
    return stat;
}

/**
 * Prepend a directory to the front of the current internal path list.
 *
 * @param ncid ID ignored
 * @param dir directory to prepend
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_prepend(int ncid, const char* path)
{
    int stat = NC_NOERR;
    NC* ncp;

    stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    if((stat = ncp->dispatch->plugin_path_prepend(ncid,path))) goto done;
done:
    return stat;
}

/**
 * Remove all occurrences of a directory from the internal path sequence
 *
 * @param ncid ID ignored
 * @param dir directory to prepend
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_remove(int ncid, const char* dir)
{
    int stat = NC_NOERR;
    NC* ncp;

    stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    if((stat = ncp->dispatch->plugin_path_remove(ncid,dir))) goto done;
done:
    return stat;
}

/**
 * Empty the current internal path sequence
 * and replace with the sequence of directories
 * parsed from the paths argument.
 * The path argument has the following syntax:
 *    paths := <empty> | dirlist
 *    dirlist := dir | dirlist separator dir
 *    separator := ';' | ':'
 *    dir := <OS specific directory path>
 * Note that the ':' separator is not legal on Windows machines.
 * The ';' separator is legal on all machines.
 *
 * @param ncid ID ignored
 * @param paths to overwrite the current internal path list
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
 *
 * In effect, this is sort of bulk loader for the path list.
*/

EXTERNL int
nc_plugin_path_load(int ncid, const char* paths)
{
    int stat = NC_NOERR;
    NC* ncp;

    stat = NC_check_id(ncid,&ncp);
    if(stat != NC_NOERR) return stat;
    if((stat = ncp->dispatch->plugin_path_load(ncid,paths))) goto done;
done:
    return stat;
}

int
nc_parse_plugin_pathlist(const char* path0, NClist* list)
{
    int i,stat = NC_NOERR;
    char* path = NULL;
    char* p;
    int count;
    size_t plen;
#ifdef _WIN32
    const char* seps = ";";
#else
    const char* seps = ";:";
#endif    

    if(path0 == NULL || path0[0] == '\0') goto done;
    if(list == NULL) {stat = NC_EINVAL; goto done;}
    plen = strlen(path0);
    if((path = malloc(plen+1+1))==NULL) {stat = NC_ENOMEM; goto done;}
    memcpy(path,path0,plen);
    path[plen] = '\0'; path[plen+1] = '\0';  /* double null term */
    for(count=0,p=path;*p;p++) {
	if(strchr(seps,*p) != NULL) {*p = '\0'; count++;}
    }
    count++; /* for last piece */
    for(p=path,i=0;i<count;i++) {
	size_t len = strlen(p);
	if(len > 0)
	    nclistpush(list,strdup(p));
        p = p+len+1; /* point to next piece */
    }

done:
    nullfree(path);
    return stat;
}

