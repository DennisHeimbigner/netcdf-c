/* This is part of the netCDF package.
   Copyright 2018 University Corporation for Atmospheric Research/Unidata
   See COPYRIGHT file for conditions of use.

   Test RC Glob Matching
   Dennis Heimbigner
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "netcdf.h"
#include "ncrc.h"

#undef DEBUG

#ifndef nullfree
#define nullfree(x) {if((x)!=NULL) free(x);}
#endif

#ifdef DEBUG
static void
printrc(NCglobalstate* ngs)
{
    size_t i,nentries = 0;
    NCRCinfo* info = NULL;
    NCRCentry* entry = NULL;

    info = ngs->rcinfo;
    if(info->ignore) {
	fprintf(stderr,".rc ignored\n");
	return;
    }

    /* Print out the .rc entries */
    if((nentries = NC_rcfile_length(info))==0) {
        printf("<empty>\n");
	exit(0);
    }
    for(i=0;i<nentries;i++) {
	entry = NC_rcfile_ith(info,i);
	if(entry == NULL) abort();
        if(entry->host != NULL) {
	    printf("[%s ",entry->host);
            if(entry->urlpath != NULL)
	        printf("/%s] ",entry->urlpath);
	    printf("]");					
        }
	printf("|%s|->|%s|\n",entry->key,entry->value);
    }
}
#endif /*DEBUG*/

static void
usage(const char* msg, int err);
{
    fprintf(stderr,"Usage: tst_rcglob <key> [<search url>]\n");
    if(msg != NULL && strlen(msg) > 0) fprintf(stderr,"Error: %s\n",msg);
    if(err != 0)
        fprintf(stderr,"*** FAIL: (%d) %s\n",nc_strerror(err));
    exit(err==0?0:1);
}

int
main(int argc, char **argv)
{
    int stat = NC_NOERR;
    char* key = NULL;
    char* url = NULL;
    char* value = NULL;

    nc_initialize(); /* load .ncrc, etc */

    /* get the arguments */
    switch (argc) {
    case 0: case 1: usage("Too few arguments",0); break;
    case 2: key = nulldup(argv[1]); break;
    default: key = nulldup(argv[1]); url = nulldup(argv[2]); break;
    }
    if(url && strlen(uri)==0) url = NULL;
    
    if(uri == NULL) {
        value = nc_rc_get(key);
    } else {
	value = NC_rclookup_with_uri(key, url);
    }
    if(value == NULL) value = "";

    nc_finalize();
    return 0;
}
