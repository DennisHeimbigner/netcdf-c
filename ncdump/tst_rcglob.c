/* This is part of the netCDF package.
   Copyright 2018 University Corporation for Atmospheric Research/Unidata
   See COPYRIGHT file for conditions of use.

   Test RC Glob Matching
   Dennis Heimbigner
*/

#include "config.h"
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
usage(const char* msg)
{
    fprintf(stderr,"Usage: tst_rcglob <key> [<search url>]\n");
    if(msg != NULL && strlen(msg) > 0) fprintf(stderr,"Error: %s\n",msg);
    exit(1);
}

int
main(int argc, char **argv)
{
    char* key = NULL;
    char* url = NULL;
    char* value = NULL;

    nc_initialize(); /* load .ncrc, etc */

    /* get the arguments */
    switch (argc) {
    case 0: case 1: usage("Too few arguments"); break;
    case 2: key = nulldup(argv[1]); break;
    default: key = nulldup(argv[1]); url = nulldup(argv[2]); break;
    }
    if(url && strlen(url)==0) {nullfree(url); url = NULL;}
    if(url == NULL) {
        value = nc_rc_get(key);
    } else {
	value = nulldup(NC_rclookup_with_uri(key, url));
    }
    if(value == NULL) value = strdup("");

    printf("%s",value);
    nullfree(key)
    nullfree(url)
    nullfree(value);
    nc_finalize();
    return 0;
}
