#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "netcdf.h"
#include "ncrc.h"
#include "nc4internal.h"

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
        if(entry->uri != NULL) {
	    char* uristr = ncuribuild(entry->uri,NULL,NULL,NCURIBASE);
	    printf("[%s] ",uristr);
	}
	printf("|%s|->|%s|\n",entry->key,entry->value);
    }
}

int
main(void)
{
    NCglobalstate* ngs = NC_getglobalstate();

    /* Cause the .rc files to be read and merged */
    nc_initialize();

    if((ngs = NC_getglobalstate())==NULL) abort();
    printrc(ngs);

    return 0;
}
