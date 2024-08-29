/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stddef.h>
#include <stdio.h>
#include <string.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

#if defined(_WIN32) && !defined(__MINGW32__)
#include "XGetopt.h"
#endif

#include "netcdf.h"
#include "netcdf_filter.h"

extern int NC4_hdf5_printpluginlist(void);
extern int NC_printpluginlist(void);

typedef enum Action {
ACT_NONE=0,
ACT_LIST=1,
ACT_APPEND=2,
ACT_PREPEND=3,
ACT_REMOVE=4,
ACT_LOAD=5
} Action;

static struct ActionTable {
    Action op;
    const char* opname;
} actiontable[] = {
{ACT_NONE,"none"},
{ACT_LIST,"list"},
{ACT_APPEND,"append"},
{ACT_PREPEND,"prepend"},
{ACT_REMOVE,"remove"},
{ACT_LOAD,"load"},
{ACT_NONE,NULL}
};

static struct FormatXTable {
    const char* name;
    int formatx;
} formatxtable[] = {
{"default",0},
{"hdf5",NC_FORMATX_NC_HDF5},
{"nczarr",NC_FORMATX_NCZARR},
{"zarr",NC_FORMATX_NCZARR},
{NULL,0}
};

/* Command line options */
struct Dumpptions {
    int debug;
    Action aop;
    char action[4096];
    char arg[4096];
    int xflags;
#	define XNOZMETADATA 1	
} dumpoptions;

/* Forward */

static void printpluginlist(void);

#define NCCHECK(expr) nccheck((expr),__LINE__)
static void nccheck(int stat, int line)
{
    if(stat) {
	fprintf(stderr,"%d: %s\n",line,nc_strerror(stat));
	fflush(stderr);
	exit(1);
    }
}

static void
pluginusage(void)
{
    fprintf(stderr,"usage: tst_pluginpath [-d] -x <command> <arg>\n");
    fprintf(stderr,"\twhere command is one of: list append prepend remove list.\n");
    exit(1);
}

static int
decodeformatx(const char* name)
{
    struct FormatXTable* p = formatxtable;
    for(;p->name != NULL;p++) {
	if(strcasecmp(p->name,name)==0) return p->formatx;
    }
    return 0;
}

static Action
decodeop(const char* name)
{
    struct ActionTable* p = actiontable;
    for(;p->opname != NULL;p++) {
	if(strcasecmp(p->opname,name)==0) return p->op;
    }
    return ACT_NONE;
}

static int
testlist(void)
{
   int stat = NC_NOERR;
   int format = decodeformatx(strlen(dumpoptions.arg)==0?"default":dumpoptions.arg);
   switch (format) {
   default:
	fprintf(stderr,"list: Illegal argument: %s\n",dumpoptions.arg);
	stat = NC_EINVAL;
	break;
   case NC_FORMATX_NC_HDF5:
	NC4_hdf5_printpluginlist();
	break;
   case NC_FORMATX_NCZARR:
	NC_printpluginlist();
	break;	
   case 0:
       printpluginlist();
       break;
   }
   return stat;
}

static int
testappend(void)
{
   int stat = NC_NOERR;
   if(dumpoptions.arg == NULL) {stat = NC_EINVAL; goto done;}
   if((stat=nc_plugin_path_append(dumpoptions.arg))) goto done;
   printpluginlist();
done:
   return stat;
}

static int
testprepend(void)
{
   int stat = NC_NOERR;
   if(dumpoptions.arg == NULL) {stat = NC_EINVAL; goto done;}
   if((stat=nc_plugin_path_prepend(dumpoptions.arg))) goto done;
done:
   printpluginlist();

   return stat;
}

static int
testremove(void)
{
   int stat = NC_NOERR;
   if(dumpoptions.arg == NULL) {stat = NC_EINVAL; goto done;}
   if((stat=nc_plugin_path_remove(dumpoptions.arg))) goto done;   
   printpluginlist();
done:
   return stat;
}

static int
testload(void)
{
   int stat = NC_NOERR;
   if(dumpoptions.arg == NULL) {stat = NC_EINVAL; goto done;}
   if((stat=nc_plugin_path_load(dumpoptions.arg))) goto done;   
   printpluginlist();
done:
   return stat;
}

int
main(int argc, char** argv)
{
    int stat = NC_NOERR;
    int c;
    char* p;

    nc_initialize();

    /* Init options */
    memset((void*)&dumpoptions,0,sizeof(dumpoptions));

    while ((c = getopt(argc, argv, "dx:")) != EOF) {
	switch(c) {
	case 'd': 
	    dumpoptions.debug = 1;	    
	    break;
	case 'v': 
	    pluginusage();
	    goto done;
	case 'x':
	    strncpy(dumpoptions.action,optarg,sizeof(dumpoptions.action));
	    dumpoptions.aop = decodeop(optarg);
	    break;
	case 'X':
	    for(p=optarg;*p;p++) {
		switch (*p) {
		case 'm': dumpoptions.xflags |= XNOZMETADATA; break;
	        default: fprintf(stderr,"Unknown -X argument: %c",*p); break;
		}
	    };
	    break;
	case '?':
	   fprintf(stderr,"unknown option\n");
	   {stat = NC_EINVAL; goto done;}
	}
    }

    /* get action rgument */
    argc -= optind;
    argv += optind;

    if (argc == 0) {
	dumpoptions.arg[0] = '\0';
    } else {
	strcpy(dumpoptions.arg,optarg);
    }

    switch (dumpoptions.aop) {
    default:
	fprintf(stderr,"Illegal action: %s\n",dumpoptions.action);
	pluginusage();
	break;
    case ACT_LIST: if((stat=testlist())) goto done; break;
    case ACT_APPEND: if((stat=testappend())) goto done; break;
    case ACT_PREPEND: if((stat=testprepend())) goto done; break;
    case ACT_REMOVE: if((stat=testremove())) goto done; break;
    case ACT_LOAD: if((stat=testload())) goto done; break;
    }    

done:
    nc_finalize();
    if(stat)
	fprintf(stderr,"fail: %s\n",nc_strerror(stat));
    return (stat ? 1 : 0);    
}

static void
printpluginlist(void)
{
    void* p = p; p = printpluginlist;
    size_t npaths = 0;
    char** pathlist = NULL;
    size_t i;

    NCCHECK(nc_plugin_path_list(&npaths,NULL));
    if(npaths == 0) {
	printf("<empty>\n");
        goto done;
    }
    if((pathlist=(char**)calloc(npaths,sizeof(char*)))==NULL) abort();
    NCCHECK(nc_plugin_path_list(&npaths,pathlist));
    for(i=0;i<npaths;i++) {
	const char* dir = pathlist[i];
	printf("%s%s",(i==0?"":";"),dir);
    }
done:
    printf("\n");
}
