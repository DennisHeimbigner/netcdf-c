/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *	See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "config.h"

#include <stdlib.h>
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
#include "netcdf_aux.h"

extern int NC4_hdf5_plugin_path_sync(int formatx);
extern int NCZ_plugin_path_sync(int formatx);
extern const char* NC4_hdf5_plugin_path_tostring(void);
extern const char* NCZ_plugin_path_tostring(void);
extern const char* NC_plugin_path_tostring(void);

#undef DEBUG

/* Define max number of -x actions */
#define NACTIONS 64
/* Define max length of -x action string */
#define NACTIONLEN 4096

/* Define max no. of dirs in path list */
#define NDIRSMAX 64

typedef enum Action {
ACT_NONE=0,
ACT_GETALL=1,
ACT_APPEND=2,
ACT_PREPEND=3,
ACT_REMOVE=4,
ACT_LOAD=5,
ACT_SYNC=6,
ACT_LENGTH=7,
/* Synthetic Actions */
ACT_GETITH=8,
ACT_CLEAR=9,
} Action;

static struct ActionTable {
    Action op;
    const char* opname;
} actiontable[] = {
{ACT_NONE,"none"},
{ACT_GETALL,"getall"},
{ACT_APPEND,"append"},
{ACT_PREPEND,"prepend"},
{ACT_REMOVE,"remove"},
{ACT_LOAD,"load"},
{ACT_SYNC,"sync"},
{ACT_LENGTH,"length"},
{ACT_GETITH,"getith"},
{ACT_CLEAR,"clear"},
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

/* command line options */
struct Dumpptions {
    int debug;
    size_t nactions;
    struct Execute {
	Action action;
	char name[NACTIONLEN+1];
	char arg[NACTIONLEN+1];
    } actions[NACTIONS];
    int xflags;
#	define XNOZMETADATA 1	
} dumpoptions;

/* Forward */

#define NCCHECK(expr) nccheck((expr),__LINE__)
static void ncbreakpoint(int stat) {stat=stat;}
static int nccheck(int stat, int line)
{
    if(stat) {
	fprintf(stderr,"%d: %s\n",line,nc_strerror(stat));
	fflush(stderr);
	ncbreakpoint(stat);
	exit(1);
    }
    return stat;
}

static void
pluginusage(void)
{
    fprintf(stderr,"usage: tst_pluginpath [-d] -x <command>[:<arg>],<command>[:<arg>]...\n");
    fprintf(stderr,"\twhere <command> is one of: list append prepend remove list.\n");
    fprintf(stderr,"\twhere <arg> is arbitrary string (with '\\,' to escape commas); arg can be missing or empty.\n");
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

/* Unescape all escaped characters in s */
static void
descape(char* s)
{
    char* p;
    char* q;
    if(s == NULL) goto done;
    for(p=s,q=s;*p;) {
	if(*p == '\\' && p[1] != '\0') p++;
	*q++ = *p++;
    }
    *q = *p; /* nul terminate */
done:
    return;
}

/* A version of strchr that takes escapes into account */
static char*
xstrchr(char* s, char c)
{
    int leave;
    char* p;
    for(leave=0,p=s;!leave;p++) {
	switch (*p) {
	case '\\': p++; break;
	case '\0': return NULL; break;
	default: if(*p == c) return p; break;
	}
    }
    return NULL;
}

static void
parseactionlist(const char* cmds0)
{
    size_t i,cmdlen;
    char cmds[NACTIONLEN+2];
    char* p;
    char* q;
    size_t ncmds;
    int leave;

    memset(cmds,0,sizeof(cmds));
    if(cmds0 == NULL) cmdlen = 0; else cmdlen = strlen(cmds0);
    if(cmdlen == 0) {fprintf(stderr,"error: -x must have non-empty argument.\n"); pluginusage();}
    if(cmdlen > NACTIONLEN) {fprintf(stderr,"error: -x argument too lone; max is %zu\n",(size_t)NACTIONLEN); pluginusage();}
    strncpy(cmds,cmds0,cmdlen);
    /* split into command + arg strings and count */
    ncmds = 0;
    for(leave=0,p=cmds;!leave;p=q) {
	q = xstrchr(p,',');
	if(q == NULL) {
	    q = cmds+cmdlen; /* point to trailing nul */
	    leave = 1;
	} else {
	    *q++ = '\0'; /* overwrite ',' and skip to start of the next command*/
	}	
	ncmds++;
    }
    if(ncmds > NACTIONS) {fprintf(stderr,"error: -x must have not more than %zu commands.\n",(size_t)NACTIONS); pluginusage();}
    dumpoptions.nactions = ncmds;
    /* Now process each command+arg pair */
    for(p=cmds,i=0;i<ncmds;i++) {
	size_t clen,alen;
	clen = strlen(p);
	if(clen > NACTIONLEN) {fprintf(stderr,"error: -x cmd '%s' too long; max is %zu\n",p,(size_t)NACTIONLEN); pluginusage();}
	/* search for ':' taking escapes into account */
	q = xstrchr(p,':');
	if(q == NULL)
	    q = p+clen; /* point to trailing nul */
	else
	    *q++ = '\0'; /* overwrite ':' and skip to start of the arg*/
	strncpy(dumpoptions.actions[i].name,p,NACTIONLEN);
	/* Get the argument, if any */
	alen = strlen(q);
	if(alen > 0) {
	    strncpy(dumpoptions.actions[i].arg,q,NACTIONLEN);
	}
	p += (clen+1); /* move to next cmd+arg pair */
    }
    /* De-escape names and args and compute action enum */
    for(i=0;i<dumpoptions.nactions;i++) {
	descape(dumpoptions.actions[i].name);
	descape(dumpoptions.actions[i].arg);
	dumpoptions.actions[i].action = decodeop(dumpoptions.actions[i].name);
    }
    return;
}

static int
clearstringvec(size_t n, char** vec)
{
    int stat = NC_NOERR;
    size_t i;
    if(vec == NULL) goto done;
    for(i=0;i<n;i++) {
	if(vec[i] != NULL) free(vec[i]);
	vec[i] = NULL;
    }
done:
    return stat;
}

static int
freestringvec(size_t n, char** vec)
{
    int stat = NC_NOERR;
    if((stat = clearstringvec(n,vec))) goto done;
    if(vec) free(vec);
done:
    return stat;
}

/**************************************************/

static int
actionappend(const char* arg)
{
    int stat = NC_NOERR;
    if(arg == NULL || strlen(arg)==0) {stat = NC_EINVAL; goto done;}
    if((stat=nc_plugin_path_append(arg))) goto done;
done:
    return NCCHECK(stat);
}

static int
actionclear(const char* arg)
{
    int stat = NC_NOERR;
    if((stat=nc_plugin_path_load(NULL))) goto done;   
done:
    return NCCHECK(stat);
}

static int
actiongetall(const char* arg)
{
    int stat = NC_NOERR;
    int format;
    size_t alen;
    const char* text = NULL;
   
    if(arg == NULL) alen = 0; else alen = strlen(arg);
    format = decodeformatx(alen==0?"default":arg);
    switch (format) {
    default:
	fprintf(stderr,"actiongetall: Illegal argument: %s\n",arg);
	stat = NC_EINVAL;
	text = "";
	break;
    case NC_FORMATX_NC_HDF5:
	text = NC4_hdf5_plugin_path_tostring();
	break;
    case NC_FORMATX_NCZARR:
	text = NCZ_plugin_path_tostring();
	break;	
    case 0:
       text = NC_plugin_path_tostring();
       break;
    }
    if(stat == NC_NOERR) printf("%s\n",text);   
    return NCCHECK(stat);
}

static int
actiongetith(const char* arg)
{
    int stat = NC_NOERR;
    size_t ith = 0;
    char* dir = NULL;

    if(arg != NULL && strlen(arg)==0) arg = NULL;
    if(arg == NULL) {stat = NC_EINVAL; goto done;}
    if(sscanf(arg,"%zu",&ith) != 1) {stat = NC_EINVAL; goto done;}
    if((stat = nc_plugin_path_getith(ith,&dir))) goto done;
    printf("%s\n",dir);
      
done:
    nullfree(dir);
    return NCCHECK(stat);
}

static int
actionlength(const char* arg)
{
    int stat = NC_NOERR;
    size_t len = 0;
    if((stat=nc_plugin_path_getall(&len,NULL))) goto done;
    printf("%zu\n",len);
done:
    return NCCHECK(stat);
}

static int
actionload(const char* arg)
{
    int stat = NC_NOERR;
    if(arg != NULL && strlen(arg)==0) arg = NULL;
    if((stat=nc_plugin_path_load(arg))) goto done;   
done:
    return NCCHECK(stat);
}

static int
actionprepend(const char* arg)
{
    int stat = NC_NOERR;
    if(arg == NULL || strlen(arg)==0) {stat = NC_EINVAL; goto done;}
    if((stat=nc_plugin_path_prepend(arg))) goto done;
done:
    return NCCHECK(stat);
}

static int
actionremove(const char* arg)
{
    int stat = NC_NOERR;
    if(arg == NULL || strlen(arg)==0) {stat = NC_EINVAL; goto done;}
    if((stat=nc_plugin_path_remove(arg))) goto done;   
done:
    return NCCHECK(stat);
}

static int
actionsync(const char* arg)
{
    int stat = NC_NOERR;
    size_t alen;
    int format;
    
    if(arg != NULL && strlen(arg)==0) arg = NULL;
    if(arg == NULL) alen = 0; else alen = strlen(arg);
    format = decodeformatx(alen==0?"default":arg);
    switch (format) {
    default:
	fprintf(stderr,"actiongetall: Illegal argument: %s\n",arg);
	stat = NC_EINVAL;
	break;
    case NC_FORMATX_NC_HDF5:
	stat = NC4_hdf5_plugin_path_sync(format);
	break;
    case NC_FORMATX_NCZARR:
	stat = NCZ_plugin_path_sync(format);
	break;	
    case 0:
       stat = nc_plugin_path_sync(format);
       break;
    }

    return NCCHECK(stat);
}

int
main(int argc, char** argv)
{
    int stat = NC_NOERR;
    int c;
    char* p;
    size_t i;

    /* Init options */
    memset((void*)&dumpoptions,0,sizeof(dumpoptions));

    while ((c = getopt(argc, argv, "dvx:X:")) != EOF) {
	switch(c) {
	case 'd': 
	    dumpoptions.debug = 1;	    
	    break;
	case 'v': 
	    pluginusage();
	    goto done;
	case 'x':
	    parseactionlist(optarg);
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

    for(i=0;i<dumpoptions.nactions;i++) {
#ifdef DEBUG
fprintf(stderr,">>>> [%zu] %s(%d) : %s\n",i,
					  dumpoptions.actions[i].name,
					  dumpoptions.actions[i].action,
					  dumpoptions.actions[i].arg);
#endif
	switch (dumpoptions.actions[i].action) {
	default:
	    fprintf(stderr,"Illegal action: %s\n",dumpoptions.actions[i].name);
	    pluginusage();
	    break;
	case ACT_CLEAR: if((stat=actionclear(dumpoptions.actions[i].arg))) goto done; break;
	case ACT_APPEND: if((stat=actionappend(dumpoptions.actions[i].arg))) goto done; break;
	case ACT_GETALL: if((stat=actiongetall(dumpoptions.actions[i].arg))) goto done; break;
	case ACT_GETITH: if((stat=actiongetith(dumpoptions.actions[i].arg))) goto done; break;
	case ACT_LENGTH: if((stat=actionlength(dumpoptions.actions[i].arg))) goto done; break;
	case ACT_LOAD: if((stat=actionload(dumpoptions.actions[i].arg))) goto done; break;
	case ACT_PREPEND: if((stat=actionprepend(dumpoptions.actions[i].arg))) goto done; break;
	case ACT_REMOVE: if((stat=actionremove(dumpoptions.actions[i].arg))) goto done; break;
	case ACT_SYNC: if((stat=actionsync(dumpoptions.actions[i].arg))) goto done; break;
	}
    }

done:
    fflush(stdout);
    if(stat)
	fprintf(stderr,"fail: %s\n",nc_strerror(stat));
    return (stat ? 1 : 0);    
}

#if 0
static void
printpluginlist(void)
{
    void* p = p; p = printpluginlist;
    size_t npaths = 0;
    char** pathlist = NULL;
    char* pathstr = NULL;

    NCCHECK(nc_plugin_path_getall(&npaths,NULL));
    if((pathlist=(char**)calloc(npaths,sizeof(char*)))==NULL) abort();
    NCCHECK(nc_plugin_path_getall(&npaths,pathlist));
    if((pathstr = ncaux_pathlist_concat(npaths,pathlist))==NULL) goto done;
    printf("%s",pathstr);
done:
    nullfree(pathstr);
    freestringvec(npaths,pathlist);
    printf("\n");
}
#endif
