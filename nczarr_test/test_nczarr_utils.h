/* This is part of the netCDF package.
   Copyright 2018 University Corporation for Atmospheric Research/Unidata
   See COPYRIGHT file for conditions of use.

   @Author Dennis Heimbigner
*/

/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
/* $Id: main.c,v 1.33 2010/05/26 21:43:36 dmh Exp $ */
/* $Header: /upc/share/CVS/netcdf-3/ncgen/main.c,v 1.33 2010/05/26 21:43:36 dmh Exp $ */

#ifndef TEST_NCZARR_UTILS_H
#define TEST_NCZARR_UTILS_H

#include "config.h"
#include "ncbytes.h"

#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

#ifdef _MSC_VER
#include "XGetopt.h"
#endif

static char* progname = NULL;

static char* path;
static NCZM_IMPL zmap_impl = NCZM_UNDEF;
static char* cloud = NULL;
static char* otherfragments = NULL;

static void
test_usage(void)
{
    fprintf(stderr,"usage: <test> [-e <zmapimpl>]\n");
    exit(1);
}

/* strip off leading path; result is malloc'd */
static char *
ubasename(char *logident)
{
    char* sep;

    sep = strrchr(logident,'/');
#ifdef MSDOS
    if(sep == NULL) sep = strrchr(logident,'\\');
#endif
    if(sep == NULL) return logident;
    sep++; /* skip past the separator */
    return sep;
}

static void
setimpl(const char* name)
{
    if(strcasecmp(name,"s3")) zmap_impl = NCZM_S3;
    else if(strcasecmp(name,"nz4")) zmap_impl = NCZM_NC4;
    else if(strcasecmp(name,"nzf")) zmap_impl = NCZM_FILE;
    else test_usage();
}

static const char*
implname(void)
{
    switch (zmap_impl) {
    case NCZM_S3: return "s3";
    case NCZM_NC4: return "nz4";
    case NCZM_FILE: return "nzf";
    default: test_usage();
    }
    return NULL;
}

static void
buildpath(const char* target)
{
    NCbytes* buf = ncbytesnew();
    switch(zmap_impl) {
    case NCZM_NC4:
    case NCZM_FILE:
	ncbytescat(buf,"file://");
	ncbytescat(buf,target);
	ncbytescat(buf,"#mode=nczarr")
	ncbytescat(buf,",%s",implname());
	break;
    case NCZM_S3:
	ncbytescat(buf,cloud);
	if(cloud[strlen(cloud)-1] != '/'
	    ncbytescat(buf,"/");
	ncbytescat(buf,target);
	ncbytescat(buf,"#mode=nczarr")
	ncbytescat(buf,",%s",implname());
	break;
    default: test_usage();
    }
    if(otherfragments != NULL) ncbytescat(buf,",%s",otherfragments);
    path = ncbytesextract(buf);
    ncbytesfree(buf);
}

static void
processoptions(int argc, char** argv, const char* base_file_name)
{
    if(argc == 1) test_usage();    
    progname = nulldup(ubasename(argv[0]));

    while ((c = getopt(argc, argv, "e:c:F:")) != EOF)
      switch(c) {
	case 'e': /* zmap choice */
	    setimpl(optarg);
	    break;
	case 'c': /* cloud appliance url prefix*/
	    cloud = strdup(optarg);
	    break;
	case 'F': /* fragments */
	    otherfragments = strdup(optarg);
	    break;
	case '?':
  	    test_usage();
	    break;
      }

    argc -= optind;
    argv += optind;

    if (argc > 1 || argc ==0)
	fprintf(stderr,"%s: require exactly one input file argument",progname);
	test_usage();
    }

    if(zmap_impl == NCZM_UNDEF) zmap_impl = NCZM_FILE;
    if(zmap_impl == NCZM_S3 && s3prefix == NULL) test_usage();

    buildpath(base_file_name);

}

#endif /*TEST_NCZARR_UTILS_H*/
