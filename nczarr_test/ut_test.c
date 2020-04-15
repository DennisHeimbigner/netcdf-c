/*
 *      Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "config.h"

#ifdef HAVE_UNISTD_H
#include "unistd.h"
#endif

#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

#ifdef _WIN32
#include "XGetopt.h"
int opterr;
int optind;
#endif

#include "zincludes.h"
#include "ut_test.h"
#include "ut_projtest.h"

struct Options options;

void
usage(int err)
{
    if(err) {
	fprintf(stderr,"error: (%d) %s\n",err,nc_strerror(err));
    }
    fprintf(stderr,"usage:");
        fprintf(stderr," -D/*debug*/");
        fprintf(stderr," -x<cmd,cmd,...>");
        fprintf(stderr," -f<inputfilename>");
        fprintf(stderr," -o<outputfilename>");
        fprintf(stderr," -k<kind>");
        fprintf(stderr," -d<dim>=<len>");
        fprintf(stderr," -v<type>var(<dim/chunksize,dim/chunksize...>)");
        fprintf(stderr," -s<slices>");
        fprintf(stderr," -W<int>,<int>...");
	fprintf(stderr,"\n");	
    fflush(stderr);
    exit(1);
}

int
ut_init(int argc, char** argv, struct Options * options)
{
    int stat = NC_NOERR;
    int c;
    Dimdef* dimdef = NULL;
    Vardef* vardef = NULL;

    nc_initialize();

    if(options != NULL) {
	options->dimdefs = nclistnew();
	options->vardefs = nclistnew();
        while ((c = getopt(argc, argv, "Dx:f:o:k:d:v:s:W:")) != EOF) {
            switch(c) {
            case 'D':  
                options->debug = 1;     
                break;
            case 'x': /*execute*/
		if(parsestringvector(optarg,0,&options->cmds) <= 0) usage(0);
                break;
            case 'f':
		options->file = strdup(optarg);
                break;
            case 'o':
		options->output = strdup(optarg);
                break;
            case 'k': /*implementation*/
		options->kind = strdup(optarg);
                break;
            case 'd': /*dimdef*/
		if((stat=parsedimdef(optarg,&dimdef))) usage(stat);
		nclistpush(options->dimdefs,dimdef);
		dimdef = NULL;
                break;
            case 'v': /*vardef*/
		if((stat=parsevardef(optarg,options->dimdefs,&vardef))) usage(stat);
		nclistpush(options->vardefs,vardef);
		vardef = NULL;
                break;
            case 's': /*slices*/
		if((stat=parseslices(optarg,options->slices))) usage(stat);
                break;
            case 'W': /*walk data*/
		options->idatalen = parseintvector(optarg,4,(void**)&options->idata);
                break;
            case '?':
               fprintf(stderr,"unknown option: '%c'\n",c);
               stat = NC_EINVAL;
               goto done;
            }
        }
    }

done:
    return stat;
}

void
nccheck(int stat, int line)
{
    if(stat) {
        fprintf(stderr,"%d: %s\n",line,nc_strerror(stat));
        fflush(stderr);
        exit(1);
    }
}

char*
makeurl(const char* file, NCZM_IMPL kind)
{
    char wd[4096];
    char* url = NULL;
    NCbytes* buf = ncbytesnew();
    NCURI* uri = NULL;
    
    if(file && strlen(file) > 0) {
	switch (kind) {
	case NCZM_NC4: /* fall thru */
	case NCZM_FILE:
            ncbytescat(buf,"file://");
            if(file[0] != '/') {
                (void)getcwd(wd, sizeof(wd));
                ncbytescat(buf,wd);
                ncbytescat(buf,"/");
            }
            ncbytescat(buf,file);
            ncbytescat(buf,"#mode=nczarr"); /* => use default file: format */
	    break;
	case NCZM_S3:
	    /* Assume that we have a complete url */
	    if(ncuriparse(file,&uri)) return NULL;
	    if(strcasecmp(uri->protocol,"s3")==0)
		ncurisetprotocol(uri,"https");
	    if(strcasecmp(uri->protocol,"http")!=0 && strcasecmp(uri->protocol,"https")!=0)
	        return NULL;
	    ncbytescat(buf,file);
	    break;
	default: abort();
	}
	url = ncbytesextract(buf);
    }
    ncbytesfree(buf);
    fprintf(stderr,"url=|%s|\n",url);
    fflush(stderr);
    return url;
}

#if 0
int
setup(int argc, char** argv)
{
    int stat = NC_NOERR;
    int c;
    memset((void*)&options,0,sizeof(options));
    while ((c = getopt(argc, argv, "dc:")) != EOF) {
        switch(c) {
        case 'd': 
            options.debug = 1;      
            break;
        case 'c':
            if(options.cmd != NULL) {
                fprintf(stderr,"error: multiple commands specified\n");
                stat = NC_EINVAL;
                goto done;
            }
            if(optarg == NULL || strlen(optarg) == 0) {
                fprintf(stderr,"error: bad command\n");
                stat = NC_EINVAL;
                goto done;
            }
            options.cmd = strdup(optarg);
            break;
        case '?':
           fprintf(stderr,"unknown option\n");
           stat = NC_EINVAL;
           goto done;
        }
    }
    if(options.cmd == NULL) {
        fprintf(stderr,"no command specified\n");
        stat = NC_EINVAL;
        goto done;
    }
done:
    return stat;
}
#endif

struct Test*
findtest(const char* cmd, struct Test* tests)
{
    struct Test* t = NULL;
    for(t=tests;t->cmd;t++) {
        if(strcasecmp(t->cmd,cmd)==0) return t;
    }
    return NULL;
}

int
runtests(const char** cmds, struct Test* tests)
{
    int stat = NC_NOERR;
    struct Test* test = NULL;
    const char** cmd = NULL;
    if(cmds == NULL) return NC_EINVAL;
    for(cmd=cmds;*cmd;cmd++) {
        for(test=tests;test->cmd;test++) {
	    if(strcmp(test->cmd,*cmd)==0) {
		if(test->cmd == NULL) return NC_EINVAL;
		if((stat=test->test())) goto done; /* Execute */
	    }
	}
    }
done:
    return stat;
}
