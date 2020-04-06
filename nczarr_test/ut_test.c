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
        fprintf(stderr," -d/*debug*/");
        fprintf(stderr," -x<cmd,cmd,...>");
        fprintf(stderr," -f<inputfilename>");
        fprintf(stderr," -o<outputfilename>");
        fprintf(stderr," -k<kind>");
	fprintf(stderr,"\n");	
    fflush(stderr);
    exit(1);
}

int
ut_init(int argc, char** argv, struct Options * options)
{
    int stat = NC_NOERR;
    int c;

    nc_initialize();

    if(options != NULL) {
        while ((c = getopt(argc, argv, "dx:f:o:k:")) != EOF) {
            switch(c) {
            case 'd':  
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
makeurl(const char* file)
{
    char wd[4096];
    char* url = NULL;
    NCbytes* buf = ncbytesnew();
    if(file && strlen(file) > 0) {
        ncbytescat(buf,"file://");
        if(file[0] != '/') {
            (void)getcwd(wd, sizeof(wd));
            ncbytescat(buf,wd);
            ncbytescat(buf,"/");
        }
        ncbytescat(buf,file);
        ncbytescat(buf,"#mode=nczarr"); /* => use default file: format */
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
    struct Test* test = NULL;
    const char** cmd = NULL;
    for(cmd=cmds;*cmd;cmd++) {
        for(test=tests;test->cmd;test++) {
	    if(strcmp(test->cmd,*cmd)==0) {
		if(test->cmd == NULL) return NC_EINVAL;
		test->test(); /* Execute */
	    }
	}
    }
    return NC_NOERR;
}
