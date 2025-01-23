/*
 *      Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "ut_includes.h"
#include "ncpathmgr.h"
#include "nclog.h"

#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

#if defined(_WIN32) && !defined(__MINGW32__)
#include "XGetopt.h"
#endif

#ifdef UTTESST
struct ZUTEST zutester;
#endif

struct UTOptions utoptions;

/*Forward*/
static void canonicalfile(char** fp);

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
ut_init(int argc, char** argv, struct UTOptions * options)
{
    int stat = NC_NOERR;
    int c;
    Dimdef* dimdef = NULL;
    Vardef* vardef = NULL;

    nc_initialize();

    if(options != NULL) {
	options->dimdefs = nclistnew();
	options->vardefs = nclistnew();
        while ((c = getopt(argc, argv, "T:Dx:f:o:p:k:d:v:s:W:")) != EOF) {
            switch(c) {
            case 'T':  
	        nctracelevel(atoi(optarg));
                break;
            case 'D':  
                options->debug = 1;     
                break;
            case 'x': /*execute*/
		if(parsestringvector(optarg,0,&options->cmds) <= 0) usage(THROW(0));
                break;
            case 'f':
		options->file = strdup(optarg);
                break;
            case 'o':
		options->output = strdup(optarg);
                break;
            case 'p':
		options->profile = strdup(optarg);
                break;
            case 'k': /*implementation*/
		options->kind = strdup(optarg);
                break;
            case 'd': /*dimdef*/
		if((stat=parsedimdef(optarg,&dimdef))) usage(THROW(stat));
		nclistpush(options->dimdefs,dimdef);
		dimdef = NULL;
                break;
            case 'v': /*vardef*/
		if((stat=parsevardef(optarg,options->dimdefs,&vardef))) usage(THROW(stat));
		nclistpush(options->vardefs,vardef);
		vardef = NULL;
                break;
            case 's': /*slices*/
		if((stat=parseslices(optarg,&options->nslices,options->slices))) usage(THROW(stat));
                break;
            case '?':
               fprintf(stderr,"unknown option: '%c'\n",c);
               stat = NC_EINVAL;
               goto done;
            }
        }
    }

    canonicalfile(&options->file);
    canonicalfile(&options->output);
    
done:
    return THROW(stat);
}

void
ut_final(void)
{
    nc_finalize();
}

#if 0
static void
getpathcwd(char** cwdp)
{
    char buf[4096];
    (void)NCgetcwd(buf,sizeof(buf));
    if(cwdp) *cwdp = strdup(buf);
}
#endif

static void
canonicalfile(char** fp)
{
    size_t len;
    char* f = NULL;
    char* abspath = NULL;
    NCURI* uri = NULL;
#ifdef _WIN32
    int fwin32=0, cwd32=0;
#endif

    if(fp == NULL || *fp == NULL) return;
    f = *fp;
    len = strlen(f);
    if(len <= 1) return;
    ncuriparse(f,&uri);
    if(uri != NULL) {ncurifree(uri); return;} /* its a url */

#if 1
    abspath = NCpathabsolute(f);
#else
    if(f[0] == '/' || f[0] == '\\' || hasdriveletter(f))
        return; /* its already absolute */
#ifdef _WIN32
    for(p=f;*p;p++) {if(*p == '\\') {*p = '/';}}
#endif
    if(len >= 2 && memcmp(f,"./",2)==0) {
	offset = 1; /* leave the '/' */
    } else if(len >= 3 && memcmp(f,"../",3)==0) {
	offset = 2;
    } else
        offset = 0;
    getpathcwd(&cwd);
    len2 = strlen(cwd);
#ifdef _WIN32
    for(cwd32=0,p=cwd;*p;p++) {if(*p == '\\') {*p = '/'; cwd32 = 1;}}
#endif
    if(offset == 2) {
        p = strrchr(cwd,'/');
        /* remove last segment including the preceding '/' */
	if(p == NULL) {cwd[0] = '\0';} else {*p = '\0';}
    }
    len2 = (len-offset)+strlen(cwd);
    if(offset == 0) len2++; /* need to add '/' */
    abspath = (char*)malloc(len2+1);
    abspath[0] = '\0';
    strlcat(abspath,cwd,len2+1);
    if(offset == 0) strlcat(abspath,"/",len2+1);
    strlcat(abspath,f+offset,len2+1);
#ifdef _WIN32
    if(fwin32)
     for(p=abspath;*p;p++) {if(*p == '/') {*p = '\\';}}
#endif
    nullfree(cwd);
#endif
    nullfree(f);
fprintf(stderr,"canonicalfile: %s\n",abspath);
    *fp = abspath;
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
makeurl(const char* file, NCZM_IMPL impl, struct UTOptions* options)
{
    char* url = NULL;
    NCbytes* buf = ncbytesnew();
    NCURI* uri = NULL;
    const char* kind = impl2kind(impl);
    char* urlpath = NULL;
    char* p;

    if(file && strlen(file) > 0) {
	switch (impl) {
	case NCZM_FILE:
	case NCZM_ZIP:
            /* Massage file to make it usable as URL path */
	    urlpath = strdup(file);
	    for(p=urlpath;*p;p++) {if(*p == '\\') *p = '/';}
            ncbytescat(buf,"file://");
            ncbytescat(buf,urlpath);
	    nullfree(urlpath); urlpath = NULL;
            ncbytescat(buf,"#mode=nczarr"); /* => use default file: format */
	    ncbytescat(buf,",");
	    ncbytescat(buf,kind);
	    break;
	case NCZM_S3:
	    /* Assume that we have a complete url */
	    if(ncuriparse(file,&uri)) return NULL;
	    if(options->profile) {
		const char* profile = ncurifragmentlookup(uri,"aws.profile");
		if(profile == NULL) {
		    ncurisetfragmentkey(uri,"aws.profile",options->profile);
		    /* rebuild the url */
		    file = (const char*)ncuribuild(uri,NULL,NULL,NCURIALL); /* BAD but simple */
		}
	    }
	    ncbytescat(buf,file);
	    break;
	default: abort();
	}
	url = ncbytesextract(buf);
    }
    ncurifree(uri);
    ncbytesfree(buf);
    fprintf(stderr,"url=|%s|\n",url);
    fflush(stderr);
    return url;
}

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
    if(cmds == NULL) return THROW(NC_EINVAL);
    for(cmd=cmds;*cmd;cmd++) {
        for(test=tests;test->cmd;test++) {
	    if(strcmp(test->cmd,*cmd)==0) {
		if(test->cmd == NULL) return THROW(NC_EINVAL);
		if((stat=test->test())) goto done; /* Execute */
	    }
	}
    }
done:
    return THROW(stat);
}

int
main(int argc, char** argv)
{
    int stat = NC_NOERR;

    if((stat = ut_init(argc, argv, &utoptions))) goto done;
    if(utoptions.file == NULL && utoptions.output != NULL) utoptions.file = strdup(utoptions.output);
    if(utoptions.output == NULL && utoptions.file != NULL)utoptions.output = strdup(utoptions.file);

    /* localize */
    if((stat = ut_localize(utoptions.file,&utoptions.file))) goto done;
    if((stat = ut_localize(utoptions.output,&utoptions.output))) goto done;

    impl = kind2impl(utoptions.kind);
    url = makeurl(utoptions.file,impl,&utoptions);

    if((stat = runtests((const char**)utoptions.cmds,tests))) goto done;
    
done:
    nullfree(url); url = NULL;
    nullfree(keyprefix);
    ut_final();
    if(stat) usage(THROW(stat));
    return 0;
}

/* Do a simple create */
static int
simplecreate(void)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    char* truekey = NULL;

    title(__func__);

    if((stat = nczmap_truncate(impl,url)))
	goto done;
    report(PASS,"truncate",map);

    switch(stat = nczmap_create(impl,url,0,0,NULL,&map)) {
    case NC_EOBJECT: break; /* already exists */
    case NC_NOERR: break; /*created*/
    default: goto done;
    }
    
    printf("Pass: create: create: %s\n",url);

    truekey = makekey(Z2METAROOT);
    if((stat = nczmap_write(map, truekey, 0, NULL)))
	goto done;
    printf("Pass: create: defineobj: %s\n",truekey);
    
    /* Do not delete */
    if((stat = nczmap_close(map,0)))
	goto done;
    map = NULL;
    printf("Pass: create: close\n");
    
    /* Reopen and see if exists */
    if((stat = nczmap_open(impl,url,0,0,NULL,&map)))
	goto done;
    printf("Pass: create: open: %s\n",url);
    
    if((stat = nczmap_exists(map,truekey)))
	goto done;
    printf("Pass: create: exists: %s\n",truekey);
    
    /* close again */
    if((stat = nczmap_close(map,0)))
	goto done;
    map = NULL;
    printf("Pass: create: close\n");

done:
    nullfree(truekey);
    return THROW(stat);
}

/* Do a simple delete of previously created file */
static int
simpledelete(void)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;

    title(__func__);

    switch ((stat = nczmap_open(impl,url,0,0,NULL,&map))) {
    case NC_NOERR:
        report(PASS,"open",map);
	break;
    default:
        {report(FAIL,"open",map); goto done;}
    }     
    /* Delete dataset while closing */
    if((stat = nczmap_close(map,1))) goto done;
    map = NULL;
    report(PASS,"close: delete",map);

    switch ((stat = nczmap_open(impl,url,0,0,NULL,&map))) {
    case NC_NOERR:
        report(FAIL,"open",map);
	break;
    case NC_ENOOBJECT:
        report(XFAIL,"open",map);
	stat = NC_NOERR;
	break;
    default: abort();
    }     

done:
    return THROW(stat);
}

static int
simplemeta(void)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    char* key = NULL;
    char* truekey = NULL;
    size64_t size = 0;
    char* content = NULL;

    title(__func__);

    if((stat = nczmap_open(impl,url,NC_WRITE,0,NULL,&map)))
	goto done;
    report(PASS,"open",map);
	
    /* Make sure .nczarr exists (from simplecreate) */
    truekey = makekey(Z2METAROOT);
    if((stat = nczmap_exists(map,truekey)))
	goto done;
    report(PASS,".nczarr: exists",map);
    free(truekey); truekey = NULL;

    if((stat=nczm_concat(META1,Z2ARRAY,&key)))
	goto done;
    truekey = makekey(key);
    nullfree(key); key = NULL;
    if((stat = nczmap_write(map, truekey, 0, NULL)))
	goto done;
    report(PASS,".zarray: def",map);
    free(truekey); truekey = NULL;

    truekey = makekey(Z2METAROOT);
    if((stat = nczmap_write(map, truekey, strlen(metadata1), metadata1)))
	goto done;
    report(PASS,".nczarr: writemetadata",map);
    free(truekey); truekey = NULL;
    
    if((stat=nczm_concat(META1,Z2ARRAY,&key)))
	goto done;
    truekey = makekey(key);
    free(key); key = NULL;    

    if((stat = nczmap_write(map, truekey, strlen(metaarray1), metaarray1)))
	goto done;
    report(PASS,".zarray: writemetaarray1",map);
    free(truekey); truekey = NULL;
    
    if((stat = nczmap_close(map,0)))
	goto done;
    map = NULL;
    report(PASS,"close",map);

    if((stat = nczmap_open(impl,url,0,0,NULL,&map)))
	goto done;
    report(PASS,"re-open",map);

    /* Read previously written */
    truekey = makekey(Z2METAROOT);
    if((stat = nczmap_exists(map, truekey)))
	goto done;
    report(PASS,".nczarr: exists",map);
    if((stat = nczmap_len(map, truekey, &size)))
	goto done;
    report(PASS,".nczarr: len",map);
    if(size != strlen(metadata1))
        report(FAIL,".nczarr: len verify",map);
    if((content = calloc(1,strlen(metadata1)+1))==NULL)
        {stat = NC_ENOMEM; goto done;}
    if((stat = nczmap_read(map, truekey, 0, strlen(metadata1), content)))
	goto done;
    report(PASS,".nczarr: readmetadata",map);
    free(truekey); truekey = NULL;
    if(memcmp(content,metadata1,size)!=0)
        report(FAIL,".nczarr: content verify",map);
    else report(PASS,".nczarr: content verify",map);
    nullfree(content); content = NULL;

    if((stat=nczm_concat(META1,Z2ARRAY,&key)))
	goto done;
    truekey = makekey(key);
    nullfree(key); key = NULL;
    if((stat = nczmap_exists(map, truekey)))
	goto done;
    report(PASS,".zarray: exists",map);
    if((stat = nczmap_len(map, truekey, &size)))
	goto done;
    report(PASS,".zarray: len",map);
    if(size != strlen(metaarray1))
        report(FAIL,".zarray: len verify",map);
    content = calloc(1,strlen(metaarray1)+1);
    if((stat = nczmap_read(map, truekey, 0, strlen(metaarray1), content)))
	goto done;
    report(PASS,".zarray: readmeta",map);
    free(truekey); truekey = NULL;
    if(memcmp(content,metaarray1,size)!=0)
        report(FAIL,".zarray: content verify",map);
    else
        report(PASS,".zarray:content verify",map);
    nullfree(content); content = NULL;
    
    if((stat = nczmap_close(map,0)))
	goto done;
    map = NULL;
    report(PASS,"close",map);

done:
    if(map) nczmap_close(map,0);
    nullfree(content);
    nullfree(truekey);
    nullfree(key);
    return THROW(stat);
}

static int
simpledata(void)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    char* truekey = NULL;
    int data1[DATA1LEN];
    int readdata[DATA1LEN];
    int i;
    size64_t totallen, size;
    char* data1p = (char*)&data1[0]; /* byte level version of data1 */

    title(__func__);

    /* Create the data */
    for(i=0;i<DATA1LEN;i++) data1[i] = i;
    totallen = sizeof(int)*DATA1LEN;

    if((stat = nczmap_open(impl,url,NC_WRITE,0,NULL,&map)))
	goto done;
    report(PASS,"open",map);
	
    truekey = makekey(DATA1);

    if((stat = nczmap_write(map, truekey, totallen, data1p)))
	goto done;

    report(PASS,DATA1": write",map);
    
    if((stat = nczmap_close(map,0)))
	goto done;
    map = NULL;
    report(PASS,"close",map);

    if((stat = nczmap_open(impl,url,0,0,NULL,&map)))
	goto done;
    report(PASS,"re-open",map);

    /* Read previously written */
    if((stat = nczmap_exists(map, truekey)))
	goto done;
    report(PASS,DATA1":exists",map);
    if((stat = nczmap_len(map, truekey, &size)))
	goto done;
    report(PASS,DATA1": len",map);
    if(size != totallen)
        report(FAIL,DATA1": len verify",map);
    if((stat = nczmap_read(map, truekey, 0, totallen, readdata)))
	goto done;
    report(PASS,DATA1": read",map);
    if(memcmp(data1,readdata,size)!=0)
        report(FAIL,DATA1": content verify",map);
    else report(PASS,DATA1": content verify",map);
    free(truekey); truekey = NULL;

done:
    /* Do not delete so we can look at it with ncdump */
    if(map && (stat = nczmap_close(map,0)))
	goto done;
    nullfree(truekey);
    return THROW(stat);
}

static int
search(void)
{

    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    NClist* objects = nclistnew();

    if((stat = nczmap_open(impl,url,0,0,NULL,&map)))
	goto done;

    /* Do a recursive search on root to get all object keys */
    if((stat=ut_search(map,"/",objects))) goto done;

    /* Print out the list */
    for(size_t i=0;i<nclistlength(objects);i++) {
	const char* key = nclistget(objects,i);
	printf("[%zu] %s\n",i,key);
    }

done:
    (void)nczmap_close(map,0);
    nclistfreeall(objects);
    return THROW(stat);
}

#if 0
/* S3 requires knowledge of the bucket+dataset root in order to create the true key */
static void
setkeyprefix(const char* file)
{
    NCURI* uri = NULL;
    NClist* segments = NULL;

    assert(impl == NCZM_S3);
    ncuriparse(file,&uri);
    /* verify that this could be an S3 url */
    if(uri == NULL) return; /* oh well */
    if(strcmp(uri->protocol,"file")==0) return;
    
    segments = nclistnew();
    nczm_split_delim(uri->path,'/',segments);
    /* Extract the first two segments */
    if(nclistlength(segments) < 1) return; /* not enough to qualify */
    /* Remove the bucket */
    { char* s = nclistremove(segments,0);
    nullfree(s); /* do not nest because arg is eval'd twice */
    }
    nczm_join(segments,&keyprefix);
    nclistfreeall(segments);
    ncurifree(uri);
}
#endif

static char*
makekey(const char* key)
{
    char* truekey = NULL;
    nczm_concat(keyprefix,key,&truekey);
    return truekey;
}

static void
title(const char* func)
{
    printf("testing: %s:\n",func);
    fflush(stdout);
}

static int
report(int pf, const char* op, NCZMAP* map)
{
    const char* result;
    switch (pf) {
    case PASS: result = "Pass"; break;
    case XFAIL: result = "XFail"; break;
    case FAIL: default: result = "Fail"; break;
    }
    fprintf(stderr,"%s: %s\n",result,op);
    fflush(stderr);
    if(pf == FAIL) {
        if(map) (void)nczmap_close(map,0);
        exit(1);
    }
    return NC_NOERR;
}
