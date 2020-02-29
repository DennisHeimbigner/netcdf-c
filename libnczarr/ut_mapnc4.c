/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "config.h"
#include "zincludes.h"
#include "ztest.h"

#undef SETUP
#undef DEBUG

#define FILE1 "testmapnc4.ncz"
#define URL1 "file://" FILE1 "#mode=zarr"

#define ZARRROOT "/_nczarr"
#define META1 "/meta1"
#define META2 "/meta2"
#define DATA1 "/data1"
#define DATA1LEN 25

static const char* metadata1 = "{\n\"foo\": 42,\n\"bar\": \"apples\",\n\"baz\": [1, 2, 3, 4]}";

static const char* metadata2 = "{\n\"foo\": 42,\n\"bar\": \"apples\",\n\"baz\": [1, 2, 3, 4],\n\"extra\": 137}";

/* Forward */
static int simplecreate(void);
static int simpledelete(void);
static int writemeta(void);
static int writemeta2(void);
static int readmeta(void);
static int writedata(void);
static int readdata(void);
static int search(void);

int
main(int argc, char** argv)
{
    int stat = NC_NOERR;
    struct Test test;

    memset(&test,0,sizeof(test));
    if((stat = ut_init(argc, argv, &test))) goto done;

    if(strcmp(test.cmd,"create")==0)
	stat=simplecreate();
    else if(strcmp(test.cmd,"delete")==0)
	stat=simpledelete();
    else if(strcmp(test.cmd,"writemeta")==0)
	stat = writemeta();
    else if(strcmp(test.cmd,"writemeta2")==0)
	stat = writemeta2();
    else if(strcmp(test.cmd,"readmeta")==0)
	stat = readmeta();
    else if(strcmp(test.cmd,"writedata")==0)
	stat = writedata();
    else if(strcmp(test.cmd,"readdata")==0)
	stat = readdata();
    else if(strcmp(test.cmd,"search")==0)
	stat = search();
    else {
	fprintf(stderr,"unknown command specified\n");
	exit(1);
    }

done:
    if(stat)
	nc_strerror(stat);
    return (stat ? 1 : 0);    
}

/* Do a simple create */
static int
simplecreate(void)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    char* path = NULL;

    unlink(FILE1);

    if((stat = nczmap_create(NCZM_NC4,URL1,0,0,NULL,&map)))
	goto done;

    if((stat=nczm_suffix(NULL,ZARRROOT,&path)))
	goto done;
    if((stat = nczmap_def(map, path, NCZ_ISMETA)))
	goto done;

    /* Do not delete so we can look at it with ncdump */
    if((stat = nczmap_close(map,0)))
	goto done;
done:
    return THROW(stat);
}

/* Do a simple delete of previously created file */
static int
simpledelete(void)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;

#ifdef SETUP
    NCCHECK(simplecreate());
#endif

    if((stat = nczmap_open(NCZM_NC4,URL1,0,0,NULL,&map)))
	goto done;
    if((stat = nczmap_close(map,1)))
	goto done;

done:
    return THROW(stat);
}

static int
writemeta(void)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    char* path = NULL;

    unlink(FILE1);

    if((stat = nczmap_create(NCZM_NC4,URL1,0,0,NULL,&map)))
	goto done;

    if((stat=nczm_suffix(NULL,ZARRROOT,&path)))
	goto done;
    if((stat = nczmap_def(map, path, NCZ_ISMETA)))
	goto done;

    if((stat=nczm_suffix(NULL,META1,&path)))
	goto done;
    if((stat = nczmap_def(map, path, NCZ_ISMETA)))
	goto done;

    if((stat = nczmap_writemeta(map, META1, strlen(metadata1), metadata1)))
	goto done;

    /* Do not delete so we can look at it with ncdump */
    if((stat = nczmap_close(map,0)))
	goto done;
done:
    nullfree(path);
    return THROW(stat);
}

static int
writemeta2(void)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    char* path = NULL;

#ifdef SETUP
    NCCHECK(writemeta());
#endif
 
    if((stat = nczmap_open(NCZM_NC4,URL1,NC_WRITE,0,NULL,&map)))
	goto done;

    if((stat=nczm_suffix(NULL,META2,&path)))
	goto done;

    if((stat = nczmap_def(map,path,NCZ_ISMETA)))
	goto done;

    if((stat = nczmap_writemeta(map, META2, strlen(metadata2), metadata2)))
	goto done;

    /* Do not delete so we can look at it with ncdump */
    if((stat = nczmap_close(map,0)))
	goto done;
done:
    nullfree(path);
    return THROW(stat);
}

static int
readmeta(void)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    char* path = NULL;
    size64_t olen;
    char* content = NULL;

#ifdef SETUP
    NCCHECK(writemeta2());
#endif

    if((stat = nczmap_open(NCZM_NC4,URL1,0,0,NULL,&map)))
	goto done;

    if((stat=nczm_suffix(NULL,META1,&path)))
	goto done;

    /* Get length */
    if((stat = nczmap_len(map, path, &olen)))
	goto done;

    /* Allocate the space for reading the metadata (might be overkill) */
    if((content = malloc(olen+1)) == NULL)
	{stat = NC_ENOMEM; goto done;}

    if((stat = nczmap_readmeta(map, path, olen, content)))
	goto done;

    /* nul terminate */
    content[olen] = '\0';

    printf("%s: |%s|\n",META1,content);

    if((stat = nczmap_close(map,0)))
	goto done;
done:
    nullfree(content);
    nullfree(path);
    return THROW(stat);
}

static int
writedata(void)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    char* path = NULL;
    int data1[DATA1LEN];
    int i;
    size64_t totallen;
    char* data1p = (char*)&data1[0]; /* byte level version of data1 */

    /* Take output of writemeta2 */
#ifdef SETUP
    NCCHECK(writemeta2());
#endif

    /* Create the data */
    for(i=0;i<DATA1LEN;i++)
	data1[i] = i;
    totallen = sizeof(int)*DATA1LEN;

    if((stat = nczmap_open(NCZM_NC4,URL1,NC_WRITE,0,NULL,&map)))
	goto done;

    /* ensure object */
    if((stat=nczm_suffix(NULL,DATA1,&path)))
	goto done;

    if((stat = nczmap_def(map,path,totallen)))
	goto done;

    /* Write in 3 slices */
    for(i=0;i<3;i++) {
        size64_t start, count, third, last;
	third = (totallen+2) / 3; /* round up */
        start = i * third;
	last = start + third;
	if(last > totallen) 
	    last = totallen;
	count = last - start;
        
	if((stat = nczmap_write(map, DATA1, start, count, &data1p[start])))
	     goto done;
    }

    /* Do not delete so we can look at it with ncdump */
    if((stat = nczmap_close(map,0)))
	goto done;
done:
    nullfree(path);
    return THROW(stat);
}

static int
readdata(void)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    char* path = NULL;
    int data1[DATA1LEN];
    int i;
    size64_t chunklen, totallen;
    char* data1p = NULL; /* byte level pointer into data1 */

    /* Take output of writedata */
#ifdef SETUP
    NCCHECK(writedata());
#endif

    if((stat = nczmap_open(NCZM_NC4,URL1,0,0,NULL,&map)))
	goto done;

    /* ensure object */
    if((stat=nczm_suffix(NULL,DATA1,&path)))
	goto done;

    if((stat = nczmap_exists(map,path)))
	goto done;

    /* Read chunks in size sizeof(int)*n, where is rndup(DATA1LEN/3) */
    chunklen = sizeof(int) * ((DATA1LEN+2)/3);
    data1p = (char*)&data1[0];
    totallen = sizeof(int)*DATA1LEN;

    /* Read in 3 chunks */
    memset(data1,0,sizeof(data1));
    for(i=0;i<3;i++) {
        size64_t start, count, last;
        start = i * chunklen;
	last = start + chunklen;
	if(last > totallen) 
	    last = totallen;
	count = last - start;
	if((stat = nczmap_read(map, DATA1, start, count, &data1p[start])))
	     goto done;
    }

    /* Validate */
    for(i=0;i<DATA1LEN;i++) {
	if(data1[i] != i) {
	    fprintf(stderr,"data mismatch: is: %d should be: %d\n",data1[i],i);
	    stat = NC_EINVAL;
	    goto done;
	}
    }

    /* Do not delete so we can look at it with ncdump */
    if((stat = nczmap_close(map,0)))
	goto done;
done:
    nullfree(path);
    return THROW(stat);
}

/* Currently no tests */
static int
search(void)
{
    int i,stat = NC_NOERR;
    NCZMAP* map = NULL;
    NClist* matches = nclistnew();

    /* Take output of writedata */
#ifdef SETUP
    NCCHECK(writedata());
#endif

    if((stat = nczmap_open(NCZM_NC4,URL1,0,0,NULL,&map)))
	goto done;

    /* Do a search on root to get all objects */
    if((stat=nczmap_search(map,"/",matches)))
	goto done;

    /* Print out the list */
    for(i=0;i<nclistlength(matches);i++) {
	const char* path = nclistget(matches,i);
	printf("[%d] %s\n",i,path);
    }

    /* Do not delete so later tests can use it */
    if((stat = nczmap_close(map,0)))
	goto done;
done:
    nclistfree(matches);
    return THROW(stat);
}
