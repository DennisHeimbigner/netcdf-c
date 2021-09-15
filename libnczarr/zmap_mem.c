/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zincludes.h"
#include "zmap.h"
#include "buddy_alloc.h"

/*
Map our simplified map model to memory.
It mimics the S3 bucket + objects model.
The bucket is a sorted list of full keys pointing to objects.
The size is kept in the bucket.

Given a key, one of several things is true.
1. The key points to a content-bearing object
2. The key has no associated object
This basically means that there is no notion of not-found:
all keys are assumed to exist, but may have no content.

The implementation is based on a sorted vector of (key,objectptr) pairs.

Additionally, we need a malloc-like allocator since the
whole thing must be stored within a single contiguous
piece of memory. That also means that all pointers
must be relative to the beginning of that contiguous memory.
The allocator is in buddy_alloc.h
*/

#undef DEBUG
#define DEBUGERRORS

#define NCZM_MAP_V1 1

/* Define the "subclass" of NCZMAP */
typedef struct ZMEMMAP {
    NCZMAP map;
    struct buddy* allocator;
    NC_memio memio;
} ZMEMMAP;


static void freevector(size_t nkeys, char** list);

static int maketruekey(const char* rootpath, const char* key, char** truekeyp);

/* Define the Dataset level API */

static int
zmemcreate(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    ZMEMMAP* z3map = NULL;
    NCURI* url = NULL;
    char* prefix = NULL;
    char* truekey = NULL;
    NC_memio* memio = (NC_memio*)parameters;
	
    NC_UNUSED(flags);

    ZTRACE(6,"path=%s mode=%d flag=%llu",path,mode,flags);

    if(!zmeminitialized) zmeminitialize();

    /* Build the zmem state */
    if((zmemmap = (ZMEMMAP*)calloc(1,sizeof(ZMEMMAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    zmemmap->map.format = NCZM_MEM;
    zmemmap->map.url = strdup(path);
    zmemmap->map.mode = mode;
    zmemmap->map.flags = flags;
    zmemmap->map.api = (NCZMAP_API*)&nczmemsdkapi;

    /* Parse the URL */
    ncuriparse(path,&url);
    if(url == NULL)
        {stat = NC_EURL; goto done;}

    /* The actual file name is ignored */

    /* Convert to canonical path-style */
    if((stat = NCZ_s3urlprocess(url,&zmemmap->s3))) goto done;
    /* Verify the root path */
    if(zmemmap->s3.rootkey == NULL)
        {stat = NC_EURL; goto done;}

    if((stat=NCZ_s3sdkcreateconfig(zmemmap->s3.host, zmemmap->s3.region, &zmemmap->s3config))) goto done;
    if((stat = NCZ_s3sdkcreateclient(zmemmap->s3config,&zmemmap->s3client))) goto done;

    {
	int exists = 0;
        /* Does bucket already exist */
	if((stat = NCZ_s3sdkbucketexists(zmemmap->s3client,zmemmap->s3.bucket,&exists, &zmemmap->errmsg))) goto done;
	if(!exists) {
	    /* create it */
	    if((stat = NCZ_s3sdkbucketcreate(zmemmap->s3client,zmemmap->s3.region,zmemmap->s3.bucket,&zmemmap->errmsg)))
	        goto done;
	}
	/* The root object should not exist */
        switch (stat = NCZ_s3sdkinfo(zmemmap->s3client,zmemmap->s3.bucket,zmemmap->s3.rootkey,NULL,&zmemmap->errmsg)) {
	case NC_EEMPTY: /* no such object */
	    stat = NC_NOERR;  /* which is what we want */
	    errclear(zmemmap);
	    break;
	case NC_NOERR: stat = NC_EOBJECT; goto done; /* already exists */
	default: reporterr(zmemmap); goto done;
	}
	if(!stat) {
            /* Delete objects inside root object tree */
            s3clear(zmemmap,zmemmap->s3.rootkey);
	}
    }
    
    if(mapp) *mapp = (NCZMAP*)zmemmap;    

done:
    reporterr(zmemmap);
    ncurifree(url);
    nullfree(prefix);
    nullfree(truekey);
    if(stat) nczmap_close((NCZMAP*)zmemmap,1);
    return ZUNTRACE(stat);
}

/* The problem with open is that there
no obvious way to test for existence.
So, we assume that the dataset must have
some content. We look for that */
static int
zmemopen(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    ZMEMMAP* zmemmap = NULL;
    NCURI* url = NULL;
    NClist* content = NULL;
    size_t nkeys = 0;

    NC_UNUSED(flags);
    NC_UNUSED(parameters);

    ZTRACE(6,"path=%s mode=%d flags=%llu",path,mode,flags);

    if(!zmeminitialized) zmeminitialize();

    /* Build the z4 state */
    if((zmemmap = (ZMEMMAP*)calloc(1,sizeof(ZMEMMAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    zmemmap->map.format = NCZM_S3;
    zmemmap->map.url = strdup(path);
    zmemmap->map.mode = mode;
    zmemmap->map.flags = flags;
    zmemmap->map.api = (NCZMAP_API*)&nczmemsdkapi;

    /* Parse the URL */
    if((stat = ncuriparse(path,&url))) goto done;
    if(url == NULL)
        {stat = NC_EURL; goto done;}

    /* Convert to canonical path-style */
    if((stat = NCZ_s3urlprocess(url,&zmemmap->s3))) goto done;
    /* Verify root path */
    if(zmemmap->s3.rootkey == NULL)
        {stat = NC_EURL; goto done;}

    if((stat=NCZ_s3sdkcreateconfig(zmemmap->s3.host,zmemmap->s3.region,&zmemmap->s3config))) goto done;
    if((stat=NCZ_s3sdkcreateclient(zmemmap->s3config,&zmemmap->s3client))) goto done;

    /* Search the root for content */
    content = nclistnew();
    if((stat = NCZ_s3sdkgetkeys(zmemmap->s3client,zmemmap->s3.bucket,zmemmap->s3.rootkey,&nkeys,NULL,&zmemmap->errmsg)))
	goto done;
    if(nkeys == 0) {
	/* dataset does not actually exist; we choose to return ENOOBJECT instead of EEMPTY */
	stat = NC_ENOOBJECT;
	goto done;
    }
    if(mapp) *mapp = (NCZMAP*)zmemmap;    

done:
    reporterr(zmemmap);
    nclistfreeall(content);
    ncurifree(url);
    if(stat) nczmap_close((NCZMAP*)zmemmap,0);
    return ZUNTRACE(stat);
}

/**************************************************/
/* Object API */

/*
@return NC_NOERR if key points to a content-bearing object.
@return NC_EEMPTY if object at key has no content.
@return NC_EXXX return true error
*/
static int
zmemexists(NCZMAP* map, const char* key)
{
    int stat = NC_NOERR;
    ZTRACE(6,"map=%s key=%s",map->url,key);
    stat = zmemlen(map,key,NULL);
    return ZUNTRACE(stat);
}

/*
@return NC_NOERR if key points to a content-bearing object.
@return NC_EEMPTY if object at key has no content.
@return NC_EXXX return true error
*/
static int
zmemlen(NCZMAP* map, const char* key, size64_t* lenp)
{
    int stat = NC_NOERR;
    ZMEMMAP* zmemmap = (ZMEMMAP*)map;
    char* truekey = NULL;

    ZTRACE(6,"map=%s key=%s",map->url,key);

    if((stat = maketruekey(zmemmap->s3.rootkey,key,&truekey))) goto done;

    switch (stat = NCZ_s3sdkinfo(zmemmap->s3client,zmemmap->s3.bucket,truekey,lenp,&zmemmap->errmsg)) {
    case NC_NOERR: break;
    case NC_EEMPTY:
	if(lenp) *lenp = 0;
	goto done;
    default:
        goto done;
    }
done:
    nullfree(truekey);
    reporterr(zmemmap);
    return ZUNTRACE(stat);
}

/*
@return NC_NOERR if object at key was read
@return NC_EEMPTY if object at key has no content.
@return NC_EXXX return true error
*/
static int
zmemread(NCZMAP* map, const char* key, size64_t start, size64_t count, void* content)
{
    int stat = NC_NOERR;
    ZMEMMAP* zmemmap = (ZMEMMAP*)map; /* cast to true type */
    size64_t size = 0;
    char* truekey = NULL;
    
    ZTRACE(6,"map=%s key=%s start=%llu count=%llu",map->url,key,start,count);

    if((stat = maketruekey(zmemmap->s3.rootkey,key,&truekey))) goto done;
    
    switch (stat=NCZ_s3sdkinfo(zmemmap->s3client, zmemmap->s3.bucket, truekey, &size, &zmemmap->errmsg)) {
    case NC_NOERR: break;
    case NC_EEMPTY: goto done;
    default: goto done; 	
    }
    /* Sanity checks */
    if(start >= size || start+count > size)
        {stat = NC_EEDGE; goto done;}
    if(count > 0)  {
        if((stat = NCZ_s3sdkread(zmemmap->s3client, zmemmap->s3.bucket, truekey, start, count, content, &zmemmap->errmsg)))
            goto done;
    }
done:
    nullfree(truekey);
    reporterr(zmemmap);
    return ZUNTRACE(stat);
}

/*
@return NC_NOERR if key content was written
@return NC_EEMPTY if object at key has no content.
@return NC_EXXX return true error
*/
static int
zmemwrite(NCZMAP* map, const char* key, size64_t start, size64_t count, const void* content)
{
    int stat = NC_NOERR;
    ZMEMMAP* zmemmap = (ZMEMMAP*)map; /* cast to true type */
    char* chunk = NULL; /* use char* so we can do arithmetic with it */
    size64_t objsize = 0;
    size64_t memsize = 0;
    size64_t endwrite = start+count; /* first pos just above overwritten data */
    char* truekey = NULL;
    int isempty = 0;
	
    ZTRACE(6,"map=%s key=%s start=%llu count=%llu",map->url,key,start,count);

    if((stat = maketruekey(zmemmap->s3.rootkey,key,&truekey))) goto done;

    /* Apparently S3 has no write byterange operation, so we need to read the whole object,
       copy data, and then rewrite */       
    switch (stat=NCZ_s3sdkinfo(zmemmap->s3client, zmemmap->s3.bucket, truekey, &objsize, &zmemmap->errmsg)) {
    case NC_NOERR: /* Figure out the memory size of the object */
	memsize = (endwrite > objsize ? endwrite : objsize);
        break;
    case NC_EEMPTY:
	memsize = endwrite;
	isempty = 1;
        break;
    default: reporterr(zmemmap); goto done;
    }

    if(isempty)
        chunk = (char*)calloc(1,memsize); /* initialize it */
    else
        chunk = (char*)malloc(memsize);
    if(chunk == NULL)
	{stat = NC_ENOMEM; goto done;}
    if(start > 0 && objsize > 0) { /* must read to preserve data before start */
        if((stat = NCZ_s3sdkread(zmemmap->s3client, zmemmap->s3.bucket, truekey, 0, objsize, (void*)chunk, &zmemmap->errmsg)))
            goto done;
    }
#if 0
    if(newsize > objsize) {
        /* Zeroize the part of the object added */
	memset(((char*)chunk)+objsize,0,(newsize-objsize));
	objsize = newsize;
    }
#endif
    /* overwrite the relevant part of the memory with the contents */
    if(count > 0)
        memcpy(((char*)chunk)+start,content,count); /* there may be data above start+count */
    /* (re-)write */
    if((stat = NCZ_s3sdkwriteobject(zmemmap->s3client, zmemmap->s3.bucket, truekey, memsize, (void*)chunk, &zmemmap->errmsg)))
        goto done;

done:
    nullfree(truekey);
    reporterr(zmemmap);
    nullfree(chunk);
    return ZUNTRACE(stat);
}

static int
zmemclose(NCZMAP* map, int deleteit)
{
    int stat = NC_NOERR;
    ZMEMMAP* zmemmap = (ZMEMMAP*)map;

    ZTRACE(6,"map=%s deleteit=%d",map->url, deleteit);

    if(deleteit) 
        s3clear(zmemmap,zmemmap->s3.rootkey);
    if(zmemmap->s3client && zmemmap->s3config && zmemmap->s3.bucket && zmemmap->s3.rootkey) {
        NCZ_s3sdkclose(zmemmap->s3client, zmemmap->s3config, zmemmap->s3.bucket, zmemmap->s3.rootkey, deleteit, &zmemmap->errmsg);
    }
    reporterr(zmemmap);
    zmemmap->s3client = NULL;
    zmemmap->s3config = NULL;
    nullfree(zmemmap->s3.bucket);
    nullfree(zmemmap->s3.region);
    nullfree(zmemmap->s3.host);
    nullfree(zmemmap->errmsg);
    nullfree(zmemmap->s3.rootkey)
    nczm_clear(map);
    nullfree(map);
    return ZUNTRACE(stat);
}

/*
Return a list of full keys immediately "below" a specified prefix,
but not including the prefix.
In theory, the returned list should be sorted in lexical order,
but it possible that it is not.
@return NC_NOERR if success, even if no keys returned.
@return NC_EXXX return true error
*/
static int
zmemsearch(NCZMAP* map, const char* prefix, NClist* matches)
{
    int i,stat = NC_NOERR;
    ZMEMMAP* zmemmap = (ZMEMMAP*)map;
    char** list = NULL;
    size_t nkeys;
    NClist* tmp = NULL;
    char* trueprefix = NULL;
    char* newkey = NULL;
    const char* p;

    ZTRACE(6,"map=%s prefix0=%s",map->url,prefix);
    
    if((stat = maketruekey(zmemmap->s3.rootkey,prefix,&trueprefix))) goto done;
    
    if(*trueprefix != '/') return NC_EINTERNAL;
    if((stat = NCZ_s3sdkgetkeys(zmemmap->s3client,zmemmap->s3.bucket,trueprefix,&nkeys,&list,&zmemmap->errmsg)))
        goto done;
    if(nkeys > 0) {
	size_t tplen = strlen(trueprefix);
	tmp = nclistnew();
	/* Remove the trueprefix from the front of all the returned keys */
        for(i=0;i<nkeys;i++) {
	    const char* l = list[i];
	    if(memcmp(trueprefix,l,tplen)==0) {
		p  = l+tplen; /* Point to start of suffix */
		/* If the key is same as trueprefix, ignore it */
		if(*p == '\0') continue;
		if(nczm_segment1(p,&newkey)) goto done;
	        nclistpush(tmp,newkey); newkey = NULL;
	    }
        }
	/* Now remove duplicates */
	for(i=0;i<nclistlength(tmp);i++) {
	    int j;
	    int duplicate = 0;
	    const char* is = nclistget(tmp,i);
	    for(j=0;j<nclistlength(matches);j++) {
	        const char* js = nclistget(matches,j);
	        if(strcmp(js,is)==0) {duplicate = 1; break;} /* duplicate */
	    }	    
	    if(!duplicate)
	        nclistpush(matches,strdup(is));
	}
	nclistfreeall(tmp); tmp = NULL;
    }
	
#ifdef DEBUG
    for(i=0;i<nclistlength(matches);i++) {
	const char* is = nclistget(matches,i);
	fprintf(stderr,"search: %s\n",is);
    }
#endif

done:
    nullfree(newkey);
    nullfree(trueprefix);
    reporterr(zmemmap);
    nclistfreeall(tmp);
    freevector(nkeys,list);
    return ZUNTRACEX(stat,"|matches|=%d",(int)nclistlength(matches));
}

/**************************************************/
/* S3 Utilities */

/*
Remove all objects with keys which have
rootkey as prefix; rootkey is a truekey
*/
static int
s3clear(ZMEMMAP* zmemmap, const char* rootkey)
{
    int stat = NC_NOERR;
    char** list = NULL;
    char** p;
    size_t nkeys = 0;

    if(zmemmap->s3client && zmemmap->s3.bucket && rootkey) {
        if((stat = NCZ_s3sdksearch(zmemmap->s3client, zmemmap->s3.bucket, rootkey, &nkeys, &list, &zmemmap->errmsg)))
            goto done;
        if(list != NULL) {
            for(p=list;*p;p++) {
	        /* If the key is the rootkey, skip it */
	        if(strcmp(rootkey,*p)==0) continue;
#ifdef S3DEBUG
fprintf(stderr,"s3clear: %s\n",*p);
#endif
                if((stat = NCZ_s3sdkdeletekey(zmemmap->s3client, zmemmap->s3.bucket, *p, &zmemmap->errmsg)))	
	            goto done;
	    }
        }
    }

done:
    reporterr(zmemmap);
    NCZ_freestringvec(nkeys,list);
    return THROW(stat);
}

/* Prefix key with path to root to make true key */
static int
maketruekey(const char* rootpath, const char* key, char** truekeyp)
{
    int  stat = NC_NOERR;
    char* truekey = NULL;
    size_t len, rootlen, keylen;

    if(truekeyp == NULL) goto done;
    rootlen = strlen(rootpath);
    keylen = strlen(key);
    len = (rootlen+keylen+1+1+1);
    
    truekey = (char*)malloc(len+1);
    if(truekey == NULL) {stat = NC_ENOMEM; goto done;}
    truekey[0] = '\0';
    if(rootpath[0] != '/')    
        strlcat(truekey,"/",len+1);
    strlcat(truekey,rootpath,len+1);
    if(rootpath[rootlen-1] != '/')
        strlcat(truekey,"/",len+1);
    if(key[0] == '/') key++;
    strlcat(truekey,key,len+1);
    if(key[keylen-1] == '/') /* remove any trailing '/' */
	truekey[strlen(truekey)-1] = '\0';
    *truekeyp = truekey; truekey = NULL;       

done:
    nullfree(truekey);
    return stat;
}

static void
freevector(size_t nkeys, char** list)
{
    size_t i;
    if(list) {
        for(i=0;i<nkeys;i++) nullfree(list[i]);
	nullfree(list);
    }
}

/**************************************************/
/* External API objects */

NCZMAP_DS_API zmap_s3sdk;
NCZMAP_DS_API zmap_s3sdk = {
    NCZM_S3SDK_V1,
    ZMEM_PROPERTIES,
    zmemcreate,
    zmemopen,
};

static NCZMAP_API
nczmemsdkapi = {
    NCZM_S3SDK_V1,
    zmemclose,
    zmemexists,
    zmemlen,
    zmemread,
    zmemwrite,
    zmemsearch,
};
