/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zincludes.h"
#include "zmap.h"
#include "zs3sdk.h"

/*
Map our simplified map model to an S3 bucket + objects.
The whole data

For the API, the mapping is as follows:
1. The whole dataset is mapped to a bucket.
2. Containment is simulated using the S3 key conventions.
3. Every object (e.g. group or array) is mapped to an S3 object
4. Meta data objects (e.g. .zgroup, .zarray, etc) are kept as an S3 object.
5. Actual variable data (for e.g. chunks) is stored as
   using an S3 object per chunk.

Notes:
1. Our canonical URLs use path style rather than virtual-host
*/

#undef DEBUG

#define NCZM_S3SDK_V1 1

#define AWSHOST ".amazonaws.com"

enum URLFORMAT {UF_NONE, UF_VIRTUAL, UF_PATH, UF_OTHER};

/* Define the "subclass" of NCZMAP */
typedef struct ZS3MAP {
    NCZMAP map;
    enum URLFORMAT urlformat;
    char* host; /* non-null if other*/
    char* bucket; /* bucket name */
    char* region; /* region */
    void* s3config;
    void* s3client;
    char* errmsg;
} ZS3MAP;

/* Forward */
static NCZMAP_API nczs3sdkapi; // c++ will not allow static forward variables
static int zs3exists(NCZMAP* map, const char* key);
static int zs3len(NCZMAP* map, const char* key, size64_t* lenp);
static int zs3define(NCZMAP* map, const char* key, size64_t len);
static int zs3read(NCZMAP* map, const char* key, size64_t start, size64_t count, void* content);
static int zs3write(NCZMAP* map, const char* key, size64_t start, size64_t count, const void* content);
static int zs3readmeta(NCZMAP* map, const char* key, size64_t avail, char* content);
static int zs3writemeta(NCZMAP* map, const char* key, size64_t count, const char* content);
static int zs3search(NCZMAP* map, const char* prefix, NClist* matches);

static int zs3close(NCZMAP* map, int deleteit);
static int z3createobj(ZS3MAP*, const char* key, size64_t len);

static int processurl(ZS3MAP* z3map, NCURI* url);
static int endswith(const char* s, const char* suffix);

static void zs3initialize(void);
static int s3clear(ZS3MAP* z3map);
static int isLegalBucketName(const char* bucket);

static void
reporterr(ZS3MAP* z3map)
{
    if(z3map->errmsg) {
        nclog(NCLOGERR,z3map->errmsg);
	nullfree(z3map->errmsg);
    }
    z3map->errmsg = NULL;
}

/* Define the Dataset level API */

static int zs3initialized = 0;

static void
zs3initialize(void)
{
    if(!zs3initialized)
        NCZ_s3sdkinitialize();
    zs3initialized = 1;
}

#if 0
static void
zs3finalize(void)
{
    if(zs3initialized)
        NCZ_s3sdkfinalize();
    zs3initialized = 0;
}
#endif

static int
zs3create(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    ZS3MAP* z3map = NULL;
    NCURI* url = NULL;
	
    NC_UNUSED(flags);
    NC_UNUSED(parameters);

    if(!zs3initialized) zs3initialize();

    /* Build the z4 state */
    if((z3map = (ZS3MAP*)calloc(1,sizeof(ZS3MAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    /* Parse the URL */
    ncuriparse(path,&url);
    if(url == NULL)
        {stat = NC_EURL; goto done;}
    if((stat = processurl(z3map,url))) goto done;

    z3map->map.format = NCZM_S3;
    z3map->map.url = strdup(path);
    z3map->map.mode = mode;
    z3map->map.flags = flags;
    z3map->map.api = (NCZMAP_API*)&nczs3sdkapi;

    if((stat=NCZ_s3sdkcreateconfig(z3map->host, z3map->region, &z3map->s3config))) goto done;
    if((stat = NCZ_s3sdkcreateclient(z3map->s3config,&z3map->s3client))) goto done;
    {
	int exists = 0;
        /* Does bucket already exist */
	if((stat = NCZ_s3sdkbucketexists(z3map->s3client,z3map->bucket,&exists, &z3map->errmsg))) goto done;
	if(!exists) {
	    /* create it */
	    if((stat = NCZ_s3sdkbucketcreate(z3map->s3client,z3map->region,z3map->bucket,&z3map->errmsg)))
	        goto done;
	}
        /* Delete objects in bucket */
        s3clear(z3map);
    }
    
    if(mapp) *mapp = (NCZMAP*)z3map;    

done:
    if(z3map->errmsg) reporterr(z3map);
    ncurifree(url);
    if(stat) nczmap_close((NCZMAP*)z3map,1);
    return (stat);
}

static int
zs3open(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    ZS3MAP* z3map = NULL;
    NCURI* url = NULL;
    
    NC_UNUSED(flags);
    NC_UNUSED(parameters);

    if(!zs3initialized) zs3initialize();

    /* Build the z4 state */
    if((z3map = (ZS3MAP*)calloc(1,sizeof(ZS3MAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    /* Parse the URL */
    if((stat = ncuriparse(path,&url))) goto done;
    if(url == NULL)
        {stat = NC_EURL; goto done;}
    if((stat = processurl(z3map,url))) goto done;

    z3map->map.format = NCZM_S3;
    z3map->map.url = strdup(path);
    z3map->map.mode = mode;
    z3map->map.flags = flags;
    z3map->map.api = (NCZMAP_API*)&nczs3sdkapi;

    if((stat=NCZ_s3sdkcreateconfig(z3map->host,z3map->region,&z3map->s3config))) goto done;
    if((stat=NCZ_s3sdkcreateclient(z3map->s3config,&z3map->s3client))) goto done;

    if(mapp) *mapp = (NCZMAP*)z3map;    

done:
    ncurifree(url);
    if(stat) nczmap_close((NCZMAP*)z3map,0);
    return (stat);
}

static int
isLegalBucketName(const char* bucket)
{
    return 1;
}

/**************************************************/
/* Object API */

static int
zs3exists(NCZMAP* map, const char* key)
{
    return zs3len(map,key,NULL);
}

static int
zs3len(NCZMAP* map, const char* key, size64_t* lenp)
{
    int stat = NC_NOERR;
    ZS3MAP* z3map = (ZS3MAP*)map;
    if((stat = NCZ_s3sdkinfo(z3map->s3client,z3map->bucket,key,lenp,&z3map->errmsg)))
        goto done;
done:
    if(z3map->errmsg) reporterr(z3map);
    return (stat);
}

static int
zs3define(NCZMAP* map, const char* key, size64_t len)
{
    int stat = NC_NOERR;
    ZS3MAP* z3map = (ZS3MAP*)map; /* cast to true type */

    if((stat = zs3exists(map,key)) == NC_NOERR) 
	goto done; /* Already exists */
    else if(stat != NC_EACCESS) /* Some other kind of failure */
	goto done;
    if((stat = z3createobj(z3map,key,len)))
	goto done;

done:
    return (stat);
}

static int
zs3read(NCZMAP* map, const char* key, size64_t start, size64_t count, void* content)
{
    int stat = NC_NOERR;
    ZS3MAP* z3map = (ZS3MAP*)map; /* cast to true type */
 
    if((stat=NCZ_s3sdkinfo(z3map->s3client, z3map->bucket, key, NULL, &z3map->errmsg)))
	goto done; 	
    if((stat = NCZ_s3sdkread(z3map->s3client, z3map->bucket, key, start, count, content, &z3map->errmsg)))
        goto done;
done:
    if(z3map->errmsg) reporterr(z3map);
    return (stat);
}

static int
zs3write(NCZMAP* map, const char* key, size64_t start, size64_t count, const void* content)
{
    int stat = NC_NOERR;
    ZS3MAP* z3map = (ZS3MAP*)map; /* cast to true type */
    void* chunk = NULL;
    size64_t objsize = 0;
    unsigned char* newchunk = NULL;
    size64_t newsize = start + count;
    int exists;
	
    if(count == 0) goto done;

    /* Apparently S3 has no write byterange operation, so we need to read the whole object,
       copy data, and then rewrite */       
    switch (stat=NCZ_s3sdkinfo(z3map->s3client, z3map->bucket, key, &objsize, &z3map->errmsg)) {
    case NC_NOERR: exists = 1; break;
    case NC_EACCESS: exists = 0; break;
    default: exists = 0; goto done;
    }
    if(exists) {
        if((stat = NCZ_s3sdkreadobject(z3map->s3client, z3map->bucket, key, &objsize, &chunk, &z3map->errmsg)))
            goto done;
    } else { /* fake it */
	objsize = newsize;	
	if((chunk = (unsigned char*)malloc(objsize))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	memset(chunk,0,objsize);	
    }
    if(newsize > objsize) {
	/* Reallocate */
	/* TODO: consider multipart write */
	if((newchunk = (unsigned char*)malloc(newsize))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	if(start > 0) memcpy(newchunk,chunk,start);
	memcpy(&newchunk[start],content,count);
	free(chunk);
	chunk = newchunk;
	newchunk = NULL;
	objsize = newsize;
    } else
	memcpy(&((unsigned char*)chunk)[start],content,count); /* remember there may be data above start+count */
    if((stat = NCZ_s3sdkwriteobject(z3map->s3client, z3map->bucket, key, objsize, chunk, &z3map->errmsg)))
        goto done;

done:
    nullfree(chunk);
    nullfree(newchunk);
    return (stat);
}

static int
zs3readmeta(NCZMAP* map, const char* key, size64_t avail, char* content)
{
    return zs3read(map,key,0,avail,content);
}

static int
zs3writemeta(NCZMAP* map, const char* key, size64_t count, const char* content)
{
    return zs3write(map,key,0,count,content);
}

static int
zs3close(NCZMAP* map, int deleteit)
{
    int stat = NC_NOERR;
    ZS3MAP* z3map = (ZS3MAP*)map;

    if(deleteit)
        s3clear(z3map);
    NCZ_s3sdkclose(z3map->s3client, z3map->s3config);
    z3map->s3client = NULL;
    z3map->s3config = NULL;
    nullfree(z3map->bucket);
    nullfree(z3map->region);
    nullfree(z3map->host);
    nullfree(z3map->errmsg);
    nczm_clear(map);
    nullfree(map);
    return (stat);
}

/*
Return a list of keys immediately "below" a specified prefix.
In theory, the returned list should be sorted in lexical order,
but it possible that it is not.
*/
static int
zs3search(NCZMAP* map, const char* prefix, NClist* matches)
{
    int i,stat = NC_NOERR;
    ZS3MAP* z3map = (ZS3MAP*)map;
    char** list = NULL;
    size_t nkeys;

    if(*prefix == '/') prefix++; /* Elide leading '/' */
    if((stat = NCZ_s3sdkgetkeys(z3map->s3client,z3map->bucket,prefix,"/",&nkeys,&list,&z3map->errmsg)))
        goto done;
    for(i=0;i<nkeys;i++)
	nclistpush(matches,list[i]);
    
done:
    nullfree(list);
    if(z3map->errmsg) reporterr(z3map);
    return THROW(stat);
}

/**************************************************/
/* Utilities */

static int
processurl(ZS3MAP* z3map, NCURI* url)
{
    int stat = NC_NOERR;
    NClist* segments = NULL;
    NCbytes* pathhost = NULL;
    
    if(url == NULL)
        {stat = NC_EURL; goto done;}
    /* do some verification */
    if(strcmp(url->protocol,"https") != 0)
        {stat = NC_EURL; goto done;}

    /* Distinguish path-style from virtual-host style from other:
       Virtual: https://bucket-name.s3.Region.amazonaws.com
       Path: https://s3.Region.amazonaws.com/bucket-name
       Other: https://<host>/bucketname
    */
    if(url->host == NULL || strlen(url->host) == 0)
        {stat = NC_EURL; goto done;}
    if(endswith(url->host,AWSHOST)) { /* Virtual or path */
        segments = nclistnew();
        /* split the hostname by "." */
        if((stat = nczm_split_delim(url->host,'.',segments))) goto done;
	switch (nclistlength(segments)) {
	default: stat = NC_EURL; goto done;
	case 4:
            if(strcasecmp(nclistget(segments,0),"s3")!=0)
	        {stat = NC_EURL; goto done;}
	    z3map->urlformat = UF_PATH; 
	    z3map->region = strdup(nclistget(segments,1));
	    break;
	case 5:
            if(strcasecmp(nclistget(segments,1),"s3")!=0)
	        {stat = NC_EURL; goto done;}
	    z3map->urlformat = UF_VIRTUAL;
	    z3map->region = strdup(nclistget(segments,2));
    	    z3map->bucket = strdup(nclistget(segments,0));
	    break;
	}
	/* Rebuild host into Path form */
	pathhost = ncbytesnew();
	ncbytescat(pathhost,"s3.");
	ncbytescat(pathhost,z3map->region);
	ncbytescat(pathhost,AWSHOST);
        z3map->host = ncbytesextract(pathhost);
    } else {
        z3map->urlformat = UF_OTHER;
        if((z3map->host = strdup(url->host))==NULL)
	    {stat = NC_ENOMEM; goto done;}
    }
    if(z3map->urlformat == UF_PATH || z3map->urlformat == UF_OTHER) {
	/* We have to process the path to get the bucket */
	if(url->path != NULL && strlen(url->path) > 0) {
            /* split the path by "/" */
   	    nclistfreeall(segments);
	    segments = nclistnew();
            if((stat = nczm_split_delim(url->path,'/',segments))) goto done;
	    z3map->bucket = strdup((char*)nclistget(segments,0));
	}
    }
    if(z3map->bucket != NULL && !isLegalBucketName(z3map->bucket))
	{stat = NC_EURL; goto done;}
done:
    ncbytesfree(pathhost);
    nclistfreeall(segments);
    return stat;
}


/* Create an object corresponding to a key */
static int
z3createobj(ZS3MAP* z3map, const char* key, size64_t size)
{
    int stat = NC_NOERR;
    unsigned char empty[1];

    empty[0] = 0;
    if((stat = NCZ_s3sdkwriteobject(z3map->s3client, z3map->bucket, key, 1, empty, &z3map->errmsg)))
        goto done;

done:
    if(z3map->errmsg) reporterr(z3map);
    return THROW(stat);
    

}

/**************************************************/
/* S3 Utilities */

static int
endswith(const char* s, const char* suffix)
{
    ssize_t ls, lsf, delta;
    if(s == NULL || suffix == NULL) return 0;
    ls = strlen(s);
    lsf = strlen(suffix);
    delta = (ls - lsf);
    if(delta < 0) return 0;
    if(memcmp(s+delta,suffix,lsf)!=0) return 0;
    return 1;
}


static int
s3clear(ZS3MAP* z3map)
{
    int stat = NC_NOERR;
    char** list = NULL;
    char** p;
    size_t nkeys;

    if((stat = NCZ_s3sdkgetkeys(z3map->s3client, z3map->bucket, "", "/", &nkeys, &list, &z3map->errmsg)))
        goto done;
    
    for(p=list;*p;p++) {
        if((stat = NCZ_s3sdkdeletekey(z3map->s3client, z3map->bucket, *p, &z3map->errmsg)))	
	    goto done;
    }

done:
    if(z3map->errmsg) reporterr(z3map);
    NCZ_freestringvec(nkeys,list);
    return THROW(stat);
}

/**************************************************/
/* External API objects */

NCZMAP_DS_API zmap_s3sdk;
NCZMAP_DS_API zmap_s3sdk = {
    NCZM_S3SDK_V1,
    zs3create,
    zs3open,
};

static NCZMAP_API
nczs3sdkapi = {
    NCZM_S3SDK_V1,
    zs3exists,
    zs3len,
    zs3define,
    zs3read,
    zs3write,
    zs3readmeta,
    zs3writemeta,
    zs3close,
    zs3search,
};
