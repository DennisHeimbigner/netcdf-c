/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zincludes.h"
#include "awsincludes.h"
#include <streambuf>
#include <istream>

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

#define NCZM_S3_V1 1

/* Define the "subclass" of NCZMAP */
typedef struct ZS3MAP {
    NCZMAP map;
    char* bucket; /* bucket name */
    char* region; /* region */
    char* bucketurl; /* url prefix in path style */
    Aws::SDKOptions options;
    Aws::ClientConfiguration config;
} ZS3MAP;


/* Forward */
static NCZMAP_API zapi;
static int zs3exists(NCZMAP* map, const char* key);
static int zs3len(NCZMAP* map, const char* key, size64_t* lenp);
static int zs3define(NCZMAP* map, const char* key, size64_t len);
static int zs3read(NCZMAP* map, const char* key, size64_t start, size64_t count, void* content);
static int zs3write(NCZMAP* map, const char* key, size64_t start, size64_t count, const void* content);
static int zs3readmeta(NCZMAP* map, const char* key, size64_t avail, char* content);
static int zs3writemeta(NCZMAP* map, const char* key, size64_t count, const char* content);
static int zs3search(NCZMAP* map, const char* prefix, int deep, NClist* matches);

static int zs3close(NCZMAP* map, int delete);
static int zlookupgroup(ZS3MAP*, NClist* segments, int nskip, int* grpidp);
static int zlookupobj(ZS3MAP*, NClist* segments, int* objidp);
static int zcreategroup(ZS3MAP* z3map, NClist* segments, int nskip, int* grpidp);
static int zcreateobj(ZS3MAP*, NClist* segments, size64_t, int* objidp);
static int zcreatedim(ZS3MAP*, size64_t dimsize, int* dimidp);

static int s3clear(ZS3MAP* z3map);
static int s3buildaccesspoint(ZS3MAP* zmap, NCbytes* apoint);
static int s3exists(ZS3MAP* z3map);
static int s3createbucket(ZS3MAP* z3map);
static int s3createobject(ZS3MAP* z3map);
static int s3bucketpath(const char* bucket, const char* region, char** bucketp);
static Awd::S3::Model::BucketLocationConstraint s3findregion(const char* name);

/* Define the Dataset level API */

static int zs3initialized = 0;

static zs3initialize(void)
{
    Aws::SDKOptions options;
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    Aws::InitAPI(options);
    zs3initialized = 1;
}


static int
zs3create(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    char* bucketurl = NULL;
    char* region = NULL;
    char* bucket = NULL;
    ZS3MAP* z3map = NULL;
    NClist* segments = nclistnew();
    NCURI* url = NULL;
	
    NC_UNUSED(flags);
    NC_UNUSED(parameters);

    /* Parse the URL */
    if((stat = ncuriparse(path,&url))) goto done;
    if(url == NULL)
        {stat = NC_EURL; goto done;}
    /* do some verification */
    if(strcmp(url->protocol,"https") != 0)
        {stat = NC_EURL; goto done;}

    /* split the hostname by "." */
    if((stat = nczm_split_delim(url->host,'.',segments))) goto done;

    /* Distinguish path-style from virtual-host style:
       Virtual: https://bucket-name.s3.Region.amazonaws.com
       Path: https://s3.Region.amazonaws.com/bucket-name
    */
    if(nclistlength(segments) == 5) { /* virtual */
	/* verify */
	if(strcasecmp(nclistget(segments,1),"s3")!=0)
	    {stat = NC_EURL;goto done;}
	region = strdup(nclistget(segments,2));
	bucket = strdup(nclistget(segments,0));	
    } else if(nclistlength(segments) == 4) { /* path */
	if(strcasecmp(nclistget(segments,0),"s3")!=0)
	    {stat = NC_EURL;goto done;}
	region = strdup(nclistget(segments,1));
	/* We have to process the path to get the bucket */
        /* split the path by "/" */
	nclistfreeall(segments);
	segments = nclistnew();
        if((stat = nczm_split_delim(url->path,'/',segments))) goto done;
	bucket = strdup(nclistget(segments,0));
    } else
	{stat = NC_EURL; goto done;}

    /* Standardize on path style URLs */
    if((stat=s3bucketpath(bucket,region,&bucketurl)))
	goto done;

    /* Build the z4 state */
    if((z3map = calloc(1,sizeof(ZS3MAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    z3map->map.format = NCZM_S3;
    z3map->map.url = strdup(path);
    z3map->map.mode = mode;
    z3map->map.flags = flags;
    z3map->map.api = (NCZMAP_API*)&zapi;
    z3map->bucket = bucket; /* bucket name */
    z3map->region = region; /* region */
    z3map->bucketurl = bucketurl; /* url prefix in path style */
    bucket = (region = (bucketurl = NULL));
    z3map->url = ncbytesnew();
    z3map->buf = ncbytesnew();

    Aws::InitAPI(z3map->options);

    if(mapp) *mapp = (NCZMAP*)z3map;    

    /* If bucket does not exist, create it */
    if((stat=s3exists(z3map))) {
	if((stat = s3createbucket(z3map))) goto done;
    }

    /* Delete objects in bucket */
    s3clear(z3map);

done:
    ncurifree(url);
    nullfree(region);
    nullfree(bucket);
    nullfree(bucketurl);
    if(stat) zs3close((NCZMAP*)z3map,1);
    return (stat);
}

static int
zs3open(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    char* region = NULL;
    char* bucket = NULL;
    char* bucketurl = NULL;
    ZS3MAP* z3map = NULL;
    NClist* segments = nclistnew();
    NCURI* url = NULL;
    
    NC_UNUSED(flags);
    NC_UNUSED(parameters);

    /* Parse the URL */
    if((stat = ncuriparse(path,&url))) goto done;
    if(url == NULL)
        {stat = NC_EURL; goto done;}
    /* do some verification */
    if(strcmp(url->protocol,"https") != 0)
        {stat = NC_EURL; goto done;}

    /* split the hostname by "." */
    if((stat = nczm_split_delim(url->host,'.',segments))) goto done;

    /* Distinguish path-style from virtual-host style:
       Virtual: https://bucket-name.s3.Region.amazonaws.com
       Path: https://s3.Region.amazonaws.com/bucket-name
    */
    if(nclistlength(segments) == 5) { /* virtual */
	/* verify */
	if(strcasecmp(nclistget(segments,1),"s3")!=0)
	    {stat = NC_EURL;goto done;}
	region = strdup(nclistget(segments,2));
	bucket = strdup(nclistget(segments,0));	
    } else if(nclistlength(segments) == 4) { /* path */
	if(strcasecmp(nclistget(segments,0),"s3")!=0)
	    {stat = NC_EURL;goto done;}
	region = strdup(nclistget(segments,1));
	/* We have to process the path to get the bucket */
        /* split the path by "/" */
	nclistfreeall(segments);
	segments = nclistnew();
        if((stat = nczm_split_delim(url->path,'/',segments))) goto done;
	bucket = strdup(nclistget(segments,0));	
    } else
	{stat = NC_EURL; goto done;}

    /* Standardize on path style URLs */
    if((stat=s3bucketpath(bucket,region,&bucketurl)))
	goto done;

    /* Build the z4 state */
    if((z3map = calloc(1,sizeof(ZS3MAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    z3map->map.format = NCZM_S3;
    z3map->map.url = strdup(path);
    z3map->map.mode = mode;
    z3map->map.flags = flags;
    z3map->map.api = (NCZMAP_API*)&zapi;
    z3map->bucket = bucket; /* bucket name */
    z3map->region = region; /* region */
    z3map->bucketurl = bucketurl; /* url prefix in path style */
    bucket = (region = (bucketurl = NULL));
    z3map->url = ncbytesnew();
    z3map->buf = ncbytesnew();

    Aws::InitAPI(z3map->options);

    z3map->clientConfig.scheme = Aws::Http::Scheme::HTTPS;
    z3map->clientConfig.region = z3map->region;
    z3map->clientConfig.connectTimeoutMs = 30000;
    z3map->clientConfig.requestTimoutMs = 600000;

    if(mapp) *mapp = (NCZMAP*)z3map;    

done:
    ncurifree(url);
    nullfree(region);
    nullfree(bucket);
    nullfree(bucketurl);
    if(stat) zs3close((NCZMAP*)z3map,0);
    return (stat);
}

/**************************************************/
/* Object API */

static int
zs3exists(NCZMAP* map, const char* key)
{
    int stat = NC_NOERR;

    if((stat = zs3len(map, key, NULL)))
	{stat = NC_EACCESS; goto done;}

done:
    return (stat);
}

static int
zs3len(NCZMAP* map, const char* key, size64_t* lenp)
{
    int stat = NC_NOERR;
    ZS3MAP* z3map = (ZS3MAP*)map;

    if((stat=zs3buildurl(z3map, key)))
	goto done;
    if((stat = nc_http_size(z3map->curl,ncbytescontents(z3map->url),lenp))
	{stat = NC_EACCESS; goto done;}

done:
    nullfree(objurl);
    return (stat);
}

static int
zs3define(NCZMAP* map, const char* key, size64_t len)
{
    int stat = NC_NOERR;
    int grpid;
    ZS3MAP* z3map = (ZS3MAP*)map; /* cast to true type */

    if((stat = zs3exists(z3map,key)) == NC_NOERR) 
	goto done; /* Already exists */
    else if(stat != NC_EACCESS) /* Some other kind of failure */
	goto done;

    if((stat = zcreateobj(???,segments,len,&grpid)))
	goto done;

done:
    return (stat);
}

static int
zs3read(NCZMAP* map, const char* key, size64_t start, size64_t count, void* content)
{
    int stat = NC_NOERR;
    ZFMAP* zfmap = (ZFMAP*)map; /* cast to true type */
    Aws::S3::S3Client s3_client(z3map->clientConfig);
    Aws::S3::Model::GetObjectRequest object_request;

    object_request.SetBucket(z3map->bucket);
    object_request.SetKey(key);
    object_request.SetContentLength((long long)count);
    object_request.SetBody(content);
    auto put_object_outcome = s3_client.PutObject(object_request);
    if(!put_object_outcome.IsSuccess()) {
        auto error = put_object_outcome.GetError();
        nclog(NCLOGERR,"%s: %s\n",
 		error.GetExceptionName(),
                error.GetMessage());
        stat = NC_ES3;
	goto done;
    }
var s3Client = new AmazonS3Client(AccessKeyId, SecretKey, Amazon.RegionEndpoint.USEast1);
    using (s3Client)
    {
        MemoryStream ms = new MemoryStream();
        GetObjectRequest getObjectRequest = new GetObjectRequest();
        getObjectRequest.BucketName = BucketName;
        getObjectRequest.Key = awsFileKey;

        using (var getObjectResponse = s3Client.GetObject(getObjectRequest))
        {
            getObjectResponse.ResponseStream.CopyTo(ms);
        }
        var download = new FileContentResult(ms.ToArray(), "image/png"); //"application/pdf"
        download.FileDownloadName = ToFilePath;
        return download;
    }


done:
    return (stat);
}

static int
zs3write(NCZMAP* map, const char* key, size64_t start, size64_t count, const void* content)
{
    int stat = NC_NOERR;
    ZFMAP* zfmap = (ZFMAP*)map; /* cast to true type */
    Aws::S3::S3Client s3_client(z3map->clientConfig);
    Aws::S3::Model::PutObjectRequest object_request;
    object_request.SetBucket(z3map->bucket);
    object_request.SetKey(key);
    object_request.SetContentLength((long long)count);
    auto input_data = Aws::MakeShared<Aws::StringStream>(
                                  std::ios_base::in | std::ios_base::binary);

    object_request.SetBody(input_data);
    auto put_object_outcome = s3_client.PutObject(object_request);
    if(!put_object_outcome.IsSuccess()) {
        auto error = put_object_outcome.GetError();
        nclog(NCLOGERR,"%s: %s\n",
 		error.GetExceptionName(),
                error.GetMessage());
        stat = NC_ES3;
	goto done;
    }
done:
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
zs3close(NCZMAP* map, int delete)
{
    int stat = NC_NOERR;
    ZS3MAP* z3map = (ZS3MAP*)map;

    if(delete)
        deletetree(z3map);
    nullfree(z3map->bucket);
    nullfree(z3map->region);
    nullfree(z3map->bucketurl);
    nullfree(z3map->accountid);
    ncbytesfree(z3map->url);        
    ncbytesfree(z3map->buf);
    Aws::ShutdownAPI(z3map->options);

done:
    nullfree(z3map);
    nczm_clear(map);
    return (stat);
}

/*
Return a list of keys immediately "below" a specified prefix.
In theory, the returned list should be sorted in lexical order,
but it possible that it is not.
*/
static int
zs3search(NCZMAP* map, const char* prefix, int deep, NClist* matches)
{
    int stat = NC_NOERR;
    ZS3MAP* z3map = (ZS3MAP*)map;
    NClist* segments = nclistnew();
    NCbytes* tmp = ncbytesnew();
    NCbytes* accpoint = ncbytesnew();
    ezxml_t* dom = NULL;
    ezxml_t x;

    /* Build the search query */
    ncbytesclear(tmp);
    ncbytescat(tmp,"list-type=2"
    ncbytescat(tmp,"&Bucket=");
        ncbytescat(tmp,z3map->bucket);
    if(!deep)
        ncbytescat(tmp,"&Delimiter=/");    
    ncbytescat(tmp,"&Prefix=");
        ncbytescat(tmp,prefix);
    /* Build complete URL */
    ncbytesclear(z3map->url);
    ncbytescat(z3map->url,"?");
    ncbytescat(z3map->url,ncbytescontents(tmp));
    /* Make the request */
    if((stat = curl_execute(z3map->curl,HTTPGET,z43map->buf,&httpcode)))
	goto done;
    /* Parse the returned XML */
    if((dom = ezxml_parse_str(ncbytescontents(z3map->buf),nclistlength(z3map->buf))) == NULL)
	{stat = NC_ETRANSLATION; goto done;}
    /* Extract the relevant keys */
    /* Root should be ListObjectsV2Output */
    if(strcmp(dom->->name,"ListobjectsV2Ouput")!=0)
	{stat = NC_ETRANSLATION; goto done;}
#if 0
    /* See if truncated result */
    if((x=ezxml_child(tree, "IsTruncated")) != NULL) {
	if(strcasecmp(ezxml_txt(x),"true")==0) istruncated = 1;
    }
#endif
    if((x=ezxml_child(dom, "Contents")) == NULL)
	goto done;
    for(x=ezxml_child(x, "Key");x != NULL;x = ezxml_next(x)) {
	char* suffix = ezxml_txt(segment);
	/* Include this key iff it has no slashes */
	assert(suffix != NULL && strchr(suffix,'/') == NULL)
        nclistpush(subnodes,suffix);
    }

done:
    return THROW(stat);
}

#if 0
/* Return a list of keys for all child nodes of the parent;
   It is up to the caller to figure out the type of the node.
   Assume that parentkey refers to a group; fail otherwise.
   The list includes subgroups.
*/
int
zs3children(NCZMAP* map, const char* parentkey, NClist* children)
{
    int stat = NC_NOERR;
    ZS3MAP* zs3map = (ZS3MAP*)map;
    NClist* segments = nclistnew();
    int grpid;
    int ngrps;
    int* subgrps = NULL;
    int i;

    if((stat=nczm_split(parentkey,segments)))
	goto done;    
    if((stat = zlookupgroup(???,segments,0,&grpid)))
	goto done;
    /* Start by getting any subgroups */
    if((stat = nc_inq_grps(grpid,&ngrps,NULL)))
	goto done;
    if(ngrps > 0) {
        if((subgrps = calloc(1,sizeof(int)*ngrps)) == NULL)
	    {stat = NC_ENOMEM; goto done;}
        if((stat = nc_inq_grps(grpid,&ngrps,subgrps)))
	    goto done;
	/* Get the names of the subgroups */
	for(i=0;i<ngrps;i++) {
	    char name[NC_MAX_NAME];
	    char zname[NC_MAX_NAME];
	    char* path = NULL;
	    if((stat = nc_inq_grpname(subgrps[i],name)))
		goto done;
	    /* translate name */
	    zify(name,zname);
	    /* Create a full path */
	    if((stat = nczm_suffix(parentkey,zname,&path)))
		goto done;
	    /* Add to list of children */
	    nclistpush(children,path);
	    path = NULL; /* avoid mem errors */
	}		
    }

done:
    return stat;
}
#endif

/**************************************************/
/* Utilities */

/* Lookup a group by parsed path (segments)*/
/* Return NC_EACCESS if not found */
static int
z3lookupgroup(ZS3MAP* z3map, NClist* segments, int nskip, int* grpidp)
{
    int stat = NC_NOERR;
    int i, len, grpid;

    len = nclistlength(segments);
    len += nskip; /* leave off last nskip segments */
    grpid = Z3MAP->ncid;
    for(i=0;i<len;i++) {
	int grpid2;
	const char* seg = nclistget(segments,i);
	char s3name[NC_MAX_NAME];
	s3ify(seg,s3name);	
	if((stat=nc_inq_grp_ncid(grpid,s3name,&grpid2)))
	    {stat = NC_EACCESS; goto done;}
	grpid = grpid2;
    }
    /* ok, so grpid should be it */
    if(grpidp) *grpidp = grpid;

done:
    return (stat);
}

/* Lookup an object */
/* Return NC_EACCESS if not found */
static int
z3lookupobj(ZS3MAP* z3map, NClist* segments, int* grpidp)
{
    int stat = NC_NOERR;
    int grpid;

    /* Lookup thru the final object group */
    if((stat = zlookupgroup(???,segments,0,&grpid)))
	goto done;
    if(grpidp) *grpidp = grpid;

done:
    return (stat);    
}

/* Create a group; assume all intermediate groups exist
   (do nothing if it already exists) */
static int
z3creategroup(ZS3MAP* z3map, NClist* segments, int nskip, int* grpidp)
{
    int stat = NC_NOERR;
    int i, len, grpid, grpid2;
    const char* gname = NULL;
    char s3name[NC_MAX_NAME];

    len = nclistlength(segments);
    len -= nskip; /* leave off last nskip segments (assume nskip > 0) */
    gname = nclistget(segments,len-1);
    grpid = Z3MAP->ncid;
    /* Do all but last group */
    for(i=0;i<(len-1);i++) {
	const char* seg = nclistget(segments,i);
	/* Does this group exist? */
	if((stat=nc_inq_grp_ncid(grpid,s3seg,&grpid2)) == NC_ENOGRP) {
	    {stat = NC_EACCESS; goto done;} /* missing intermediate */
	}
	grpid = grpid2;
    }
    /* Check status of last group */
    s3ify(gname,s3name);
    if((stat = nc_inq_grp_ncid(grpid,s3name,&grpid2))) {
	if(stat != NC_ENOGRP) goto done;
        if((stat = nc_def_grp(grpid,s3name,&grpid2)))
	    goto done;
	grpid = grpid2;
    }

    if(grpidp) *grpidp = grpid;

done:
    return (stat);
}

/* Create an object group corresponding to a key; create any
   necessary intermediates.
 */
static int
z3createobj(ZS3MAP* z3map, const char* key)
{
    int i,nsegs,stat = NC_NOERR;
    NClist* segments = nclistnew();
    NCbytes* path = ncbytesnew();

    /* Split key so we can create intermediates */ 
    if((stat=nczm_split(key,segments)))
	goto done;

    /* Create the path prefix*/
    if((stat=zs3buildurl(z3map, NULL)))
	goto done;
    nsegs = nclistlength(segments);
    for(i=0;i <nsegs; i++) {
	const char* seg = nclistget(segments,i);
	/* create key for intermediate */
        switch (stat=zs3extendurl(z3map, seg)) {
	case NC_NOERR: break; /* exists */
	case NC_EACCESS: /* create it */
            if((stat = zcreateobject(z3map,ncbytescontents(z3map->url))))
		goto done;
	    break;
        default: goto done;
    }

done:
    return (stat);    
}

#if 0
/* Extract the bucket path from a url string */
static int
getbucketurl(const char* surl, char** bucketp)
{
    int stat = NC_NOERR;
    NCURI* uri = NULL;
    char* bucket = NULL;
    if(!ncuriparse(surl,&uri)) {
	int blen;
	/* Check the protocol and extract the file part */	
	if(strcasecmp(uri->protocol,"s3") != 0)
	   && strcasecmp(uri->protocol,"http") != 0)
	   && strcasecmp(uri->protocol,"https") != 0)
	    {stat = NC_EURL; goto done;}
        /* Extract protocol + host plus port */	
	blen = strlen("https://");
        blen += strlen(uri->host)
	if(uri->port) blen += strlen(port) + 1;
	if((bucket = malloc(blen+1)) == NULL)
	   {stat = NC_ENOMEM; goto done;}
	bucket[0] = '\0';
	strlcat(bucket,"https://");
	strlcat(bucket,uri->host);
	if(uri->port != NULL) {
	    strlcat(bucket,":");
	    strlcat(bucket,uri->port);	    
	}
        if(bucketp) {*bucketp = bucket; bucket = NULL;}
    } else {
	stat = NC_EACCESS;
	goto done;
    }

done:
    ncurifree(uri);
    nullfree(bucket);
    return stat;
}
#endif

/**************************************************/
/* S3 Utilities */

static int
s3exists(ZS3MAP* z3map)
{
    abort();
}

static int
s3createbucket(ZS3MAP* z3map)
{
    abort();
}

static int
s3clear(ZS3MAP* z3map)
{
    return NC_NOERR;
}

static int
s3bucketpath(const char* bucket, const char* region, char** bucketp)
{
    /* Build path-style url.
       e.g. https://s3.Region.amazonaws.com/bucket-name*/
    NCbytes* bpath = ncbytesnew();
    ncbytescat(bpath, "https://s2.");
    ncbytescat(bpath, region);
    ncbytescat(bpath, ".amazonaws.com/");
    ncbytescat(bpath, bucket);
    if(bucketp) *bucketp = ncbytesextract(bpath);
    return NC_NOERR;
}

static int
s3buildaccesspoint(ZS3MAP* zmap, NCbytes* apoint)
{
    ncbytescat(apoint,zmap->accesspointname);
    ncbytescat(apoint,"-");
    ncbytescat(apoint,zmap->accountid);
    ncbytescat(apoint,".s3-accesspoint.");
    ncbytescat(apoint,zmap->region);
    ncbytescat(apoint,".amazonaws.com");
    return NC_NOERR;
}

static Awd::S3::Model::BucketLocationConstraint
s3findregion(const char* name)
{
    return Aws::S3::Model::BucketLocationConstraint::BucketLocationConstraintMapper::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(name);
}

struct membuf: std::streambuf {
    membuf(char const* base, size_t size) {
        char* p(const_cast<char*>(base));
        this->setg(p, p, p + size);
    }
};

struct imemstream: virtual membuf, std::istream {
    imemstream(char const* base, size_t size)
        : membuf(base, size)
        , std::istream(static_cast<std::streambuf*>(this)) {
    }
};
/**************************************************/
/* External API objects */

NCZMAP_DS_API zmap_s3 = {
    NCZM_S3_V1,
    zs3create,
    zs3open,
};

static NCZMAP_API zapi = {
    NCZM_S3_V1,
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
