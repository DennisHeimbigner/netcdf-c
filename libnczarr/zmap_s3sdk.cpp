/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zincludes.h"
#include "awsincludes.h"
#include <streambuf>
#include <istream>
#include "zmap.h"

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
    Aws::Client::ClientConfiguration config;
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

static int zs3close(NCZMAP* map, int deleteit);
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
static Aws::S3::Model::BucketLocationConstraint s3findregion(const char* name);

/* Define the Dataset level API */

static int zs3initialized = 0;

static void
zs3initialize(void)
{
    Aws::SDKOptions options;
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Error;
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

    if(!zs3initialized) zs3initialize();
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
	if(strcasecmp((char*)nclistget(segments,1),"s3")!=0)
	    {stat = NC_EURL;goto done;}
	region = strdup((char*)nclistget(segments,2));
	bucket = strdup((char*)nclistget(segments,0));	
    } else if(nclistlength(segments) == 4) { /* path */
	if(strcasecmp((char*)nclistget(segments,0),"s3")!=0)
	    {stat = NC_EURL;goto done;}
	region = strdup((char*)nclistget(segments,1));
	/* We have to process the path to get the bucket */
        /* split the path by "/" */
	nclistfreeall(segments);
	segments = nclistnew();
        if((stat = nczm_split_delim(url->path,'/',segments))) goto done;
	bucket = strdup((char*)nclistget(segments,0));
    } else
	{stat = NC_EURL; goto done;}

    /* Standardize on path style URLs */
    if((stat=s3bucketpath(bucket,region,&bucketurl)))
	goto done;

    /* Build the z4 state */
    if((z3map = (ZS3MAP*)calloc(1,sizeof(ZS3MAP))) == NULL)
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

    if(!zs3initialized) zs3initialize();
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
	if(strcasecmp((char*)nclistget(segments,1),"s3")!=0)
	    {stat = NC_EURL;goto done;}
	region = strdup((char*)nclistget(segments,2));
	bucket = strdup((char*)nclistget(segments,0));	
    } else if(nclistlength(segments) == 4) { /* path */
	if(strcasecmp((char*)nclistget(segments,0),"s3")!=0)
	    {stat = NC_EURL;goto done;}
	region = strdup((char*)nclistget(segments,1));
	/* We have to process the path to get the bucket */
        /* split the path by "/" */
	nclistfreeall(segments);
	segments = nclistnew();
        if((stat = nczm_split_delim(url->path,'/',segments))) goto done;
	bucket = strdup((char*)nclistget(segments,0));	
    } else
	{stat = NC_EURL; goto done;}

    /* Standardize on path style URLs */
    if((stat=s3bucketpath(bucket,region,&bucketurl)))
	goto done;

    /* Build the z4 state */
    if((z3map = (ZS3MAP*)calloc(1,sizeof(ZS3MAP))) == NULL)
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

    z3map->config.scheme = Aws::Http::Scheme::HTTPS;
    z3map->config.region = z3map->region;
    z3map->config.connectTimeoutMs = 30000;
    z3map->config.requestTimeoutMs = 600000;

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
    Aws::S3::S3Client s3_client;
    Aws::S3::Model::ListObjectsRequest objects_request;
    objects_request.WithBucket(z3map->bucket);
    objects_request.SetMarker(key);
    objects_request.SetMaxKeys(1);
    auto list_objects_outcome = s3_client.ListObjects(objects_request);
    if(list_objects_outcome.IsSuccess()) {
	Aws::Vector<Aws::S3::Model::Object> object_list = list_objects_outcome.GetResult().GetContents();
	if(object_list.size() != 1) {
	    {stat = NC_EACCESS; goto done;}
        if(lenp) *lenp = (size64_t)(object_list.front().GetSize());
    } else{
        nclog(NCLOGDEBUG,"%s",list_objects_outcome.GetError().GetExceptionName());
	stat = NC_ES3;
	goto done;
    }
done:
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
    auto writebuf = Aws::MakeShared<oMemStream>();
    object_request.SetBody(writebuf);
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
    auto readbuf = Aws::MakeShared<iMemStream>(content,count);
    object_request.SetBody(readbuf);
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
zs3close(NCZMAP* map, int deleteit)
{
    int stat = NC_NOERR;
    ZS3MAP* z3map = (ZS3MAP*)map;

    if(deleteit)
        deletetree(z3map);
    nullfree(z3map->bucket);
    nullfree(z3map->region);
    nullfree(z3map->bucketurl);
    nullfree(z3map->accountid);
    ~z3map->config();
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
    Aws::S3::S3Client s3_client(zs3map->config);
    Aws::S3::Model::ListObjectsRequest objects_request;

    objects_request.WithBucket(z3map->bucket);

    /* Build the search query */
    if(!deep)
        objects_request.SetDelimiter("/");
    objects_request.SetPrefix(prefix);

    auto list_objects_outcome = s3_client.ListObjects(objects_request);
    if(list_objects_outcome.IsSuccess()) {
        Aws::Vector<Aws::S3::Model::Object> object_list =
            list_objects_outcome.GetResult().GetContents();
        for (auto const &s3_object : object_list) {
	    nclistpush(matches,(const char*)s3_object.GetKey());
        }
    } else {
        nclog(NCLOGERR,"%s: %s",
            list_objects_outcome.GetError().GetExceptionName(),
            list_objects_outcome.GetError().GetMessage());
	stat = NC_ES3;
	goto done;
    }

done:
    return THROW(stat);
}

/**************************************************/
/* Utilities */

/* Create an object corresponding to a key */
static int
z3createobj(ZS3MAP* z3map, const char* key, size64_t size)
{
    int stat = NC_NOERR;
    Aws::S3::S3Client s3_client(z3map->clientConfig);
    Aws::S3::Model::PutObjectRequest object_request;
    

    object_request.SetBucket(z3map->bucket);
    object_request.SetKey(key);
    object_request.SetContentLength(0);
    char* emptydata[1] = 0;
    struct iMemStream memstream(emptydata,0);
    const std::shared_ptr<Aws::IOStream> input_data = 
        Aws::MakeShared<MemStream>(emptydata,0);
    object_request.SetBody(input_data);
    auto put_object_outcome = s3_client.PutObject(object_request);
    if(!put_object_outcome.IsSuccess()) {
        auto error = put_object_outcome.GetError();
        nclog(NCLOGERR,"%s: %s",error.GetExceptionName(),error.getMessage());
        stat = NC_ES3;
	goto done;
    }
done:
    return (stat);    
}

/**************************************************/
/* S3 Utilities */

static Aws::S3::Model::BucketLocationConstraint
s3findregion(const char* name)
{
    return Aws::S3::Model::BucketLocationConstraint::BucketLocationConstraintMapper::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(name);
}

struct MemBuf: std::streambuf {
    MemBuf(char* base, size_t size) {
        char* p(base);
        this->setg(p, p, p + size);
    }
};

struct iMemstream: virtual MemBuf, std::istream {
    iMemstream(char const* base, size_t size)
        : MemBuf((char*)base, size)
        , std::ostream(static_cast<std::streambuf*>(this)) {
    }
};

struct oMemstream: virtual MemBuf, std::ostream {
    oMemstream(char * base, size_t size)
        : MemBuf(base, size)
        , std::ostream(static_cast<std::streambuf*>(this)) {
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
