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
extern NCZMAP_API nczs3api; // c++ will not allow static forward variables
static int zs3exists(NCZMAP* map, const char* key);
static int zs3len(NCZMAP* map, const char* key, size64_t* lenp);
static int zs3define(NCZMAP* map, const char* key, size64_t len);
static int zs3read(NCZMAP* map, const char* key, size64_t start, size64_t count, void* content);
static int zs3write(NCZMAP* map, const char* key, size64_t start, size64_t count, const void* content);
static int zs3readmeta(NCZMAP* map, const char* key, size64_t avail, char* content);
static int zs3writemeta(NCZMAP* map, const char* key, size64_t count, const char* content);
static int zs3search(NCZMAP* map, const char* prefix, int deep, NClist* matches);

static int zs3close(NCZMAP* map, int deleteit);
static int z3createobj(ZS3MAP*, const char* key, size64_t len);

static int s3buildaccesspoint(ZS3MAP* zmap, NCbytes* apoint);
static int s3bucketexists(ZS3MAP* z3map);
static int s3createbucket(ZS3MAP* z3map);
static int s3createobject(ZS3MAP* z3map);
static int s3bucketpath(const char* bucket, const char* region, char** bucketp);
static Aws::S3::Model::BucketLocationConstraint s3findregion(const char* name);
static int s3clear(ZS3MAP* z3map);
static int isLegalBucketName(const char* bucket);

/* Define the Dataset level API */

static int zs3initialized = 0;
static Aws::SDKOptions zs3options;

static void
zs3initialize(void)
{
    zs3options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Error;
    Aws::InitAPI(zs3options);
    zs3initialized = 1;
}

static void
zs3finalize(void)
{
    Aws:ShutdownAPI(zs3options);
    zs3initialized = 0;
}


static int
zs3create(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    char* region = NULL;
    char* bucket = NULL;
    ZS3MAP* z3map = NULL;
    NClist* segments = nclistnew();
    NCURI* url = NULL;
    const char* exception = NULL;
    const char* message = NULL;
	
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
	if(!isLegalBucketName(bucket))
	    {stat = NC_EURL; goto done;}
    } else
	{stat = NC_EURL; goto done;}

    /* Build the z4 state */
    if((z3map = (ZS3MAP*)calloc(1,sizeof(ZS3MAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    z3map->map.format = NCZM_S3;
    z3map->map.url = strdup(path);
    z3map->map.mode = mode;
    z3map->map.flags = flags;
    z3map->map.api = (NCZMAP_API*)&nczs3api;
    z3map->bucket = bucket; /* bucket name */
    z3map->region = region; /* region */
    region = (bucket = NULL);

    {
	int exists = 0;
        /* Does bucket already exist */
	Aws::S3::S3Client s3_client(z3map->config);
        auto result = s3_client.ListBuckets();
        if(!result.IsSuccess()) {
	    exception = strdup(result.GetError().GetExceptionName().c_str());
    	    message = strdup(result.GetError().GetMessage().c_str());
	    goto done;
	}
        Aws::Vector<Aws::S3::Model::Bucket> bucket_list = result.GetResult().GetBuckets();
        for(auto const &awsbucket : bucket_list) {
	   const char* name = awsbucket.GetName().c_str();
	   if(strcmp(name,bucket)==0) {exists = 1; break;}
        }
	if(!exists) {
	    /* create it */
            const Aws::S3::Model::BucketLocationConstraint &awsregion = s3findregion(region);
	    if(awsregion == Aws::S3::Model::BucketLocationConstraint::NOT_SET)
	        {stat = NC_EURL; goto done;}
	    /* Set up the request */
	    Aws::S3::Model::CreateBucketRequest create_request;
	    create_request.SetBucket(bucket);
            /* Specify the region as a location constraint */
            Aws::S3::Model::CreateBucketConfiguration bucket_config;
            bucket_config.SetLocationConstraint(awsregion);
            create_request.SetCreateBucketConfiguration(bucket_config);
	    /* Create the bucket */
            auto create_result = s3_client.CreateBucket(create_request);
            if(!create_result.IsSuccess()) {
		exception = strdup(create_result.GetError().GetExceptionName().c_str());
		message = strdup(create_result.GetError().GetMessage().c_str());
		goto done;
	    }
	}
        /* Delete objects in bucket */
        s3clear(z3map);
    }
    
    if(mapp) *mapp = (NCZMAP*)z3map;    

done:
    if(exception) {
        nclog(NCLOGERR,"%s: %s",exception,message);
    }
    ncurifree(url);
    nullfree(region);
    nullfree(bucket);
    if(stat) zs3close((NCZMAP*)z3map,1);
    return (stat);
}

static int
zs3open(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
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

    /* Build the z4 state */
    if((z3map = (ZS3MAP*)calloc(1,sizeof(ZS3MAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    z3map->map.format = NCZM_S3;
    z3map->map.url = strdup(path);
    z3map->map.mode = mode;
    z3map->map.flags = flags;
    z3map->map.api = (NCZMAP_API*)&nczs3api;
    z3map->bucket = bucket; /* bucket name */
    z3map->region = region; /* region */
    bucket = (region = NULL);

    z3map->config.scheme = Aws::Http::Scheme::HTTPS;
    z3map->config.region = z3map->region;
    z3map->config.connectTimeoutMs = 30000;
    z3map->config.requestTimeoutMs = 600000;

    if(mapp) *mapp = (NCZMAP*)z3map;    

done:
    ncurifree(url);
    nullfree(region);
    nullfree(bucket);
    if(stat) zs3close((NCZMAP*)z3map,0);
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
    Aws::S3::S3Client s3_client(z3map->config);
    Aws::S3::Model::ListObjectsRequest objects_request;
    objects_request.WithBucket(z3map->bucket);
    objects_request.SetMarker(key);
    objects_request.SetMaxKeys(1);
    auto list_objects_outcome = s3_client.ListObjects(objects_request);
    if(list_objects_outcome.IsSuccess()) {
	Aws::Vector<Aws::S3::Model::Object> object_list = list_objects_outcome.GetResult().GetContents();
	if(object_list.size() != 1)
	    {stat = NC_EACCESS; goto done;}
        if(lenp) *lenp = (size64_t)(object_list.front().GetSize());
    } else{
        nclog(NCLOGERR,"%s",list_objects_outcome.GetError().GetExceptionName());
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
    Aws::S3::S3Client s3_client(z3map->config);
    Aws::S3::Model::GetObjectRequest object_request;

    object_request.SetBucket(z3map->bucket);
    object_request.SetKey(key);
    auto get_object_result = s3_client.GetObject(object_request);
    if(!get_object_result.IsSuccess()) {
        auto error = get_object_result.GetError();
        nclog(NCLOGERR,"%s: %s\n",
 		error.GetExceptionName(),
                error.GetMessage());
        stat = NC_ES3;
	goto done;
    } else {
        Aws::IOStream &result = get_object_result.GetResultWithOwnership().GetBody();
    }
done:
    return (stat);
}

static int
zs3write(NCZMAP* map, const char* key, size64_t start, size64_t count, const void* content)
{
    int stat = NC_NOERR;
    ZS3MAP* z3map = (ZS3MAP*)map; /* cast to true type */
    Aws::S3::S3Client s3_client(z3map->config);
    Aws::S3::Model::PutObjectRequest object_request;

    object_request.SetBucket(z3map->bucket);
    object_request.SetKey(key);

    auto data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream", std::stringstream::in | std::stringstream::out | std::stringstream::binary);
    data->write(reinterpret_cast<const char*>(content), count);
    object_request.SetBody(data);
    auto put_object_result = s3_client.PutObject(object_request);
    if(!put_object_result.IsSuccess()) {
        auto error = put_object_result.GetError();
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
        s3clear(z3map);
    nullfree(z3map->bucket);
    nullfree(z3map->region);
    delete &z3map->config;

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
zs3search(NCZMAP* map, const char* prefix, NClist* matches)
{
    int stat = NC_NOERR;
    ZS3MAP* z3map = (ZS3MAP*)map;
    Aws::S3::S3Client s3_client(z3map->config);
    Aws::S3::Model::ListObjectsRequest objects_request;

    objects_request.WithBucket(z3map->bucket);

    /* Build the search query */
//    if(!deep) objects_request.SetDelimiter("/");
    objects_request.SetPrefix(prefix);

    auto list_objects_outcome = s3_client.ListObjects(objects_request);
    if(list_objects_outcome.IsSuccess()) {
        Aws::Vector<Aws::S3::Model::Object> object_list =
            list_objects_outcome.GetResult().GetContents();
        for (auto const &s3_object : object_list) {
	    const char* s = strdup(s3_object.GetKey().c_str());
	    nclistpush(matches,s);
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
    Aws::S3::S3Client s3_client(z3map->config);
    Aws::S3::Model::PutObjectRequest object_request;

    object_request.SetBucket(z3map->bucket);
    object_request.SetKey(key);
    auto data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream", std::stringstream::in | std::stringstream::out | std::stringstream::binary);
    data->write("", 0);
    object_request.SetBody(data);

    auto put_result = s3_client.PutObject(object_request);
    if(!put_result.IsSuccess()) {
        nclog(NCLOGERR,"%s: %s",put_result.GetError().GetExceptionName(),put_result.GetError().GetMessage());
        stat = NC_ES3;
	goto done;
    }
done:
    return (stat);    
}

/**************************************************/
/* S3 Utilities */

#if 0
static int
s3bucketexists(ZS3MAP* z3map, const char* bucket)
{
    int stat = NC_NOERR;
    Aws::S3::S3Client s3_client(z3map->config);
    Aws::S3::Model::ListObjectsRequest objects_request;
    objects_request.WithBucket(z3map->bucket);
    objects_request.SetMarker(key);
    objects_request.SetMaxKeys(1);
    auto list_objects_outcome = s3_client.ListObjects(objects_request);
    if(list_objects_outcome.IsSuccess()) {
	Aws::Vector<Aws::S3::Model::Object> object_list = list_objects_outcome.GetResult().GetContents();
	if(object_list.size() != 1)
	    {stat = NC_EACCESS; goto done;}
        if(lenp) *lenp = (size64_t)(object_list.front().GetSize());
    } else{
        nclog(NCLOGERR,"%s",list_objects_outcome.GetError().GetExceptionName());
	stat = NC_ES3;
	goto done;
    }
}
#endif

static int
s3clear(ZS3MAP* z3map)
{
    int stat = NC_NOERR;
    Aws::S3::S3Client s3_client(z3map->config);
    Aws::S3::Model::ListObjectsRequest list_request;
    Aws::S3::Model::DeleteObjectRequest delete_request;

    list_request.WithBucket(z3map->bucket);
    /* Build the search query */
    list_request.SetPrefix("/");
    auto list_objects_outcome = s3_client.ListObjects(list_request);
    if(list_objects_outcome.IsSuccess()) {
        Aws::Vector<Aws::S3::Model::Object> object_list =
            list_objects_outcome.GetResult().GetContents();
        for (auto const &s3_object : object_list) {
	    const char* key = strdup(s3_object.GetKey().c_str());
	    /* Delete this key object */
	    delete_request.WithBucket(z3map->bucket);
    	    delete_request.WithKey(key);
	    auto delete_result = s3_client.DeleteObject(delete_request);
	    if(!delete_result.IsSuccess()) {
		nclog(NCLOGERR,"5s: %s",
			delete_result.GetError().GetExceptionName(),
			delete_result.GetError().GetMessage());
		stat = NC_ES3;
		goto done;
	    }
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

static Aws::S3::Model::BucketLocationConstraint
s3findregion(const char* name)
{
    return Aws::S3::Model::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(name);
}

struct MemBuf: std::streambuf {
    MemBuf(char* base, size_t size) {
        char* p(base);
        this->setg(p, p, p + size);
    }
};


/**************************************************/
/* External API objects */

extern "C" NCZMAP_DS_API zmap_s3;
NCZMAP_DS_API zmap_s3 = {
    NCZM_S3_V1,
    zs3create,
    zs3open,
};

NCZMAP_API
nczs3api = {
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
