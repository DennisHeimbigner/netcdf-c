/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#define NOOP


#include "awsincludes.h"
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <streambuf>
#include "netcdf.h"
#include "zs3sdk.h"

#define size64_t unsigned long long

static Aws::SDKOptions zs3options;

/* Forward */
static Aws::S3::Model::BucketLocationConstraint s3findregion(const char* name);
static int s3objectsinfo(Aws::Vector<Aws::S3::Model::Object> list, const char* prefix, char*** keysp, size64_t** lenp);
static void freestringenvv(char** ss);
static int getbucket(const char* pathkey, char** bucketp, const char** keyp);
    
#ifndef nullfree
#define nullfree(x) {if(x) {free(x);}}
#endif

void
NCZ_s3sdkinitialize(void)
{
//    zs3options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Error;
    Aws::InitAPI(zs3options);
}

void
NCZ_s3sdkfinalize(void)
{
    Aws::ShutdownAPI(zs3options);
}

static char*
makeerrmsg(const Aws::Client::AWSError<Aws::S3::S3Errors> err, const char* key="")
{
    char* errmsg;
    size_t len;
    len = strlen(err.GetExceptionName().c_str()) + strlen(err.GetMessage().c_str()) + strlen(key) + 10;
    if((errmsg = (char*)malloc(len+1))==NULL)
        return NULL;
    snprintf(errmsg,len,"%s %s key=%s",
		err.GetExceptionName().c_str(),
		err.GetMessage().c_str(),
		key);
    return errmsg;
}


int
NCZ_s3sdkcreateconfig(const char* host, const char* region, void** configp)
{
    int stat = NC_NOERR;
    Aws::Client::ClientConfiguration *config = new Aws::Client::ClientConfiguration();
    config->scheme = Aws::Http::Scheme::HTTPS;
    config->connectTimeoutMs = 30000;
    config->requestTimeoutMs = 600000;
    if(region) config->region = region;
    if(host) config->endpointOverride = host;
    config->enableEndpointDiscovery = true;
    config->followRedirects = true;
    if(configp) * configp = config;
    return stat;
}

int
NCZ_s3sdkcreateclient(void* config0, void** clientp)
{
    Aws::Client::ClientConfiguration* config = (Aws::Client::ClientConfiguration*) config0;
    Aws::S3::S3Client *s3client
        = new Aws::S3::S3Client(*config,
                               Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::RequestDependent,
			       false,
			       Aws::S3::US_EAST_1_REGIONAL_ENDPOINT_OPTION::NOT_SET
			       );
    if(clientp) *clientp = (void*)s3client;
    return NC_NOERR;
}

int
NCZ_s3sdkbucketexists(void* s3client0, const char* bucket, int* existsp, char** errmsgp)
{
    int stat = NC_NOERR;
    int exists = 0;
    Aws::S3::S3Client* s3client = (Aws::S3::S3Client*)s3client0;

    if(errmsgp) *errmsgp = NULL;
    auto result = s3client->ListBuckets();
    if(!result.IsSuccess()) {
	if(errmsgp) *errmsgp = makeerrmsg(result.GetError());	
	stat = NC_ES3;
    } else {
        Aws::Vector<Aws::S3::Model::Bucket> bucket_list = result.GetResult().GetBuckets();
        for(auto const &awsbucket : bucket_list) {
	   auto name = awsbucket.GetName();
	   if(name == bucket) {exists = 1; break;}
	}
    }
    if(existsp) *existsp = exists;
    return stat;    
}

int
NCZ_s3sdkbucketcreate(void* s3client0, const char* region, const char* bucket, char** errmsgp)
{
    int stat = NC_NOERR;
    Aws::S3::S3Client* s3client = (Aws::S3::S3Client*)s3client0;
    if(errmsgp) *errmsgp = NULL;
    const Aws::S3::Model::BucketLocationConstraint &awsregion = s3findregion(region);
    if(awsregion == Aws::S3::Model::BucketLocationConstraint::NOT_SET)
        return NC_EURL;
        /* Set up the request */
    Aws::S3::Model::CreateBucketRequest create_request;
    create_request.SetBucket(bucket);
    if(region) {
        /* Specify the region as a location constraint */
        Aws::S3::Model::CreateBucketConfiguration bucket_config;
        bucket_config.SetLocationConstraint(awsregion);
        create_request.SetCreateBucketConfiguration(bucket_config);
    }
#ifdef NOOP
    /* Create the bucket */
    auto create_result = s3client->CreateBucket(create_request);
    if(!create_result.IsSuccess()) {
	if(errmsgp) *errmsgp = makeerrmsg(create_result.GetError());	
	stat = NC_ES3;;
    }
#else
    fprintf(stderr,"create bucket: %s\n",bucket); fflush(stderr);
#endif

    return stat;    
}

int
NCZ_s3sdkbucketdelete(void* s3client0, void* config0, const char* region, const char* bucket, char** errmsgp)
{
    int stat = NC_NOERR;
    Aws::S3::S3Client* s3client = (Aws::S3::S3Client*)s3client0;
    Aws::Client::ClientConfiguration *config = (Aws::Client::ClientConfiguration*)config0;

    if(errmsgp) *errmsgp = NULL;
    const Aws::S3::Model::BucketLocationConstraint &awsregion = s3findregion(region);
    if(awsregion == Aws::S3::Model::BucketLocationConstraint::NOT_SET)
        return NC_EURL;
        /* Set up the request */
    Aws::S3::Model::DeleteBucketRequest request;
    request.SetBucket(bucket);
    if(region) {
	config->region = region; // Will this work?
    }
#ifdef NOOP
    /* Delete the bucket */
    auto result = s3client->DeleteBucket(request);
    if(!result.IsSuccess()) {
	if(errmsgp) *errmsgp = makeerrmsg(result.GetError());	
	stat = NC_ES3;;
    }
#else
    fprintf(stderr,"delete bucket: %s\n",bucket); fflush(stderr);
#endif

    return stat;    
}

/**************************************************/
/* Object API */

int
NCZ_s3sdkinfo(void* s3client0, const char* pathkey, size64_t* lenp, char** errmsgp)
{
    int stat = NC_NOERR;
    Aws::S3::S3Client* s3client = (Aws::S3::S3Client*)s3client0;
    Aws::S3::Model::ListObjectsV2Request objects_request;
    size64_t* lengths = NULL;
    size_t nkeys;
    char* bucket = NULL;
    const char* key = NULL;

    if(*key != '/') return NC_EINTERNAL;
    /* extract the bucket prefix */
    if((stat = getbucket(pathkey,&bucket,&key))) return stat;

    if(errmsgp) *errmsgp = NULL;
    objects_request.SetBucket(bucket);
    objects_request.SetPrefix(key);
    objects_request.SetMaxKeys(1);
    auto list_outcome = s3client->ListObjectsV2(objects_request);
    if(list_outcome.IsSuccess()) {
        Aws::Vector<Aws::S3::Model::Object> object_list =
            list_outcome.GetResult().GetContents();
	nkeys = (size_t)object_list.size();
	if(nkeys != 1) {stat = NC_EACCESS;}
	else {
	  if(!(stat = s3objectsinfo(object_list,pathkey,NULL,&lengths))) {
	      if(lenp) *lenp = lengths[0];
	  }
	}
    } else {
	if(errmsgp) *errmsgp = makeerrmsg(list_outcome.GetError(),key);
        stat = NC_ES3;
    }
    nullfree(lengths);
    return (stat);
}

int
NCZ_s3sdkread(void* s3client0, const char* pathkey, size64_t start, size64_t count, void* content, char** errmsgp)
{
    int stat = NC_NOERR;
    Aws::S3::S3Client* s3client = (Aws::S3::S3Client*)s3client0;
    Aws::S3::Model::GetObjectRequest object_request;
    char range[1024];
    char* bucket = NULL;
    const char* key = NULL;

    if(*key != '/') return NC_EINTERNAL;
    if(!(stat = getbucket(pathkey,&bucket,&key))) return stat;
    
    object_request.SetBucket(bucket);
    object_request.SetKey(key);
    snprintf(range,sizeof(range),"bytes=%llu-%llu",start,(start+count)-1);
    object_request.SetRange(range);
    auto get_object_result = s3client->GetObject(object_request);
    if(!get_object_result.IsSuccess()) {
	if(errmsgp) *errmsgp = makeerrmsg(get_object_result.GetError(),key);
	stat = NC_ES3;
    } else {
	/* Get the whole result */
	Aws::IOStream &result = get_object_result.GetResultWithOwnership().GetBody();
	std::string str((std::istreambuf_iterator<char>(result)),std::istreambuf_iterator<char>());
	/* Verify actual result size */
	size_t slen = str.size();
	if(slen > count) return NC_ES3;
	const char* s = str.c_str();
	if(content)
	    memcpy(content,s,slen);
    }
    return (stat);
}

int
NCZ_s3sdkreadobject(void* s3client0, const char* pathkey, size64_t* sizep, void** contentp, char** errmsgp)
{
    int stat = NC_NOERR;
    Aws::S3::S3Client* s3client = (Aws::S3::S3Client*)s3client0;
    Aws::S3::Model::GetObjectRequest object_request;
    size64_t size, red;
    void* content = NULL;
    char* bucket = NULL;
    const char* key = NULL;

    if(*key != '/') return NC_EINTERNAL;
    if(!(stat = getbucket(pathkey,&bucket,&key))) return stat;

    if(errmsgp) *errmsgp = NULL;
    object_request.SetBucket(bucket);
    object_request.SetKey(key);
    auto get_result = s3client->GetObject(object_request);
    if(!get_result.IsSuccess()) {
	if(errmsgp) *errmsgp = makeerrmsg(get_result.GetError(),key);
        stat = NC_ES3;
    } else {
	/* Get the size */
	size = (size64_t)get_result.GetResult().GetContentLength();	
	/* Get the whole result */
        Aws::IOStream &result = get_result.GetResultWithOwnership().GetBody();
#if 0
        std::string str((std::istreambuf_iterator<char>(result)),std::istreambuf_iterator<char>());
	red = str.length();
	if((content = malloc(red))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	memcpy(content,str.c_str(),red);
#else
        red = result.rdbuf()->pubseekoff(0,std::ios::ios_base::end);
        result.rdbuf()->pubseekoff(0,std::ios::ios_base::beg); /* reset for reading */
	if((content = malloc(red))==NULL)
	    stat = NC_ENOMEM;
	else
   	    result.rdbuf()->sgetn((char*)content,red);
#endif
	if(!stat) {
  	    /* Verify actual result size */
	    if(red != size) {stat = NC_ES3; goto done;}
	    if(sizep) *sizep = red;
	    if(contentp) {*contentp = content; content = NULL;}
	}
    }
done:
    nullfree(content);
    return (stat);
}

/*
For S3, I can see no way to do a byterange write;
so we are effectively writing the whole object
*/
int
NCZ_s3sdkwriteobject(void* s3client0, const char* pathkey,  size64_t count, const void* content, char** errmsgp)
{
    int stat = NC_NOERR;
    Aws::S3::S3Client* s3client = (Aws::S3::S3Client*)s3client0;
    Aws::S3::Model::PutObjectRequest object_request;
    char* bucket = NULL;
    const char* key = NULL;

    if(*key != '/') return NC_EINTERNAL;
    if(!(stat = getbucket(pathkey,&bucket,&key))) return stat;
    
    if(errmsgp) *errmsgp = NULL;
    object_request.SetBucket(bucket);
    object_request.SetKey(key);
    object_request.SetContentLength((long long)count);

    std::shared_ptr<Aws::IOStream> data = std::shared_ptr<Aws::IOStream>(new Aws::StringStream());
    data->rdbuf()->pubsetbuf((char*)content,count);
    object_request.SetBody(data);
    auto object_result = s3client->PutObject(object_request);
    if(!object_result.IsSuccess()) {
        if(errmsgp) *errmsgp = makeerrmsg(object_result.GetError(),key);
        stat = NC_ES3;
    }
    return (stat);
}

int
NCZ_s3sdkclose(void* s3client0, void* config0)
{
    int stat = NC_NOERR;
    Aws::S3::S3Client* s3client = (Aws::S3::S3Client*)s3client0;
    Aws::Client::ClientConfiguration *config = (Aws::Client::ClientConfiguration*)config0;
    delete s3client;
    delete config;
    return (stat);
}

/*
Return a list of keys "below" a specified prefix.
In theory, the returned list should be sorted in lexical order,
but it possible that it is not.
*/
int
NCZ_s3sdkgetkeys(void* s3client0, const char* prefix, size_t* nkeysp, char*** keysp, char** errmsgp)
{
    int stat = NC_NOERR;
    Aws::S3::S3Client* s3client = (Aws::S3::S3Client*)s3client0;
    Aws::S3::Model::ListObjectsV2Request objects_request;
    size_t nkeys = 0;
    char* bucket = NULL;
    const char* rootkey = NULL;

    if(*prefix != '/') return NC_EINTERNAL;
    if(!(stat = getbucket(prefix,&bucket,&rootkey))) return stat;

    if(errmsgp) *errmsgp = NULL;
    objects_request.SetBucket(bucket);
    objects_request.SetPrefix(rootkey);
    auto objects_outcome = s3client->ListObjectsV2(objects_request);
    if(objects_outcome.IsSuccess()) {
        Aws::Vector<Aws::S3::Model::Object> object_list =
            objects_outcome.GetResult().GetContents();
        nkeys = (size_t)object_list.size();
        if(nkeysp) *nkeysp = nkeys;
        stat = s3objectsinfo(object_list,prefix,keysp,NULL);
    } else {
        if(errmsgp) *errmsgp = makeerrmsg(objects_outcome.GetError());
        stat = NC_ES3;
    }
    return stat;
}

int
NCZ_s3sdkdeletekey(void* s3client0, const char* pathkey, char** errmsgp)
{
    int stat = NC_NOERR;
    Aws::S3::S3Client* s3client = (Aws::S3::S3Client*)s3client0;
    Aws::S3::Model::DeleteObjectRequest delete_request;

#ifdef NOOP
    char* bucket = NULL;
    const char* key = NULL;
    if(!(stat = getbucket(pathkey,&bucket,&key))) {
        /* Delete this key object */
        delete_request.SetBucket(bucket);
        delete_request.SetKey(key);
        auto delete_result = s3client->DeleteObject(delete_request);
        if(!delete_result.IsSuccess()) {
            if(errmsgp) *errmsgp = makeerrmsg(delete_result.GetError(),key);
            stat = NC_ES3;
	}
    }
#else
    fprintf(stderr,"delete object: %s.%s\n",bucket,key); fflush(stderr);
#endif

    return stat;
}

/*
Get Info about a vector of objects
*/
static int
s3objectsinfo(Aws::Vector<Aws::S3::Model::Object> list, const char* prefix, char*** keysp, size64_t** lenp)
{
    int stat = NC_NOERR;
    char** keys = NULL;
    size_t nkeys;
    size64_t *lengths = NULL;
    int i;
    size_t prelen = (prefix?strlen(prefix):0);

    nkeys = list.size();
    if(keysp) {
        if((keys=(char**)calloc(sizeof(char*),(nkeys+1)))==NULL)
            stat = NC_ENOMEM;
    }
    if(!stat) {
        if(lenp) {
            if((lengths=(size64_t*)calloc(sizeof(size64_t),(nkeys)))==NULL)
                stat = NC_ENOMEM;
        }
    }
    if(!stat)  {
        i = 0;
        for (auto const &s3_object : list) {
            char* cstr = NULL;
            if(keysp) {
                auto s = s3_object.GetKey();
                if(prefix) {
                    size_t klen = s.length();
                    cstr = (char*)malloc(klen+prelen+1);
                    strcpy(cstr,prefix);
                    strcat(cstr,s.c_str());
                } else              
                    cstr = strdup(s.c_str());
                if(cstr == NULL) 
                    {stat = NC_ENOMEM; break;}
                keys[i] = cstr;
                cstr = NULL;
	    }
            if(!stat) {
                if(lenp) lengths[i] = (size64_t)s3_object.GetSize();
                i++;
	    }
	}
    }
    if(!stat) {
        if(keysp) keys[nkeys] = NULL;
        if(keysp) {*keysp = keys; keys = NULL;}
        if(lenp) {*lenp = lengths; lengths = NULL;}
    }

    if(keys != NULL) freestringenvv(keys);
    if(lengths != NULL) free(lengths);
    return stat;
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

static void
freestringenvv(char** ss)
{
    char** p;
    if(ss != NULL) {
        for(p=ss;*p;p++)
            nullfree(*p);
        free(ss);
    }
}

static int
getbucket(const char* pathkey, char** bucketp, const char** keyp)
{
    const char* p;
    assert(pathkey != NULL && pathkey[0] == '/');
    p = strchr(pathkey+1,'/'); /* find end of the bucket */
    assert(p != NULL);
    if(keyp) *keyp = p+1;
    if(bucketp) {
        char* bucket = NULL;
        ptrdiff_t len = ((p - pathkey) - 1);
        if((bucket = (char*)malloc(len+1))==NULL) {return NC_ENOMEM;}
        memcpy(bucket,pathkey+1,len);
        bucket[len] = '\0';
        *bucketp = bucket; bucket = NULL;
    }
    return NC_NOERR;    
}
