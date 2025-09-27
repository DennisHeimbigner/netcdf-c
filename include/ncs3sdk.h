/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#ifndef NCS3SDK_H
#define NCS3SDK_H 1

/* Track the server type, if known */
typedef enum NCS3SVC {NCS3UNK=0, /* unknown */
	NCS3=1,     /* s3.amazon.aws */
	NCS3GS=2,   /* storage.googleapis.com */
	NCS3APP=3,   /* Arbitrary appliance url*/
} NCS3SVC;

/* Opaque Handles */
struct NClist;
struct NCawsprofile;
struct AWSprofile;
struct NCglobalstate;

/**
"Subclass" NCURI with additional S3 specific info
extracted from the URI.
*/
typedef struct NCS3URI {
    NCURI* uri;   /* parent uri */
    NCS3SVC svc;  /* kind of URL */
    char* region; /* region inferred from uri */
    char* bucket; /* bucket inferred from uri */
    NCawsprofile* const aws; /* The file AWS profile values */
} NCS3URI;

#ifndef DECLSPEC
#ifdef DLL_NETCDF
  #ifdef DLL_EXPORT /* define when building the library */
    #define DECLSPEC __declspec(dllexport)
  #else
    #define DECLSPEC __declspec(dllimport)
  #endif
#else
  #define DECLSPEC
#endif
#endif /*!DECLSPEC*/

#ifdef __cplusplus
extern "C" {
#endif

/* API for ncs3sdk_XXX.[c|cpp] */
DECLSPEC int NC_s3sdkinitialize(void);
DECLSPEC int NC_s3sdkfinalize(void);
DECLSPEC void* NC_s3sdkcreateclient(NCS3URI* context);
DECLSPEC int NC_s3sdkbucketexists(void* s3client, const char* bucket, int* existsp, char** errmsgp);
DECLSPEC int NC_s3sdkbucketcreate(void* s3client, const char* region, const char* bucket, char** errmsgp);
DECLSPEC int NC_s3sdkbucketdelete(void* s3client, NCS3URI* info, char** errmsgp);
DECLSPEC int NC_s3sdkinfo(void* client0, const char* bucket, const char* pathkey, unsigned long long* lenp, char** errmsgp);
DECLSPEC int NC_s3sdkread(void* client0, const char* bucket, const char* pathkey, unsigned long long start, unsigned long long count, void* content, char** errmsgp);
DECLSPEC int NC_s3sdkwriteobject(void* client0, const char* bucket, const char* pathkey, unsigned long long count, const void* content, char** errmsgp);
DECLSPEC int NC_s3sdkclose(void* s3client0, char** errmsgp);
DECLSPEC int NC_s3sdktruncate(void* s3client0, const char* bucket, const char* prefix, char** errmsgp);
DECLSPEC int NC_s3sdklist(void* s3client0, const char* bucket, const char* prefix, size_t* nkeysp, char*** keysp, char** errmsgp);
DECLSPEC int NC_s3sdklistall(void* s3client0, const char* bucket, const char* prefixkey0, size_t* nkeysp, char*** keysp, char** errmsgp);
DECLSPEC int NC_s3sdkdeletekey(void* client0, const char* bucket, const char* pathkey, char** errmsgp);

/* From ds3util.c */
DECLSPEC void NC_s3sdkenvironment(void);

DECLSPEC int NC_aws_load_profiles(struct NCglobalstate* gstate);
DECLSPEC void NC_s3freeprofilelist(struct NClist* profiles);
DECLSPEC int NC_gets3profile(const char* profile, struct AWSprofile** profilep);

DECLSPEC int NC_getdefaults3region(NCS3URI* s3uri, const char** regionp);
DECLSPEC int NC_s3urlprocess(NCS3URI* s3url);
DECLSPEC int NC_s3clear(NCS3URI* s3);
DECLSPEC int NC_s3clone(NCS3URI* s3, NCS3URI** news3p);
DECLSPEC const char* NC_s3dumps3info(NCS3URI* info);
DECLSPEC int NC_getactives3profile(NCS3URI* s3 uri, const char** profilep);
DECLSPEC int NC_s3profilelookup(const char* profile, const char* key, const char** valuep);
DECLSPEC void NC_s3getcredentials(NCS3URI*, const char *profile,
				  const char **region, const char **accessid, const char **accesskey);
DECLSPEC int NC_iss3(NCURI* uri, enum NCS3SVC*);
DECLSPEC int NC_s3urlrebuild(NCS3URI* s3);

#ifdef __cplusplus
}
#endif

#endif /*NCS3SDK_H*/
