/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#ifndef NCS3SDK_H
#define NCS3SDK_H 1

/* Track the server type, if known */
typedef enum NCS3SVC {
	NCS3UNK=0, /* unknown */
	NCS3=1,     /* s3.amazon.aws */
	NCS3GS=2,   /* storage.googleapis.com */
	NCS3APP=3,   /* Arbitrary appliance url*/
} NCS3SVC;

/* Opaque Handles */
struct NClist;
struct NCawsconfig;
struct AWSprofile;
struct NCglobalstate;

/**
Collected extra S3 information inferred/extracted from a URI.
*/
typedef struct NCS3NOTES {
    NCS3SVC svc;  /* Kind of URI */
    char* region; /* inferred from URI */
    char* bucket; /* inferred from URI */
    struct NCawsconfig* aws; /* current file aggregate profile */
//    char* host; /* non-null if other*/
//    char* rootkey;
//    char* profile;
} NCS3NOTES;

extern NCS3NOTES NC_s3notes_empty(void); 

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
DECLSPEC void* NC_s3sdkcreateclient(const NCS3NOTES* context);
DECLSPEC int NC_s3sdkbucketexists(void* s3client, const char* bucket, int* existsp, char** errmsgp);
DECLSPEC int NC_s3sdkbucketcreate(void* s3client, const char* region, const char* bucket, char** errmsgp);
DECLSPEC int NC_s3sdkbucketdelete(void* s3client, const NCS3NOTES* notes, char** errmsgp);
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

DECLSPEC int NC_s3buildnotes(NCURI* url, struct NCawsconfig* aws, NCURI** newurlp, struct NCS3NOTES** noteps);
DECLSPEC int NC_s3notesclear(struct NCS3NOTES* s3);
DECLSPEC int NC_s3notesclone(struct NCS3NOTES* s3, struct NCS3NOTES** news3p);
DECLSPEC const char* NC_s3dumpnotes(struct NCS3NOTES* notes);

DECLSPEC int NC_s3urlrebuild(NCURI* uri, struct NCS3NOTES* notes, NCURI** newurlp);

DECLSPEC int NC_getdefaults3region(struct NCS3NOTES* s3uri, char** regionp);
DECLSPEC int NC_getactives3profile(struct NCS3NOTES* s3, const char** profilep);

DECLSPEC int NC_s3profilelookup(const char* profile, const char* key, const char** valuep);
DECLSPEC void NC_s3getcredentials(NCS3NOTES*, const char** region, const char** accessid, const char** accesskey);
DECLSPEC int NC_iss3(NCURI* uri, enum NCS3SVC*);

#ifdef __cplusplus
}
#endif

#endif /*NCS3SDK_H*/
