/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#ifndef NCS3SDK_H
#define NCS3SDK_H 1

/* Opaque Handles */
struct NCURI;
struct NCawsconfig;

#if 0
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
#endif

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
DECLSPEC void* NC_s3sdkcreateclient(struct NCURI*);
DECLSPEC int NC_s3sdkbucketexists(void* s3client, const char* bucket, int* existsp, char** errmsgp);
DECLSPEC int NC_s3sdkbucketcreate(void* s3client, const char* region, const char* bucket, char** errmsgp);
DECLSPEC int NC_s3sdkbucketdelete(void* s3client, char** errmsgp);
DECLSPEC int NC_s3sdkinfo(void* client0, const char* bucket, const char* pathkey, unsigned long long* lenp, char** errmsgp);
DECLSPEC int NC_s3sdkread(void* client0, const char* bucket, const char* pathkey, unsigned long long start, unsigned long long count, void* content, char** errmsgp);
DECLSPEC int NC_s3sdkwriteobject(void* client0, const char* bucket, const char* pathkey, unsigned long long count, const void* content, char** errmsgp);
DECLSPEC int NC_s3sdkclose(void* s3client0, char** errmsgp);
DECLSPEC int NC_s3sdktruncate(void* s3client0, const char* bucket, const char* prefix, char** errmsgp);
DECLSPEC int NC_s3sdklist(void* s3client0, const char* bucket, const char* prefix, size_t* nkeysp, char*** keysp, char** errmsgp);
DECLSPEC int NC_s3sdklistall(void* s3client0, const char* bucket, const char* prefixkey0, size_t* nkeysp, char*** keysp, char** errmsgp);
DECLSPEC int NC_s3sdkdeletekey(void* client0, const char* bucket, const char* pathkey, char** errmsgp);

/* From ds3util.c */
DECLSPEC int NC_iss3(struct NCURI* uri);
DECLSPEC void NC_s3sdkenvironment(void);
DECLSPEC int NC_s3urlrebuild(struct NCURI* uri, struct NCURI** newurlp);

DECLSPEC int NC_getactiveawsprofile(struct NCURI*, const char** profilep);

#if 0
DECLSPEC int NC_s3urlrebuild(struct NCURI* uri, struct NCawsconfig* aws, struct NCURI** newurlp);
DECLSPEC int NC_s3buildnotes(struct NCURI* url, struct NCawsconfig* aws, struct NCURI** newurlp);
DECLSPEC int NC_s3notesclear(struct NCS3NOTES* s3);
DECLSPEC int NC_s3notesclone(struct NCS3NOTES* s3, struct NCS3NOTES** news3p);
DECLSPEC const char* NC_s3dumpnotes(struct NCS3NOTES* notes);

DECLSPEC int NC_getdefaultawsregion(NCawsconfig* aws, char** regionp);

DECLSPEC int NC_s3profilelookup(const char* profile, const char* key, const char** valuep);
DECLSPEC void NC_s3getcredentials(NCS3NOTES*, const char** region, const char** accessid, const char** accesskey);
#endif

#ifdef __cplusplus
}
#endif

#endif /*NCS3SDK_H*/
