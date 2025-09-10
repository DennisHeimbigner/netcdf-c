/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "zincludes.h"
#include "ncrc.h"
#include "ncjson.h"
#include "ncpathmgr.h"

#ifdef NETCDF_ENABLE_NCZARR_ZIP
#include <zip.h>
#endif

/**************************************************/

/* Tag search parameter */
struct TagParam {
    int zarrformat;
    int nczarrformat;
    int haszmetadata;
};

struct ZarrObjects {
    const char* name;
    int zarr_version;
    int haszmetadata;
} zarrobjects[] = {
{"/.zgroup",	ZARRFORMAT2,	0},
{"/.zarray",	ZARRFORMAT2,	0},
{"/.zattrs",	ZARRFORMAT2,	0},
{"/.zmetadata",	ZARRFORMAT2,	1},
{NULL,		0,		0},	
};

/**************************************************/
/**
Given various pieces of information and a URL,
infer the map type: currently file,zip,s3.

The current rules are as follows.

Creation:
1. Use the store type specified in the URL: "file", "zip", "s3", gs3.

Read:
1. If the URL specifies a store type, then use that type unconditionally.
2. If the URL protocol is "file", then treat the URL path as a file path.
2.1 If the path references a directory, then the store type is "file".
2.2 If the path references a file, and can be opened by libzip, then the store type is "zip"
2.3 Otherwise fail with NC_ENOTZARR.
3. If the url protocol is "http" or "https" then:
3.1 Apply the function NC_iss3 and if it succeeds, the store type is s3|gs3.
3.2 If the mode contains "file", then storetype is file -- meaning REST API to a file store.
*/

static int
NCZ_infer_storage_type(NC_FILE_INFO_T* file, NCURI* url, NCZM_IMPL* implp)
{
    int ret = NC_NOERR;
    int create;
    NCZM_IMPL impl = NCZM_UNDEF;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;

    NC_UNUSED(file);

    assert(zfile != NULL);
    create = zfile->creating;

    /* mode storetype overrides all else */
    if(NC_testmode(url, "file")) impl = NCZM_FILE;
#ifdef NETCDF_ENABLE_S3
    else if(NC_testmode(url, "s3")) impl = NCZM_S3;
    else if(NC_testmode(url, "gs3")) impl = NCZM_GS3;
#ifdef NETCDF_ENABLE_ZOH
    else if(NC_testmode(url, "zoh")) impl = NCZM_ZOH;
#endif
#endif
#ifdef NETCDF_ENABLE_NCZARR_ZIP
    else if(NC_testmode(url, "zip")) impl = NCZM_ZIP;
#endif
    if(!create) { /* Reading a file of some kind */
	if(strcasecmp(url->protocol,"file")==0) {
	    struct stat buf;
	    /* Storage: file,zip,... */
	    if(NCstat(url->path,&buf)<0) {ret = errno; goto done;}
	    if(S_ISDIR(buf.st_mode))
		impl = NCZM_FILE; /* only possibility */
#ifdef NETCDF_ENABLE_NCZARR_ZIP
	    else if(S_ISREG(buf.st_mode)) {
		/* Should be zip, but verify */
                zip_flags_t zipflags = ZIP_RDONLY;
		zip_t* archive = NULL;
	        int zerrno = ZIP_ER_OK;
	        /* Open the file */
                archive =  zip_open(url->path,(int)zipflags,&zerrno);
		if(archive != NULL) {
		    impl = NCZM_ZIP;
		    zip_close(archive);
		}		
	    }
#endif
	}
    }

    if(impl == NCZM_UNDEF)
	{ret = NC_EURL; goto done;}

    if(implp) *implp = impl;
done:
    return THROW(ret);
}

/**
Figure out the storage type and create and return a corresponding map.

@param file
@param url
@param mode
@param constraints
@param params
@return NC_NOERR | NC_EXXX
*/

int
NCZ_infer_map(NC_FILE_INFO_T* file, NCURI* url, mode_t mode, size64_t constraints, void* params, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    int create = 0;
    NCZMAP* map = NULL;
    NCZM_IMPL impl = NCZM_UNDEF;
    NCZ_FILE_INFO_T* zfile = NULL;
    char* path = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    create = zfile->creating;

    if((stat = NCZ_infer_storage_type(file, url, &impl))) goto done;
    
    if((path = ncuribuild(url,NULL,NULL,NCURIALL))==NULL) {stat = NC_ENCZARR; goto done;}

    switch (impl) {
    case NCZM_FILE: case NCZM_ZIP: case NCZM_S3: case NCZM_GS3:
	if(create)
	    {if((stat = nczmap_create(impl,path,mode,constraints,params,&map))) goto done;}
	else
    	    {if((stat = nczmap_open(impl,path,mode,constraints,params,&map))) goto done;}
	break;
#ifdef NETCDF_ENABLE_ZOH
    case NCZM_ZOH:
	if(create) {stat = NC_ENOTZARR; goto done;}
	constraints |= FLAG_ZOH;
	if((stat = nczmap_open(impl,path,mode,constraints,params,&map))) goto done;
	break;
#endif
    case NCZM_UNDEF:
	stat = NC_EURL;
	goto done;
    default:
        stat = NC_ENOTZARR;
	goto done;
    }

    if(mapp) {*mapp = map; map = NULL;}

done:
    nullfree(path);
    if(map) (void)nczmap_close(map,0);
    return THROW(stat);
}

