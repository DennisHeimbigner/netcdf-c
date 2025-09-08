/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

/**
Given various pieces of information and a map,
infer the format to be used.
Note that format1 (the oldest NCZarr format is now disallowed).

The current rules are as follows.

Creation:
1. Use the Zarr format specified in the mode flags, if any.
2. Otherwise use the default Zarr format
3. Use the chosen Zarr format for the NCZarr format also.
4. Use pure zarr if mode has "zarr" or "xarray" or "noxarray" tag.

Read:
2. If root contains ".zgroup", then
2.1 Zarr version is 2, and is verified by the .zgroup key "format".
2.2 If .zgroup contains key "_nczarr_superblock" then NCZarr version is 2.0.0 and can be verified by key "version".
2.3 Otherwise NCZarr version is NULL (i.e. pure Zarr).
3. If root subtree contains an object named "zarr.json" then
3.1 the Zarr format is V3.
3.2 If zarr.json is in root and contains key "_nczarr_superblock" then NCZarr version is 3.0.0 and can be verified by key "version".
4. If Zarr version is still unknown, then it defaults to 2.
5. If NCZarr version is still unknown then the NCZarr version is NULL (i.e. pure zarr).
*/

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
/*Forward*/

/**************************************************/

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
NCZ_get_map(NC_FILE_INFO_T* file, NCURI* url, mode_t mode, size64_t constraints, void* params, NCZMAP** mapp)
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

