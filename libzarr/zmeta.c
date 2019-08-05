/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

/**
 * @file
 * @internal Translation code from parsed json to libsrc4 objects.
 *
 * @author Dennis Heimbigner
 */

#include "zincludes.h"

/**
 * Provide an API to build a netcdf4 node tree (via libhdf4).
 * Basically a wrapper for libsrc4 objects. 
 * The various build functions here -- groups, variables, etc --
 * are invoked elsewhere by walking a json tree (of type NCJson*).
 */

/***************************************************/
/* Forwards */

static int NCZ_open_rootgroup(NCZ_FILE_INFO* zfile);
static int findtype(NC_FILE_INFO_T* h5, nc_type xtype, NC_TYPE_INFO_T** typep);
static int NCZ_annotate(NC_OBJ* object, void* format_data);

/***************************************************/
/* API */

/**
@internal Create the topmost dataset object.
@param zinfo - [in] the internal state
@return NC_NOERR
*/

int
NCZ_create_dataset(NCZ_FILE_INFO* zinfo)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    NCbytes* json = NULL;
    char* path = NULL;

    map = zinfo->map;

    /* Create the .zarr object */
    if((stat=nczmap_def(map,ZMETAROOT,0)))
	goto done;

    /* Write .zarr contents */
    {
	char version[1024];
        void* content = NULL;
        ncbytescat(json,"\"zarr_format\": ");
	snprintf(version,sizeof(version),"%d",zinfo->zarr.zarr_version);
	ncbytescat(json,version);	
        ncbytescat(json,"\"nczarr_version\": ");
	snprintf(version,sizeof(version),"%d",zinfo->zarr.nczarr_version);
	ncbytescat(json,version);	
	content = ncbytescontents(json);
	if((stat = nczmap_writemeta(map,ZMETAROOT,ncbyteslength(json),content)))
	    goto done;
    }

    /* Write root group contents /.zgroup */
    {
	char version[1024];
        void* content = NULL;
	/* Construct .zgroup full path */
        if((stat = nczm_suffix("/",ZGROUPSUFFIX,&path)))
	    goto done;
	/* Create the object */
        if((stat=nczmap_def(map,path,0)))
	     goto done;
	/* Create the .zgroup content */
        ncbytescat(json,"\"zarr_format\": ");
	snprintf(version,sizeof(version),"%d",zinfo->zarr.zarr_version);
	ncbytescat(json,version);	
	content = ncbytescontents(json);
	if((stat = nczmap_writemeta(map,path,ncbyteslength(json),content)))
	    goto done;
	nullfree(path); path = NULL; /* avoid mem errors */
    }

done:
    return stat;
}

/**
@internal Open the topmost dataset object.
@param zinfo - [in] the internal state
@return NC_NOERR
*/

int
NCZ_open_dataset(NCZ_FILE_INFO* zinfo)
{
    int stat = NC_NOERR;
    int i;
    off64_t len;
    void* content = NULL;
    NCjson* json = NULL;

   if((stat = nczmap_len(zinfo->map, ZMETAROOT, &len)))
	goto  done;
    if((content = malloc(len)) == NULL)
	{stat = NC_ENOMEM; goto done;}
    /* Read the top-level dataset object */
    if((stat = nczmap_readmeta(zinfo->map, ZMETAROOT, len, content)))
	goto done;

    /* Parse it as json */
    if((stat = NCjsonparse((char*)content,0,&json)))
	goto done;
    if(json->sort != NCJ_DICT)
	{stat = NC_ENOTNC; goto done;}
    /* Extract the information from it */
    for(i=0;i<nclistlength(json->dict);i+=2) {
	const NCjson* key = nclistget(json->dict,i);
	const NCjson* value = nclistget(json->dict,i+1);
	if(strcmp(key->value,"zarr_format")==0) {
	    if(sscanf(value->value,"%d",&zinfo->zarr.zarr_version)!=1)
		{stat = NC_ENOTNC; goto done;}		
	} else if(strcmp(key->value,"nczarr_version")==0) {
	    if(sscanf(value->value,"%d",&zinfo->zarr.nczarr_version)!=1)
		{stat = NC_ENOTNC; goto done;}		
	}
    }

    /* Build the root group */
    if((stat = NCZ_open_rootgroup(zinfo)))
	goto done;

done:
    if(json) NCJreclaim(json);
    nullfree(content);
    return stat;
}

/**
@internal Open the root group object
@param dataset - [in] the root dataset object
@param rootp - [out] created root group
@return NC_NOERR
*/
static int
NCZ_open_rootgroup(NCZ_FILE_INFO* zfile)
{
    int stat = NC_NOERR;
    int i;
    NC_FILE_INFO_T* dataset = NULL;
    NC_GRP_INFO_T* root = NULL;
    off64_t len;
    void* content = NULL;
    char* rootpath = NULL;
    NCjson* json = NULL;

    if((stat=nczm_suffix(NULL,ZGROUPSUFFIX,&rootpath)))
	goto done;
   if((stat = nczmap_len(zfile->map, rootpath, &len)))
	goto  done;
    if((content = malloc(len)) == NULL)
	{stat = NC_ENOMEM; goto done;}
    /* Read the top-level group object */
    if((stat = nczmap_readmeta(zfile->map, rootpath, len, content)))
	goto done;
    /* Parse it as json */
    if((stat = NCjsonparse((char*)content,0,&json)))
	goto done;
    if(json->sort != NCJ_DICT)
	{stat = NC_ENOTNC; goto done;}
    /* create the root group */
    dataset = zfile->dataset;
    if((stat=nc4_grp_list_add(dataset, NULL, NULL, &root))) goto done;
    /* Process the json */ 
    for(i=0;i<nclistlength(json->dict);i+=2) {
	const NCjson* key = nclistget(json->dict,i);
	const NCjson* value = nclistget(json->dict,i+1);
	if(strcmp(key->value,"zarr_format")==0) {
	    int zversion;
	    if(sscanf(value->value,"%d",&zversion)!=1)
		{stat = NC_ENOTNC; goto done;}		
	    /* Verify against the dataset */
	    if(zversion != zfile->zarr.zarr_version)
		{stat = NC_ENOTNC; goto done;}
	}
    }

done:
    if(json) NCJreclaim(json);
    nullfree(rootpath);
    nullfree(content);
    return stat;
}

/**
@internal Create a dimension
@param file - [in] the containing file
@param parent - [in] the containing group
@param u8name - [in] the UTF8 name (NULL => anonymous)
@param len - [in] the dimension length
@param dimp - [out] created dimension
@return NC_NOERR
*/
int
NCZ_make_dim(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, char* u8name, off64_t len, NC_DIM_INFO_T** dimp)
{
    int stat = NC_NOERR;
    if((stat=nc4_dim_list_add(parent, u8name, (size_t)len, 0, dimp))) goto done;
done:
    return stat;
}

/**
@internal Create a variable
@param file - [in] the containing file
@param parent - [in] the containing group
@param u8name - [in] the UTF8 name
@param basetype - [in] basetype
@param dims - [in] NClist<NC_DIM_INFO_T*> dimension references (|dims|==0 => scalar)
@param varp - [out] created variable
@return NC_NOERR
*/
int
NCZ_make_var(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, char* u8name, nc_type basetype,
		NClist* dims, NC_VAR_INFO_T** varp)
{
    int stat = NC_NOERR;
    NC_VAR_INFO_T* var = NULL;
    NCZ_VAR_INFO* zvar = NULL;
    NC_TYPE_INFO_T* type = NULL;
    int i;

    if((stat=nc4_var_list_add(parent, u8name, nclistlength(dims), &var))) goto done;
    for(i=0;i<nclistlength(dims);i++) {
	NC_DIM_INFO_T* dim = nclistget(dims,i);
	var->dimids[i] = dim->hdr.id;
    }
    if((stat=findtype(file,basetype,&type)))
	goto done;

    if((zvar = calloc(1,sizeof(NCZ_VAR_INFO))) == NULL)
	{stat = NC_ENOMEM; goto done;}
    
    if((stat = NCZ_annotate((NC_OBJ*)var,zvar))) goto done;

    if(varp) *varp = var;
done:
    return stat;
}

/**
@internal Create an attribute
@param file - [in] the containing file
@param parent - [in] the containing group or variable
@param u8name - [in] the UTF8 name
@param basetype - [in] basetype
@param attp - [out] created attribute
@return NC_NOERR
*/
int
NCZ_make_attr(NC_FILE_INFO_T* file, NC_OBJ* parent, char* u8name, nc_type basetype, NC_ATT_INFO_T** attp)
{
    int stat = NC_NOERR;
    NC_ATT_INFO_T* att = NULL;
    NCZ_ATT_INFO* zatt = NULL;
    NCindex* attrs = NULL;

    if(parent->sort == NCGRP) {
	attrs = ((NC_VAR_INFO_T*)parent)->att;
    } else {
	attrs = ((NC_GRP_INFO_T*)parent)->att;
    }

    if((stat=nc4_att_list_add(attrs,u8name,&att))) goto done;
    att->nc_typeid = basetype;

    if((zatt = calloc(1,sizeof(NCZ_ATT_INFO))) == NULL)
	{stat = NC_ENOMEM; goto done;}
    if((stat = NCZ_annotate((NC_OBJ*)att,zatt))) goto done;

    if(attp) *attp = att;
done:
    return stat;
}

/**
@internal Annotate an object with its zarr specific data
@param object - [in] object to annotate
@param format_data - [in] annotation
@return NC_NOERR
*/
static int
NCZ_annotate(NC_OBJ* object, void* format_data)
{
    switch (object->sort) {
    case NCGRP: ((NC_GRP_INFO_T*)object)->format_grp_info = format_data; break;
    case NCVAR: ((NC_VAR_INFO_T*)object)->format_var_info = format_data; break;
    case NCDIM: ((NC_DIM_INFO_T*)object)->format_dim_info = format_data; break;
    case NCATT: ((NC_ATT_INFO_T*)object)->format_att_info = format_data; break;
    case NCTYP: ((NC_TYPE_INFO_T*)object)->format_type_info = format_data; break;
    case NCFLD: ((NC_FIELD_INFO_T*)object)->format_field_info = format_data; break;
    default: return NC_EINTERNAL;
    }
    return NC_NOERR;
}

static int
findtype(NC_FILE_INFO_T* h5, nc_type xtype, NC_TYPE_INFO_T** typep)
{
    int stat = NC_NOERR;
    size_t len;
    NC_TYPE_INFO_T* type = NULL;
    NCZ_TYPE_INFO* ztype = NULL;

    assert(xtype <= NC_STRING);

    /* Get type length. */
    if ((stat = nc4_get_typelen_mem(h5, xtype, &len)))
        goto done;

    /* Create new NC_TYPE_INFO_T struct for this atomic type. */
    if ((stat = nc4_type_new(len, nc4_atomic_name[xtype], xtype, &type)))
        goto done;
    type->endianness = NC_ENDIAN_NATIVE;
    type->size = len;

    /* Allocate storage for zarr-specific type info. */
    if ((ztype = calloc(1, sizeof(NCZ_TYPE_INFO))) == NULL)
        {stat = NC_ENOMEM; goto done;}
    if((stat = NCZ_annotate((NC_OBJ*)type,ztype))) goto done;

    /* Set the "class" of the type */
    type->nc_type_class = xtype;

    if(typep) *typep = type;

done:
    return stat;

}
