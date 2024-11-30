/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "zincludes.h"
#include "zfill.h"

/**************************************************/
/* Make a strut of the possible fill values indexed by NC_NAT..NC_STRING.
   Note that we do not use a union because it cannot be statically initialized.
*/
static struct DFALTFILL {
    int			natv;
    char		bytev;
    char		charv;
    short		shortv;
    int			intv;
    float		floatv;
    double		doublev;
    unsigned char	ubytev;
    unsigned short	ushortv;
    unsigned int	uintv;
    long long		int64v;
    unsigned long long	uint64v;
    char*		stringv;
} dfaltfill = {
	0,		/*NC_NAT*/
	NC_FILL_BYTE,	/*NC_BYTE*/
	NC_FILL_CHAR,	/*NC_CHAR*/
	NC_FILL_SHORT,	/*NC_SHORT*/
	NC_FILL_INT,	/*NC_INT*/
	NC_FILL_FLOAT,	/*NC_FLOAT*/
	NC_FILL_DOUBLE,	/*NC_DOUBLE*/
	NC_FILL_UBYTE,	/*NC_UBYTE*/
	NC_FILL_USHORT,	/*NC_USHORT*/
	NC_FILL_UINT,	/*NC_UINT*/
	NC_FILL_INT64,	/*NC_INT64*/
	NC_FILL_UINT64,	/*NC_UINT64*/
	NC_FILL_STRING	/*NC_STRING*/
};

/**************************************************/
/**************************************************/

/* (over-) write the NC_VAR_INFO_T.fill_value; always make copy of fillvalue argument */
int
NCZ_set_fill_value(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int no_fill, const void* fillvalue)
{
    int stat = NC_NOERR;
    size_t size;
    nc_type tid = var->type_info->hdr.id;

    if(no_fill) {
	stat = NCZ_fillvalue_disable(file,var);
	goto done;
    }

    if ((stat = nc4_get_typelen_mem(file, tid, &size))) goto done;
    assert(size);

    /* Reclaim any existing var->fill_value */
    if(var->fill_value != NULL) {
	if((stat = NC_reclaim_data_all(file->controller,tid,var->fill_value,1))) goto done;
        var->fill_value = NULL;
    }
    if(fillvalue == NULL) {/* use default fill value */
	/* initialize the fill_value to the default */
	if((stat = NCZ_set_fill_value(file,var,var->no_fill,NCZ_getdfaltfillvalue(var->type_info->hdr.id)))) goto done;
        var->fill_val_changed = 0;
    } else {
        /* overwrite the fill value */
	assert(var->fill_value == NULL);
        if((stat = NC_copy_data_all(file->controller,tid,fillvalue,1,&var->fill_value))) goto done;
	var->fill_val_changed = 1;
    }
    var->no_fill = 0;
    stat = NCZ_reclaim_fill_chunk(((NCZ_VAR_INFO_T*)var->format_var_info)->cache); /* Reclaim any existing fill_chunk */
    
done:
    return THROW(stat);
}

/* (over-) write/create the _FillValue attribute */
int
NCZ_set_fill_att(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NC_ATT_INFO_T* att, int no_fill, const void* fillvalue)
{
    int stat = NC_NOERR;
    if(no_fill) {
	stat = NCZ_fillvalue_disable(file,var);
    } else {
	if(att == NULL) {
	    int isnew = 0;
	    if((stat = NCZ_getattr(file, (NC_OBJ*)var, NC_FillValue, var->type_info->hdr.id, &att, &isnew))) goto done;
	}    
        assert(att != NULL && strcmp(att->hdr.name,NC_FillValue)==0); /* Verify */
        if((stat = NCZ_sync_dual_att(file,(NC_OBJ*)var,NC_FillValue,DA_FILLVALUE,FIXATT))) goto done;
	var->no_fill = NC_FALSE;
	var->fill_val_changed = 1;
    }

done:
    return THROW(stat);
}

#if 0
/* Sync from NC_VAR_INFO_T.fill_value to attribute _FillValue */
int
NCZ_copy_var_to_fillatt(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NC_ATT_INFO_T* att)
{
    int stat = NC_NOERR;
    struct NCZ_AttrInfo ainfo = NCZ_emptyAttrInfo();

    if(var->no_fill) {
	/* disable fill value */
        stat = NCZ_fillvalue_disable(file,var);
    } else if(att == NULL) {
	NCZ_clearAttrInfo(file,&ainfo);
	ainfo.name = NC_FillValue;
	ainfo.nctype = var->type_info->hdr.id;
	ainfo.datalen = 1;
	ainfo.data = var->fill_value;
        stat = ncz_makeattr(file,(NC_OBJ*)var,&ainfo,NULL);
    } else { /* presumably already exists */
	assert(strcmp(NC_FillValue,att->hdr.name)==0);
	stat = NCZ_copy_value_to_att(file,att,1,var->fill_value);
    }
    NCZ_clearAttrInfo(file,&ainfo);
    return THROW(stat);
}

/* Sync from Attribute_FillValue to NC_VAR_INFO_T.fill_value */
int
NCZ_copy_fillatt_to_var(NC_FILE_INFO_T* file, NC_ATT_INFO_T* att, NC_VAR_INFO_T* var)
{
    int stat = NC_NOERR;

    if(att == NULL) {
        /* The att _FillValue must exist */
	att = (NC_ATT_INFO_T*)ncindexlookup(var->att,NC_FillValue);
    }
    assert(var != NULL && att != NULL);
    assert(strcmp(NC_FillValue,att->hdr.name)==0 && att->len == 1);
    if((stat = NCZ_copy_value_to_var_fillvalue(file,var,att->data))) goto done;
assert(var->fill_value != att->data);
    var->fill_val_changed = 1;
    var->no_fill = 0;
    /* Reclaim any existing fill_chunk */
    stat = NCZ_reclaim_fill_chunk(((NCZ_VAR_INFO_T*)var->format_var_info)->cache);
        
done:
    return THROW(stat);
}
#endif

/* Turn off FillValue */
int
NCZ_fillvalue_disable(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var)
{
    int stat = NC_NOERR;
    nc_type tid = var->type_info->hdr.id;

    if(!var->no_fill) var->fill_val_changed = 1;
    var->no_fill = 1;
    /* Reclaim the fill value */
    if((stat = NC_reclaim_data_all(file->controller,tid,var->fill_value,1))) goto done;
    var->fill_value = NULL;
    stat = NCZ_reclaim_fill_chunk(((NCZ_VAR_INFO_T*)var->format_var_info)->cache); /* Reclaim any existing fill_chunk */
    stat = NCZ_attr_delete(file,var->att,NC_FillValue);
    if (stat && stat != NC_ENOTATT) return stat; else stat = NC_NOERR;
done:
    return THROW(stat);
}

/**************************************************/
/* Basic operations are:
1. copy from a src to att->data
2. reclaim and clear data in att->data
3. copy from a src to var->fill_value.
4. reclaim and clear data in var->fill_value
*/

#if 0
int
NCZ_copy_value_to_var_fillvalue(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const void* src)
{
    int stat = NC_NOERR;
    nc_type tid = var->type_info->hdr.id;
    assert(var != NULL);
    if((stat = NCZ_reclaim_var_fillvalue(file, var))) goto done; /* reclaim old data */
    /* Now fill var->fill_value */
    if((stat = NC_copy_data_all(file->controller,tid,src,1,&var->fill_value))) goto done;
    
done:
    return stat;
}

int
NCZ_reclaim_var_fillvalue(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var)
{
    int stat = NC_NOERR;
    int tid = var->type_info->hdr.id;

    if(var->fill_value != NULL) {
	stat = NC_reclaim_data_all(file->controller,tid,var->fill_value,1);
	var->fill_value = NULL;
	/* Leave var->no_fill as is */
    }
    return stat;
}
#endif

/* get the default fillvalue  */
void*
NCZ_getdfaltfillvalue(nc_type nctype)
{
    void* fillval = NULL;
    switch (nctype) {
    case NC_BYTE: fillval = (void*)&dfaltfill.bytev; break;
    case NC_CHAR: fillval = (void*)&dfaltfill.charv; break;
    case NC_SHORT: fillval = (void*)&dfaltfill.shortv; break;
    case NC_INT: fillval = (void*)&dfaltfill.intv; break;
    case NC_FLOAT: fillval = (void*)&dfaltfill.floatv; break;
    case NC_DOUBLE: fillval = (void*)&dfaltfill.doublev; break;
    case NC_UBYTE: fillval = (void*)&dfaltfill.ubytev; break;
    case NC_USHORT: fillval = (void*)&dfaltfill.ushortv; break;
    case NC_UINT: fillval = (void*)&dfaltfill.uintv; break;
    case NC_INT64: fillval = (void*)&dfaltfill.int64v; break;
    case NC_UINT64: fillval = (void*)&dfaltfill.uint64v; break;
    case NC_STRING: fillval = (void*)&dfaltfill.stringv; break;
    default: break;
    }
    return fillval;
}

/* Test if fillvalue is default */
int
NCZ_isdfaltfillvalue(nc_type nctype, void* fillval)
{
    switch (nctype) {
    case NC_BYTE: if(NC_FILL_BYTE == *((signed char*)fillval)) return 1; break;
    case NC_CHAR: if(NC_FILL_CHAR == *((char*)fillval)) return 1; break;
    case NC_SHORT: if(NC_FILL_SHORT == *((short*)fillval)) return 1; break;
    case NC_INT: if(NC_FILL_INT == *((int*)fillval)) return 1; break;
    case NC_FLOAT: if(NC_FILL_FLOAT == *((float*)fillval)) return 1; break;
    case NC_DOUBLE: if(NC_FILL_DOUBLE == *((double*)fillval)) return 1; break;
    case NC_UBYTE: if(NC_FILL_UBYTE == *((unsigned char*)fillval)) return 1; break;
    case NC_USHORT: if(NC_FILL_USHORT == *((unsigned short*)fillval)) return 1; break;
    case NC_UINT: if(NC_FILL_UINT == *((unsigned int*)fillval)) return 1; break;
    case NC_INT64: if(NC_FILL_INT64 == *((long long int*)fillval)) return 1; break;
    case NC_UINT64: if(NC_FILL_UINT64 == *((unsigned long long int*)fillval)) return 1; break;
    case NC_STRING: if(strcmp(NC_FILL_STRING,*((char**)fillval))) return 1; break;
    default: break;
    }
    return 0;
}

