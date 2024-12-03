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

#if 0
/* (over-) write the NC_VAR_INFO_T.fill_value; always make copy of fillvalue argument.
   Takes no_fill flag into account. Sync with attribute
*/
int
NCZ_set_fill_value(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int no_fill, const void* fillvalue)
{
    int stat = NC_NOERR;
    size_t size;
    nc_type tid = var->type_info->hdr.id;

    if(no_fill) {
	stat = NCZ_disable_fill(file,var);
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
    var->no_fill = NC_FILL;
    
    stat = NCZ_reclaim_fill_chunk(((NCZ_VAR_INFO_T*)var->format_var_info)->cache); /* Reclaim any existing fill_chunk */
    
done:
    return THROW(stat);
}

/* (over-) write/create the _FillValue attribute; always makes copy of NC_VAR_INFO_T.fill_value.
   Takes no_fill flag into account. Does not sync with NC_VAR_INFO_T.fill_value.
*/
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
#endif

/* Turn off var.no_fill and var.fill_value. Sync with attribute */
int
NCZ_disable_fill(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var)
{
    int stat = NC_NOERR;
    nc_type tid = var->type_info->hdr.id;
    NC_ATT_INFO_T* att = NULL;
    int isnew = 0;

    /* Reclaim the fill value, if any */
    if(var->fill_value != NULL) {
	if((stat = NC_reclaim_data_all(file->controller,tid,var->fill_value,1))) goto done;
        var->fill_value = NULL;
    }

    /* And the fill_chunk */
    if((stat = NCZ_reclaim_fill_chunk(((NCZ_VAR_INFO_T*)var->format_var_info)->cache))); /* Reclaim any existing fill_chunk */

    /* And kill off the _FillValue attribute */
    if((stat = NCZ_attr_delete(file,var->att,NC_FillValue)));
    if (stat && stat != NC_ENOTATT) goto done;
    stat = NC_NOERR;

    /* set the _NoFill attribute
       iff var->no_fill was NC_FILL and file->fill_mode == NC_NOFILL */
    if(var->no_fill == NC_FILL &&  file->fill_mode == NC_NOFILL) {
	int nofill = 1;
	if((stat = NCZ_getattr(file,(NC_OBJ*)var, "_NoFill", NC_INT,&att,&isnew))) goto done;
	if((stat = NCZ_set_att_data(file,att,1,&nofill))) goto done;
    }

    if(var->no_fill == NC_FILL) var->fill_val_changed = 1;
    var->no_fill = NC_NOFILL;

done:
    return THROW(stat);
}

/**************************************************/

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

