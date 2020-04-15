/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#include "zincludes.h"

/* Mnemonic */
#define RAW 1

#ifdef ZCATCH
/* Place breakpoint here to catch errors close to where they occur*/
int
zbreakpoint(int err)
{
    return err;
}

int
zthrow(int err, const char* file, int line)
{
    if(err == 0) return err;
#ifdef ZDEBUG
    fprintf(stderr,"THROW: %s/%d: (%d) %s\n",file,line,err,nc_strerror(err));
    fflush(stderr);
#endif
    return zbreakpoint(err);
}
#endif

/**************************************************/
/* Data Structure  */

#ifdef ZUT
struct ZUTEST zutest;
#endif

/**************************************************/
/* Data Structure printers */

char*
nczprint_slice(NCZSlice slice)
{
    return nczprint_slicex(slice,!RAW);
}

char*
nczprint_slicex(NCZSlice slice, int raw)
{
    char* result = NULL;
    NCbytes* buf = ncbytesnew();
    char value[64];

    if(raw)
        ncbytescat(buf,"[");
    else
        ncbytescat(buf,"Slice{");
    snprintf(value,sizeof(value),"%lu",(unsigned long)slice.start);
    ncbytescat(buf,value);
    ncbytescat(buf,":");
    snprintf(value,sizeof(value),"%lu",(unsigned long)slice.stop);
    ncbytescat(buf,value);
    if(slice.stride != 1) {
        ncbytescat(buf,":");
        snprintf(value,sizeof(value),"%lu",(unsigned long)slice.stride);
        ncbytescat(buf,value);
    }
    ncbytescat(buf,"|");
    snprintf(value,sizeof(value),"%lu",(unsigned long)slice.len);
    ncbytescat(buf,value);
    if(raw)
        ncbytescat(buf,"]");
    else
        ncbytescat(buf,"}");
    result = ncbytesextract(buf);
    ncbytesfree(buf);
    return result;
}

char*
nczprint_slices(int rank, NCZSlice* slices)
{
    return nczprint_slicesx(rank, slices, !RAW);
}

char*
nczprint_slicesx(int rank, NCZSlice* slices, int raw)
{
    int i;
    char* result = NULL;
    NCbytes* buf = ncbytesnew();

    for(i=0;i<rank;i++) {
	char* ssl;
	if(!raw)
            ncbytescat(buf,"[");
	ssl = nczprint_slicex(slices[i],raw);
	ncbytescat(buf,ssl);
	nullfree(ssl); ssl = NULL;
	if(!raw)
	    ncbytescat(buf,"]");
    }
    result = ncbytesextract(buf);
    ncbytesfree(buf);
    return result;
}

char*
nczprint_slab(int rank, NCZSlice* slices)
{
    return nczprint_slicesx(rank,slices,RAW);
}

char*
nczprint_odom(NCZOdometer* odom)
{
    char* result = NULL;
    NCbytes* buf = ncbytesnew();
    char value[128];
    char* txt = NULL;

    snprintf(value,sizeof(value),"Odometer{rank=%d,",odom->rank);
    ncbytescat(buf,value);

    ncbytescat(buf," start=");
    txt = nczprint_vector(odom->rank,odom->start);
    ncbytescat(buf,txt);
    ncbytescat(buf," stop=");
    txt = nczprint_vector(odom->rank,odom->stop);
    ncbytescat(buf,txt);
    ncbytescat(buf," stride=");
    txt = nczprint_vector(odom->rank,odom->stride);
    ncbytescat(buf,txt);
    ncbytescat(buf," max=");
    txt = nczprint_vector(odom->rank,odom->max);
    ncbytescat(buf,txt);
    ncbytescat(buf," index=");
    txt = nczprint_vector(odom->rank,odom->index);
    ncbytescat(buf,txt);
  
    ncbytescat(buf," offset=");
    snprintf(value,sizeof(value),"%llu",nczodom_offset(odom));
    ncbytescat(buf,value);
    
#ifdef ENABLE_NCZARR_SLAB
    if(odom->useslabs) {
        ncbytescat(buf," psweudorank=");
        snprintf(value,sizeof(value),"%d",odom->pseudorank);
        ncbytescat(buf,value);
        ncbytescat(buf," slabprod=");
        snprintf(value,sizeof(value),"%llu",odom->slabprod);
        ncbytescat(buf,value);
    }
#endif

    ncbytescat(buf,"}");
    result = ncbytesextract(buf);
    ncbytesfree(buf);
    return result;
}

char*
nczprint_projection(NCZProjection proj)
{
   return nczprint_projectionx(proj,!RAW);
}

char*
nczprint_projectionx(NCZProjection proj, int raw)
{
    char* result = NULL;
    NCbytes* buf = ncbytesnew();
    char value[128];

    ncbytescat(buf,"Projection{");
    snprintf(value,sizeof(value),"id=%d,",proj.id);
    ncbytescat(buf,value);
    snprintf(value,sizeof(value),"chunkindex=%lu",(unsigned long)proj.chunkindex);
    ncbytescat(buf,value);
    snprintf(value,sizeof(value),",first=%lu",(unsigned long)proj.first);
    ncbytescat(buf,value);
    snprintf(value,sizeof(value),",last=%lu",(unsigned long)proj.last);
    ncbytescat(buf,value);
    snprintf(value,sizeof(value),",len=%lu",(unsigned long)proj.len);
    ncbytescat(buf,value);
    snprintf(value,sizeof(value),",limit=%lu",(unsigned long)proj.limit);
    ncbytescat(buf,value);
    snprintf(value,sizeof(value),",iopos=%lu",(unsigned long)proj.iopos);
    ncbytescat(buf,value);
    snprintf(value,sizeof(value),",iocount=%lu",(unsigned long)proj.iocount);
    ncbytescat(buf,value);
    ncbytescat(buf,",chunkslice=");
    result = nczprint_slicex(proj.chunkslice,raw);
    ncbytescat(buf,result);
    nullfree(result);
    ncbytescat(buf,",memslice=");
    result = nczprint_slicex(proj.memslice,raw);
    ncbytescat(buf,result);
    nullfree(result);
    result = ncbytesextract(buf);
    ncbytesfree(buf);
    return result;
}

char*
nczprint_sliceprojections(NCZSliceProjections slp)
{
    return nczprint_sliceprojectionsx(slp,!RAW);
}

char*
nczprint_sliceprojectionsx(NCZSliceProjections slp, int raw)
{
    char* result = NULL;
    NCbytes* buf = ncbytesnew();
    char digits[64];
    int i;

    ncbytescat(buf,"SliceProjection{r=");
    snprintf(digits,sizeof(digits),"%lu",(unsigned long)slp.r);
    ncbytescat(buf,digits);
    ncbytescat(buf,",projections=[\n");
    for(i=0;i<slp.count;i++) {
	NCZProjection* p = (NCZProjection*)&slp.projections[i];
	ncbytescat(buf,"\t");
        result = nczprint_projectionx(*p,raw);
        ncbytescat(buf,result);
	ncbytescat(buf,"\n");
    }
    result = NULL;
    ncbytescat(buf,"]");
    ncbytescat(buf,"}");
    result = ncbytesextract(buf);
    ncbytesfree(buf);
    return result;
}

char*
nczprint_chunkrange(NCZChunkRange range)
{
    char* result = NULL;
    NCbytes* buf = ncbytesnew();
    char digits[64];

    ncbytescat(buf,"ChunkRange{start=");
    snprintf(digits,sizeof(digits),"%llu",range.start);
    ncbytescat(buf,digits);
    ncbytescat(buf," stop=");
    snprintf(digits,sizeof(digits),"%llu",range.stop);
    ncbytescat(buf,digits);
    ncbytescat(buf,"}");
    result = ncbytesextract(buf);
    ncbytesfree(buf);
    return result;
}

char*
nczprint_vector(size_t len, size64_t* vec)
{
    char* result = NULL;
    int i;
    char value[128];
    NCbytes* buf = ncbytesnew();

    ncbytescat(buf,"(");
    for(i=0;i<len;i++) {
        if(i > 0) ncbytescat(buf,",");
        snprintf(value,sizeof(value),"%lu",(unsigned long)vec[i]);	
	ncbytescat(buf,value);
    }
    ncbytescat(buf,")");
    result = ncbytesextract(buf);
    ncbytesfree(buf);
    return result;
}

#ifdef ZDEBUG
void
zdumpcommon(struct Common* c)
{
    int r;
    fprintf(stderr,"Common:\n");
#if 0
    fprintf(stderr,"\tfile: %s\n",c->file->controller->path);
    fprintf(stderr,"\tvar: %s\n",c->var->hdr.name);
    fprintf(stderr,"\treading=%d\n",c->reading);
#endif
    fprintf(stderr,"\trank=%d",c->rank);
    fprintf(stderr," dimlens=%s",nczprint_vector(c->rank,c->dimlens));
    fprintf(stderr," chunklens=%s",nczprint_vector(c->rank,c->chunklens));
#if 0
    fprintf(stderr,"\tmemory=%p\n",c->memory);
    fprintf(stderr,"\ttypesize=%d\n",c->typesize);
    fprintf(stderr,"\tswap=%d\n",c->swap);
#endif
    fprintf(stderr," shape=%s\n",nczprint_vector(c->rank,c->shape));
    fprintf(stderr,"\tallprojections:\n");
    for(r=0;r<c->rank;r++)
        fprintf(stderr,"\t\t[%d] %s\n",r,nczprint_sliceprojectionsx(c->allprojections[r],RAW));
    fflush(stderr);
}
#endif
