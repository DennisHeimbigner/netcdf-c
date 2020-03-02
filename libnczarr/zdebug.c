/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#include "zincludes.h"

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
/* Data Structure printers */

char*
nczprint_slice(NCZSlice slice)
{
    char* result = NULL;
    NCbytes* buf = ncbytesnew();
    char value[64];

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
    ncbytescat(buf,"}");
    result = ncbytesextract(buf);
    ncbytesfree(buf);
    return result;
}

char*
nczprint_odom(NCZOdometer odom)
{
    char* result = NULL;
    NCbytes* buf = ncbytesnew();
    char value[128];
    int r;

    snprintf(value,sizeof(value),"Odometer{rank=%lu,",(unsigned long)odom.rank);
    ncbytescat(buf,value);

    ncbytescat(buf,"start=[");
    for(r=0;r<odom.rank;r++) {
	if(r > 0) ncbytescat(buf,",");
        snprintf(value,sizeof(value),"%lu",(unsigned long)odom.slices[r].start);
        ncbytescat(buf,value);
    }
    ncbytescat(buf,"]");
    ncbytescat(buf," stop=[");
    for(r=0;r<odom.rank;r++) {
	if(r > 0) ncbytescat(buf,",");
        snprintf(value,sizeof(value),"%lu",(unsigned long)odom.slices[r].stop);
        ncbytescat(buf,value);
    }
    ncbytescat(buf,"]");
    ncbytescat(buf," stride=[");
    for(r=0;r<odom.rank;r++) {
	if(r > 0) ncbytescat(buf,",");
        snprintf(value,sizeof(value),"%lu",(unsigned long)odom.slices[r].stride);
        ncbytescat(buf,value);
    }
    ncbytescat(buf,"]");

    ncbytescat(buf," index=[");
    for(r=0;r<odom.rank;r++) {
	if(r > 0) ncbytescat(buf,",");
        snprintf(value,sizeof(value),"%lu",(unsigned long)odom.index[r]);
        ncbytescat(buf,value);
    }
    ncbytescat(buf,"]");

    ncbytescat(buf,"}");
    result = ncbytesextract(buf);
    ncbytesfree(buf);
    return result;
}

char*
nczprint_projection(NCZProjection proj)
{
    char* result = NULL;
    NCbytes* buf = ncbytesnew();
    char value[128];

    ncbytescat(buf,"Projection{");
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
    ncbytescat(buf,",slice={");
    result = nczprint_slice(proj.slice);
    ncbytescat(buf,result);
    result = NULL;
    ncbytescat(buf,"}");
    ncbytescat(buf,"}");
    result = ncbytesextract(buf);
    ncbytesfree(buf);
    return result;
}

char*
nczprint_sliceprojections(NCZSliceProjections slp)
{
    char* result = NULL;
    NCbytes* buf = ncbytesnew();
    char digits[64];
    int i;

    ncbytescat(buf,"SliceProjection{r=");
    snprintf(digits,sizeof(digits),"%lu",(unsigned long)slp.r);
    ncbytescat(buf,digits);
    ncbytescat(buf,",projections=[\n");
    for(i=0;i<nclistlength(slp.projections);i++) {
	NCZProjection* p = (NCZProjection*)nclistget(slp.projections,i);
	ncbytescat(buf,"\t");
        result = nczprint_projection(*p);
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
