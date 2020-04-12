/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#include "zincludes.h"

void
nczodom_reset(NCZOdometer* odom)
{
    int r;
    for(r=0;r<odom->rank;r++)
        odom->index[r] = odom->start[r];
}

NCZOdometer*
nczodom_new(size_t rank, const size64_t* start, const size64_t* stop, const size64_t* stride, const size64_t* len)
{
    int i;
    NCZOdometer* odom = NULL;
    if((odom = calloc(1,sizeof(NCZOdometer))) == NULL)
	goto done;   
    odom->rank = rank;
    for(i=0;i<rank;i++) { 
	odom->start[i] = (size64_t)start[i];
	odom->stop[i] = (size64_t)stop[i];
	odom->stride[i] = (size64_t)stride[i];
	odom->max[i] = (size64_t)len[i];
    }
    nczodom_reset(odom);
    for(i=0;i<rank;i++)
        assert(stop > 0 && stride[i] > 0 && len[i] >= stop[i]);
#ifdef ENABLE_NCZARR_SLAB
    odom->useslabs = 0;
    odom->slabprod = 1;
#endif
done:
    return odom;
}

NCZOdometer*
nczodom_fromslices(size_t rank, const NCZSlice* slices)
{
    size_t i;
    NCZOdometer* odom = NULL;

    if((odom = calloc(1,sizeof(NCZOdometer))) == NULL)
	goto done;   
    odom->rank = rank;
    for(i=0;i<rank;i++) {    
	odom->start[i] = slices[i].start;
	odom->stop[i] = slices[i].stop;
	odom->stride[i] = slices[i].stride;
	odom->max[i] = slices[i].len;
    }
    nczodom_reset(odom);
    for(i=0;i<rank;i++)
        assert(slices[i].stop > 0 && slices[i].stride > 0 && slices[i].len >= slices[i].stop);
#ifdef ENABLE_NCZARR_SLAB
    odom->useslabs = 0;
    odom->slabprod = 1;
#endif
done:
    return odom;
}
  
void
nczodom_free(NCZOdometer* odom)
{
    nullfree(odom);
}

int
nczodom_more(NCZOdometer* odom)
{
#ifdef ENABLE_NCZARR_SLAB
return (!odom->useslabs || odom->slab1 > 0) && (odom->index[0] < odom->stop[0]);
#else
return (odom->index[0] < odom->stop[0]);
#endif
}

int
nczodom_next(NCZOdometer* odom)
{
    size64_t i;
    int more = 0;
#ifdef ENABLE_NCZARR_SLAB
    int rank = odom->slab1;
#else
    int rank = odom->rank;
#endif
    for(i=rank-1;i>=0;i--) {
	odom->index[i] += odom->stride[i];
        if(odom->index[i] < odom->stop[i]) break;
        if(i == 0) goto done; /* leave the 0th entry if it overflows */
        odom->index[i] = odom->start[i]; /* reset this position */
    }
    more = 1;
done:
    return more;
}
  
/* Get the value of the odometer */
size64_t*
nczodom_indices(NCZOdometer* odom)
{
    return odom->index;
}

size64_t
nczodom_offset(NCZOdometer* odom)
{
    size64_t i,offset;
#ifdef ENABLE_NCZARR_SLAB
    int rank = odom->slab1;
#else
    int rank = odom->rank;
#endif

    offset = 0;
    for(i=0;i<rank;i++) {
        offset *= odom->max[i];
        offset += odom->index[i];
    } 
#ifdef ENABLE_NCZARR_SLAB
    if(odom->useslabs) offset *= odom->slabprod;
#endif
    return offset;
}

#ifdef ENABLE_NCZARR_SLAB
void
nczodom_slabify(NCZOdometer* odom)
{
    int i;
    for(i=odom->rank-1;i>=0;i--) {if(odom->stride[i] != 1) break;}
    /* use rank to signal odom->stride[rank-1] > 1 */
    odom->slab1 = (i < 0 ? odom->rank : i);
    odom->slabprod = 1;
    for(i=odom->slab1;i<odom->rank;i++)
        odom->slabprod *= odom->max[i];
    odom->useslabs = 1;
}
#endif
