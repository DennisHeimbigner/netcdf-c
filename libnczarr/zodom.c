/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#include "zincludes.h"

/*Forward*/
static int buildodom(int rank, NCZOdometer** odomp);

void
nczodom_reset(NCZOdometer* odom)
{
    int r;
    for(r=0;r<odom->rank;r++)
        odom->index[r] = odom->start[r];
}

NCZOdometer*
nczodom_new(int rank, const size64_t* start, const size64_t* stop, const size64_t* stride, const size64_t* len)
{
    int i;
    NCZOdometer* odom = NULL;
    if(buildodom(rank,&odom)) return NULL;
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
    return odom;
}

NCZOdometer*
nczodom_fromslices(int rank, const NCZSlice* slices)
{
    size_t i;
    NCZOdometer* odom = NULL;

    if(buildodom(rank,&odom)) return NULL;
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
    return odom;
}
  
void
nczodom_free(NCZOdometer* odom)
{
    if(odom == NULL) return;
    nullfree(odom->start);
    nullfree(odom->stop);
    nullfree(odom->stride);
    nullfree(odom->max);
    nullfree(odom->index);
    nullfree(odom);
}

int
nczodom_more(NCZOdometer* odom)
{
    return (odom->index[0] < odom->stop[0]);
}

void
nczodom_next(NCZOdometer* odom)
{
    int i;
    int rank;
#ifdef ENABLE_NCZARR_SLAB
    if(odom->useslabs) {
        rank = odom->pseudorank;
        if(rank == 0) {
	    /* fake !more by incrementing the index[0] */
	    odom->index[0] = odom->stop[0];
	    return;
	}
    } else
#endif
        rank = odom->rank;
    for(i=rank-1;i>=0;i--) {
	odom->index[i] += odom->stride[i];
        if(odom->index[i] < odom->stop[i]) break;
        if(i == 0) goto done; /* leave the 0th entry if it overflows */
        odom->index[i] = odom->start[i]; /* reset this position */
    }
done:
    return;
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
    int i;
    size64_t offset;
    int rank = odom->pseudorank;

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

static int
buildodom(int rank, NCZOdometer** odomp)
{
    int stat = NC_NOERR;
    NCZOdometer* odom = NULL;
    if(odomp) {
        if((odom = calloc(1,sizeof(NCZOdometer))) == NULL)
	    goto done;   
        odom->rank = rank;
        if((odom->start=malloc(sizeof(size64_t)*rank))==NULL) goto nomem;
        if((odom->stop=malloc(sizeof(size64_t)*rank))==NULL) goto nomem;
        if((odom->stride=malloc(sizeof(size64_t)*rank))==NULL) goto nomem;
        if((odom->max=malloc(sizeof(size64_t)*rank))==NULL) goto nomem;
        if((odom->index=malloc(sizeof(size64_t)*rank))==NULL) goto nomem;
        *odomp = odom; odom = NULL;
    }
done:
    nczodom_free(odom);
    return stat;
nomem:
    stat = NC_ENOMEM;
    goto done;
}

#ifdef ENABLE_NCZARR_SLAB
void
nczodom_slabify(NCZOdometer* odom)
{
    int i;
    size64_t product;
    /* Walk right to left thru the leftmost point P where:
       1. stride[P..rank-1] == 1
       2. start[P..rank-1] == 0
       3. stop[P..rank-1] == max
    */
    for(i=odom->rank-1;i>=0;i--) {
	if(odom->stride[i] != 1
	   || odom->start[i] != 0
	   || odom->stop[i] != odom->max[i])
	   break;
    }
    /* Record the point P as pseudorank */
    odom->pseudorank = (i + 1); /* 0=>all, rank=>none */
    /* Compute the crossproduct of max[P..rank-1] */
    product = 1;
    for(i=odom->rank-1;i>=odom->pseudorank;i--) {
        product *= odom->max[i];
    }
    odom->slabprod = product;
    odom->useslabs = 1;
}
#endif
