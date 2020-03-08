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
        odom->index[r] = odom->slices[r].start;
}

NCZOdometer*
nczodom_new(size_t rank, const size_t* start, const size_t* stop, const size_t* stride)
{
    int i;
    NCZOdometer* odom = NULL;
    if((odom = calloc(1,sizeof(NCZOdometer))) == NULL)
	goto done;   
    odom->rank = rank;
    for(i=0;i<rank;i++) { 
	odom->slices[i].start = (size64_t)start[i];
	odom->slices[i].stop = (size64_t)stop[i];
	odom->slices[i].stride = (size64_t)stride[i];
    }
    nczodom_reset(odom);
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
	odom->slices[i] = slices[i];
    }
    nczodom_reset(odom);
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
    return (odom->index[0] < odom->slices[0].stop);
}
  
int
nczodom_next(NCZOdometer* odom)
{
    size_t i;
    for(i=odom->rank-1;i>=0;i--) {
	odom->index[i] += odom->slices[i].stride;
        if(odom->index[i] < odom->slices[i].stop) break;
        if(i == 0) return 0; /* leave the 0th entry if it overflows */
        odom->index[i] = odom->slices[i].start; /* reset this position */
    }
    return 1;
}
  
/* Get the value of the odometer */
size64_t*
nczodom_indices(NCZOdometer* odom)
{
    return odom->index;
}
