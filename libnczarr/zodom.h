/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef ZODOM_H
#define ZODOM_H

struct NCZSlice;

typedef struct NCZOdometer {
    size64_t rank; /*rank */
    size64_t start[NC_MAX_VAR_DIMS];
    size64_t stop[NC_MAX_VAR_DIMS]; /* start + (count*stride) */
    size64_t stride[NC_MAX_VAR_DIMS];
    size64_t max[NC_MAX_VAR_DIMS]; /* full dimension length */
    size64_t index[NC_MAX_VAR_DIMS]; /* current value of the odometer*/
    /* Slab optimizations */
    int useslabs; /* 1 => modify odom behavior */
    size64_t slab1; /* least i s.t. stride[j] == 1 for i<=k<rank */
    size64_t slabprod; /* max[slab1]*max[slab1+1]...*max[rank] */
} NCZOdometer;

/**************************************************/
/* From zodom.c */
extern NCZOdometer* nczodom_new(size_t, const size64_t*, const size64_t*, const size64_t*, const size64_t*);
extern NCZOdometer* nczodom_fromslices(size_t rank, const struct NCZSlice* slices);
extern int nczodom_more(NCZOdometer*);
extern int nczodom_next(NCZOdometer*);
extern size64_t* nczodom_indices(NCZOdometer*);
extern size64_t nczodom_offset(NCZOdometer*);
extern void nczodom_reset(NCZOdometer* odom);
extern void nczodom_free(NCZOdometer*);
extern void nczodom_slabify(NCZOdometer*);

#endif /*ZODOM_H*/
