/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#include "zincludes.h"

static int initialized = 0;

/* Combine some values to simplify internal argument lists */
struct Common {
    NC_FILE_INFO_T* file;
    NC_VAR_INFO_T* var;
    int reading; /* 1=> read, 0 => write */
    size_t rank;
    size64_t dimlens[NC_MAX_VAR_DIMS];
    size64_t chunklens[NC_MAX_VAR_DIMS];
    void* memory;
    size_t typesize;
    size64_t shape[NC_MAX_VAR_DIMS]; /* shape of the output hyperslab */
    size64_t chunksize;
};

/* Forward */
static size64_t computelinearoffset(size_t, const size64_t*, const size64_t*);
static int NCZ_walk(NCZProjection** projv, NCZOdometer* slpodom, const struct Common common);

/**************************************************/
int
ncz_chunking_init(void)
{
    initialized = 1;
    return NC_NOERR;
}

/**************************************************/

/**
Goal: Given the slices being applied to the variable, create
and walk all possible combinations of projection vectors that
can be evaluated to provide the output data.

@param file Controlling file
@param var Controlling variable
@param slices Slices being applied to variable
@param memory target or source of data
@param typesize Size of type being written
*/

int
NCZ_transferslice(NC_VAR_INFO_T* var, int reading,
		  NCZSlice* slices, void* memory, size_t typesize)
{
    int r,stat = NC_NOERR;
    size64_t dimlens[NC_MAX_VAR_DIMS];
    size64_t chunklens[NC_MAX_VAR_DIMS];

    for(r=0;r<var->ndims;r++) {
	dimlens[r] = var->dim[r]->len;
	chunklens[r] = var->chunksizes[r];
    }
    if((stat = NCZ_projectslice(var->ndims, reading,
		  dimlens, chunklens, slices,
		  memory, typesize)))
	goto done;
done:
    return stat;
}

/* Break out this piece so we can use it for unit testing */
int
NCZ_projectslice(size_t rank, int reading,
		  size64_t* dimlens,
		  size64_t* chunklens,
		  NCZSlice* slices,
		  void* memory, size_t typesize)
{
    int stat = NC_NOERR;
    int i;
    struct Common common;
    NCZOdometer* odom = NULL;
    NCZSliceProjections allprojections[NC_MAX_VAR_DIMS];
    NCZChunkRange ranges[NC_MAX_VAR_DIMS];
    size_t start[NC_MAX_VAR_DIMS];
    size_t stop[NC_MAX_VAR_DIMS];

    if(!initialized) ncz_chunking_init();

    memset(&common,0,sizeof(common));
    memset(allprojections,0,sizeof(allprojections));
    memset(ranges,0,sizeof(ranges));

    /* Package common arguments */
    common.reading = reading;
    common.rank = rank;
    memcpy(common.dimlens,dimlens,rank*sizeof(size64_t));
    memcpy(common.chunklens,chunklens,rank*sizeof(size64_t));
    for(common.chunksize=1,i=0;i<rank;i++) {
	common.chunksize *= common.chunklens[i];
    }
    common.memory = memory;
    common.typesize = typesize;

    /* Compute the chunk ranges for each chunk in a given dim */
    if((stat = NCZ_compute_chunk_ranges(common.rank,slices,common.chunklens,ranges)))
	goto done;

    /* Compute the slice index vector */
    if((stat=NCZ_compute_all_slice_projections(common.rank,slices,common.dimlens,common.chunklens,ranges,allprojections)))
	goto done;

    /* Compute the shape vector */
    for(i=0;i<common.rank;i++) {
	int j;
	size64_t iocount = 0;
	NClist* projections = allprojections[i].projections;
	for(j=0;j<nclistlength(projections);j++) {
	    NCZProjection* proj = (NCZProjection*)nclistget(projections,j);
	    iocount += proj->iocount;
	}
	common.shape[i] = iocount;
    }

#if 0
    /* Compute the nchunks vector */
    for(i=0;i<common.rank;i++) {
	NCZChunkRange* range = &ranges[i];
	nchunks[i] = (range->stop - range->start);
    }
#endif

    /* Create an odometer to walk all the range combinations */
    for(i=0;i<common.rank;i++) {
	start[i] = ranges[i].start; 
	stop[i] = ranges[i].stop;
    }	

    if((odom = nczodom_new(common.rank,start,stop,NC_coord_one)) == NULL)
	{stat = NC_ENOMEM; goto done;}

    /* iterate over the odometer: all combination of chunk
       indices in the projections */
    for(;nczodom_more(odom);nczodom_next(odom)) {
	int r;
	size64_t* projindices = NULL;
        NCZOdometer* slpodom = NULL;
        NCZSlice slpslices[NC_MAX_VAR_DIMS];
        NCZProjection* proj[NC_MAX_VAR_DIMS];
	projindices = nczodom_indices(odom);
	for(r=0;r<common.rank;r++) {
	    NClist* projlist = allprojections[r].projections;
  	    /* use projindices[r] to find the corresponding projection slice */
	    proj[r] = nclistget(projlist,projindices[r]); /* note the 2 level indexing */
	}
	for(r=0;r<common.rank;r++) {
	    slpslices[r] = proj[r]->slice;
	}
	slpodom = nczodom_fromslices(common.rank,slpslices);
	/* This is the key action: walk this set of slices and transfer data */
	if((stat = NCZ_walk(proj,slpodom,common))) goto done;
    }

done:
    return stat;
}

static int
NCZ_walk(NCZProjection** projv, NCZOdometer* slpodom, const struct Common common)
{
    int stat = NC_NOERR;

    if(nczprinter) {
	nczprinter->printsort = PRINTSORT_WALK1;
	nczprinter->rank = common.rank;
	nczprinter->pvector = projv;
	nczprinter->printer(nczprinter);
    }

#ifdef IO
    /* compute the iopos vector */
    for(r=0;r<common.rank;r++) {
	NCZProjection* proj = projv[r];
	
    }
#endif
    while(nczodom_more(slpodom)) {
	size64_t* indices = nczodom_indices(slpodom);
	/* Convert the indices to a linear offset WRT to chunk */
	size64_t offset = computelinearoffset(common.rank,indices,common.chunklens);
	if(nczprinter) {
	    nczprinter->printsort = PRINTSORT_WALK2;
	    nczprinter->rank = common.rank;
	    nczprinter->count = offset;
	    nczprinter->indices = indices;
	    nczprinter->printer(nczprinter);
        }
	nczodom_next(slpodom);
    }

    return stat;    
}

/***************************************************/
/* Utilities */

/* Goal: Given a set of per-dimension indices,
     compute the corresponding linear position.
*/
static size64_t
computelinearoffset(size_t R, const size64_t* indices, const size64_t* dimlens)
{
      size64_t offset;
      int i;

      offset = 0;
      for(i=0;i<R;i++) {
          offset *= dimlens[i];
          offset += indices[i];
      } 
      return offset;
}

/**************************************************/
/* Unit test entry points */

int
NCZ_chunkindexodom(size_t rank, const NCZChunkRange* ranges, NCZOdometer** odomp)
{
    int stat = NC_NOERR;
    int r;
    NCZOdometer* odom = NULL;
    size_t start[NC_MAX_VAR_DIMS];
    size_t stop[NC_MAX_VAR_DIMS];

    for(r=0;r<rank;r++) {
	start[r] = ranges[r].start; 
	stop[r] = ranges[r].stop;
    }	

    if((odom = nczodom_new(rank, start, stop, NC_coord_one))==NULL)
	{stat = NC_ENOMEM; goto done;}
    if(nczprinter) {
	nczprinter->printsort = PRINTSORT_RANGE;
        while(nczodom_more(odom)) {
	    size64_t* indices = nczodom_indices(odom);
	    nczprinter->rank = rank;
	    nczprinter->indices = indices;
	    nczprinter->printer(nczprinter);
	    nczodom_next(odom);
	}
    }
    if(odomp) {*odomp = odom; odom = NULL;}

done:
    nczodom_free(odom);
    return stat;
}
