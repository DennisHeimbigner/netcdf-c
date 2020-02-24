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
static int walkchunkindices(const size64_t*, const NCZSliceProjections*, const struct Common);
static int transferdata(const NCZSlice*, size64_t, size64_t, void*, const struct Common);
static size64_t computelinearoffset(size_t, const size64_t*, const size64_t*);

/**************************************************/
int
ncchunking_init(void)
{
    initialized = 1;
    return NC_NOERR;
}

/**************************************************/

/**
Goal: Given the slices being applied to the variable, create
and walk all possible combinations of projection vectors that
can be evaluated to provide the output data.

In following code:
- R is the rank of var

@param file Controlling file
@param var Controlling variable
@param slices Slices being applied to variable
@param memory
@param typesize Size of type being written
*/

static int
transferslice(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int reading,
		  NCZSlice* slices, void* memory, size_t typesize)
{
    int stat = NC_NOERR;
    int i;
    struct Common common;
    NCZOdometer* odom = NULL;
    size_t nchunks[NC_MAX_VAR_DIMS];
    NCZSliceProjections allprojections[NC_MAX_VAR_DIMS];

    if(!initialized) ncchunking_init();

    memset(allprojections,0,sizeof(allprojections));

    /* Package common arguments */
    memset(&common,0,sizeof(common));
    common.file = file;
    common.var = var;
    common.reading = reading;
    common.rank = var->ndims;
    common.chunksize = 1;
    for(i=0;i<var->ndims;i++) {
	common.dimlens[i] = var->dim[i]->len;
	common.chunklens[i] = var->chunksizes[i];
	common.chunksize *= common.chunklens[i];
    }
    common.memory = memory;
    common.typesize = typesize;

    /* Compute the slice index vector */
    if((stat=NCZ_compute_all_slice_projections(common.rank,slices,common.dimlens,common.chunklens,allprojections)))
	goto done;

    /* Compute the nchunks and shape vectors */
    for(i=0;i<common.rank;i++) {
	int j;
	size64_t iocount = 0;
	NClist* projections = allprojections[i].projections;
	NCZChunkRange* range = &allprojections[i].range;
	nchunks[i] = (range->stop - range->start);
	for(j=0;j<nclistlength(projections);j++) {
	    NCZProjection* proj = (NCZProjection*)nclistget(projections,j);
	    iocount += proj->iocount;
	}
	common.shape[i] = iocount;
    }

    /* Create an odometer to walk nchunk combinations */
    /* iterate each "wheel[i]" over 0..nchunk[i] with R wheels */
    if((odom = nczodom_new(common.rank,NC_coord_zero,nchunks,NC_coord_one)) == NULL)
	{stat = NC_ENOMEM; goto done;}

    /* iterate over the odometer: all combination of chunk
       indices in the projections */
    for(;nczodom_more(odom);nczodom_next(odom)) {
	size64_t* projindices = nczodom_indices(odom);
	if((walkchunkindices(projindices,allprojections,common)))
	    goto done;
    }

done:
    return stat;
}

/* Goal: given a vector of chunk indices from projections,
         extract the corresponding data and store it into the
         output target
*/
static int
walkchunkindices(const size64_t* projindices, const NCZSliceProjections* allprojections, const struct Common common)
{
    int stat = NC_NOERR;
    int i;
    NCZProjection* projections[NC_MAX_VAR_DIMS];
    NCZSlice slices[NC_MAX_VAR_DIMS];
    size64_t iopos[NC_MAX_VAR_DIMS];
    size64_t chunkindices[NC_MAX_VAR_DIMS]; /* global chunk indices */
    size64_t iostart;
    NCZ_FILE_INFO_T* zfile = NULL;
    void* data = NULL;
    size64_t datalen;

    zfile = (NCZ_FILE_INFO_T *)common.file->format_file_info;

    /* This is complicated. We need to construct a vector (of size R)
       of slices where the ith slice is determined from a projection
       for the ith chunk index of chunkindices. We then iterate over
       that odometer to extract values and store them in the output.
    */
    for(i=0;i<common.rank;i++) {
      const NCZSliceProjections* slp = &allprojections[i];
      size64_t projchunk = projindices[i]; /* projection index */
      projections[i] = nclistget(slp->projections,projchunk); /* corresponding projection */
      slices[i] = projections[i]->slice;   
      iopos[i] = projections[i]->iopos;
      chunkindices[i] = projections[i]->chunkindex + projchunk; /* global chunk index */
    }
    /* Compute where the extracted data will go in the I/O vector */
    iostart = computelinearoffset(common.rank,iopos,common.shape);  
    /* read the chunk */
    if((stat=NCZ_read_cache_chunk(zfile->cache,common.var,chunkindices,&datalen,&data)))
	goto done;
    /* Transfer the relevant data */
    if((stat=transferdata(slices,iostart,datalen,data,common))) goto done;
    if(!common.reading) { /* Mark chunk modified */
        if((stat=ncz_chunk_cache_modified(zfile->cache,common.var,chunkindices)))
   	    goto done;
    }

done:
    return stat;
}
  
/* Goal: given a set of indices pointing to projections,
         extract the corresponding data and store it into the
         io target.
*/
static int
transferdata(const NCZSlice* slices, size64_t iostart, size64_t chunklen, void* chunkdata, const struct Common common)
{
    int stat = NC_NOERR;
    NCZOdometer* sliceodom = NULL;
    char* memory = NULL;
  
    if((sliceodom = nczodom_fromslices(common.rank,slices)) == NULL)
	{stat = NC_ENOMEM; goto done;}
  
    /* iterate over the odometer to get a point in the chunk space */
    for(memory=chunkdata;nczodom_more(sliceodom);nczodom_next(sliceodom)) {
        size64_t* indices = nczodom_indices(sliceodom);
        size64_t offset = computelinearoffset(common.rank,indices,common.dimlens);
        size64_t pos = offset * common.typesize;
	if(common.reading)
            memcpy(memory,chunkdata+pos,common.typesize);
	else
            memcpy(chunkdata+pos,memory,common.typesize);
	memory += common.typesize;
    }
done:
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

/* Construct the odometer for walking all the chunk indices */

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
    if(odomp) *odomp = odom;

done:
    return stat;
}
