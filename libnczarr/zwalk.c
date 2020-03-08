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
    int swap; /* var->format_info_file->native_endianness == var->endianness */
    size64_t shape[NC_MAX_VAR_DIMS]; /* shape of the output hyperslab */
    size64_t chunkcount; /* cross product of the chunk counts */
    NCZSliceProjections* allprojections;
};

/* Forward */
static int NCZ_walk(NCZProjection** projv, NCZOdometer* slpodom, const struct Common* common, void* chunkdata);
static int rangecount(NCZChunkRange range);
static int NCZ_fillchunk(size64_t chunklen, void* chunkdata, struct Common*);

#ifdef ZUT
static void zwalkprint(int sort,...);
#endif

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
Note that we do not actually pass NCZSlice but rather
(start,count,stride) vectors.

@param file Controlling file
@param var Controlling variable
@param slices Slices being applied to variable
@param memory target or source of data
@param typesize Size of type being written
*/

int
NCZ_transferslice(NC_VAR_INFO_T* var, int reading,
		  size64_t* start, size64_t* count, size64_t* stride,
		  void* memory, size_t typesize)
{
    int r,stat = NC_NOERR;
    size64_t dimlens[NC_MAX_VAR_DIMS];
    size64_t chunklens[NC_MAX_VAR_DIMS];
    NCZSlice slices[NC_MAX_VAR_DIMS];
    NCZOdometer* chunkodom = NULL;
    struct Common common;
    NCZ_FILE_INFO_T* zfile = NULL;

    memset(&common,0,sizeof(common));

    for(r=0;r<var->ndims;r++) {
	dimlens[r] = var->dim[r]->len;
	chunklens[r] = var->chunksizes[r];
	slices[r].start = start[r];
	slices[r].stride = stride[r];
	slices[r].stop = start[r]+(count[r]*stride[r]);
    }

    common.var = var;
    common.file = (var->container)->nc4_info;
    zfile = common.file->format_file_info;
    common.reading = reading;
    common.memory = memory;
    common.typesize = typesize;
    common.rank = var->ndims;
    common.swap = (zfile->native_endianness == var->endianness ? 0 : 1);

    if((stat = NCZ_projectslices(dimlens, chunklens, slices,
		  &common, &chunkodom)))
	goto done;

    /* iterate over the odometer: all combination of chunk
       indices in the projections */
    for(;nczodom_more(chunkodom);nczodom_next(chunkodom)) {
	int r;
	size64_t* chunkindices = NULL;
        NCZOdometer* slpodom = NULL;
        NCZSlice slpslices[NC_MAX_VAR_DIMS];
        NCZProjection* proj[NC_MAX_VAR_DIMS];
	NCZ_VAR_INFO_T* zvar = NULL;
	void* chunkdata = NULL;

	chunkindices = nczodom_indices(chunkodom);

	/* Read from cache */
	zvar = common.var->format_var_info;
	stat = NCZ_read_cache_chunk(zvar->cache, chunkindices, &chunkdata);
	switch (stat) {
	case NC_EACCESS: /* cache created the chunk */
	    /* Figure out fill value */
	    if((stat=NCZ_fillchunk(NCZ_cache_entrysize(zvar->cache),chunkdata,&common)))
		goto done;
	    break;
	case NC_NOERR: break;
	default: goto done;
	}
	for(r=0;r<common.rank;r++) {
	    NClist* projlist = common.allprojections[r].projections;
  	    /* use chunkindices[r] to find the corresponding projection slice */
	    proj[r] = nclistget(projlist,chunkindices[r]); /* note the 2 level indexing */
	}
	for(r=0;r<common.rank;r++) {
	    slpslices[r] = proj[r]->slice;
	}
	slpodom = nczodom_fromslices(common.rank,slpslices);
#ifdef ZUT
	zwalkprint(PRINTSORT_WALK1, common.rank, proj, chunkindices);
#endif
	/* This is the key action: walk this set of slices and transfer data */
	if((stat = NCZ_walk(proj,slpodom,&common,chunkdata))) goto done;
    }

done:
    return stat;
}

/* Break out this piece so we can use it for unit testing */
int
NCZ_projectslices(size64_t* dimlens,
		  size64_t* chunklens,
		  NCZSlice* slices,
		  struct Common* common, 
		  NCZOdometer** odomp)
{
    int stat = NC_NOERR;
    int r;
    NCZOdometer* odom = NULL;
    NCZSliceProjections* allprojections = NULL;
    NCZChunkRange ranges[NC_MAX_VAR_DIMS];
    size_t start[NC_MAX_VAR_DIMS];
    size_t stop[NC_MAX_VAR_DIMS];

    if(!initialized) ncz_chunking_init();

    if((allprojections = calloc(common->rank,sizeof(NCZSliceProjections))) == NULL)
	{stat = NC_ENOMEM; goto done;}
    memset(ranges,0,sizeof(ranges));

    /* Package common arguments */
    memcpy(common->dimlens,dimlens,common->rank*sizeof(size64_t));
    memcpy(common->chunklens,chunklens,common->rank*sizeof(size64_t));
    for(common->chunkcount=1,r=0;r<common->rank;r++) {
	common->chunkcount *= common->chunklens[r];
    }

    /* Compute the chunk ranges for each chunk in a given dim */
    if((stat = NCZ_compute_chunk_ranges(common->rank,slices,common->chunklens,ranges)))
	goto done;

    /* Compute the slice index vector */
    if((stat=NCZ_compute_all_slice_projections(common->rank,slices,common->dimlens,common->chunklens,ranges,allprojections)))
	goto done;

    /* Verify */
    for(r=0;r<common->rank;r++) {
	assert(rangecount(ranges[r]) == nclistlength(allprojections[r].projections));
    }

    /* Compute the shape vector */
    for(r=0;r<common->rank;r++) {
	int j;
	size64_t iocount = 0;
	NClist* projections = allprojections[r].projections;
	for(j=0;j<nclistlength(projections);j++) {
	    NCZProjection* proj = (NCZProjection*)nclistget(projections,j);
	    iocount += proj->iocount;
	}
	common->shape[r] = iocount;
    }
    common->allprojections = allprojections;
    allprojections = NULL;

    /* Create an odometer to walk all the range combinations */
    for(r=0;r<common->rank;r++) {
	start[r] = ranges[r].start; 
	stop[r] = ranges[r].stop;
    }	

    if((odom = nczodom_new(common->rank,start,stop,NC_coord_one)) == NULL)
	{stat = NC_ENOMEM; goto done;}
    if(odomp) *odomp = odom;

done:
    return stat;
}

static int
NCZ_walk(NCZProjection** projv, NCZOdometer* slpodom, const struct Common* common, void* chunkdata)
{
    int r,stat = NC_NOERR;
    size64_t ioposv[NC_MAX_VAR_DIMS];
    size64_t memoffset = 0;

    /* compute the iopos vector */
    for(r=0;r<common->rank;r++) {
	NCZProjection* proj = projv[r];
	ioposv[r] = proj->iopos;	
    }

    /* Warning; memoffset is in units of common->typesize, not absolute */
    memoffset = NCZ_computelinearoffset(common->rank,ioposv,common->shape);

#ifdef ZUT
    if(nczprinter) nczprinter->used = 0;
    zwalkprint(PRINTSORT_WALK2, common->rank, memoffset, ioposv, common->shape);
#endif
    while(nczodom_more(slpodom)) {
	size64_t* indices = nczodom_indices(slpodom);
	/* Convert the indices to a linear offset WRT to chunk */
	size64_t chunkoffset = NCZ_computelinearoffset(common->rank,indices,common->chunklens);
#ifdef ZUT
	zwalkprint(PRINTSORT_WALK3, common->rank, chunkoffset, memoffset, slpodom);
#endif
	/* transfer data */
	if(common->reading) {
            size_t chunkpos = chunkoffset * common->typesize;
	    unsigned char* memdst = ((unsigned char*)common->memory)+(memoffset*common->typesize);
	    unsigned char* chunksrc = ((unsigned char*)chunkdata)+chunkpos;
	    memcpy(memdst,chunksrc,common->typesize);
	    if(common->swap)
		NCZ_swapatomicdata(common->typesize,chunksrc,common->typesize);
	} else {
            size_t chunkpos = chunkoffset * common->typesize;
	    unsigned char* memsrc = ((unsigned char*)common->memory)+(memoffset*common->typesize);
	    unsigned char* chunkdst = ((unsigned char*)chunkdata)+chunkpos;
	    memcpy(chunkdst,memsrc,common->typesize);
	    if(common->swap)
		NCZ_swapatomicdata(common->typesize,chunkdst,common->typesize);
 	}
        memoffset++;
	nczodom_next(slpodom);
    }
#ifdef ZUT
    if(nczprinter) nczprinter->used = (size_t)memoffset;
#endif
    return stat;    
}

/* This function may not be necessary if code in zvar does it instead */
static int
NCZ_fillchunk(size64_t chunklen, void* chunkdata, struct Common* common)
{
    memset(chunkdata,0,chunklen);
    return NC_NOERR;    
}

/***************************************************/
/* Utilities */

static int
rangecount(NCZChunkRange range)
{
    return (range.stop - range.start);
}

/* Goal: Given a set of per-dimension indices,
     compute the corresponding linear position.
*/
size64_t
NCZ_computelinearoffset(size_t R, const size64_t* indices, const size64_t* dimlens)
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
#ifdef ZUT
    zwalkprint(PRINTSORT_RANGE, rank, odom);
#endif
    if(odomp) {*odomp = odom; odom = NULL;}

done:
    nczodom_free(odom);
    return stat;
}

#ifdef ZUT
static void
zwalkprint(int sort,...)
{
    va_list vl;
    size_t rank;  
    size64_t* indices = NULL;
    size64_t* vector = NULL;
    NCZOdometer* odom = NULL;
    void** pvector = NULL;
    size64_t offset, count;

    if(!nczprinter) return;
    nczprinter->printsort = sort;

    va_start(vl,sort);

    switch (sort) {
    case PRINTSORT_RANGE:
	rank = va_arg(vl,size_t);
	odom = va_arg(vl,NCZOdometer*);
        while(nczodom_more(odom)) {
	    indices = nczodom_indices(odom);
	    nczprinter->rank = rank;
	    nczprinter->indices = indices;
	    nczprinter->printer(nczprinter);
	    nczodom_next(odom);
	}
	nczodom_reset(odom);
	break;
    case PRINTSORT_WALK1:
	rank = va_arg(vl,size_t);
	pvector = va_arg(vl,void**);
	indices = va_arg(vl,size64_t*);
	nczprinter->rank = rank;
	nczprinter->pvector = pvector;
	nczprinter->indices = indices;
        nczprinter->printer(nczprinter);
	break;
    case PRINTSORT_WALK2:
	rank = va_arg(vl,size_t);
	pvector = va_arg(vl,void**);
	indices = va_arg(vl,size64_t*);
	nczprinter->rank = rank;
	nczprinter->pvector = pvector;
	nczprinter->indices = indices;
        nczprinter->printer(nczprinter);
	break;
    case PRINTSORT_WALK3:
	rank = va_arg(vl,size_t);
	count = va_arg(vl,size64_t);
	offset = va_arg(vl,size64_t);
	odom = va_arg(vl,NCZOdometer*);
        nczprinter->rank = rank;
	nczprinter->count = count;
	nczprinter->offset = offset;
	nczprinter->odom = odom;
	nczprinter->vector = vector;
	break;
    default: abort();
    }
    va_end(vl);
}
#endif
