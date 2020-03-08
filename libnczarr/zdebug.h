/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#ifndef ZDEBUG_H
#define ZDEBUG_H

#define ZUT /* unit test support is enabled */

#undef ZDEBUG /* general debug */

#define ZCATCH /* Warning: significant performance impact */
#define ZTRACING /* Warning: significant performance impact */

#ifdef ZCATCH
/* Place breakpoint on zbreakpoint to catch errors close to where they occur*/
#define THROW(e) zthrow((e),__FILE__,__LINE__)
#define ZCHECK(e) if((e)) {THROW(stat); goto done;} else {}
extern int zbreakpoint(int err);
extern int zthrow(int err, const char* fname, int line);
#else
#define ZCHECK(e) {if((e)) {goto done;}}
#define THROW(e) (e)
#endif

#include "nclog.h"
#ifdef ZTRACING
#define ZLOG(level,msg,...) nclog(level,msg,__VA_ARGS__)
#define ZTRACE() nclog(NCLOGDBG,"trace: %s.%s",__FILE__,__func__)
#else
#define ZLOG(level,msg,...)
#define ZTRACE()
#endif

/* printers */
extern char* nczprint_slice(NCZSlice);
extern char* nczprint_slices(size_t, NCZSlice*);
extern char* nczprint_odom(NCZOdometer);
extern char* nczprint_chunkrange(NCZChunkRange);
extern char* nczprint_projection(NCZProjection);
extern char* nczprint_sliceprojections(NCZSliceProjections);
extern char* nczprint_vector(size_t,size64_t*);

#ifdef ZUT
/* Expose functions for unit tests */
typedef struct NCZ_UT_PRINTER {
    int printsort;
#define PRINTSORT_RANGE 1
#define PRINTSORT_WALK1 2
#define PRINTSORT_WALK2 3
#define PRINTSORT_WALK3 4
    void (*printer)(struct NCZ_UT_PRINTER*);
    /* Union of all fields */
    size_t rank;
    size64_t count;
    size64_t offset;
    size64_t* indices;
    size64_t* vector;
    void** pvector;
    NCZOdometer* odom;
    void* output;
    size_t used;
} NCZ_UT_PRINTER;

extern NCZ_UT_PRINTER* nczprinter;
#endif /* ZUT */

#endif /*ZDEBUG_H*/

