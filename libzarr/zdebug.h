/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#ifndef NCZDEBUG_H
#define NCZDEBUG_H

#undef DZDEBUG /* general debug */

#define DZCATCH /* Warning: significant performance impact */

extern int ncdzdebug;

#ifdef DZCATCH
/* Place breakpoint on dapbreakpoint to catch errors close to where they occur*/
#define THROW(e) dzthrow(e)
#define THROWCHK(e) (void)dzthrow(e)
extern int dzbreakpoint(int err);
extern int dzthrow(int err);
#else
#define THROW(e) (e)
#define THROWCHK(e)
#endif

#define ZLOG(level,msg,...) nclog(level,msg,__VA_ARGS__)
#define ZTRACE() nclog(NCLOGDBG,"trace: %s.%s",__FILE__,__func__)

extern char* nczprint_slice(NCZSlice slice);
extern char* nczprint_odom(NCZOdometer odom);
extern char* nczprint_projection(NCZProjection proj);
extern char* nczprint_sliceindex(NCZSliceIndex si);

#endif /*NCZDEBUG_H*/
