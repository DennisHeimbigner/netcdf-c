/*********************************************************************
  *   Copyright 2018, UCAR/Unidata
  *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
  *********************************************************************/

/*
External functions and state variables are
defined here, including function-like #defines.
*/

#ifndef NCZFUNCS_H
#define NCZFUNCS_H 1

/**************************************************/
/* Constants */

#define RCFILEENV "NCZRCFILE"

/* Figure out a usable max path name max */
#ifdef PATH_MAX /* *nix* */
#define NC_MAX_PATH PATH_MAX
#else
#  ifdef MAX_PATH /*windows*/
#    define NC_MAX_PATH MAX_PATH
#  else
#    define NC_MAX_PATH 4096
#  endif
#endif

/**************************************************/

#ifndef nullfree
#define nullfree(m) ((m)==NULL?NULL:(free(m),NULL))
#endif

/**************************************************/

extern int NCZ_loadmetadata(NCZINFO*);


/**************************************************/

#endif /*NCZFUNC_H*/

