/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef TST_UTILS__H
#define TST_UTILS_H

/* Define the prefix for our S3 bucket */
#define S3PREFIX "s3://stratus.ucar.edu/unidata-netcdf-zarr-testing"

struct Defaults {
    int formatx;
};

EXTERNL int getdefaultoptions(int* argcp, char*** argvp, struct Defaults* dfalts);
EXTERNL void cleardefaults(struct Defaults*);
EXTERNL int nc4_timeval_subtract (struct timeval *result, struct timeval *x, struct timeval*y);
EXTERNL int nc4_buildpath(const char* base, int formatx, char** pathp);
EXTERNL int string2formatx(const char* fmt);

#endif /*TST_UTILS_H*/










