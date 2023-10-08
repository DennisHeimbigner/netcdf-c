/* This is part of the netCDF package.
   Copyright 2018 University Corporation for Atmospheric Research/Unidata
   See COPYRIGHT file for conditions of use.
*/

#include <nc_tests.h>
#include "err_macros.h"

#define FILE_NAME "tst_cmpd.h5"
#define COMPOUND_NAME "Struct"
#define FIELD_NAME1 "i1"
#define FIELD_NAME2 "i2"
#define DIM1_LEN 1
#define ARRAY_LEN 16384
//#define ARRAY_LEN 64
#define VAR_NAME "v"

struct s2 {
     int i1;
     int i2[ARRAY_LEN];
};

static const int field_offset1 = NC_COMPOUND_OFFSET(struct s2, i1);
static const int field_offset2 = NC_COMPOUND_OFFSET(struct s2, i2);


static int
report(int stat)
{
    fprintf(stderr,"err = (%d) %s\n",stat,nc_strerror(stat));
    ERR;
}
#define ERRR return report(stat)

int
main()
{
   int stat = NC_NOERR;
   int ncid, varid, tid;
   size_t dims[1];
   int dimids[1];
   int fdims[1];
   struct s2 data2[DIM1_LEN];
   char *dummy;
   int i, j;
   size_t chunks[1];

   /* REALLY initialize the data (even the gaps in the structs). This
    * is only needed to pass valgrind. */
   if (!(dummy = calloc(sizeof(struct s2), DIM1_LEN))) ERRR;
   memcpy((void *)data2, (void *)dummy, sizeof(struct s2) * DIM1_LEN);
   free(dummy);

   for (i=0; i<DIM1_LEN; i++)
   {
      data2[i].i1 = -99;
      for (j=0; j<ARRAY_LEN; j++)
         data2[i].i2[j] = 99;
  }

   printf("*** Checking compound type which contains an array...");

   {
      /* Open file and create group. */
      if((stat = nc_create(FILE_NAME, NC_NETCDF4|NC_CLOBBER, &ncid))) ERRR;

      /* Create a compound type containing an array. */
      if((stat = nc_def_compound(ncid,sizeof(struct s2), COMPOUND_NAME, &tid))) ERRR;
      if((stat = nc_insert_compound(ncid,tid,FIELD_NAME1,field_offset1,NC_INT))) ERRR;
      fdims[0] = ARRAY_LEN;
      if((stat = nc_insert_array_compound(ncid,tid,FIELD_NAME2,field_offset2,NC_INT,1,fdims))) ERRR;

      /* Create a dimension */
      dims[0] = DIM1_LEN;
      if((stat = nc_def_dim(ncid,"d1",dims[0],&dimids[0]))) ERRR;

      /* Create a variable of this compound type. */
      if((stat = nc_def_var(ncid, VAR_NAME, tid, 1, dimids, &varid))) ERRR;

      /* Set the file storage */
      chunks[0] = DIM1_LEN; if((stat = nc_def_var_chunking(ncid,varid,NC_CHUNKED,chunks))) ERRR;
//      if((stat = nc_def_var_chunking(ncid,varid,NC_CONTIGUOUS,NULL))) ERRR;

      if((stat = nc_close(ncid))) {fprintf(stderr,"err = (%d) %s\n",stat,nc_strerror(stat)); ERRR;}

#if 0
      /* Now open the file and read it. */
      if((ncid = H5Fopen(FILE_NAME, H5F_ACC_RDONLY, H5P_DEFAULT)) < 0) ERRR;
      if((osmonds_grpid = H5Gopen1(ncid, OSMONDS)) < 0) ERRR;
      if((datasetid = H5Dopen1(osmonds_grpid, BOOZE_VAR)) < 0) ERRR;
      if((typeid = H5Dget_type(datasetid)) < 0) ERRR;
      if(H5Tget_class(typeid) != H5T_COMPOUND) ERRR;
      if(H5Tget_nmembers(typeid) != 2) ERRR;
      /* This doesn't work because all I have is a reference to the type!
         if(H5Iget_name(typeid, type_name, STR_LEN) < 0) ERRR;
         if(strcmp(type_name, COMPOUND_NAME)) ERRR;*/

      /* Release all resources. */
      if(H5Dclose(datasetid) < 0 ||
          H5Tclose(typeid) < 0 ||
          H5Gclose(osmonds_grpid) < 0 ||
          H5Fclose(ncid) < 0) ERRR;
#endif
   }

   SUMMARIZE_ERR;
   FINAL_RESULTS;
}
