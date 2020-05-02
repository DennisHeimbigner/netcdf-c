#include <stdio.h>
#include <stdlib.h>
#include <netcdf.h>


static size_t var_chunksizes[4] = {4, 4, 4, 4} ;
static unsigned int var_0_filterparams[1] = {9U} ;

void
check_err(const int stat, int line, const char* file, const char* func) {
    if (stat != NC_NOERR) {
        (void)fprintf(stderr,"line %d of %s.%s: %s\n", line, file, func, nc_strerror(stat));
        fflush(stderr);
        exit(1);
    }
}

#define CHECK_ERR(err) check_err(err, __LINE__, __FILE__, __func__)

int
main() {/* create bzip2.nc */

    int  stat;  /* return status */
    int  ncid;  /* netCDF id */

    /* group ids */
    int bzip2_grp;

    /* dimension ids */
    int dim0_dim;
    int dim1_dim;
    int dim2_dim;
    int dim3_dim;

    /* dimension lengths */
    size_t dim0_len = 4;
    size_t dim1_len = 4;
    size_t dim2_len = 4;
    size_t dim3_len = 4;

    /* variable ids */
    int var_id;

    /* rank (number of dimensions) for each variable */
#   define RANK_var 4

    /* variable shapes */
    int var_dims[RANK_var];

    /* enter define mode */
    stat = nc_create("bzip2.nc", NC_CLOBBER|NC_NETCDF4, &ncid);
    CHECK_ERR(stat);
    bzip2_grp = ncid;

    /* define dimensions */
    stat = nc_def_dim(bzip2_grp, "dim0", dim0_len, &dim0_dim);
    CHECK_ERR(stat);
    stat = nc_def_dim(bzip2_grp, "dim1", dim1_len, &dim1_dim);
    CHECK_ERR(stat);
    stat = nc_def_dim(bzip2_grp, "dim2", dim2_len, &dim2_dim);
    CHECK_ERR(stat);
    stat = nc_def_dim(bzip2_grp, "dim3", dim3_len, &dim3_dim);
    CHECK_ERR(stat);

    /* define variables */

    var_dims[0] = dim0_dim;
    var_dims[1] = dim1_dim;
    var_dims[2] = dim2_dim;
    var_dims[3] = dim3_dim;
    stat = nc_def_var(bzip2_grp, "var", NC_FLOAT, RANK_var, var_dims, &var_id);
    CHECK_ERR(stat);
    stat = nc_def_var_chunking(bzip2_grp, var_id, NC_CHUNKED, var_chunksizes);
    CHECK_ERR(stat);
    stat = nc_def_var_fill(bzip2_grp, var_id, NC_NOFILL, NULL);
    CHECK_ERR(stat);
    stat = nc_def_var_filter(bzip2_grp, var_id, 307, 1, var_0_filterparams);
    CHECK_ERR(stat);

    /* leave define mode */
    stat = nc_enddef (bzip2_grp);
    CHECK_ERR(stat);

    /* assign variable data */

    {
    float var_data[4] = {((float)0), ((float)1), ((float)2), ((float)3)} ;
    size_t var_startset[4] = {0, 0, 0, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)4), ((float)5), ((float)6), ((float)7)} ;
    size_t var_startset[4] = {0, 0, 1, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)8), ((float)9), ((float)10), ((float)11)} ;
    size_t var_startset[4] = {0, 0, 2, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)12), ((float)13), ((float)14), ((float)15)} ;
    size_t var_startset[4] = {0, 0, 3, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)16), ((float)17), ((float)18), ((float)19)} ;
    size_t var_startset[4] = {0, 1, 0, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)20), ((float)21), ((float)22), ((float)23)} ;
    size_t var_startset[4] = {0, 1, 1, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)24), ((float)25), ((float)26), ((float)27)} ;
    size_t var_startset[4] = {0, 1, 2, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)28), ((float)29), ((float)30), ((float)31)} ;
    size_t var_startset[4] = {0, 1, 3, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)32), ((float)33), ((float)34), ((float)35)} ;
    size_t var_startset[4] = {0, 2, 0, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)36), ((float)37), ((float)38), ((float)39)} ;
    size_t var_startset[4] = {0, 2, 1, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)40), ((float)41), ((float)42), ((float)43)} ;
    size_t var_startset[4] = {0, 2, 2, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)44), ((float)45), ((float)46), ((float)47)} ;
    size_t var_startset[4] = {0, 2, 3, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)48), ((float)49), ((float)50), ((float)51)} ;
    size_t var_startset[4] = {0, 3, 0, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)52), ((float)53), ((float)54), ((float)55)} ;
    size_t var_startset[4] = {0, 3, 1, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)56), ((float)57), ((float)58), ((float)59)} ;
    size_t var_startset[4] = {0, 3, 2, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)60), ((float)61), ((float)62), ((float)63)} ;
    size_t var_startset[4] = {0, 3, 3, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)64), ((float)65), ((float)66), ((float)67)} ;
    size_t var_startset[4] = {1, 0, 0, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)68), ((float)69), ((float)70), ((float)71)} ;
    size_t var_startset[4] = {1, 0, 1, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)72), ((float)73), ((float)74), ((float)75)} ;
    size_t var_startset[4] = {1, 0, 2, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)76), ((float)77), ((float)78), ((float)79)} ;
    size_t var_startset[4] = {1, 0, 3, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)80), ((float)81), ((float)82), ((float)83)} ;
    size_t var_startset[4] = {1, 1, 0, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)84), ((float)85), ((float)86), ((float)87)} ;
    size_t var_startset[4] = {1, 1, 1, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)88), ((float)89), ((float)90), ((float)91)} ;
    size_t var_startset[4] = {1, 1, 2, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)92), ((float)93), ((float)94), ((float)95)} ;
    size_t var_startset[4] = {1, 1, 3, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)96), ((float)97), ((float)98), ((float)99)} ;
    size_t var_startset[4] = {1, 2, 0, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)100), ((float)101), ((float)102), ((float)103)} ;
    size_t var_startset[4] = {1, 2, 1, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)104), ((float)105), ((float)106), ((float)107)} ;
    size_t var_startset[4] = {1, 2, 2, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)108), ((float)109), ((float)110), ((float)111)} ;
    size_t var_startset[4] = {1, 2, 3, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)112), ((float)113), ((float)114), ((float)115)} ;
    size_t var_startset[4] = {1, 3, 0, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)116), ((float)117), ((float)118), ((float)119)} ;
    size_t var_startset[4] = {1, 3, 1, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)120), ((float)121), ((float)122), ((float)123)} ;
    size_t var_startset[4] = {1, 3, 2, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)124), ((float)125), ((float)126), ((float)127)} ;
    size_t var_startset[4] = {1, 3, 3, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)128), ((float)129), ((float)130), ((float)131)} ;
    size_t var_startset[4] = {2, 0, 0, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)132), ((float)133), ((float)134), ((float)135)} ;
    size_t var_startset[4] = {2, 0, 1, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)136), ((float)137), ((float)138), ((float)139)} ;
    size_t var_startset[4] = {2, 0, 2, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)140), ((float)141), ((float)142), ((float)143)} ;
    size_t var_startset[4] = {2, 0, 3, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)144), ((float)145), ((float)146), ((float)147)} ;
    size_t var_startset[4] = {2, 1, 0, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)148), ((float)149), ((float)150), ((float)151)} ;
    size_t var_startset[4] = {2, 1, 1, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)152), ((float)153), ((float)154), ((float)155)} ;
    size_t var_startset[4] = {2, 1, 2, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)156), ((float)157), ((float)158), ((float)159)} ;
    size_t var_startset[4] = {2, 1, 3, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)160), ((float)161), ((float)162), ((float)163)} ;
    size_t var_startset[4] = {2, 2, 0, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)164), ((float)165), ((float)166), ((float)167)} ;
    size_t var_startset[4] = {2, 2, 1, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)168), ((float)169), ((float)170), ((float)171)} ;
    size_t var_startset[4] = {2, 2, 2, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)172), ((float)173), ((float)174), ((float)175)} ;
    size_t var_startset[4] = {2, 2, 3, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)176), ((float)177), ((float)178), ((float)179)} ;
    size_t var_startset[4] = {2, 3, 0, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)180), ((float)181), ((float)182), ((float)183)} ;
    size_t var_startset[4] = {2, 3, 1, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)184), ((float)185), ((float)186), ((float)187)} ;
    size_t var_startset[4] = {2, 3, 2, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)188), ((float)189), ((float)190), ((float)191)} ;
    size_t var_startset[4] = {2, 3, 3, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)192), ((float)193), ((float)194), ((float)195)} ;
    size_t var_startset[4] = {3, 0, 0, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)196), ((float)197), ((float)198), ((float)199)} ;
    size_t var_startset[4] = {3, 0, 1, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)200), ((float)201), ((float)202), ((float)203)} ;
    size_t var_startset[4] = {3, 0, 2, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)204), ((float)205), ((float)206), ((float)207)} ;
    size_t var_startset[4] = {3, 0, 3, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)208), ((float)209), ((float)210), ((float)211)} ;
    size_t var_startset[4] = {3, 1, 0, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)212), ((float)213), ((float)214), ((float)215)} ;
    size_t var_startset[4] = {3, 1, 1, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)216), ((float)217), ((float)218), ((float)219)} ;
    size_t var_startset[4] = {3, 1, 2, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)220), ((float)221), ((float)222), ((float)223)} ;
    size_t var_startset[4] = {3, 1, 3, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)224), ((float)225), ((float)226), ((float)227)} ;
    size_t var_startset[4] = {3, 2, 0, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)228), ((float)229), ((float)230), ((float)231)} ;
    size_t var_startset[4] = {3, 2, 1, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)232), ((float)233), ((float)234), ((float)235)} ;
    size_t var_startset[4] = {3, 2, 2, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)236), ((float)237), ((float)238), ((float)239)} ;
    size_t var_startset[4] = {3, 2, 3, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)240), ((float)241), ((float)242), ((float)243)} ;
    size_t var_startset[4] = {3, 3, 0, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)244), ((float)245), ((float)246), ((float)247)} ;
    size_t var_startset[4] = {3, 3, 1, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)248), ((float)249), ((float)250), ((float)251)} ;
    size_t var_startset[4] = {3, 3, 2, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    {
    float var_data[4] = {((float)252), ((float)253), ((float)254), ((float)255)} ;
    size_t var_startset[4] = {3, 3, 3, 0} ;
    size_t var_countset[4] = {1, 1, 1, 4};
    stat = nc_put_vara(bzip2_grp, var_id, var_startset, var_countset, var_data);
    CHECK_ERR(stat);
    }


    stat = nc_close(bzip2_grp);
    CHECK_ERR(stat);
    return 0;
}
