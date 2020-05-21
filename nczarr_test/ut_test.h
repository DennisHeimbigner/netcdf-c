/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#ifndef ZTEST_H
#define ZTEST_H

/* Arguments from command line */
struct Options {
    int debug;
    char** cmds;
    char* file;
    char* output;
    char* kind;
    NCZChunkRange ranges[NC_MAX_VAR_DIMS];
    int nslices;
    NCZSlice slices[NC_MAX_VAR_DIMS];
    NClist* dimdefs; /*List<struct Dimdef*> */
    NClist* vardefs; /*List<struct Vardef*> */
    size_t idatalen;
    int* idata;
};

struct Test {
    char* cmd;
    int (*test)(void);
};

extern struct Options options;

#define NCCHECK(expr) nccheck((expr),__LINE__)

extern int ut_init(int argc, char** argv, struct Options* test);
extern void usage(int err);
extern int ut_typesize(nc_type t);
extern int ut_typeforname(const char* tname);

extern void nccheck(int stat, int line);
extern char* makeurl(const char* file,NCZM_IMPL);
//extern int setup(int argc, char** argv);
extern struct Test* findtest(const char* cmd, struct Test* tests);
extern void usage(int);
extern NCZM_IMPL kind2impl(const char* kind);
extern const char* impl2kind(NCZM_IMPL impl);
extern int runtests(const char** cmds, struct Test* tests);
extern size64_t computelinearoffset(int R, const size64_t* indices, const size64_t* maxlen, size64_t* productp);
extern void slices2vector(int rank, NCZSlice* slices, size64_t** startp, size64_t** stopp, size64_t** stridep, size64_t** maxp);
extern void printoptions(struct Options*);

#endif /*ZTEST_H*/
