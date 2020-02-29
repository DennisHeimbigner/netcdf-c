/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#ifndef ZTEST_H
#define ZTEST_H

/* Arguments from command line */
struct Options {
    int debug;
    const char* cmd;
};

struct Test {
    char* cmd;
    int (*test)(void);
};

extern char* url;
extern struct Options options;

#define NCCHECK(expr) nccheck((expr),__LINE__)

extern void nccheck(int stat, int line);
extern void makeurl(const char* file);
extern int setup(int argc, char** argv);
extern int findtest(const char* cmd, struct Test* tests, struct Test** thetest);

#endif /*ZTEST_H*/
