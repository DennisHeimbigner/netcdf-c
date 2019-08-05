/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file This header file contains definitions
 * for the (nc)zarr file format.
 *
 * @author Dennis Heimbigner
*/

#ifndef ZMETA_H
#define ZMETA_H

#define ZMETAROOT "/.zarr"
#define ZGROUPSUFFIX ".zgroup"

extern int NCZ_open_dataset(NCZ_FILE_INFO* zinfo);
extern int NCZ_create_dataset(NCZ_FILE_INFO* zinfo);

#endif /*ZMETA_H*/

