Thread-Safe Support in NetCDF-C
============================
<!-- double header is needed to workaround doxygen bug -->

# Thread-Safe Support in NetCDF-C

\tableofcontents

# Introduction {#threadsafe_introduction}

To date, the netcdf-c library has not supported
thread-safe use. However, this is one of the most
requested extensions to the library.

As of the current version, support for thread-safe access is now
provided.  The support is **HIGHLY EXPERIMENTAL** since it has
not been tested to any significant extent.

# Implementation {#threadsafe_impl}

The implementation is patterned after the HDF5 thread-safety support.
For those platforms that support it, *pthreads* is used,
and specifically the *pthread_mutex* functionality.
For Windows, the Windows CriticalSection functionality is used.

There is a single global mutex object that controls access to
the netcdf-c API functions. This means that all netcdf-c
operations are effectively serialized. As with HDF5, which also uses
a global mutex, this limits performance in a parallel environment.

The body of each netcdf-c API function in *netcdf.h* is
wrapped in calls to lock the global mutex on entrance and then
unlock it on exit. This required some modest code modifications
to ensure that unlocking always occurs at exit.

A critical feature of the netcdf thread-safe implementation is
that it uses recursive mutex objects.
Suppose a wrapped API function internally calls another wrapped API
function. In the simplest case, this would lead to a deadlock
since the outer function would have locked the mutex and
locking the second call would block waiting for the mutext to be released,
which would never happen.

When a recursive mutex attempts to lock an already locked
mutex, it examines the owner of the lock and if it is the same
thread as the current one -- the one attempting the lock --
then it just bumps a reference count and continues on.
Similarly for unlocking. The reference count is decremented
and if it is zero then the mutex is actually released.

# Known Issues
* Filters are implicitly locked because the nc_get/put_varX functions are locked.
* Any functionality that uses libcurl does not appear to work correctly at the moment. This means that S3 support, Byterange support and DAP support need
to be disabled if using the thread safe option.

# Initialization and Finalization {#threadsafe_init}

The global mutex is initialized when the *nc_initialize* API
function is invoked. Usually explicit calls to this function
are unnecessary since it is invoked implicitly when calling
*nc_open* or *nc_create*.

The situation with finalization is a bit more complex.  The
global mutex is finalized when the *nc_finalize* API function is
invoked. However, the user needs to call this function
explicitly because there is no way for the library to know when
the user is finished with the library. But fortunately calling
*nc_finalize* is usually not necessary. 

# Lock and Unlock API {#threadsafe_lockapi}

When adding a new netcdf-c API function, you will
need to wrap the body of the function int a lock
and unlock pair.

There are two relevant functions.
````
void NC_lock(void);
void NC_unlock(void);
````
These functions are wrapped in macros to allow adding
additional debug information.
````
#define NCLOCK NC_lock()
#define NCUNLOCK NC_unlock()
````
# API Example {#threadsafe_apiexample}

When adding new netcdf-c API functions, you will need to follow
this template example taken from *nc_sync*.
````
int nc_sync(int ncid)
{
    NC* ncp;
    int stat = NC_NOERR;
    NCLOCK;
    stat = NC_check_id(ncid, &ncp);
    if(stat != NC_NOERR) goto done;
    stat = ncp->dispatch->sync(ncid);
done:
    NCUNLOCK;
    return stat;
}
````
Note that the code needs to be set up so that NCLOCK is the evaluated
before any other function. Similarly with NCUNLOCK.
A common approach is to define a single exit via the label *done*.
This means that the body of the function never does a *return*,
but rather sets the status and does a *goto done*. 

Some optimizations are possible. If there are preliminary tests that
only operate on the arguments to the function, then those tests
can be performed before *NCLOCK* is invoked. This can avoid
some of the locking overhead.

# Extensions {#threadsafe_extend}

Eventually, it should be possible to convert
total API serialization into a finer grain of
locking so that a mutex is locked only when accessing
a shared object such as the open files vector.
This should significantly improve parallel performance.
Unfortunately, implementing this level of locking
is quite complex, so it is unlikely to be realized
any time soon.

# Configuration {#threadsafe_config}

Enabling thread-safety is controlled at build time
using options to *configure* (Automake) and
*cmake* (Cmake).

## Automake
The option to enable thread-safety is
````
--enable-threadsafe
````
It defaults to disabled.

## Cmake
The option to enable thread-safety is
````
-DENABLE-THREADSAFE=on
````
It defaults to disabled.

--------------
Point of Contact
--------------

__Author__: Dennis Heimbigner<br>
__Email__: dmh at ucar dot edu
__Initial Version__: 9/9/2022<br>
__Last Revised__: 9/9/2022<br>


