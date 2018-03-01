/* This file contains constants used in the framework. ************************/

#ifndef _ALL
#define _ALL

/* Coroutine related staff */
/* Using boost coroutine   */
#include<boost/coroutine/all.hpp>

typedef boost::coroutines::symmetric_coroutine<void>::call_type coroutine_func_t;
typedef boost::coroutines::symmetric_coroutine<void>::yield_type yield_func_t;


/***** hardware parameters ********/
#define CACHE_LINE_SZ 64                // cacheline size of x86 platform
#define MAX_MSG_SIZE  4096              // max msg size used

#define HUGE_PAGE_SZ (2 * 1024 * 1024)  // huge page size supported

#define MAX_SERVERS 16                  // maxium number of machines in the cluster
#define MAX_SERVER_TO_SENT MAX_SERVERS  // maxium broadcast number of one msg

/**********************************/


/* some usefull macros  ******************************************************/
#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x)   __builtin_expect(!!(x), 1)

#endif
