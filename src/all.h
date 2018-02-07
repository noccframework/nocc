#ifndef _ALL
#define _ALL

/* Coroutine related staff */
/* Using boost coroutine   */
#include<boost/coroutine/all.hpp>

#define MASTER_ROUTINE_ID 0
#define WORKER_NUM 1

#define RO_ROUTINE_NUM 1
#define RO_ROUTINE_ID  (1 + WORKER_NUM)

typedef boost::coroutines::symmetric_coroutine<void>::call_type coroutine_func_t;
typedef boost::coroutines::symmetric_coroutine<void>::yield_type yield_func_t;

#define CACHE_LINE_SZ 64
#define MAX_MSG_SIZE  4096
#define MAX_REMOTE_SET_SIZE (MAX_MSG_SIZE)

#define HUGE_PAGE_SZ (2 * 1024 * 1024)

//**** hardware parameters *******//
#define MAX_SERVERS 16
#define MAX_SERVER_TO_SENT 32


/* some usefull macros  ******************************************************/

#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x)   __builtin_expect(!!(x), 1)


#endif
