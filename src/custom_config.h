// Buffer related config
#define STORE_SIZE 8192      // unit: M
#define BUF_SIZE   20480     // unit: M
#define MAX_INFLIGHT_REPLY 2048
#define MAX_INFLIGHT_REQS  768

// for micro benchmark usage
#if 0
#define STORE_SIZE 256
#define BUF_SIZE   1024
#define MAX_INFLIGHT_REQS 16
#define MAX_INFLIGHT_REPLY 1024
#endif

#define COMMIT_NAIVE 0

#define ATOMIC_LOCK 0
#define ONE_WRITE 0 // use one-sided RDMA write to commit values
#define FASST 0 // merge read & lock req

/* comment *******************************************************************/

//**** log parameters *********
#define RAD_LOG 0 // whether to log timer log information
#define RPC_LOG 0
#define TEST_LOG 0
//***************************//

#define USE_LOGGER 1
#define LOGGER_USE_RPC 2
#define USE_BACKUP_STORE 1
#define USE_BACKUP_BENCH_WORKER 0

#define LONG_KEY 1 //whether to use long key, required in TPC-E

/* comment *******************************************************************/

/* TX execution flag  ********************************************************/
#define ONLY_EXE 0     // only exe phase
#define NO_ABORT 0     // does not retry if the TX is abort, and do the execution
#define NO_TS    0     // In SI, does no manipulate the TS using RDMA
#define OCC_RETRY      // OCC does not issue another round of checks
#define OCC_RO_CHECK 1 // whether do ro validation of OCC

#define PROFILE_RW_SET 0     // whether profile TX's read/write set
#define PROFILE_SERVER_NUM 0 // whether profile number of server accessed per TX

#define CALCULATE_LAT 1
#define LATENCY 1 // calculate the latency as a workload mix

#define NO_EXE_ABORT 1	// does not goto abort phase if db_worker's end() meets with conflict 


#define LOCAL_LOCK_USE_RDMA 0

#define QP_NUMS 5

/* comment *******************************************************************/

/* benchmark technique configured ********************************************/
#define ONE_SIDED     1   // using drtm store
#define CACHING       1   // drtm caching
/* comment *******************************************************************/








/* reset some setting according to specific implementations  ****************/
// Warning: Below shall not change!!

#if !defined(FARM) && !defined(DRTMR) // start FaRM related setting

#undef  CACHING
#define CACHING 0   // Only one-sided version requires caching
#endif

#if defined(FARM) || defined(DRTMR) // also make some checks
#undef  FASST
#define FASST 0     // Only RPC verison can merge read/lock request
#if ONE_SIDED == 0
#error FaRM require one-sided support!
#endif
#endif      // end FaRM related setting


#if FASST == 1
#undef NAIVE
#define NAIVE 0
#endif

#if ONE_SIDED == 0 // if one-sided operation is not enabled, then no caching
#undef ONE_WRITE
#undef CACHING

#define ONE_WRITE 0
#define CACHING   0

#endif

#if ATOMIC_LOCK == 1 	// if one-sided atomicity lock is used, one-write must be set
						// so that no local CAS is used and rdma atomic verbs can work
#undef ONE_WRITE

#define ONE_WRITE 1

#endif
/* comment *******************************************************************/
