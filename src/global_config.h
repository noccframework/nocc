// compile config parameters

#ifndef NOCC_CONFIG_H_
#define NOCC_CONFIG_H_

#include "all.h"

#define FASST 0           // FaSST style's TX commit
#define STORE_SIZE 8192   // The size of data store.(in G) per machine
#define QP_NUMS 5

// logger related configuration
#define USE_LOGGER 0
#define LOGGER_USE_RPC 2
#define USE_BACKUP_STORE 1
#define USE_BACKUP_BENCH_WORKER 0

// execuation related configuration
#define ONLY_EXE 0
#define NO_ABORT 0           // does not retry if the TX is abort, and do the execution
#define OCC_RETRY            // OCC does not issue another round of checks
#define OCC_RO_CHECK 1       // whether do ro validation of OCC

#define PROFILE_RW_SET 0     // whether profile TX's read/write set
#define PROFILE_SERVER_NUM 0 // whether profile number of server accessed per TX

// other stuffs
#define ONE_SIDED     1
#define CACHING       1

// end of global configuration
#endif
