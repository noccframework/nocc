// compile config parameters

#ifndef NOCC_CONFIG_H_
#define NOCC_CONFIG_H_

#include "all.h"

#define MASTER_EPOCH 10 // listener's epoch

//**** debug parameters *******
//#define RPC_TIMEOUT_FLAG // whether to check RPC returned
#ifdef RPC_TIMEOUT_FLAG
#define RPC_TIMEOUT_TIME 10000000 // 2 second
#endif

//#define RPC_VERBOSE //whether to record the last RPC's info
//#define RPC_CHECKSUM // whether to do checksum on RPCs

//***************************//

#define USE_UD_MSG 1 // using ud msg


//**** print-parameters *****//
#define LOG_RESULTS // whether log results to a log file
#define LISTENER_PRINT_PERF 1 // whether print results to stdout
/* comment *******************************************************************/

/** per-thread performance counter **/
#define PER_THREAD_LOG 0
/* comment *******************************************************************/


// RDMA related configurations
#define SINGLE_MR 0
/* comment *******************************************************************/


/* Breakdown time statistics *************************************************/
#define POLL_CYCLES 1 // count the pull cycle
/* comment *******************************************************************/

#define HUGE_PAGE 1 // whether to enable huge page

#include "custom_config.h" // contains flags not regarding to the framework




/* Some sanity checks about the setting of flags to be consistent ************/
#if USE_UD_MSG == 1
#undef  MAX_MSG_SIZE
#define MAX_MSG_SIZE 4096 // UD can only support msg size up to 4K
#endif

#endif
