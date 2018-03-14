#ifndef NOCC_FRAMEWORK_CONFIG_H_
#define NOCC_FRAMEWORK_CONFIG_H_

#define MASTER_EPOCH 10 // runtime of test

// rdma related stuffs
#define HUGE_PAGE  1
#define USE_UD_MSG 1
#define SINGLE_MR  0
#define BUF_SIZE   20480 // RDMA buffer size registered

// rpc related stuffs
//#define RPC_TIMEOUT_FLAG
#define RPC_TIMEOUT_TIME 10000000
//#define RPC_VERBOSE
//#define RPC_CHECKSUM
#define MAX_INFLIGHT_REPLY 2048
#define MAX_INFLIGHT_REQS  768


// print statements
#define LOG_RESULTS           // log results to a file
#define LISTENER_PRINT_PERF 1 // print the results to the screen
#define PER_THREAD_LOG 0      // per thread detailed log
#define POLL_CYCLES    0      // print the polling cycle
#define CALCULATE_LAT  1
#define LATENCY 1

#endif
