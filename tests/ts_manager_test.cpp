#include "ts_manager.h"

#include <stdio.h> 
#include <assert.h>
#include <unistd.h>

#define THREAD_ID_OFFSET (54)
#define TID_TS_OFFSET (0)
#define SECOND_OFFSET (22)
#define SECOND_MASK (0xffffffff)

/* The counter is carefully choosn so that increasing it will not overlap second */
#define COUNTER_MASK (0x3fffff)
#define THREAD_ID_MASK (0x3ff)
#define TIMESTAMP_MASK (0x3fffffffffffff)

#define GEN_TIME(sec,counter) (  ((sec) << SECOND_OFFSET) | (counter) )
#define GEN_TID(tid,time) ( ((tid << THREAD_ID_OFFSET ) | (time) ) )
#define GET_TIMESTAMP(tid) ( (tid) & TIMESTAMP_MASK)
#define GET_TID(tid) ( (tid) >> THREAD_ID_OFFSET)
#define GET_SEC(tid) ( ((tid) & TIMESTAMP_MASK) >> SECOND_OFFSET)


int main() {

  TimestampManager tm;
  while(!tm.ready()) {
    
  }

  for(uint64_t i = 0;i < 1024;++i) {
    for(uint64_t tid = 12;tid < 73;++tid) {
      uint64_t counter = 0x1234;
      uint64_t tx = GEN_TID(tid, GEN_TIME(i,counter));
      //      fprintf(stdout,"%x %x get time %x, gen time %x\n",12 << 48,tx,GET_TIMESTAMP(tx),GEN_TIME(i,counter));
      assert(GET_TIMESTAMP(tx) == GEN_TIME(i,counter));
      assert(GET_TID(tx) == tid);
      assert(GET_SEC(tx) == i);
    }
  }

  uint64_t local_seconds = global_seconds;

  for(int i = 0;i < 10;++i) {
    sleep(1);
    assert(local_seconds < global_seconds);
    local_seconds = global_seconds;
    fprintf(stdout,"local %lu\n",local_seconds);
  }

  fprintf(stdout,"done\n");

  
  return 0;
}
