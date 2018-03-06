#include <mutex>
#include <boost/bind.hpp>

#include "routine.h"
#include "framework.h"

extern int    coroutine_num; // global config parameter

namespace nocc {

  // default routines to bind
  extern __thread coroutine_func_t *routines_;
  extern __thread BenchWorker* worker;

  namespace oltp {

    __thread std::queue<RoutineMeta *> *ro_pool;

    // add a routine chain to Rpc
    __thread RoutineMeta *next_routine_array;
    __thread RoutineMeta *routine_tailer;   // the header of the scheduler
    __thread RoutineMeta *routine_header;   // the tailer of the scheduler

    __thread RoutineMeta *one_shot_routine_pool;

    __thread one_shot_func_t *one_shot_callbacks;

    bool inited = false;
    std::mutex routine_mtx;

    void one_shot_func(yield_func_t &yield,RoutineMeta *routine) {

      while(1) {
        // callback
        //fprintf(stdout,"one shot\n");
        one_shot_callbacks[routine->info_.req_id](yield,routine->info_.from_mac,
                                                   routine->info_.from_routine,routine->info_.msg,
                                                   NULL);
        ro_pool->push(routine);
        free(routine->info_.msg);
        // yield back
        //fprintf(stdout,"one shot done\n");
        // need to reset-the context if necessary
        //routine->yield_from_routine_list(yield);
        worker->indirect_must_yield(yield);
      }
    }

    void RoutineMeta::thread_local_init(int num_ros) {
      // init next routine array
      next_routine_array = new RoutineMeta[coroutine_num + 1];

      for(uint i = 0;i < coroutine_num + 1;++i) {
        next_routine_array[i].id_   = i;
        next_routine_array[i].routine_ = routines_ + i;
      }

      for(uint i = 0;i < coroutine_num + 1;++i) {
        next_routine_array[i].prev_ = next_routine_array + i - 1;
        next_routine_array[i].next_ = next_routine_array + i + 1;
      }

      routine_header = &(next_routine_array[0]);
      routine_tailer = &(next_routine_array[coroutine_num]);

      // set master routine's status
      next_routine_array[0].prev_ = routine_header;

      // set tail routine's chain
      next_routine_array[coroutine_num].next_ = routine_header; //loop back

      // init ro routine pool
      ro_pool = new std::queue<RoutineMeta *>();
      for(uint i = 0;i < num_ros;++i) {
        RoutineMeta *meta = new RoutineMeta;
        meta->prev_ = NULL; meta->next_ = NULL; meta->id_ = coroutine_num + 1;// a magic number
        meta->routine_ = new coroutine_func_t(bind(one_shot_func,_1,meta));
        ro_pool->push(meta);
      }

      one_shot_callbacks = new one_shot_func_t[MAX_ONE_SHOT_CALLBACK];
    }

    void RoutineMeta::register_callback(one_shot_func_t callback,int id) {
      register_one_shot(callback,id);
    }

    void global_init() {
      // not used anymore
      assert(false);
    }

  };

};
