#include "util/util.h"
#include "util/spinlock.h"
#include "util/mapped_log.h"
#include "util/printer.h"

#include "db/txs/tx_handler.h"

#include "framework.h"
#include "routine.h"
#include "backup_worker.h"
#include "view_manager.h"
#include "req_buf_allocator.h"

// rdma related libs
#include "rdmaio.h"
#include "ralloc.h"
#include "ring_msg.h"
#include "ud_msg.h"

// For nop pause
#include "utils/amd64.h"

// System or library headers
#include <iostream>
#include <queue>
#include <unistd.h>
#include <stdio.h>
#include <signal.h>

#include <atomic>
#include <chrono> // for print current system time in a nicer way

#include <boost/bind.hpp>

using namespace std;
using namespace rdmaio;
using namespace rdmaio::ringmsg;

#define RESERVED_WORKERS 2
#define NOCC_BENCH_MAX_TX 16

#define NOCC_BENCH_MAX_CORO 16

/* global config constants */
size_t nthreads = 1;                      // total of worker in used
size_t coroutine_num = 1;                 // number of concurrent request per worker

size_t backup_nthreads = 0;                   // number of log cleaner
size_t scale_factor = 0;                  // the scale of the database
size_t total_partition = 1;               // total of machines in used
size_t current_partition = 0;             // current worker id

size_t distributed_ratio = 1; // the distributed transaction's ratio


// per-thread log handler
__thread MappedLog local_log;

namespace nocc {

  volatile bool running;
  __thread bool init = false;

  __thread oltp::BenchWorker *worker = NULL;
  __thread coroutine_func_t *routines_ = NULL;
  __thread TXHandler   **txs_ = NULL;

  __thread RPCMemAllocator *msg_buf_alloctors = NULL;

  __thread db::TXProfile  *profile;
  __thread db::TXProfile   profiles[16];

#ifdef RPC_TIMEOUT_FLAG
  __thread struct  timeval   *routine_timeouts_;
#endif

#ifdef RPC_VERBOSE
  extern __thread rpc_info *rpc_infos_;
#endif

  void sigint_handler(int) {
    // exit handler
    running = false;
    exit(0);
  }

  namespace oltp {
    //DZY drtmr
    View *my_view = NULL;

    std::atomic<int> live_counts(0);

    // used to generate benchmark's random numbers ////////////////////////////
    __thread util::fast_random *random_generator = NULL;

    // used to calculate benchmark information ////////////////////////////////
    __thread std::vector<size_t> *txn_counts = NULL;
    __thread std::vector<size_t> *txn_aborts = NULL;
    __thread std::vector<size_t> *txn_remote_counts = NULL;

    // used to calculate the latency of each workloads
    __thread workload_desc_vec_t *workloads;

    std::string config_file_name;

    SpinLock exit_lock;

    RdmaCtrl *cm = NULL;
    char *rdma_buffer = NULL;
    char *store_buffer = NULL; // the buffer used to store memstore
    char *free_buffer = NULL;  // the free RDMA space which can be use
    uint64_t r_buffer_size = 0;

    uint64_t total_ring_sz;
    uint64_t ring_padding;
    uint64_t ringsz;

    void new_master_routine( yield_func_t &yield, int cor_id, BenchWorker *context);
    void worker_routine( yield_func_t &yield, int cor_id, BenchWorker *context);

    void master_exit(BenchWorker *context);


    static void check_rdma_connectivity(int worker_id) {
      //
      std::vector<Qp*> qp_vec;
      for(uint i = 0;i < cm->get_num_nodes();++i) {
        qp_vec.push_back(cm->get_rc_qp(worker_id,i));
        assert(qp_vec[i] != NULL);
      }

      uint64_t *send_buffer = (uint64_t *)(Rmalloc(sizeof(uint64_t)));
      uint64_t *recv_buffer = (uint64_t *)(Rmalloc(sizeof(uint64_t)));

      for(uint i = 0;i < cm->get_num_nodes();++i) {

        uint64_t val = 73 + current_partition + i;
        uint64_t off = 0 + (current_partition + 1) * nthreads * sizeof(uint64_t);

        *send_buffer = val;

        auto ret = qp_vec[i]->rc_post_send(IBV_WR_RDMA_READ,(char *)recv_buffer,sizeof(uint64_t),
                                           0,
                                           IBV_SEND_SIGNALED);
        assert(ret == Qp::IO_SUCC);
        ret = qp_vec[i]->poll_completion();
        assert(ret == Qp::IO_SUCC);
        if(73 + i != *recv_buffer){ // make sure the destination is the destination
          fprintf(stdout,"failed val %lu, at prob %d\n",*recv_buffer,i);
          assert(false);
        }

        ret = qp_vec[i]->rc_post_send(IBV_WR_RDMA_WRITE,(char *)send_buffer,sizeof(uint64_t),
                                off + sizeof(uint64_t) * worker_id,
                                IBV_SEND_SIGNALED);
        assert(ret == Qp::IO_SUCC);
        ret = qp_vec[i]->poll_completion();
        assert(ret == Qp::IO_SUCC);

        ret = qp_vec[i]->rc_post_send(IBV_WR_RDMA_READ,(char *)recv_buffer,sizeof(uint64_t),
                                      off + sizeof(uint64_t) * worker_id,
                                      IBV_SEND_SIGNALED);
        assert(ret == Qp::IO_SUCC);
        ret = qp_vec[i]->poll_completion();
        assert(ret == Qp::IO_SUCC);

        if(*recv_buffer != val) {
          fprintf(stdout,"failed val %lu, required %lu @%d\n",*recv_buffer,val,worker_id);
          assert(false);
        }
        //fprintf(stdout,"check %d %d pass\n",worker_id,i);
      }
      Rfree((void *)send_buffer);
      Rfree((void *)recv_buffer);
    }

    void ThreadLocalInit (BenchWorker *context) {

      //    fprintf(stdout,"thread local init\n");
      if(!init) {
        worker = context;
        /* worker routines related stuff */
        routines_         = new coroutine_func_t[1 + coroutine_num];
        txs_              = new TXHandler*[1 + coroutine_num];
#if 1
        msg_buf_alloctors = new RPCMemAllocator[1 + coroutine_num];
#endif
        random_generator = new util::fast_random[ 1 + coroutine_num];

        for(uint i = 0;i < 1 + coroutine_num;++i){
          random_generator[i].set_seed0(context->rand_generator_.next());
        }

        routines_[0] = coroutine_func_t(bind(new_master_routine, _1, 0,context));
        for(uint i = 1;i <= coroutine_num;++i) {
          routines_[i] = coroutine_func_t(bind(worker_routine, _1, i,context));
        }

        txn_counts = new std::vector<size_t> ();
        txn_aborts = new std::vector<size_t> ();
        txn_remote_counts = new std::vector<size_t> ();

        for(uint i = 0;i < NOCC_BENCH_MAX_TX;++i) {
          txn_counts->push_back(0);
          txn_aborts->push_back(0);
          txn_remote_counts->push_back(0);
        }

        // init tx profile
        for(uint i = 0;i < 16;++i)
          profiles[i].reset();

        // init workloads
        workloads = new workload_desc_vec_t[coroutine_num + 1];
#if 0
        // init debug logger
        char log_path[16];
        sprintf(log_path,"./%lu_%u.log",current_partition,context->worker_id_);
        new_mapped_log(log_path, &local_log,4096);
#endif
        init = true;
      }
    }


    // A new event loop channel
    void  new_master_routine(yield_func_t &yield,int cor_id,BenchWorker *context) {

      auto routine_meta = get_routine_meta(MASTER_ROUTINE_ID);

      while( true ) {

        if(unlikely(!context->running)) {
          return master_exit(context);
        }

        // first we need to poll rpcs
#if USE_UD_MSG == 1
        context->msg_handler_->poll_comps();
#else
        context->rpc_handler_->poll_comps();
#endif
        context->rdma_sched_->poll_comps();

        auto next = routine_header->next_;
        //if(current_partition != 0) continue;
        if(next != routine_meta) {
          context->tx_ = txs_[next->id_];
          context->cor_id_ = next->id_;
          context->routine_meta_ = next;
          next->yield_to(yield);
        } else {

        }
        // end main worker forever loop
      }
    }

    void __attribute__((optimize("O1"))) // this flag is very tricky, it should be set this way
    worker_routine(yield_func_t &yield, int cor_id, BenchWorker *context) {

      using namespace db;
      /* worker routine that is used to run transactions */
      workloads[cor_id] = context->get_workload();
      auto &workload = workloads[cor_id];

      // Used for OCC retry
      unsigned int backoff_shifts = 0;
      unsigned long abort_seed = 73;

      while(abort_seed == random_generator[cor_id].get_seed()) {
        // avoids seed collision
        abort_seed += 1;
      }

      uint64_t retry_count(0);
      while(true) {

        /* select the workload */
        double d = random_generator[cor_id].next_uniform();

        uint tx_idx = 0;
        for(size_t i = 0;i < workload.size();++i) {
          if((i + 1) == workload.size() || d < workload[i].frequency) {
            tx_idx = i;
            break;
          }
          d -= workload[i].frequency;
        }

        const unsigned long old_seed = random_generator[cor_id].get_seed();
#if CALCULATE_LAT == 1
        if(cor_id == 1) {
          // only profile the latency for cor 1
#if LATENCY == 1
          context->latency_timer_.start();
#else
          (workload[tx_idx].latency_timer).start();
#endif
        }
#endif

      abort_retry:
        context->ntxn_executed_ += 1;

        auto ret = workload[tx_idx].fn(context,yield);
#if NO_ABORT == 1
        ret.first = true;
#endif
        // if(current_partition == 0){
        if(likely(ret.first)) {
          // commit case
#if CALCULATE_LAT == 1
          if(cor_id == 1) {
#if LATENCY == 1
            context->latency_timer_.end();
#else
            workload[tx_idx].latency_timer.end();
#endif
          }
#endif
          context->ntxn_commits_ += 1;
          (*txn_counts)[tx_idx] += 1;
#if PROFILE_RW_SET == 1 || PROFILE_SERVER_NUM == 1
          if(ret.second > 0)
            workload[tx_idx].p.process_rw(ret.second);
#endif
        } else {
          // abort case
          if(old_seed != abort_seed) {
            /* avoid too much calculation */
            context->ntxn_abort_ratio_ += 1;
            abort_seed = old_seed;
            (*txn_aborts)[tx_idx] += 1;
          }
          context->ntxn_aborts_ += 1;
          //fprintf(stdout,"return %d\n",cor_id);
          //yield(routines_[MASTER_ROUTINE_ID]);

          // yield as a back-off
          //yield_to(next_routine_array[cor_id].next,yield);
          context->yield_next(yield);

          // reset the old seed
          random_generator[cor_id].set_seed(old_seed);
          goto abort_retry;
        }
        //assert(false);
      // }
        context->yield_next(yield);
        // end worker main loop
      }
    }

    BenchWorker::BenchWorker (unsigned worker_id,bool set_core, unsigned seed, uint64_t total_ops,
                              spin_barrier *a,spin_barrier *b,BenchRunner *context, DBLogger *db_logger)
      :initilized_(false),
       worker_id_(worker_id),
       set_core_id_(set_core),
       ntxn_commits_(0),
       ntxn_aborts_(0),
       ntxn_executed_(0),
       ntxn_abort_ratio_(0),
       ntxn_remote_counts_(0),
       ntxn_strict_counts_(0),
       total_ops_(total_ops),
       rand_generator_(seed),
       barrier_a_(a),
       barrier_b_(b),
       running(false), // true flag of whether to start the worker
       inited(false),
       cor_id_(0),
       context_(context),
       db_logger_(db_logger),
       // r-set some local members
       rdma_sched_(NULL),
       msg_handler_(NULL),
       routine_1_tx_(NULL)
    {
      assert(cm != NULL);
      cm_ = cm;
      assert(context != NULL);

      INIT_LAT_VARS(yield);
    }

    BenchWorker::~BenchWorker() { /* TODO*/ }


    void BenchWorker::create_qps() {
      // FIXME: hard code dev id and port id
      //int use_port = worker_id_ % 2;
      int use_port = 1;
#if 1
      if(worker_id_ >= util::CorePerSocket()) {
        use_port = 0;
      }
#endif
      int dev_id = cm->get_active_dev(use_port);
      int port_idx = cm->get_active_port(use_port);

      Debugger::debug_fprintf(stdout,"[Bench worker %d] create qps at dev %d, port %d\n",worker_id_,
                              dev_id,port_idx);

      cm->thread_local_init();
      cm->open_device(dev_id);
      cm->register_connect_mr(dev_id); // register memory on the specific device
#if 1
      for(uint i = 0; i < QP_NUMS; i++){
        cm-> // create qp for one_sided RDMA which logging and Farm use
          link_connect_qps(worker_id_, dev_id, port_idx, i, IBV_QPT_RC);
      }
#endif
      //cm-> // create qp for SI's timestamp management
      //        link_connect_qps(worker_id_ + nthreads + 2, dev_id_2, port_idx_2, 0, IBV_QPT_RC);
      //cm-> // normal usage QP for RC based RDMA messaging
      //        link_connect_qps(worker_id_, dev_id, port_idx, 0, IBV_QPT_RC);

#if USE_UD_MSG == 0
      cm-> // normal usage QP for RC based RDMA messaging
        link_connect_qps(worker_id_, dev_id, port_idx, 0, IBV_QPT_RC);
#elif USE_UD_MSG == 1
      rdmaio::udmsg::bootstrap_ud_qps(cm,worker_id_,dev_id,port_idx,1);
#endif // USE_UD_MSG

      //fprintf(stdout,"[WORKER %d] create QPs done\n",worker_id_);
    }

    void BenchWorker::run() {

      // bind the core to improve the performance
      BindToCore(worker_id_); // really specified to platforms
      //this->binding(worker_id_);

      /* init ralloc */
      RThreadLocalInit();

      auto now = std::chrono::system_clock::now();
      auto now_c = std::chrono::system_clock::to_time_t(now);

      std::stringstream time_buffer;
      time_buffer << std::put_time(std::localtime(&now_c), "%c");

      //      Debugger::debug_fprintf(stdout,"[Bench worker %d] create qps at %s\n",worker_id_,
      //time_buffer.str().c_str());
      // clear the time buffer
      time_buffer.str(std::string());

      /* create set of qps */
      create_qps();

      // init ring message after QP has been created
      if(cm != NULL) {
#if USE_UD_MSG == 1
        using namespace rdmaio::udmsg;

        rpc_handler_ = new Rpc(NULL,worker_id_);
        //pay attention to UD_MAX_RECV_SIZE and MAX_RECV_SIZE
        msg_handler_ = new UDMsg(cm,worker_id_,
                                 2048, // for application to use
                                 //64,        // for micro benchmarks
                                 std::bind(&Rpc::poll_comp_callback,rpc_handler_,
                                           std::placeholders::_1,
                                           std::placeholders::_2));

        rpc_handler_->message_handler_ = msg_handler_; // reset the msg handler
#else
        msg_handler_ = new RingMessage(ringsz,ring_padding,worker_id_,cm,rdma_buffer + HUGE_PAGE_SZ);
        rpc_handler_ = new Rpc(msg_handler_,worker_id_);
#endif
      } else {
        assert(false); // no RDMA connection
      }

      // init RDMA scheduler
      rdma_sched_ = new RDMA_sched();

#if USE_LOGGER
      this->init_logger();
#endif

      /* init routines */
      ThreadLocalInit(this);
      this->thread_local_init();
      rdma_sched_->thread_local_init();

      // init local scheduling layer
      RoutineMeta::thread_local_init(256); // the pool has 128 routines

      register_callbacks();

      // init rpc meta data
      rpc_handler_->init();
      rpc_handler_->thread_local_init();


      now = std::chrono::system_clock::now();
      now_c = std::chrono::system_clock::to_time_t(now);
      time_buffer << std::put_time(std::localtime(&now_c), "%c");

      Debugger::debug_fprintf(stdout, "[Bench worker %d] started at %s\n",worker_id_,
              time_buffer.str().c_str());
      // clear the time buffer
      time_buffer.str(std::string());

      this->inited = true;
      while(!this->running) {
        asm volatile("" ::: "memory");
      }
      //check_rdma_connectivity(worker_id_);
      routines_[0]();
    }

    /* Abstract bench loader */
    BenchLoader::BenchLoader(unsigned long seed)
      : random_generator_(seed) {
      worker_id_ = 0; /**/
    }

    void BenchLoader::run() {
      load();
    }

    /* Abstract bench runner */
    BenchRunner::BenchRunner(std::string &config_file)
      : barrier_a_(1),barrier_b_(1),init_worker_count_(0),store_(NULL)
    {
      running = true;

      my_view = new View(config_file);
#if USE_LOGGER
      my_view->print_view();
#endif
      for(int i = 0; i < MAX_BACKUP_NUM; i++){
        backup_stores_[i] = NULL;
      }

      parse_config(config_file);

      /* reset the barrier number */
      barrier_a_.n = nthreads;
    }

    void
    BenchRunner::run() {
#if USE_LOGGER
      Debugger::debug_fprintf(stdout, "Logger enabled, RPC style: %d\n",LOGGER_USE_RPC);
#else
      Debugger::debug_fprintf(stdout, "Logger disabled\n");
#endif

#if USE_BACKUP_STORE
      Debugger::debug_fprintf(stdout, "Backup Store enabled, using %lu backup_threads\n", backup_nthreads);
#else
      Debugger::debug_fprintf(stdout, "Backup Store disabled\n");
#endif
      // init RDMA, basic parameters
      // DZY:do no forget to allow enough hugepages
      uint64_t M = 1024 * 1024;
      r_buffer_size = M * BUF_SIZE;

      int r_port = 3333;

      rdma_buffer = (char *)malloc_huge_pages(r_buffer_size,HUGE_PAGE_SZ,1);
      assert(rdma_buffer != NULL);

      // start creating RDMA
#if SINGLE_MR == 1
      cm = new RdmaCtrl(current_partition,net_def_,r_port,true);
      cm->set_connect_mr(rdma_buffer,r_buffer_size); // register the buffer
      cm->open_device();//single
      cm->register_connect_mr();//single
#else
      cm = new RdmaCtrl(current_partition,net_def_,r_port,false);
      cm->set_connect_mr(rdma_buffer,r_buffer_size); // register the buffer
#endif

      memset(rdma_buffer,0,r_buffer_size);
      uint64_t M2 = HUGE_PAGE_SZ;
#if USE_UD_MSG == 0
      // Calculating message size
      ring_padding = MAX_MSG_SIZE;
      //total_ring_sz = MAX_MSG_SIZE * coroutine_num * 32 + ring_padding + MSG_META_SZ; // used for applications
      total_ring_sz = MAX_MSG_SIZE * coroutine_num * 4 + ring_padding + MSG_META_SZ;    // used for micro benchmarks
      // round the total ring size to 2M, since 2M is the default huge page size
      //total_ring_sz = total_ring_sz + M2 - total_ring_sz % M2;

      assert(total_ring_sz < r_buffer_size);

      ringsz = total_ring_sz - ring_padding - MSG_META_SZ;

      uint64_t ring_area_sz = (total_ring_sz * net_def_.size()) * (nthreads);
      fprintf(stdout,"[Mem], Total msg buf area:  %fG\n",get_memory_size_g(ring_area_sz));
#else
      uint64_t ring_area_sz = 0;
#endif
      //      test_function();
      // Calculating logger memory size
#if USE_LOGGER == 1
      uint64_t logger_sz = DBLogger::get_memory_size(nthreads, net_def_.size());
      logger_sz = logger_sz + M2 - logger_sz % M2;
#else
      uint64_t logger_sz = 0;
#endif
      // Set logger's global offset
      DBLogger::set_base_offset(ring_area_sz + M2);
      fprintf(stdout,"[Mem], Total logger area %fG\n",get_memory_size_g(logger_sz));

      uint64_t total_sz = logger_sz + ring_area_sz + M2;
      assert(r_buffer_size > total_sz);

      uint64_t store_size = 0;
#if ONE_SIDED == 1
      if(1){
        store_size = STORE_SIZE * M;
        store_buffer = rdma_buffer + total_sz;
      }
#endif
      total_sz += store_size;

      // Init rmalloc
      free_buffer = rdma_buffer + total_sz; // use the free buffer as the local RDMA heap
      uint64_t real_alloced = RInit(free_buffer, r_buffer_size - total_sz);
      assert(real_alloced != 0);
      fprintf(stdout,"[Mem], Real rdma alloced %fG\n",get_memory_size_g(real_alloced));

      RThreadLocalInit();

      warmup_buffer(rdma_buffer);

      cm->start_server(); // listening server for receive QP connection requests
      bootstrap_with_rdma(cm);


#if SINGLE_MR == 1
      for (int i = 0; i < nthreads + 8; i++) {
        
#if USE_UD_MSG == 0
        cm-> // normal usage QP for RC based RDMA messaging
          link_connect_qps(i, 0, 1, 0, IBV_QPT_RC);
#elif USE_UD_MSG == 1
        rdmaio::udmsg::bootstrap_ud_qps(cm,i,0,1,1);
#endif // end USE_UD_MSG

        cm-> // create qp for one_sided RDMA which logging and Farm use
          link_connect_qps(i, 0, 1, 1, IBV_QPT_RC);
        cm-> // create qp for SI's timestamp management
          link_connect_qps(i + nthreads + 2, 0, 1, 0, IBV_QPT_RC);
      }
      fprintf(stdout,"Single MR create all QPs done\n");

#endif //end SINGLE_MR
      if(cm == NULL && net_def_.size() != 1) {
        fprintf(stdout,"Distributed transactions needs RDMA support!\n");
        exit(-1);
      }

#if 0
      //#ifdef SI
      if(current_partition == total_partition) {
        while(1) sleep(1);
      }
#endif

      int num_primaries = my_view->is_primary(current_partition);
      int backups[MAX_BACKUP_NUM];
      int num_backups = my_view->is_backup(current_partition,backups);
      Debugger::debug_fprintf(stdout, "num_primaries: %d, num_backups: %d\n", num_primaries, num_backups);

      /* loading database */
      init_store(store_);
      const vector<BenchLoader *> loaders = make_loaders(current_partition);
      {
        const pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();
        {
          //  scoped_timer t("dataloading", verbose);
          for (vector<BenchLoader *>::const_iterator it = loaders.begin();
               it != loaders.end(); ++it) {
            (*it)->start();
          }
          for (vector<BenchLoader *>::const_iterator it = loaders.begin();
               it != loaders.end(); ++it)
            (*it)->join();
        }
        const pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
        const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
        const double delta_mb = double(delta)/1048576.0;
        cerr << "[Runner] DB size: " << delta_mb << " MB" << endl;
      }
      init_put();

#if ONE_SIDED == 1
      {
        // fetch if possible the cached entries from remote servers
        auto mem_info_before = get_system_memory_info();
        populate_cache();
        auto mem_info_after = get_system_memory_info();
        const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
        const double delta_mb = double(delta)/1048576.0;
        cerr << "[Runner] Cache size: " << delta_mb << " MB" << endl;
      }
#endif

#if USE_BACKUP_STORE
      for(int i = 0; i < num_backups; i++){
        assert(i < MAX_BACKUP_NUM);
        init_backup_store(backup_stores_[i]);
        const vector<BenchLoader *> loaders = make_loaders(backups[i],backup_stores_[i]);
        {
          const pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();
          {
            //  scoped_timer t("dataloading", verbose);
            for (vector<BenchLoader *>::const_iterator it = loaders.begin();
              it != loaders.end(); ++it) {
                (*it)->start();
              }
            for (vector<BenchLoader *>::const_iterator it = loaders.begin();
              it != loaders.end(); ++it) {
                (*it)->join();
              }
          }
          const pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
          const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
          const double delta_mb = double(delta)/1048576.0;
          cerr << "[Runner] Backup DB[" << i << "] size: " << delta_mb << " MB" << endl;
        }
      }
      if(num_backups > 0){
        const vector<BackupBenchWorker *> backup_workers = make_backup_workers();

        for (vector<BackupBenchWorker *>::const_iterator it = backup_workers.begin();
             it != backup_workers.end(); ++it){
          (*it)->start();
        }
      }
#endif

      const pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();
      const vector<BenchWorker *> workers = make_workers();

      for (vector<BenchWorker *>::const_iterator it = workers.begin();
           it != workers.end(); ++it){
        (*it)->start();
      }


      //    barrier_a_.wait_for();
      util::timer t;
      //    barrier_b_.count_down();

#if PER_THREAD_LOG == 0
      listener_ = new BenchListener(&workers,new BenchReporter());
      listener_->start();
#else
      assert(false); // current not implemented yet
      signal(SIGINT,sigint_handler);
#endif

      while(true) {
        // end forever loop, since the bootstrap has done
        sleep(1);
      }

      uint64_t total_executed(0), total_aborted(0);
      /* Maybe we need better calculations */
      for(auto it = workers.begin();
          it != workers.end();++it) {
        volatile uint64_t committed = (*it)->ntxn_commits_;
        while(  committed < (*it)->total_ops_ ) {
          asm volatile("" ::: "memory");
          committed = (*it)->ntxn_commits_;
        }
        (*it)->rpc_handler_->report();
        (*it)->check_consistency();
        total_executed += (*it)->ntxn_commits_;
        total_aborted  += (*it)->ntxn_aborts_;
      }

      const unsigned long elapsed = t.lap(); // lap() must come after do_txn_finish(),
      const double elapsed_sec = double(elapsed) / 1000000.0;

      const pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
      const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
      const double delta_mb = double(delta)/1048576.0;
      const double agg_abort_rate = double(total_aborted) / elapsed_sec;

      cerr << "--- benchmark statistics ---" << endl;
      cerr << "runtime: " << elapsed_sec << " sec" << endl;
      cerr << "memory delta: " << delta_mb  << " MB" << endl;
      cerr << "agg_abort_rate: " << agg_abort_rate << " aborts/sec" << endl;
      cerr << "total throughput: " << (double) total_executed / elapsed_sec << " ops/sec" << endl;

      fprintf(stdout,"[RUNNER] main run ends\n");
      /* end main run */
    }

    void master_exit(BenchWorker *context) {
      // check for exit
      // fprintf(stdout,"worker %d receive exit request\n",context->worker_id_);
      if(current_partition == 0 && context->worker_id_ == 0){

        // only sample a few worker information
        auto &workload = workloads[1];

        auto second_cycle = Breakdown_Timer::get_one_second_cycle();

        exit_lock.Lock();
        fprintf(stdout,"aborts: ");
        workload[0].latency_timer.calculate_detailed();
        fprintf(stdout,"%s ratio: %f ,executed %lu, latency %f, rw_size %f, m %f, 90 %f, 99 %f\n",
                workload[0].name.c_str(),
                (double)((*txn_aborts)[0]) / ((*txn_counts)[0] + ((*txn_counts)[0] == 0)),
                (*txn_counts)[0],workload[0].latency_timer.report() / second_cycle * 1000,
                workload[0].p.report(),
                workload[0].latency_timer.report_medium() / second_cycle * 1000,
                workload[0].latency_timer.report_90() / second_cycle * 1000,
                workload[0].latency_timer.report_99() / second_cycle * 1000);

        for(uint i = 1;i < workload.size();++i) {
          workload[i].latency_timer.calculate_detailed();
          fprintf(stdout,"        %s ratio: %f ,executed %lu, latency: %f, rw_size %f, m %f, 90 %f, 99 %f\n",
                  workload[i].name.c_str(),
                  (double)((*txn_aborts)[i]) / ((*txn_counts)[i] + ((*txn_counts)[i] == 0)),
                  (*txn_counts)[i],
                  workload[i].latency_timer.report() / second_cycle * 1000,
                  workload[i].p.report(),
                  workload[i].latency_timer.report_medium() / second_cycle * 1000,
                  workload[i].latency_timer.report_90() / second_cycle * 1000,
                  workload[i].latency_timer.report_99() / second_cycle * 1000);
        }
        fprintf(stdout,"\n");

        fprintf(stdout,"total: ");
        for(uint i = 0;i < workload.size();++i) {
          fprintf(stdout," %d %lu; ",i, (*txn_counts)[i]);
        }
        fprintf(stdout,"succs ratio %f\n",(double)(context->ntxn_executed_) /
                (double)(context->ntxn_commits_));

        context->check_consistency();

        exit_lock.Unlock();

        fprintf(stdout,"master routine exit...\n");
      }
      return;
    }

    struct thread_config {
      int node_id;
      int id;
    };

    inline vector<Qp *> bootstrap_rc_qps(RdmaCtrl *cm,int tid,int dev_id,int port_idx) {
      vector<Qp *> ret;
      cm->link_connect_qps(tid,dev_id,port_idx,0,IBV_QPT_RC);
      for(uint n_id = 0;n_id < cm->get_num_nodes(); n_id++) {
        ret.push_back(cm->get_rc_qp(tid,n_id));
      }
      return ret;
    }

  }// end namespace oltp
} // end namespace nocc
