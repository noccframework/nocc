/* An rpc handler for distributed transactions */

#ifndef NOCC_OLTP_BENCH_WORKER_H
#define NOCC_OLTP_BENCH_WORKER_H

#include "all.h"
#include "config.h"
#include "./utils/macros.h"
#include "./utils/thread.h"
#include "./utils/spinbarrier.h"
#include "./utils/util.h"

#include "rpc.h"
#include "rdma_sched.h"
#include "routine.h"
#include "view_manager.h"

#include "db/db_logger.h"

#include "memstore/memdb.h"
#include "db/txs/tx_handler.h"
#include "db/db_statistics_helper.h"

#include <vector>
#include <string>
#include <stdint.h>

#define MAX_TX 16

using namespace rdmaio;
using namespace nocc::db;

namespace nocc {

  /* macid, cor_id, message, a yield function */
  typedef std::function<void(int,int,char *,yield_func_t &yield)> ro_func_t;

  extern __thread uint *next_coro_id_arr_;
  extern __thread TXHandler   **txs_;

  extern __thread coroutine_func_t *routines_;
  extern __thread int          *reply_counts_;
  extern __thread int          *pending_counts_;

  // global profile
  extern __thread TXProfile  *profile;
  extern __thread TXProfile   profiles[16];

  namespace oltp {

    extern View* my_view; // replication setting of the data

    extern std::string config_file_name;

    class BenchWorker;
    void ThreadLocalInit(BenchWorker *);

    /* Txn result return type */
    typedef std::pair<bool, double> txn_result_t;


    /* Registerered Txn execution function */
    typedef txn_result_t (*txn_fn_t)(BenchWorker *,yield_func_t &yield);
    struct workload_desc {
      workload_desc() {}
    workload_desc(const std::string &name, double frequency, txn_fn_t fn)
    : name(name), frequency(frequency), fn(fn)
      {
        ALWAYS_ASSERT(frequency > 0.0);
        ALWAYS_ASSERT(frequency <= 1.0);
      }
      std::string name;
      double frequency;
      txn_fn_t fn;
      util::Breakdown_Timer latency_timer; // calculate the latency for each TX
      Profile p; // per tx profile
    };

    typedef std::vector<workload_desc> workload_desc_vec_t;

    class BenchLoader;   /* For init loading database */
    class BenchWorker;   /* For executing transactions */
    class BenchListener; /* For reporting results */
    class BackupBenchWorker;

    /* Bench runner is used to bootstrap system */
    class BenchRunner {
    public:
      BenchRunner(std::string &config_file);
    BenchRunner() : barrier_a_(1), barrier_b_(1) {}
      std::vector<std::string> net_def_;

      /* warm up the rdma buffer, can be replaced by the application */
      void run();
    protected:
      virtual std::vector<BenchLoader *> make_loaders(int partition, MemDB *store = NULL) = 0;
      virtual std::vector<BenchWorker *> make_workers() = 0;
      virtual std::vector<BackupBenchWorker *> make_backup_workers() = 0;
      void    parse_config(std::string &config_file);

      /*   below 2 functions are used to init data structure
           The first is called before any RDMA connections are made, which is used ti init
           data structures resides on RDMA registered area.
           The second is called after RDMA connections are made, which is used to init global
           data structure that needs RDMA for communication
      */
      virtual void warmup_buffer(char *buffer) = 0;
      virtual void bootstrap_with_rdma(RdmaCtrl *r) = 0;

      // cache the remote addresses of the key-value store
      virtual void populate_cache() {}

      virtual void init_store(MemDB* &store) = 0;
      virtual void init_backup_store(MemDB* &store) = 0;
      virtual void init_put() = 0;

      spin_barrier barrier_a_;
      spin_barrier barrier_b_;
      MemDB *store_;
      MemDB *backup_stores_[MAX_BACKUP_NUM];
    private:
      BenchListener *listener_;
      SpinLock rdma_init_lock_;
      int8_t   init_worker_count_; /*used to for worker to notify qp creation doney*/
    };

    class BenchLoader : public ndb_thread {
    public:
      BenchLoader(unsigned long seed) ;
      void run();
    protected:
      virtual void load() = 0;
      util::fast_random random_generator_;
      int partition_;
    private:
      unsigned int worker_id_;
    };

    /* Main benchmark worker */
    class BenchWorker : public ndb_thread {
    public:
      uint64_t total_ops_;

      /* For statistics counts */
      size_t ntxn_commits_;
      size_t ntxn_aborts_;
      size_t ntxn_executed_;

      size_t ntxn_abort_ratio_;
      size_t ntxn_strict_counts_;
      size_t ntxn_remote_counts_;
      util::Breakdown_Timer latency_timer_;

      bool   running;
      bool   inited;

      RoutineMeta *routine_meta_;
      util::fast_random rand_generator_;

      /* methods */
      BenchWorker(unsigned worker_id,bool set_core,unsigned seed,uint64_t total_ops,
                  spin_barrier *barrier_a,spin_barrier *barrier_b,BenchRunner *context = NULL,
                  DBLogger *db_logger = NULL);
      void run();
      void create_qps();

      // simple wrapper to the underlying routine layer
      inline void context_transfer() {
        int next = routine_meta_->next_->id_;
        tx_      = txs_[next];
        cor_id_  = next;
        auto cur = routine_meta_;
        routine_meta_ = cur->next_;
      }

      inline void indirect_yield(yield_func_t& yield){
        // indirect yield will wipe this routine from the scheduler
        if(reply_counts_[cor_id_] == 0 && pending_counts_[cor_id_] == 0 ) {
          return; // FIXME! how to ensure no pending?
        }
        int next = routine_meta_->next_->id_;
        tx_      = txs_[next];
        cor_id_  = next;
        auto cur = routine_meta_;
        routine_meta_ = cur->next_;
        cur->yield_from_routine_list(yield);
      }

      //tempraraily solve indirect_yield bug for logger
      inline void indirect_must_yield(yield_func_t& yield){
        // indirect yield will wipe this routine from the scheduler
        int next = routine_meta_->next_->id_;
        // fprintf(stdout,"from %d indirect must yield to %d\n\n",routine_meta_->id_,next);
        tx_      = txs_[next];
        cor_id_  = next;
        auto cur = routine_meta_;
        routine_meta_ = cur->next_;
        cur->yield_from_routine_list(yield);
      }

      inline void yield_next(yield_func_t &yield) {
        // yield to the next routine
        int next = routine_meta_->next_->id_;
        // re-set next meta data
        tx_      = txs_[next];

        routine_meta_ = routine_meta_->next_;
        cor_id_  = next;
        routine_meta_->yield_to(yield);
      }

#if USE_LOGGER
      void  init_logger() {
        assert(db_logger_ == NULL);
        assert(cm_ != NULL);
#if LOGGER_USE_RPC == 0
        db_logger_ = new DBLogger(worker_id_,cm_,my_view,rdma_sched_);
#elif LOGGER_USE_RPC == 1
        db_logger_ = new DBLogger(worker_id_,cm_,my_view,rpc_handler_);
#elif LOGGER_USE_RPC == 2
        db_logger_ = new DBLogger(worker_id_,cm_,my_view,rdma_sched_,rpc_handler_);
#endif
        db_logger_->thread_local_init();
      };
#endif
      virtual ~BenchWorker();
      virtual workload_desc_vec_t get_workload() const = 0;
      virtual void register_callbacks() = 0;     /*register read-only callback*/
      virtual void check_consistency() {};
      virtual void thread_local_init() {};
      virtual void workload_report() {
#if POLL_CYCLES == 1
        if(rdma_sched_)
          rdma_sched_->report();
        if(msg_handler_)
          msg_handler_->report();
        if(routine_1_tx_)
          routine_1_tx_->report();
        REPORT(yield);
#endif
      };


      /* we shall init msg_handler first, then init rpc_handler with msg_handler at the
         start of run time.
      */
      Rpc *rpc_handler_;
      RdmaCtrl *cm_;
      RDMA_msg *msg_handler_;
      DBLogger *db_logger_;
      RDMA_sched *rdma_sched_;

      unsigned int worker_id_;
      volatile unsigned int cor_id_; /* identify which co-routine is executing */
      TXHandler *tx_;       /* current coroutine's tx handler */
      TXHandler *routine_1_tx_;

      LAT_VARS(yield);

    private:
      bool initilized_;
      /* Does bind the core */
      bool set_core_id_;
      spin_barrier *barrier_a_;
      spin_barrier *barrier_b_;
      BenchRunner  *context_;
    };

    class BenchReporter;
    /* Bench listener is used to monitor system wide performance */
    class BenchListener : public ndb_thread {
    public:
      Rpc *rpc_handler_;
      RDMA_msg *msg_handler_;

      BenchListener(const std::vector<BenchWorker *> *workers,BenchReporter *reporter);

      /* used to handler system exit */
      void sigint_handler(int);
      void ending();
      void run();

      double throughput;
      uint64_t aborts;
      uint64_t abort_ratio;

      int n_returned_;

      void get_result_rpc_handler(int id,int cid,char *msg,void *arg);
      void exit_rpc_handler(int id,int cid,char *msg,void *arg);
      void start_rpc_handler(int id,int cid,char *msg,void *arg);

    private:
      bool inited_; //whether all workers has inited
      BenchReporter *reporter_;
      const std::vector<BenchWorker *> *workers_;
      uint64_t epoch_;

      void thread_local_init();
    };

    class BenchReporter { // not thread safe
    public:
      virtual void init(const std::vector<BenchWorker *> *workers);
      virtual void merge_data(char *);
      virtual void report_data(uint64_t epoch,std::ofstream &log_file);
      virtual void collect_data(char *data,struct timespec &start_t); // the data is stored in *data
      virtual size_t data_len();

    private:
      double throughput;
      double aborts;
      double abort_ratio;

      std::vector<uint64_t> prev_commits_;
      std::vector<uint64_t> prev_aborts_;
      std::vector<uint64_t> prev_abort_ratio_;

      const std::vector<BenchWorker *> *workers_;

      // helper functions
      uint64_t calculate_commits(std::vector<uint64_t> &prevs);
      uint64_t calculate_aborts(std::vector<uint64_t> &prevs);
      double   calculate_abort_ratio(std::vector<uint64_t> &prevs);
      double   calculate_execute_ratio();
    };
  } // end namespace oltp
} // end namespace nocc

#endif
