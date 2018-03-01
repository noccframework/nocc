#include "framework.h"

#include <signal.h>
#include <vector>
#include <boost/bind.hpp>
#include <boost/algorithm/string.hpp>

// RDMA related
#include "ralloc.h"
#include "ring_msg.h"
#include "ud_msg.h"

#include "rpc.h"
#include "routine.h"

#include "util/util.h"
#include "util/printer.h"

/* bench listener is used to monitor the various performance field of the current system */

/* global config constants */
extern size_t nthreads;
extern size_t current_partition;
extern size_t total_partition;
extern size_t coroutine_num;
extern size_t distributed_ratio;

extern std::string exe_name;
extern std::string bench_type;

#define RPC_REPORT 1
#define RPC_EXIT   2
#define RPC_START  3

#ifdef LOG_RESULTS
#include <fstream>
std::ofstream log_file;
#endif


using namespace rdmaio;
using namespace rdmaio::ringmsg;
using namespace nocc::util;

#undef  USE_UD_MSG
#define USE_UD_MSG  1


namespace nocc {

  extern volatile bool running;

  extern __thread TXHandler **txs_;

  namespace oltp {

    extern uint64_t total_ring_sz;
    extern uint64_t ring_padding;
    extern uint64_t ringsz;

    extern RdmaCtrl *cm;
    extern char *rdma_buffer;

    //  __thread extern std::vector<size_t> *txn_aborts;

    struct listener_ping_result  {
      double throughput;
      int32_t aborts;
      int32_t abort_ratio;
    };

    boost::function<void(int)> sigint_callback;
    void sigint_callback_wrapper(int value)
    {
      sigint_callback(value);
    }

    BenchListener::BenchListener(const std::vector<BenchWorker *> *workers,BenchReporter *reporter)
      :epoch_(0),
       inited_(false),
       reporter_(reporter),
       workers_(workers),
       n_returned_(0)
    {
      assert(cm != NULL);

      /* register sigint handler */
      if(current_partition == 0) {
        sigint_callback = std::bind1st(std::mem_fun(&BenchListener::sigint_handler),this);
        signal(SIGINT,sigint_callback_wrapper);
      }

#ifdef LOG_RESULTS
      if(current_partition == 0) {
        char log_file_name[64];
        snprintf(log_file_name,64,"./results/%s_%s_%lu_%lu_%lu_%lu.log",
                 exe_name.c_str(),bench_type.c_str(),total_partition,nthreads,coroutine_num,distributed_ratio);
        Debugger::debug_fprintf(stdout,"log to %s\n",log_file_name);
        log_file.open(log_file_name,std::ofstream::out);
        if(!log_file.is_open()) {
          // create the directory if necessary

        }
        assert(log_file.is_open());
      }
#else
      //      assert(false);
#endif

      reporter->init(workers);
    }

    void BenchListener::thread_local_init() {

      // create qps and init msg handlers
      int dev_id = 0;
      int port_idx = 1;
#if SINGLE_MR == 0
      cm->thread_local_init();
      cm->open_device(dev_id);
      cm->register_connect_mr(dev_id); // register memory on the specific device
#endif

#if USE_UD_MSG == 1
      rdmaio::udmsg::bootstrap_ud_qps(cm,nthreads,dev_id,port_idx,1);
#else
      cm-> // create qp for bench listener use
        link_connect_qps(nthreads, dev_id, port_idx, 0, IBV_QPT_RC);
#endif

#if USE_UD_MSG == 1
      using namespace rdmaio::udmsg;

      rpc_handler_ = new Rpc(NULL,nthreads);
      msg_handler_ = new UDMsg(cm,nthreads,
                               MAX_SERVER_TO_SENT,std::bind(&Rpc::poll_comp_callback,rpc_handler_,
                                             std::placeholders::_1,
                                             std::placeholders::_2));

      rpc_handler_->message_handler_ = msg_handler_; // reset the msg handler
#else
      msg_handler_ = new RingMessage(ringsz,ring_padding,nthreads,cm,rdma_buffer + HUGE_PAGE_SZ);
      rpc_handler_ = new Rpc(msg_handler_,nthreads);
#endif


      if(rpc_handler_ != NULL) {
        rpc_handler_->register_callback(std::bind(&BenchListener::get_result_rpc_handler,this,
                                                  std::placeholders::_1,
                                                  std::placeholders::_2,
                                                  std::placeholders::_3,
                                                  std::placeholders::_4),RPC_REPORT);
        rpc_handler_->register_callback(std::bind(&BenchListener::exit_rpc_handler,this,
                                                  std::placeholders::_1,
                                                  std::placeholders::_2,
                                                  std::placeholders::_3,
                                                  std::placeholders::_4),RPC_EXIT);
        rpc_handler_->register_callback(std::bind(&BenchListener::start_rpc_handler,this,
                                                  std::placeholders::_1,
                                                  std::placeholders::_2,
                                                  std::placeholders::_3,
                                                  std::placeholders::_4),RPC_START);
      } else
        assert(false);

      // init routines
      RoutineMeta::thread_local_init();
    }

    void BenchListener::run() {

      // some performance constants
      auto second_cycle = util::Breakdown_Timer::get_one_second_cycle();

      fprintf(stdout,"[Listener]: Monitor running!\n");
      sleep(1);
      // add a bind?
      //util::BindToCore(nthreads);
      /* Some prepartion stuff */
      RThreadLocalInit();

      thread_local_init();

      if(rpc_handler_ != NULL) {
        rpc_handler_->init();
        rpc_handler_->thread_local_init();
      }
      else
        assert(false);

      struct  timespec start_t,end_t;
      clock_gettime(CLOCK_REALTIME,&start_t);

      // wait till all workers is running before start the worker
      int done = 0;
    WAIT_RETRY:
      for(uint i = 0;i < workers_->size();++i) {
        if( (*workers_)[i]->inited)
          done += 1;
      }
      if(done != workers_->size()) {
        done = 0;
        goto WAIT_RETRY;
      }

      if(current_partition == 0) {
        /* the first server is used to report results */
        //      rpc_handler_->force_replies(msg_handler_->get_num_nodes() - 1);
        char *msg_buf = (char *)Rmalloc(1024);
        char *payload_ptr = msg_buf + sizeof(uint64_t) + sizeof(rpc_header);

        fprintf(stdout,"[Listener]: Enter main loop\n");

        try {
        while(true) {

          if(unlikely(running == false)) {

            fprintf(stdout,"[Listener] receive ending..\n");
            rpc_handler_->clear_reqs();
            //	  rpc_handler_->get_req_buf();
            int *node_ids = new int[msg_handler_->get_num_nodes()];
            for(uint i = 1;i < msg_handler_->get_num_nodes();++i) {
              node_ids[i-1] = i;
            }
            /* so that the send requests shall poll completion */
            //msg_handler_->force_sync(node_ids,msg_handler_->get_num_nodes() - 1);
            if(msg_handler_->get_num_nodes() > 1) {
              rpc_handler_->set_msg(payload_ptr);
              rpc_handler_->send_reqs_ud(RPC_EXIT,sizeof(uint64_t),
                                      node_ids,msg_handler_->get_num_nodes() - 1,0);
            }

            // caluclate local latency
            if(current_partition == 0) {
              auto &timer = (*workers_)[0]->latency_timer_;
              timer.calculate_detailed();
              auto m_l = timer.report_medium() / second_cycle * 1000;
              auto m_9 = timer.report_90() / second_cycle * 1000;
              auto m_99 = timer.report_99() / second_cycle * 1000;
              std::cout << m_l << " " << m_9<<" " <<m_99<<std::endl;
#ifdef LOG_RESULTS
              log_file << m_l << " " << m_9<<" " <<m_99<<std::endl;
#endif
            }

            ending();
            /* shall never return... */
          }

          if(msg_handler_->get_num_nodes() == 1) {
            /* Single server special case */
            sleep(1);
          }
#if USE_UD_MSG == 1
          msg_handler_->poll_comps();
#else
          rpc_handler_->poll_comps();
#endif
          if(n_returned_ == (total_partition - 1)) {

#if 1
            if(!inited_) {
              // send start rpc to others
              fprintf(stdout,"[Listener]: Send start rpc to others\n");
              int *node_ids = new int[msg_handler_->get_num_nodes()];
              for(uint i = 1;i < msg_handler_->get_num_nodes();++i) {
                node_ids[i-1] = i;
              }
              if(msg_handler_->get_num_nodes() > 1) {
                rpc_handler_->set_msg(payload_ptr);
                rpc_handler_->send_reqs_ud(RPC_START,sizeof(uint64_t),
                                        node_ids,msg_handler_->get_num_nodes() - 1,0);
              }

              // for me to start
              start_rpc_handler(0,0,NULL,NULL);
              inited_ = true;
            }
#endif
            epoch_ += 1;

            /* Calculate the first server's performance */
            char *buffer = new char[reporter_->data_len()];
            reporter_->collect_data(buffer,start_t);
            reporter_->merge_data(buffer);
            free(buffer);
#ifdef LOG_RESULTS
            reporter_->report_data(epoch_,log_file);
#endif
            n_returned_ = 0;
            /* end monitoring */
          } // got all results
          if(epoch_ >= MASTER_EPOCH) {
            /* exit */
            fprintf(stdout,"[Listener] Master exit\n");
            //ending();
            //exit_rpc_handler(0,NULL,NULL);
            for(auto it = workers_->begin();it != workers_->end();++it) {
              (*it)->running = false;
            }
            sleep(1);

            running = false;
          }
        } }
        catch (...) {
          assert(false);
        }
      } else {

        // other server's case
        char *msg_buf = (char *)Rmalloc(1024);
        char *payload_ptr = msg_buf + sizeof(uint64_t) + sizeof(rpc_header);

        while(true) {
          /* report results one time one second */
#if USE_UD_MSG == 1
          msg_handler_->poll_comps();
#else
          rpc_handler_->poll_comps();
#endif
          sleep(1);
          /* count the throughput of current server*/

          reporter_->collect_data(payload_ptr,start_t);
          rpc_handler_->clear_reqs();

          /* master server's id is 0 */
          int master_id = 0;
          rpc_handler_->set_msg(payload_ptr);
          //fprintf(stdout,"send to master %p\n",rpc_handler_);
          rpc_handler_->send_reqs_ud(RPC_REPORT,sizeof(struct listener_ping_result),
                                  &master_id,1,0);
          epoch_ += 1;
          if(epoch_ >= (MASTER_EPOCH + 15)) {
            /* slave exit slightly later than the master */
            this->exit_rpc_handler(0,0,NULL,NULL);
            /* shall not return back! */
          }
        } // end slave's case
      }   // end forever loop
    }

    void BenchListener::start_rpc_handler(int id,int cid,char *msg,void *arg) {

      try {
        for(auto it = workers_->begin();it != workers_->end();++it) {
          (*it)->running = true;
        }
      }
      catch(...) {
        assert(false);
      }
    }

    void BenchListener::exit_rpc_handler(int id,int cid, char *msg, void *arg) {
      running = false;
      for(auto it = workers_->begin();it != workers_->end();++it) {
        (*it)->running = false;
      }
      ending();
    }

    void BenchListener::sigint_handler(int) {
      running = false;
      for(auto it = workers_->begin();it != workers_->end();++it) {
        (*it)->running = false;
      }
    }

    void BenchListener::get_result_rpc_handler(int id, int cid,char *msg, void *arg) {
#if 1
      try {
        reporter_->merge_data(msg);
        n_returned_ += 1;
      } catch(...) {
        fprintf(stdout,"[Listener]: Report RPC error.\n");
        assert(false);
      }
#endif
    }

    void BenchListener::ending() {

      sleep(1);
      if(current_partition == 0) {
        uint64_t *test_ptr = (uint64_t *)rdma_buffer;
        fprintf(stdout,"sanity checks...%lu\n",*test_ptr);
      }
      fprintf(stdout,"Benchmark ends... \n");
#ifdef LOG_RESULTS
      if(current_partition == 0) {
        fprintf(stdout,"Flush results\n");
        log_file.close();
      }
#endif
      // delete cm;
      /* wait for ending message to pass */
      sleep(1);
      exit(0);
    }
  } // end namespace oltp
}; //end namespace nocc
