#include "all.h"
#include "bench_micro.h"

#include "db/txs/dbrad.h"
#include "db/txs/dbtx.h"
#include "db/txs/dbsi.h"
#include "db/txs/db_farm.h"

#include "util/util.h"

#include "framework/req_buf_allocator.h"

extern size_t distributed_ratio; // re-use some configure parameters
extern size_t nthreads;
extern size_t total_partition;

#include "../smallbank/bank_worker.h"
using namespace nocc::oltp::bank; // use smallbank as the default workload

namespace nocc {

  extern __thread RPCMemAllocator *msg_buf_alloctors;

  namespace oltp {

    extern __thread util::fast_random   *random_generator;
    extern RdmaCtrl *cm;

    namespace micro {

      // arg communicate between RPCs
      struct MicroArg {
        uint64_t id;
        uint64_t version;
      };


      // 2 dummy handlers for test, they only send a dummy reply
      void MicroWorker::tx_one_shot_handler2(int id,int cid,char *msg,void *arg) {
        char *reply_msg = rpc_handler_->get_reply_buf();
        rpc_handler_->send_reply(CACHE_LINE_SZ,id,cid);
        return;
      }

      // This RPC is designed to yield out
      void MicroWorker::tx_one_shot_handler(yield_func_t &yield,int id,int cid,char *input) {
        char *reply_msg = rpc_handler_->get_reply_buf();
        MicroArg *arg = (MicroArg *)input;
        ASSERT_PRINT(tx_ != NULL,stderr,"tx_ %d, routine id %d\n",cor_id_, routine_meta_->id_);
        tx_->get_ro_versioned(TAB,arg->id,reply_msg,arg->version,yield);
        rpc_handler_->send_reply(CACHE_LINE_SZ,id,cid);
        return;
      }

      txn_result_t MicroWorker::micro_tx_ts(yield_func_t &yield) {
        if(current_partition == 0) return txn_result_t(false,1);
#if 1
        tx_->begin();
        tx_->end(yield);
#else
        static __thread char *local_buf = (char *)Rmalloc(4096);
        uint64_t working_space = 8 * 1024 * 1024;
        //uint64_t offset = random_generator[cor_id_].next() %
        //(working_space - MAX_MSG_SIZE);
        //uint64_t offset = current_partition * nthreads * sizeof(uint64_t) +
        //          worker_id_ * sizeof(uint64_t);
        uint64_t offset = current_partition * sizeof(uint64_t);
        auto flag = 0;
        //if(size <= 64)
        //          flag |= IBV_SEND_INLINE;
        auto qp = qps_[0];
        if(qp->first_send()) flag |= IBV_SEND_SIGNALED;
        if(qp->need_poll())  qp->poll_completion();
        qp->rc_post_send(IBV_WR_RDMA_WRITE,local_buf,sizeof(uint64_t),offset,
                         flag,
                         cor_id_);
#endif
        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_tx_rad(yield_func_t &yield) {

        // PDI related micro benchmarks
        auto num = distributed_ratio; // number of item read
        tx_->begin();

        static const int num_nodes = cm->get_num_nodes();
        //int      pid    = (current_partition + 1) % num_nodes;
        int pid = random_generator[cor_id_].next() % num_nodes;
        char *req_buf   = msg_buf_alloctors[cor_id_].get_req_buf();
        char *reply_buf = (char *)malloc(CACHE_LINE_SZ);

        int type = 0;
#ifdef RAD_TX
        type = 1;
#endif
        rpc_handler_->prepare_multi_req(reply_buf,1,cor_id_);
        rpc_handler_->append_req(req_buf,RPC_READ,CACHE_LINE_SZ,pid,cor_id_,type);
        indirect_yield(yield);
        free(reply_buf);
        tx_->end(yield);
        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_tx_rw(yield_func_t &yield) {

        // test the basic performance of read/write using PDI
        int read_ratio = distributed_ratio;
        set<uint64_t> id_set;
        char *req_buf = msg_buf_alloctors[cor_id_].get_req_buf();
        MicroArg *arg = (MicroArg *)req_buf;
        char *reply_buf = (char *)malloc(CACHE_LINE_SZ);

        tx_->begin();
        arg->version = ((DBRad *)tx_)->timestamp;
#if 1
        for(uint i = 0;i < 10;++i) {
          uint64_t id;
        retry:
          GetAccount(random_generator[cor_id_],&id);
          if(id_set.find(id) != id_set.end()) goto retry;
          else id_set.insert(id);

          int pid = AcctToPid(id);

          bool ro = random_generator[cor_id_].next() % 100 <= read_ratio;
          int idx = 0;
          if(ro) {
            arg->id = id;
            rpc_handler_->prepare_multi_req(reply_buf,1,cor_id_);
            rpc_handler_->append_req(req_buf,RPC_READ,sizeof(MicroArg),pid,cor_id_,1);
            indirect_yield(yield);
          } else {
            //fprintf(stdout,"add to %d, key %lu\n",pid,id);
            idx = tx_->add_to_remote_set(TAB,id,pid);
            assert(idx == 0);
            auto replies = tx_->remoteset->do_reads(i);
            indirect_yield(yield);
            //fprintf(stdout,"done\n");
            tx_->get_remote_results(replies);

            char *buf = NULL;
            uint64_t seq = tx_->get_cached(0,(char **)(&buf));
            tx_->remote_write(idx,buf,CACHE_LINE_SZ);
          }
        }
#endif
        bool ret = tx_->end(yield);
        free(reply_buf);
        return txn_result_t(ret,1);
      }

    }; // namespace micro

  }; // namespace oltp

};   // namespace nocc
