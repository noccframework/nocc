#include "all.h"
#include "bench_micro.h"

#include "app/smallbank/bank_worker.h" // using bank's input generator for test
#include "db/req_buf_allocator.h"

extern size_t distributed_ratio;

namespace nocc {

  extern __thread RPCMemAllocator *msg_buf_alloctors;

  namespace oltp {

    extern __thread util::fast_random   *random_generator;

    extern Breakdown_Timer *send_req_timers;
    extern Breakdown_Timer *compute_timers;

    extern char *rdma_buffer;      // start point of the local RDMA registered buffer
    extern char *free_buffer;      // start point of the local RDMA heap. before are reserved memory
    extern uint64_t r_buffer_size; // total registered buffer size
    extern RdmaCtrl *cm;

    namespace micro {

      extern uint64_t working_space;

      txn_result_t MicroWorker::micro_rpc_write_multi(yield_func_t &yield) {

        auto num = distributed_ratio;
        static uint64_t free_offset = free_buffer - rdma_buffer;
        static uint64_t total_free  = r_buffer_size - free_offset;
        static const int num_nodes = cm->get_num_nodes();

        char *req_buf = msg_buf_alloctors[cor_id_].get_req_buf();
#if NAIVE ==   0 // non batch execuation
        for(uint i = 0;i < num;++i) {

          uint64_t offset = random_generator[cor_id_].next() % (working_space - CACHE_LINE_SZ);
          int      pid    = random_generator[cor_id_].next() % num_nodes;

          // align
          //offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

          ReadReq *req = (ReadReq *)req_buf;
          req->off = offset;

          rpc_handler_->prepare_multi_req(reply_bufs_[cor_id_],1,cor_id_);
          rpc_handler_->append_req(req_buf,RPC_WRITE,sizeof(ReadReq),pid,cor_id_);
          indirect_yield(yield);
        }
        return txn_result_t(true,1);
#elif NAIVE == 1 // batch execuation

        ReadReqWrapper *req_array = (ReadReqWrapper *)req_buf;
        rpc_handler_->prepare_multi_req(reply_bufs_[cor_id_],num,cor_id_);

        for(uint i = 0;i < num;++i) {

          uint64_t offset = random_generator[cor_id_].next() % (working_space - CACHE_LINE_SZ);
          int      pid    = random_generator[cor_id_].next() % num_nodes;
          // align
          offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

          // prepare an RPC header
          req_array[i].req.off = offset;
          rpc_handler_->append_req((char *)(&(req_array[i])) + sizeof(uint64_t) + sizeof(rpc_header),
                                   RPC_WRITE,sizeof(ReadReq),pid,cor_id_);
        }
        indirect_yield(yield);
        return txn_result_t(true,1);
#elif NAIVE == 2 // add doorbell
        ReadReqWrapper *req_array = (ReadReqWrapper *)req_buf;
        //rpc_handler_->prepare_multi_req(reply_bufs_[cor_id_],num,cor_id_);
        rpc_handler_->prepare_multi_req(reply_bufs_[cor_id_],0,cor_id_);

        for(uint i = 0;i < num;++i) {

          uint64_t offset = random_generator[cor_id_].next() % (working_space - CACHE_LINE_SZ);
          int      pid    = random_generator[cor_id_].next() % num_nodes;

          // align
          offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

          // prepare an RPC header
          req_array[i].req.off = offset;
          rpc_handler_->append_req_ud((char *)(&(req_array[i])) + sizeof(uint64_t) + sizeof(rpc_header),
                                      RPC_WRITE,sizeof(ReadReq),pid,cor_id_);
        }
        rpc_handler_->end_req_ud();
        //indirect_yield(yield);
#else    // + merging

        START(post);
        std::set<int> server_set;
        int servers[MAX_REQ_NUM];
        int server_num(0);
        server_set.clear();

        char *cur_ptr = req_buf + sizeof(ReadReqHeader);

        for(uint i = 0;i < num;++i) {

          uint64_t offset = random_generator[cor_id_].next() % (working_space - CACHE_LINE_SZ);;
          int      pid    = random_generator[cor_id_].next() % num_nodes;
          // align
          offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

          // prepare an RPC header
          ReadReq *req = (ReadReq *)cur_ptr;
          req->pid = pid;
          req->off = offset;
          cur_ptr += sizeof(ReadReq);

          if(server_set.find(pid) == server_set.end()) {
            server_set.insert(pid);
            servers[server_num++] = pid;
          }
        }

        ((ReadReqHeader *)req_buf)->num = num;
        rpc_handler_->set_msg((char *)req_buf);
        END(post);
#if NAIVE == 4 // no completion
        rpc_handler_->send_reqs(RPC_BATCH_WRITE,cur_ptr - req_buf,
                                servers,server_num,cor_id_);
#else
        rpc_handler_->send_reqs(RPC_BATCH_READ,cur_ptr - req_buf,(char *)reply_bufs_[cor_id_],
                                servers,server_num,cor_id_);
        indirect_yield(yield);
#endif

#endif
        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_rpc_write(yield_func_t &yield) {

        auto size = distributed_ratio;
        assert(size > 0 && size <= MAX_MSG_SIZE);

        char *req_buf = msg_buf_alloctors[cor_id_].get_req_buf();


        static uint64_t free_offset = free_buffer - rdma_buffer;
        static uint64_t total_free  = r_buffer_size - free_offset;
        const uint64_t write_space = total_free - size;

        static const int num_nodes = cm->get_num_nodes();

        int      pid    = random_generator[cor_id_].next() % num_nodes;
#if 1
        uint64_t offset = random_generator[cor_id_].next() % write_space;
        // align
        offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

        // prepare an RPC header
        ReadReq *req = (ReadReq *)req_buf;
        req->off = offset;
        req->size = size;
#endif
        // send the RPC
        rpc_handler_->prepare_multi_req(reply_bufs_[cor_id_],1,cor_id_);
        rpc_handler_->append_req(req_buf,RPC_WRITE,sizeof(ReadReq) + size,pid,cor_id_);
        //rpc_handler_->append_req(req_buf,RPC_READ,32,pid,cor_id_);
        indirect_yield(yield);

        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_rpc_read(yield_func_t &yield) {

        auto size = distributed_ratio;
        assert(size > 0 && size <= MAX_MSG_SIZE);

        char *req_buf = msg_buf_alloctors[cor_id_].get_req_buf();

        static uint64_t free_offset = free_buffer - rdma_buffer;
        static uint64_t total_free  = r_buffer_size - free_offset;
        static const int num_nodes = cm->get_num_nodes();

        const uint64_t read_space = \
          //total_free;
          working_space;

        int      pid    = random_generator[cor_id_].next() % num_nodes;
#if 1
        uint64_t offset = random_generator[cor_id_].next() % read_space;
        // align
        offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

        // prepare an RPC header
        ReadReq *req = (ReadReq *)req_buf;
        req->off = offset;
        req->size = size;
#endif
        // send the RPC
        rpc_handler_->prepare_multi_req(reply_bufs_[cor_id_],1,cor_id_);
        START(post);
        rpc_handler_->append_req(req_buf,RPC_READ,sizeof(ReadReq),pid,cor_id_);
        END(post);
        //rpc_handler_->append_req(req_buf,RPC_READ,32,pid,cor_id_);
        indirect_yield(yield);

        return txn_result_t(true,1);
      }

      txn_result_t MicroWorker::micro_rpc_read_multi(yield_func_t &yield) {

        auto num = distributed_ratio; // number of remote objects to fetch
        assert(num > 0 && num < MAX_REQ_NUM);

        static uint64_t free_offset = free_buffer - rdma_buffer;
        static uint64_t total_free  = r_buffer_size - free_offset;
        static const int num_nodes = cm->get_num_nodes();


#if NAIVE == 0
        char *req_buf = msg_buf_alloctors[cor_id_].get_req_buf();

        // non-batch mode
        for(uint i = 0;i < num;++i) {

          uint64_t offset = random_generator[cor_id_].next() % working_space;
          int      pid    = random_generator[cor_id_].next() % num_nodes;
          // align
          offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

          // prepare an RPC header
          ReadReq *req = (ReadReq *)req_buf;
          req->off = offset;
#if 0
          rpc_handler_->set_msg((char *)req_buf);
          rpc_handler_->send_reqs(RPC_READ,sizeof(ReadReq),(char *)reply_bufs_[cor_id_],
                                  &pid,1,cor_id_);
#else
          rpc_handler_->prepare_multi_req(reply_bufs_[cor_id_],1,cor_id_);
          rpc_handler_->append_req(req_buf,RPC_READ,sizeof(ReadReq),pid,cor_id_);
#endif
          indirect_yield(yield);
         }
        return txn_result_t(true,1);
#elif NAIVE == 1 //+ batch

        char *req_buf = msg_buf_alloctors[cor_id_].get_req_buf();

        START(post);
        ReadReqWrapper *req_array = (ReadReqWrapper *)req_buf;
        //assert(false);
        rpc_handler_->prepare_multi_req(reply_bufs_[cor_id_],num,cor_id_);

        for(uint i = 0;i < num;++i) {

          uint64_t offset = random_generator[cor_id_].next() % working_space;
          int      pid    = random_generator[cor_id_].next() % num_nodes;
          // align
          offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

          // prepare an RPC header
          req_array[i].req.off = offset;
          rpc_handler_->append_req((char *)(&(req_array[i])) + sizeof(uint64_t) + sizeof(rpc_header),
                                   RPC_READ,sizeof(ReadReq),pid,cor_id_);
        }
        END(post);
        indirect_yield(yield);
        return txn_result_t(true,1);
#elif NAIVE == 2 // + doorbell batching
        START(post);
        rpc_handler_->prepare_multi_req(reply_bufs_[cor_id_],num,cor_id_);

        char *req_buf = msg_buf_alloctors[cor_id_].get_req_buf();
        for(uint i = 0;i < num;++i) {

          ReadReqWrapper *req_array = (ReadReqWrapper *)(req_buf + i * 128);

          uint64_t offset = random_generator[cor_id_].next() % working_space;
          int      pid    = random_generator[cor_id_].next() % num_nodes;

          // align
          offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

          // prepare an RPC header
          req_array[0].req.off = offset;
          rpc_handler_->append_req_ud((char *)(&(req_array[0])) + sizeof(uint64_t) + sizeof(rpc_header),
                                      RPC_READ,sizeof(ReadReq),pid,cor_id_);
        }
        rpc_handler_->end_req_ud();
        END(post);
        indirect_yield(yield);
        return txn_result_t(true,1);

#elif NAIVE == 73 // + merge message
        START(post);
        std::set<int> server_set;
        int servers[MAX_REQ_NUM];
        int server_num(0);
        server_set.clear();

        char *req_buf = msg_buf_alloctors[cor_id_].get_req_buf();
        char *cur_ptr = req_buf + sizeof(ReadReqHeader);

        for(uint i = 0;i < num;++i) {

          uint64_t offset = random_generator[cor_id_].next() % working_space;
          int      pid    = random_generator[cor_id_].next() % num_nodes;
          // align
          //offset = Round<uint64_t>(offset,CACHE_LINE_SZ);

          // prepare an RPC header
          ReadReq *req = (ReadReq *)cur_ptr;
          req->off = offset;
          req->pid = pid;
          cur_ptr += sizeof(ReadReq);

          if(server_set.find(pid) == server_set.end()) {
            server_set.insert(pid);
            servers[server_num++] = pid;
          }
        }

        ((ReadReqHeader *)req_buf)->num = num;
        rpc_handler_->set_msg((char *)req_buf);
        rpc_handler_->send_reqs(RPC_BATCH_READ,cur_ptr - req_buf,(char *)reply_bufs_[cor_id_],
                                servers,server_num,cor_id_);
        END(post);
        indirect_yield(yield);
        return txn_result_t(true,1);
#endif
      }


      txn_result_t MicroWorker::micro_rpc_stress(yield_func_t &yield) {

        using namespace nocc::oltp::bank;
        uint64_t id;
      retry:
        GetAccount(random_generator[cor_id_],&id);
        int target = AcctToPid(id);
        if(target == current_partition)
          goto retry;

        //fprintf(stdout,"send to %d\n",target);
#if 1
        const int msg_size = 128;
#if RPC_LOG
        if(ntxn_commits_ < 50) {
          char *log_buf = next_log_entry(&local_log,64);
          assert(log_buf != NULL);
          sprintf(log_buf,"routine %d send to %d\n",cor_id_,target);
        }
#endif

        char *local_buf = msg_buf_alloctors[cor_id_].get_req_buf() + sizeof(uint64_t) + sizeof(rpc_header);
        char *local_buf1 = msg_buf_alloctors[cor_id_].get_req_buf() + sizeof(uint64_t) + sizeof(rpc_header);
        //char *local_buf = (char *)Rmalloc(1024) + sizeof(uint64_t) + sizeof(rpc_header);

        //fprintf(stdout,"start , sum %lu\n",compute_timers->sum);
        compute_timers->start();
        // generate a random message
        //memcpy(local_buf,random_generator[cor_id_].next_string(msg_size).c_str(),msg_size);
        compute_timers->end();
        //fprintf(stdout,"end , sum %lu c: %d %f\n",compute_timers->sum,compute_timers->count,
        //compute_timers->report());
        //sleep(1);

        send_req_timers->start();
        rpc_handler_->set_msg((char *)local_buf);
        rpc_handler_->send_reqs(RPC_NOP,msg_size,(char *)((char *)reply_buf_ + 1024 * cor_id_),
                        &target,1,cor_id_);
        send_req_timers->end();

        //yield(routines_[MASTER_ROUTINE_ID]);
        indirect_yield(yield);

#if RPC_LOG
        if(ntxn_commits_ < 50) {
          char *log_buf = next_log_entry(&local_log,64);
          assert(log_buf != NULL);
          sprintf(log_buf,"routine %d received \n",cor_id_);
        }
        //ntxn_commits_ += 1;
#endif

        // RDMA stress tests
        //Qp *qp = cm_->get_rc_qp(this-worker_id_,target);
        //uint64_t offset = rand() %
        //memcpy(local_buf1,random_generator[cor_id_].next_string(msg_size).c_str(),msg_size);
        rpc_handler_->set_msg((char *)local_buf1);
        rpc_handler_->send_reqs(RPC_NULL,4,&target,1,cor_id_);
#endif

        return txn_result_t(true,0);
      }

      txn_result_t MicroWorker::micro_rpc_scale(yield_func_t &yield) {
        return txn_result_t(true, 0);
      }

      extern char *test_buf; // used for write
      void MicroWorker::read_rpc_handler(int id,int cid,char *msg,void *arg) {

        char *reply_msg = rpc_handler_->get_reply_buf();
        ReadReq *req = (ReadReq *)msg;

        assert(req->off >= 0 && req->off < r_buffer_size);
        //fprintf(stdout,"off %lu\n",req->off);
        //sleep(1);

        //memcpy(reply_msg,rdma_buffer + req->off,CACHE_LINE_SZ);
        //memcpy(reply_msg,test_buf + req->off,CACHE_LINE_SZ);
        rpc_handler_->send_reply(CACHE_LINE_SZ,id,cid);
      }

      void MicroWorker::various_read_rpc_handler(int id,int cid,char *msg,void *arg) {
        char *reply_msg = rpc_handler_->get_reply_buf();
        ReadReq *req = (ReadReq *)msg;
#if 0
        assert(req->off >= 0 && req->off < r_buffer_size && req->off + req->size < r_buffer_size);

        memcpy(reply_msg,rdma_buffer + req->off,req->size);
#endif
        rpc_handler_->send_reply(req->size,id,cid);
        //rpc_handler_->send_reply(32,id,cid);
      }

      void MicroWorker::batch_write_rpc_handler(int id,int cid,char *msg,void *arg) {

        char *reply_msg = rpc_handler_->get_reply_buf();
        char *reply_msg_start = reply_msg;

        ReadReqHeader *reaHeader = (ReadReqHeader *)msg;
        int req_num = reaHeader->num;
        char *traverse_ptr = msg + sizeof(ReadReqHeader);
        assert(req_num == 10);
        for (uint i = 0; i < req_num; i++){
          ReadReq *req = (ReadReq *)traverse_ptr;
          traverse_ptr += (sizeof(ReadReq) + CACHE_LINE_SZ);
          if (req->pid != current_partition)
            continue;
          assert(req->off >= 0 && req->off < r_buffer_size);
#if NAIVE == 4
          //memcpy(test_buf,(char *)req + sizeof(ReadReq),CACHE_LINE_SZ);
#else
          memcpy(test_buf,(char *)req + sizeof(ReadReq),CACHE_LINE_SZ);
#endif
        }
#if NAIVE == 4
#else
        rpc_handler_->send_reply(1,id,cid);
#endif
      }


      void MicroWorker::batch_read_rpc_handler(int id,int cid,char *msg,void *arg) {

        char *reply_msg = rpc_handler_->get_reply_buf();
        char *reply_msg_start = reply_msg;

        ReadReqHeader *reaHeader = (ReadReqHeader *)msg;
        int req_num = reaHeader->num;
        ReadReq *req = (ReadReq *)(msg + sizeof(ReadReqHeader));
        for (uint i = 0; i < req_num; i++, req++){
          if (req->pid != current_partition) continue;
          assert(req->off >= 0 && req->off < r_buffer_size);
          memcpy(reply_msg,rdma_buffer + req->off,CACHE_LINE_SZ);
          reply_msg += CACHE_LINE_SZ;
        }

        rpc_handler_->send_reply(reply_msg - reply_msg_start,id,cid);
      }

      void MicroWorker::nop_rpc_handler(int id, int cid, char *msg, void *arg) {

#if RPC_LOG
        if (ntxn_commits_ < 50) {
          char *log_buf = next_log_entry(&local_log, 32);
          assert(log_buf != NULL);
          sprintf(log_buf, "reply to %d for routine %d \n", id, cid);
        }
#endif

        char *reply_msg = rpc_handler_->get_reply_buf();
        *((int *)reply_msg) = 64;
        rpc_handler_->send_reply(sizeof(int), id, cid);
      }

      void MicroWorker::write_rpc_handler(int id,int cid,char *msg, void *arg) {

        char *reply_msg = rpc_handler_->get_reply_buf();
        ReadReq *req = (ReadReq *)msg;

        assert(req->off >= 0 && req->off < r_buffer_size && req->off + req->size < r_buffer_size);

        assert(test_buf != NULL);
        //memcpy(test_buf + req->off, msg + sizeof(ReadReq), req->size);
#if 0
        rpc_handler_->send_reply(1,id,cid); // a dummy notification
#endif
      }

      void MicroWorker::null_rpc_handler(int id, int cid, char *msg, void *arg) {
        // do nothing
        return;
      }

    } // end namespace micro

  } // end namespace nocc

} // end namespace oltp
