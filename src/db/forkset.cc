#include "all.h"
#include "forkset.h"

#include "./framework/req_buf_allocator.h"

namespace nocc {

  using namespace oltp;  // the framework namespace
  extern __thread RPCMemAllocator *msg_buf_alloctors;

  namespace db {

    ForkSet::ForkSet (Rpc *rpc,int cid) : rpc_handler_(rpc), cor_id_(cid) {
      reply_buf_  = (char *)malloc(MAX_MSG_SIZE);
      server_num_ = 0;
    };

    void ForkSet::reset() {
      //    msg_buf_start_ = rpc_handler_->get_req_buf();
      msg_buf_start_ = msg_buf_alloctors[cor_id_].get_req_buf() + sizeof(rpc_header) + sizeof(uint64_t);
      msg_buf_end_   = msg_buf_start_;
    }

    void  ForkSet::do_fork() {
      rpc_handler_->clear_reqs();
      assert(server_num_ == 0);
    }
  
    char *ForkSet::do_fork(int sizeof_header) {
      msg_buf_start_ = msg_buf_alloctors[cor_id_].get_req_buf() + sizeof(rpc_header) + sizeof(uint64_t);
      msg_buf_end_   = msg_buf_start_ + sizeof_header;
      return msg_buf_start_;
    }

    void ForkSet::add(int pid) {
      if(server_set_.find(pid) == server_set_.end()) {
        server_set_.insert(pid);
        server_lists_[server_num_++] = pid;
      }
    }
  
    char *ForkSet::add(int pid,int sizeof_payload) {
      char *ret = msg_buf_end_;
      msg_buf_end_ += sizeof_payload;
      add(pid);
      return ret;
    }

    int ForkSet::fork(int id,int type) {
      //    assert(msg_buf_start_ == rpc_handler_->msg_buf_ + sizeof(rpc_header) + sizeof(uint64_t));
      assert(cor_id_ != 0);
      rpc_handler_->set_msg(msg_buf_start_);
      return rpc_handler_->send_reqs(id,msg_buf_end_ - msg_buf_start_, reply_buf_,
                                     server_lists_,server_num_,this->cor_id_,type);
    }

    int ForkSet::fork(int id,char *msg,int size,int type) {
      rpc_handler_->clear_reqs();
      //    msg_buf_start_ = rpc_handler_->get_req_buf();
      msg_buf_start_ = msg_buf_alloctors[cor_id_].get_req_buf() + sizeof(rpc_header) + sizeof(uint64_t);
      memcpy(msg_buf_start_,msg,size);
      rpc_handler_->set_msg(msg_buf_start_);
      return rpc_handler_->send_reqs(id,size,reply_buf_,server_lists_,server_num_,this->cor_id_,type);
    }
  };

};
