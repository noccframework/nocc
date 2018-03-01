#include "bench_micro.h"
#include "framework/req_buf_allocator.h"

namespace nocc {

  extern __thread RPCMemAllocator *msg_buf_alloctors;
  
  namespace oltp {
    
    namespace micro {

      static const int out_standing_request_num = 8;
      static const int msg_size = 512;
      static const uint64_t remote_offset = (uint64_t)1024*1024*1024*12;

      __thread int qp_num = 0;
      __thread int target = 1;

      txn_result_t MicroWorker::micro_farm_qp_sharing(yield_func_t &yield) {
        // init
        int qp_index = cm_->get_num_nodes()* qp_num + 1;
        Qp* qp = qps_[qp_index];
        int send_flag;
        if(qp->first_send()) {
          send_flag = IBV_SEND_SIGNALED;
        }
        if(qp->need_poll())
          qp->poll_completion();

        // if(need_polls_[qp_index])
        //   qp->poll_completion();
        // else{
        //   need_polls_[qp_index] = true;
        // }
        qp->rc_post_send(IBV_WR_RDMA_READ,rdma_buf_,msg_size,remote_offset + msg_size * qp_num,send_flag);

        qp_num++;
        if(qp_num == per_thread_qp_num_)
          qp_num = 0;
        // qp->poll_completion();
        return txn_result_t(true,0);
      }



    } // end namespace micro

  } // end namespace nocc

} // end namespace oltp
