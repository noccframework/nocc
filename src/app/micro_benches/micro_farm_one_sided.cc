#include "bench_micro.h"
#include "./framework/req_buf_allocator.h"

namespace nocc {

  extern __thread RPCMemAllocator *msg_buf_alloctors;
  
  namespace oltp {
    
    namespace micro {

      static const int out_standing_request_num = 8;
      static const int msg_size = 512;
      static const uint64_t remote_offset = (uint64_t)1024*1024*1024*12;

      __thread micro_cycle_info* cycle_info = NULL;

      void print_row(uint64_t* array, int start, int num) {
        for (int i = 0; i < num; i++) {
          printf("[%2d]: %lu ",start+i,array[start+i]);
        }
        printf("\n");
      }
      
      void print_array(uint64_t* array, int end, int gap) {
        int start = 0;
        while (end - start > gap) {
          print_row(array, start, gap);
          start += gap;
        }
        print_row(array, start, end - start);
      }

      void print_cycle() {
          printf("/***********************************************************/\n");
          // printf("post_cycles:\n");
          //   print_array(post_cycles, out_standing_request_num, 5);
          
          printf("------------in this iteration post_cycle: %lu\n",  cycle_info->iter_tot_post_cycle);
          // printf("------------average poll_once_cycle: %f\n",  cycle_info->poll_once_cycle / (double)cycle_info->poll_counts);
          
          // printf("completion_cycles:\n");
          //   print_array(completion_cycles, out_standing_request_num, 5);
                             
          // printf("------------in this iteration poll_cycle: %lu\n",  cycle_info->iter_tot_poll_cycle);
          // printf("------average total_poll_cycle: %f\n",  cycle_info->total_poll_cycle / (double)cycle_info->poll_counts);
      }

      txn_result_t MicroWorker::micro_farm_one_sided(yield_func_t &yield) {
        if(cycle_info == NULL) cycle_info = new micro_cycle_info;
        // init         
        cycle_info->iter_tot_post_cycle = 0;
        cycle_info->iter_tot_poll_cycle = 0;
        memset(cycle_info->completion_cycles,0,out_standing_request_num*sizeof(uint64_t));

        int target_id = current_partition;
        int num_nodes = cm_->get_num_nodes();
        target_id = (target_id + 1) % num_nodes;
        
        Qp* qp = qps_[target_id];
        uint64_t head, cycle;
#if 1
        for (int i = 0; i < out_standing_request_num; i++ ) {
          head = rdtsc();
          qp->rc_post_send(IBV_WR_RDMA_READ,rdma_buf_,msg_size,remote_offset + msg_size * i,IBV_SEND_SIGNALED,cor_id_);
          rdma_sched_->add_pending(cor_id_,qp);
          cycle = rdtsc() - head;
          cycle_info->post_cycles[i] = cycle;
          cycle_info->iter_tot_post_cycle += cycle;
        }
        indirect_must_yield(yield);
#else
        char *req_buf = msg_buf_alloctors[cor_id_].get_req_buf() + sizeof(uint64_t) + sizeof(rpc_header);
        rpc_handler_->set_msg((char *)req_buf);
        rpc_handler_->send_reqs(RPC_MICRO_READ,msg_size,(char *)((char *)reply_buf_ + msg_size * cor_id_),
          &target_id,1,cor_id_);
        indirect_yield(yield);
#endif        

        // auto start = rdtsc();
        // auto poll_result = ibv_poll_cq(qp->send_cq, out_standing_request_num, wcs_);
        // cycle_info->poll_once_cycle += rdtsc() - start;         
        
        // int remaining_num = out_standing_request_num - poll_result;
        // if(remaining_num > 0) {
        //   qp->poll_completion();
        //   first_poll_cycle += rdtsc() - start;
        //   remaining_num -= 1;
        // }
        // for (int i = 0; i < remaining_num; i++ ) {
        //   head = rdtsc();
        //   qp->poll_completion();
        //   cycle = rdtsc() - head;
        //   completion_cycles[poll_result+i] = cycle;
        //   cycle_info->iter_tot_poll_cycle += cycle;
        // }

        cycle_info->poll_counts += 1;
        if(cycle_info->poll_counts % 200000 == 0) {
          print_cycle();
        }

        // char *local_buf = msg_buf_alloctors[cor_id_].get_req_buf();
        // rpc_handler_->set_msg((char *)local_buf);
        // rpc_handler_->send_reqs(RPC_MICRO_READ,msg_size,(char *)((char *)reply_buf_ + msg_size * cor_id_),
        //                 &target_id,1,cor_id_);
        // indirect_yield(yield);
        // rpc_handler_->send_reqs(RPC_NULL,msg_size,&target_id,1,cor_id_);
        return txn_result_t(true,0);
      }

      void MicroWorker::micro_read_rpc_handler(int id, int cid, char *msg, void *arg) {
        char *reply_msg = rpc_handler_->get_reply_buf();
        for (int i = 0; i < out_standing_request_num; i++ ) {
          memcpy(reply_msg,(char*)(cm_->conn_buf_ + remote_offset + msg_size * i),msg_size);
        }
        //rpc_handler_->reply_msg_buf_  = reply_msg;
        rpc_handler_->send_reply(msg_size, id, cid);
        return;
      }

    } // end namespace micro

  } // end namespace nocc

} // end namespace oltp
