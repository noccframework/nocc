#ifndef DB_RDMA_SCHED
#define DB_RDMA_SCHED

#include <deque>

#include "config.h"
#include "rdmaio.h"

//#include "db_statistics_helper.h"

namespace nocc {
  namespace oltp {

    class RDMA_sched {
    public:
      RDMA_sched();
      ~RDMA_sched();

      // add pending qp to corresponding coroutine
      void add_pending(int cor_id,rdmaio::Qp *qp);

      // poll all the pending qps of the thread and schedule
      void poll_comps();

      void thread_local_init();

      void report();
    private:
      //std::map<rdmaio::Qp *,int> pending_qps_;
      std::deque<rdmaio::Qp *> pending_qps_;

      //struct ibv_wc wcs_[1024];
      struct ibv_wc wc_;

      /* Some performance counting statistics ********************************/
      uint64_t total_costs_;
      uint64_t pre_total_costs_;

      uint64_t poll_costs_;
      uint64_t pre_poll_costs_;

      uint64_t counts_;
      uint64_t pre_counts_;
    };

  }; // namespace db

};   // namespace nocc

#endif // DB_RDMA_SCHED
