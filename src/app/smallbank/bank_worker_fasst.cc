// This file implement Bank's TX using FaSST's protocol
// The implementatio is seperate since FaSST requires known read-write set

#include "bank_worker.h"
#include "db/txs/dbrad.h"
#include "db/txs/dbtx.h"
#include "db/txs/dbsi.h"
#include "db/txs/si_ts_manager.h"

#include "db/forkset.h"

#include "framework/routine.h"
#include "framework/req_buf_allocator.h"

#include "util/util.h"

#include <boost/bind.hpp>

#define unlikely(x) __builtin_expect(!!(x), 0)

extern nocc::db::TSManager *ts_manager;

extern __thread RemoteHelper *remote_helper;

extern size_t coroutine_num;
extern size_t current_partition;

namespace nocc {

  extern __thread RPCMemAllocator *msg_buf_alloctors;

  namespace oltp {

    extern __thread util::fast_random   *random_generator;
    extern __thread ro_func_t *ro_callbacks_;

    namespace bank {

      extern Breakdown_Timer compute_timer;
      extern Breakdown_Timer send_timer;

      txn_result_t BankWorker::txn_deposit_checking_fasst(yield_func_t &yield) {

        tx_->begin(db_logger_);

        float amount = 1.3;
      retry:
        uint64_t id;
        GetAccount(random_generator[cor_id_],&id);
        int pid = AcctToPid(id);
        //if(pid == current_partition) goto retry;

        checking::value *cv = NULL;

#if FASST == 0
        tx_->add_to_remote_set(CHECK,id,pid,yield);
#else
        tx_->remoteset->add(REQ_READ_LOCK,pid,CHECK,id);
#endif
        tx_->do_remote_reads();

        indirect_yield(yield);

#if FASST == 1
        bool res = tx_->remoteset->get_results_readlock(1);
#else
        tx_->get_remote_results(1);
#endif

        uint64_t seq = tx_->get_cached(0,(char **)(&cv));
        cv->c_balance += 1;
        tx_->remote_write(0,(char *)cv,sizeof(checking::value));


#if FASST == 1
        if(unlikely(res == false)) {
          // abort case
          tx_->remoteset->release_remote();
          tx_->remoteset->clear_for_reads();
          tx_->abort();
          return txn_result_t(false,0);
        }
        bool ret = tx_->end_fasst(yield);
#else
        bool ret = tx_->end(yield);
        //if(!ret) ntxn_commits_ += 1;
#endif
#if 0 // sanity checks
        if(ret == 1) {
          fprintf(stdout,"commits\n");
          yield_next(yield); // yield for processing RPC

          tx_->begin(db_logger_);
          tx_->add_to_remote_set(CHECK,id,pid);
          indirect_yield(yield);
          checking::value *cv1;
          auto seq = tx_->get_cached(0,(char **)(&cv1));
          fprintf(stdout,"after %f, seq %lu\n",cv1->c_balance,seq);
          seq = tx_->get(CHECK,id,(char **)(&cv1),sizeof(checking::value));
          fprintf(stdout,"after2 %f, seq %lu\n",cv1->c_balance,seq);
          assert(false);
        }
#endif
        return txn_result_t(ret,73);
      }

      txn_result_t BankWorker::txn_send_payment_fasst(yield_func_t &yield) {

        tx_->begin(db_logger_);
        uint64_t id0,id1;
        GetTwoAccount(random_generator[cor_id_],&id0,&id1);
        float amount = 5.0;

        checking::value *c0, *c1; int idx0,idx1;

        int pid = AcctToPid(id0);
#if FASST == 1
        tx_->remoteset->add(REQ_READ_LOCK,pid,CHECK,id0);
#else
        tx_->add_to_remote_set(CHECK,id0,pid,yield);
#endif
        pid = AcctToPid(id1);
#if FASST == 1
        tx_->remoteset->add(REQ_READ_LOCK,pid,CHECK,id1);
#else
        tx_->add_to_remote_set(CHECK,id1,pid,yield);
#endif

#if FASST == 1
        auto replies = tx_->remoteset->do_reads(2);
#else
        auto replies = tx_->do_remote_reads();
#endif

        indirect_yield(yield);

#if FASST == 1
        bool ret = tx_->remoteset->get_results_readlock(replies);
#else
        tx_->get_remote_results(replies);
#endif

        tx_->get_cached(0,(char **)(&c0));
        tx_->get_cached(1,(char **)(&c1));

        if(c0->c_balance < amount) {
        } else {
          c0->c_balance -= amount;
          c1->c_balance += amount;
        }

        tx_->remote_write(0,(char *)c0,sizeof(checking::value));
        tx_->remote_write(1,(char *)c1,sizeof(checking::value));

#if FASST == 1
        if(unlikely(!ret)) {
            tx_->remoteset->release_remote();
            tx_->remoteset->clear_for_reads();
            tx_->abort();
            return txn_result_t(false,0);
        }
        ret = tx_->end_fasst(yield);
#else
        auto ret = tx_->end(yield);
#endif
        return txn_result_t(ret,73);
      }

      txn_result_t BankWorker::txn_transact_savings_fasst(yield_func_t &yield) {

        tx_->begin(db_logger_);
        float amount   = 20.20; //from original code
        uint64_t id;
        GetAccount(random_generator[cor_id_],&id);
        int pid = AcctToPid(id);

#if FASST == 1
        tx_->remoteset->add(REQ_READ_LOCK,pid,SAV,id);
        tx_->remoteset->do_reads(1);
#else
        tx_->add_to_remote_set(SAV,id,pid,yield);
        tx_->do_remote_reads();
#endif

        indirect_yield(yield);


#if FASST == 1
        bool ret = tx_->remoteset->get_results_readlock(1);
#else
        tx_->get_remote_results(1);
#endif

        savings::value *sv;
        uint64_t seq = tx_->get_cached(0,(char **)(&sv));
        sv->s_balance += amount;
        tx_->remote_write(0,(char *)sv,sizeof(savings::value));
#if FASST == 1
        if(unlikely(!ret)) {
          tx_->remoteset->release_remote();
          tx_->remoteset->clear_for_reads();
          tx_->abort();
          return txn_result_t(false,0);
        }

        ret = tx_->end_fasst(yield);
#else
        auto ret = tx_->end(yield);
#endif
        return txn_result_t(ret,73);
      }

      txn_result_t BankWorker::txn_write_check_fasst(yield_func_t &yield) {

        tx_->begin(db_logger_);
        float amount = 5.0; //from original code

        uint64_t id;
        GetAccount(random_generator[cor_id_],&id);
        int pid = AcctToPid(id);

#if FASST == 1
        tx_->remoteset->add(REQ_READ,pid,SAV,id);
        tx_->remoteset->add(REQ_READ_LOCK,pid,CHECK,id);
#else
        tx_->add_to_remote_set(SAV,id,pid,yield);
        tx_->add_to_remote_set(CHECK,id,pid,yield);
#endif
        //auto replies = tx_->remoteset->do_reads(0);
        auto replies = tx_->do_remote_reads();

        indirect_yield(yield);

#if FASST == 1
        bool res = tx_->remoteset->get_results_readlock(replies);
#else
        tx_->get_remote_results(replies);
#endif

        savings::value *sv;
        checking::value *cv;
        auto seq = tx_->get_cached(0,(char **)(&sv));
        //assert(seq != 0);
        seq = tx_->get_cached(1,(char **)(&cv));
        //assert(seq != 0);

        auto total = sv->s_balance + cv->c_balance;
        if(total < amount) {
          cv->c_balance -= (amount - 1);
        } else
          cv->c_balance -= amount;

        tx_->remote_write(1,(char *)cv,sizeof(checking::value));

#if FASST == 1
        if(unlikely(!res)) {
            tx_->remoteset->release_remote();
            tx_->remoteset->clear_for_reads();
            tx_->abort();
            return txn_result_t(false,0);
        }
#endif

#if FASST == 1
        bool ret = tx_->end_fasst(yield);
#else
        bool ret = tx_->end(yield);
#endif
        return txn_result_t(ret,73);
      }

      txn_result_t BankWorker::txn_amal_fasst(yield_func_t &yield) {

        tx_->begin(db_logger_);
        uint64_t id0,id1;
      retry:
        GetTwoAccount(random_generator[cor_id_],&id0,&id1);

        checking::value *c0,*c1; savings::value *s0,s1;

        int pid0 = AcctToPid(id0);
        int pid1 = AcctToPid(id1),idx1;
#if FASST == 1
        tx_->remoteset->add(REQ_READ_LOCK,pid0,SAV,id0);
        tx_->remoteset->add(REQ_READ_LOCK,pid0,CHECK,id0);
        tx_->remoteset->add(REQ_READ_LOCK,pid1,CHECK,id1);
#else
        tx_->add_to_remote_set(SAV,id0,pid0,yield);
        tx_->add_to_remote_set(CHECK,id0,pid0,yield);
        tx_->add_to_remote_set(CHECK,id1,pid1,yield);
#endif

        auto replies = tx_->do_remote_reads();

        indirect_yield(yield);
#if FASST == 1
        bool res = tx_->remoteset->get_results_readlock(replies);
#else
        tx_->get_remote_results(replies);
#endif

        double total = 0;

        auto seq = tx_->get_cached(0,(char **)(&s0));
        //assert(seq != 0);
        seq = tx_->get_cached(1,(char **)(&c0));
        //        assert(seq != 0);

        total = s0->s_balance + c0->c_balance;

        s0->s_balance = 0;
        c0->c_balance = 0;

        tx_->remote_write(0,(char *)s0,sizeof(savings::value));
        tx_->remote_write(1,(char *)c0,sizeof(checking::value));

        seq = tx_->get_cached(2,(char **)(&c1));
        //assert(seq != 0);
        c1->c_balance += total;
        tx_->remote_write(2,(char *)c1,sizeof(checking::value));

#if FASST == 1
        if(unlikely(!res)) {
          // no early aborts here
            tx_->remoteset->release_remote();
            tx_->remoteset->clear_for_reads();
            tx_->abort();
            return txn_result_t(false,0);
        }

        bool ret = tx_->end_fasst(yield);
#else
        bool ret = tx_->end(yield);
#endif
        return txn_result_t(ret,73); // since readlock success, so no need to abort
      }

    };

  };
};
