#include "bank_worker.h"
#include "db/txs/dbtx.h"
#include "db/txs/dbsi.h"
#include "db/txs/db_farm.h"
#include "db/forkset.h"

#include "db/txs/si_ts_manager.h"

#include "util/util.h"

#include <boost/bind.hpp>

//#define FASST
#define unlikely(x) __builtin_expect(!!(x), 0)

extern nocc::db::TSManager *ts_manager;

extern __thread RemoteHelper *remote_helper;

extern size_t coroutine_num;
extern size_t current_partition;

namespace nocc {

  namespace oltp {

    extern __thread util::fast_random   *random_generator;
    extern RdmaCtrl *cm;

    namespace bank {

      Breakdown_Timer compute_timer;
      Breakdown_Timer send_timer;

      extern unsigned g_txn_workload_mix[6];

        /* input generation */
        void GetAccount(util::fast_random &r, uint64_t *acct_id) {
          uint64_t nums_global;
          if(r.next() % 100 < TX_HOT) {
            nums_global = NumAccounts();
          } else {
            nums_global = NumHotAccounts();
          }
          *acct_id = r.next() % nums_global;
        }

        void GetTwoAccount(util::fast_random &r,
                           uint64_t *acct_id_0, uint64_t *acct_id_1)  {
          uint64_t nums_global;
          if(r.next() % 100 < TX_HOT) {
            nums_global = NumAccounts();
          } else {
            nums_global = NumHotAccounts();
          }
          *acct_id_0 = r.next() % nums_global;
          *acct_id_1 = r.next() % nums_global;
          while(*acct_id_1 == *acct_id_0) {
            *acct_id_1 = r.next() % nums_global;
          }
        }


        BankWorker::BankWorker(unsigned int id,unsigned long seed,MemDB *db,uint64_t total_ops,
                               spin_barrier *a, spin_barrier *b,BenchRunner *context):
          BenchWorker(id,true,seed,total_ops,a,b,context),
          store_(db){

          // clear timer states
          compute_timer.report();
          send_timer.report();
        }

        void BankWorker::register_callbacks() {
        }

        void BankWorker::check_consistency() {

        }

        void BankWorker::balance_piece(int id, int cid, char *input, yield_func_t &yield) {

          balance_req_header *header = (balance_req_header *)input;
          assert(false);
#ifdef OCC_TX
          RemoteHelper *h =  remote_helper;
          h->begin(_QP_ENCODE_ID(id,cid + 1));
#endif

#ifdef SI_TX
          uint64_t timestamp = (uint64_t)(&(header->ts_vec));
#else
          uint64_t timestamp = header->time;
#endif

          checking::value cv;
          savings::value sv;
          uint64_t seq = tx_->get_ro_versioned(CHECK,header->id,(char *)(&cv),timestamp,yield);
          //      if(seq == 1) {
          //	fprintf(stdout,"id %lu failed\n",header->id);
          //	assert(false);
          //      }
          seq = tx_->get_ro_versioned(SAV,header->id,(char *)(&sv),timestamp,yield);
          assert(seq != 1);

          double res = cv.c_balance + sv.s_balance;
          double *reply_msg  = (double *)(rpc_handler_->get_reply_buf());
          *reply_msg = res;
          rpc_handler_->send_reply(sizeof(double), id, (char *)reply_msg,cid);
        }

        txn_result_t BankWorker::txn_balance2(yield_func_t &yield) {

          tx_->begin();
          uint64_t id;
        retry:
          GetAccount(random_generator[cor_id_],&(id));
          int pid = AcctToPid(id);

          double res = 0.0;
          if(pid != current_partition) {

            tx_->add_to_remote_set(CHECK,id,pid,yield);
            tx_->add_to_remote_set(SAV,id,pid,yield);

            tx_->do_remote_reads();
            indirect_yield(yield);
            tx_->get_remote_results(1);

            checking::value *cv;
            savings::value  *sv;
            tx_->get_cached(0,(char **)(&cv));
            tx_->get_cached(1,(char **)(&sv));

            res = cv->c_balance + sv->s_balance;

          } else {
            checking::value *cv;
            savings::value *sv;
            tx_->get(CHECK,id,(char **)(&cv),sizeof(checking::value));
            tx_->get(SAV,id,(char **)(&sv),sizeof(savings::value));
            res = cv->c_balance + sv->s_balance;
          }


          bool ret = tx_->end(yield);
          return txn_result_t(true,(uint64_t)res);
        }

        txn_result_t BankWorker::txn_write_check(yield_func_t &yield) {

          tx_->begin(db_logger_);

          float amount = 5.0; //from original code

          uint64_t id;
          GetAccount(random_generator[cor_id_],&id);
          int pid = AcctToPid(id);

          if(pid != current_partition) {
            tx_->add_to_remote_set(SAV,id,pid);
            tx_->add_to_remote_set(CHECK,id,pid);
            tx_->do_remote_reads();
            indirect_yield(yield);
            //yield(routines_[MASTER_ROUTINE_ID]);
            tx_->get_remote_results(1);

            savings::value *sv;
            checking::value *cv;

            tx_->get_cached(0,(char **)(&sv));
            tx_->get_cached(1,(char **)(&cv));

            auto total = sv->s_balance + cv->c_balance;
            if(total < amount) {
              cv->c_balance -= (amount - 1);
            } else
              cv->c_balance -= amount;

            tx_->remote_write(0,(char *)sv,sizeof(savings::value));
            tx_->remote_write(1,(char *)cv,sizeof(checking::value));

          } else {
            savings::value *sv;
            checking::value *cv;
            tx_->get(SAV,id,(char **)(&sv),sizeof(savings::value));
            tx_->write();
            tx_->get(CHECK,id,(char **)(&cv),sizeof(checking::value));
            tx_->write();

            auto total = sv->s_balance + cv->c_balance;
            if(total < amount) {
              cv->c_balance -= (amount - 1);
            } else
              cv->c_balance -= amount;

          }
          //tx_->remoteset->need_validate_ = true;
          bool ret = tx_->end(yield);
          return txn_result_t(ret,9);
        }

        txn_result_t BankWorker::txn_transact_savings(yield_func_t &yield) {

          tx_->begin(db_logger_);

          float amount   = 20.20; //from original code
          uint64_t id;
          GetAccount(random_generator[cor_id_],&id);
          int pid = AcctToPid(id);

          if(pid != current_partition) {
            tx_->add_to_remote_set(SAV,id,pid);
            tx_->do_remote_reads();
            indirect_yield(yield);
            tx_->get_remote_results(1);

            savings::value *sv;
            tx_->get_cached(0,(char **)(&sv));
            sv->s_balance += amount;
            tx_->remote_write(0,(char *)sv,sizeof(savings::value));

          } else {
            savings::value *sv;
            tx_->get(SAV,id,(char **)(&sv),sizeof(savings::value));
            sv->s_balance += amount;
            tx_->write();
          }

          bool ret = tx_->end(yield);
          return txn_result_t(ret,73);
        }

        txn_result_t BankWorker::txn_balance(yield_func_t &yield) {

          ForkSet fork_handler(rpc_handler_,cor_id_);
#ifdef OCC_TX
          ((DBTX *)tx_)->local_ro_begin();
#else
          tx_->begin();
#endif

          balance_req_header req;
          GetAccount(random_generator[cor_id_],&(req.id));
          int pid = AcctToPid(req.id);

          double res = 0;
          if(pid != current_partition) {
#ifdef RAD_TX
            req.time = ((DBRad *)tx_)->get_timestamp();
#endif
            fork_handler.add(pid);
            fork_handler.fork(0,(char *)(&req),sizeof(struct balance_req_header));
            yield(routines_[MASTER_ROUTINE_ID]);

#ifdef OCC_TX
            fork_handler.reset();
            //	fork_handler.do_fork(1024);
            fork_handler.fork(RPC_R_VALIDATE,0);
            //	assert(rpc_handler_->required_replies_ > 0);
            indirect_yield(yield);
            /* parse reply */
            int8_t *reply_p = (int8_t *)( fork_handler.reply_buf_);
#if 1
            if(!reply_p[0]) {
              return txn_result_t(false,0);
            }
#endif
            return txn_result_t(true,res);
#endif

          } else {
            /* local part */
            checking::value cv;
            savings::value sv;
            tx_->get_ro(CHECK,req.id,(char *)(&cv),yield);
            tx_->get_ro(SAV,req.id,(char *)(&sv),yield);
            res = cv.c_balance + sv.s_balance;
          }
#ifdef OCC_TX
          bool ret = ((DBTX *)tx_)->end_ro();
          return txn_result_t(ret,res);
#endif
          return txn_result_t(true,res);
        }

        txn_result_t
        BankWorker::txn_deposit_checking(yield_func_t &yield) {

          tx_->begin(db_logger_);
          float amount = 1.3;
        retry:
          uint64_t id;
          GetAccount(random_generator[cor_id_],&id);
          int pid = AcctToPid(id);
          //if(pid == current_partition) goto retry;
          //          fprintf(stdout,"key %lu, pid %d\n",id,pid);
          checking::value *cv = NULL;

          if(pid != current_partition) {
            tx_->add_to_remote_set(CHECK,id,pid);
            tx_->do_remote_reads();
            indirect_yield(yield);
            tx_->get_remote_results(1); // only single server operation

            uint64_t seq = tx_->get_cached(0,(char **)(&cv));
            assert(seq != 1);
            cv->c_balance += 1;
            tx_->remote_write(0,(char *)cv,sizeof(checking::value));

          } else {
            uint64_t seq = tx_->get(CHECK,id,(char **)(&cv),sizeof(checking::value));
            assert(seq != 1);
            cv->c_balance += 1;
            tx_->write();
          }
          bool ret = tx_->end(yield);
          return txn_result_t(ret,73);
        }

        txn_result_t BankWorker::txn_amal(yield_func_t &yield) {

          tx_->begin(db_logger_);
          uint64_t id0,id1;
          GetTwoAccount(random_generator[cor_id_],&id0,&id1);

          checking::value *c0,*c1; savings::value *s0,s1;
          int remote_cnt(0);

          int pid0 = AcctToPid(id0);
          int pid1 = AcctToPid(id1),idx1;

          if(pid0 != current_partition) {
            int idx = tx_->add_to_remote_set(SAV,id0,pid0);
            assert(idx == 0);
            tx_->add_to_remote_set(CHECK,id0,pid0);
            remote_cnt += 1;
          }
          if(pid1 != current_partition) {
            idx1 = tx_->add_to_remote_set(CHECK,id1,pid1);
            remote_cnt += 1;
          }

          int num_servers = 0;
          if(remote_cnt) {
            num_servers = tx_->do_remote_reads();
            indirect_yield(yield);
            //yield(routines_[MASTER_ROUTINE_ID]);
            tx_->get_remote_results(num_servers);
          } else {

          }

          double total;
          if(pid0 != current_partition) {
            tx_->get_cached(0,(char **)(&s0));
            tx_->get_cached(1,(char **)(&c0));
            total = s0->s_balance + c0->c_balance;
            s0->s_balance = 0;
            tx_->remote_write(0,(char *)(s0),sizeof(savings::value));
            c0->c_balance = 0;
            tx_->remote_write(1,(char *)(c0),sizeof(checking::value));
          } else {
            tx_->get(SAV,id0,(char **)(&s0),sizeof(savings::value));
            tx_->write();
            tx_->get(CHECK,id0,(char **)(&c0),sizeof(checking::value));
            tx_->write();

            total = s0->s_balance + c0->c_balance;
            s0->s_balance = 0;
            c0->c_balance = 0;
          }
          if(pid1 != current_partition) {
            tx_->get_cached(idx1,(char **)(&c1));
            c1->c_balance += total;
            tx_->remote_write(idx1,(char *)(c1),sizeof(checking::value));
          } else {
            tx_->get(CHECK,id1,(char **)(&c1),sizeof(checking::value));
            c1->c_balance += total;
            tx_->write();
          }

          bool ret = tx_->end(yield);
          return txn_result_t(ret,73);
        }

        txn_result_t BankWorker::txn_send_payment(yield_func_t &yield) {

          tx_->begin(db_logger_);
          uint64_t id0,id1;
          GetTwoAccount(random_generator[cor_id_],&id0,&id1);
          //      fprintf(stdout,"get id %lu %lu\n",id0,id1);
          //      sleep(1);
          float amount = 5.0;
          //      fprintf(stdout,"start check %p\n",store_);
          //      assert(store_->Get(CHECK,id0) != NULL);

          checking::value *c0, *c1; int idx0,idx1;
          assert(id0 != id1);
          int remote_cnt(0);
          int pid0 = AcctToPid(id0);
          if(pid0 != current_partition) {
            idx0 = tx_->add_to_remote_set(CHECK,id0,pid0);
            remote_cnt += 1;
          }
          int pid1 = AcctToPid(id1);
          if(pid1 != current_partition) {
            idx1 = tx_->add_to_remote_set(CHECK,id1,pid1);
            remote_cnt += 1;
          }

          int num_servers = 0;
          if(remote_cnt) {
            num_servers = tx_->do_remote_reads();
            indirect_yield(yield);
            //yield(routines_[MASTER_ROUTINE_ID]);
            tx_->get_remote_results(num_servers);
          }

          if(pid0 != current_partition) {

            uint64_t seq = tx_->get_cached(idx0,(char **)(&c0));
#ifdef SI_TX
            if(unlikely(seq == 1)) return txn_result_t(false,0);
#else
            assert(seq != 1);
#endif
            tx_->remote_write(idx0,(char *)c0,sizeof(checking::value));
          } else {
            uint64_t seq = tx_->get(CHECK,id0,(char **)(&c0),sizeof(checking::value));
#ifdef SI_TX
            if(unlikely(seq == 1)) return txn_result_t(false,0);
#else
            assert(seq != 1);
#endif
            tx_->write();
          }
          if(pid1 != current_partition) {
            uint64_t seq = tx_->get_cached(idx1,(char **)(&c1));
            assert(seq != 1);
            tx_->remote_write(idx1,(char *)c1,sizeof(checking::value));
          } else {
            uint64_t seq = tx_->get(CHECK,id1,(char **)(&c1),sizeof(checking::value));
            assert(seq != 1);
            tx_->write();
          }

          if(c0->c_balance < amount) {
            tx_->remoteset->clear_for_reads();
            tx_->remoteset->update_write_buf();
            tx_->abort();
            return txn_result_t(true,73);
          }

          c0->c_balance -= amount;
          c1->c_balance += amount;

          bool ret = tx_->end(yield);
          return txn_result_t(ret,73);
        }

        workload_desc_vec_t BankWorker::get_workload() const {
          return _get_workload();
        }

        workload_desc_vec_t BankWorker::_get_workload() {

          workload_desc_vec_t w;
          unsigned m = 0;
          for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
            m += g_txn_workload_mix[i];
          ALWAYS_ASSERT(m == 100);

          if(g_txn_workload_mix[0]) {
            w.push_back(workload_desc("SendPayment", double(g_txn_workload_mix[0])/100.0, TxnSendPayment));
          }
          if(g_txn_workload_mix[1]) {
            w.push_back(workload_desc("DepositChecking",double(g_txn_workload_mix[1])/100.0,TxnDepositChecking));
          }
          if(g_txn_workload_mix[2]) {
            w.push_back(workload_desc("Balance",double(g_txn_workload_mix[2])/100.0,TxnBalance));
          }
          if(g_txn_workload_mix[3]) {
            w.push_back(workload_desc("Transact saving",double(g_txn_workload_mix[3])/100.0,TxnTransactSavings));
          }
          if(g_txn_workload_mix[4]) {
            w.push_back(workload_desc("Write check",double(g_txn_workload_mix[4])/100.0,TxnWriteCheck));
          }
          if(g_txn_workload_mix[5]) {
            w.push_back(workload_desc("Txn amal",double(g_txn_workload_mix[5])/100.0,TxnAmal));
          }
          return w;
        }

        void BankWorker::thread_local_init() {

          assert(store_ != NULL);
          for(uint i = 0;i < coroutine_num + 1;++i) {
            // init TXs
#ifdef RAD_TX
            txs_[i] = new DBRad(store_,worker_id_,rpc_handler_,i);
#elif defined(OCC_TX)
            txs_[i] = new DBTX(store_,worker_id_,rpc_handler_,i);
            remote_helper = new RemoteHelper(store_,total_partition,coroutine_num + 1);
            //#elif defined(SI_TX)
#elif defined(FARM)
            txs_[i] = new DBFarm(cm,rdma_sched_,store_,worker_id_,rpc_handler_,i);
#elif defined(SI_TX)
            txs_[i] = new DBSI(store_,worker_id_,rpc_handler_,ts_manager,i);
#else
            fprintf(stderr,"No transaction layer used!\n");
            assert(false);
#endif
          }
          /* init local tx so that it is not a null value */
          tx_ = txs_[cor_id_];
          for(int i = 1 + coroutine_num; i < 1 + coroutine_num; i++){
            txs_[i] = tx_;
          }
          routine_1_tx_ = txs_[1]; // used for report
        } // end func: thread_local_init

      };
    };
  };
