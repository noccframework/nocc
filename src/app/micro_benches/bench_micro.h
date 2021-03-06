#ifndef NOCC_OLTP_MICRO_H
#define NOCC_OLTP_MICRO_H

#include "all.h"
#include "./app/config.h"  // application specific config
#include "global_config.h" // global config
#include "db/db_statistics_helper.h"

#include "framework/backup_worker.h"
#include "framework/framework.h"
#include "db/txs/tx_handler.h"

#define MAX_REQ_NUM 100

extern size_t current_partition;

namespace nocc {

	namespace oltp {

		namespace micro {

			enum RPC_TYPE {
				RPC_NOP = 1,
				RPC_NULL,
				RPC_MICRO_READ,
				RPC_READ,
				RPC_BATCH_READ,
				RPC_WRITE,
				RPC_BATCH_WRITE
			};

			enum MICRO_TYPE {
				MICRO_RPC_SCALE  = 1,
				MICRO_RDMA_SCALE = 2,
				MICRO_RDMA_DOORBELL_SCALE,
				MICRO_RPC_STRESS,
				MICRO_LOGGER_FUNC = 5,
				MICRO_RDMA_SCHED,
				MICRO_FARM_ONE_SIDED,
				MICRO_FARM_QP_SHARING = 8,
				MICRO_RDMA_RW,
				MICRO_RDMA_READ_MULTI,
				MICRO_RDMA_WRITE_MULTI = 11,
				MICRO_LOGGER_WRITE,
				MICRO_RPC_READ,
				MICRO_RDMA_READ = 14,
				MICRO_RDMA_WRITE,
				MICRO_RDMA_ATOMIC,
				MICRO_RDMA_ATOMIC_MULTI = 17
			};

			// main test function
			void MicroTest(int argc,char **argv) ;

			struct Stat {
				int count;
			};

			struct micro_cycle_info {
				uint64_t post_cycles[64];
				uint64_t completion_cycles[64];
				uint64_t iter_tot_post_cycle = 0;
				uint64_t iter_tot_poll_cycle = 0;
				uint64_t poll_once_cycle = 0;
				uint64_t total_poll_cycle = 0;
				uint64_t poll_counts = 0;
				struct ibv_wc wcs_[64];
				micro_cycle_info(){}
			};


			class MicroWorker : public BenchWorker {

				struct ReadReqHeader {
					uint8_t num;
				};

				struct ReadReq {
					uint64_t off  : 40;
			        uint64_t pid  : 8;
					uint64_t size : 16;
				};

				struct ReadReqWrapper {
					uint64_t header;
					struct rpc_header  rpc_padding;
					struct ReadReq req;
					uint64_t tailer;
				} __attribute__ ((aligned(8)));


			public:
				MicroWorker(unsigned int worker_id,unsigned long seed,int micro_type,
							uint64_t total_ops, spin_barrier *a,spin_barrier *b,BenchRunner *c);

				virtual void register_callbacks();
				virtual void thread_local_init();

				virtual void workload_report();

				// worker functions
				txn_result_t micro_rpc_scale(yield_func_t &yield);
				txn_result_t micro_rdma_scale(yield_func_t &yield);
				txn_result_t micro_rdma_doorbell_scale(yield_func_t &yield);
				txn_result_t micro_rpc_stress(yield_func_t &yield);
				txn_result_t micro_rdma_sched(yield_func_t &yield);
				txn_result_t micro_farm_one_sided(yield_func_t &yield);
				txn_result_t micro_farm_qp_sharing(yield_func_t &yield);

				// Context: used to test the raw throughput of RDMA one-sided READ/WRITE
				txn_result_t micro_rdma_one_op(yield_func_t &yield);

				// Context: TX issues multiple read requests to remote data store(s)
				txn_result_t micro_rdma_read_multi(yield_func_t &yield); // one-sided version
				txn_result_t micro_rpc_read_multi(yield_func_t &yield);  // RPC version of this micro

				// Context: TX issues multiple write requests to remote data store(s)
				txn_result_t micro_rdma_write_multi(yield_func_t &yield);
				txn_result_t micro_rpc_write_multi(yield_func_t &yield); // RPC version

				// Context: TX issues various rdma write requestds to remote data store(s)
				txn_result_t micro_rdma_write(yield_func_t &yield);
				txn_result_t micro_rpc_write(yield_func_t &yield);

				/*
					Context: Logging part:
						- logger_func: issue a full loggging function
						- logger_write: emulate a Write-based logger
						- logger_RPC: emulate a RPC-based logger
				*/
				txn_result_t micro_logger_func(yield_func_t &yield);
				txn_result_t micro_logger_write(yield_func_t &yield);

				// Context: TX issues reads with various payload to remote data store(s)
				txn_result_t micro_rpc_read(yield_func_t &yield);  // rpc version
				txn_result_t micro_rdma_read(yield_func_t &yield); // one-sided version

				// Context: TX issues reads with various payload to remote data store(s)
				txn_result_t micro_rdma_atomic(yield_func_t &yield);
				txn_result_t micro_rdma_atomic_multi(yield_func_t &yield);

				/* comment ***************************************************/

				// micro rpc handlers
				void nop_rpc_handler(int id,int cid,char *msg,void *arg);
				void null_rpc_handler(int id,int cid,char *msg,void *arg);
				void micro_read_rpc_handler(int id,int cid,char *msg,void *arg);

				// used for micro_rpc_read_multi(non batching), read a CACHELINE for app
				void read_rpc_handler(int id,int cid,char *msg,void *arg);
				void various_read_rpc_handler(int id,int cid,char *msg,void *arg);
				void batch_read_rpc_handler(int id,int cid,char *msg,void *arg);
				void write_rpc_handler(int id,int cid,char *msg,void *arg);
				void batch_write_rpc_handler(int id,int cid,char *msg,void *arg);

				workload_desc_vec_t get_workload() const ;

			private:
				static  workload_desc_vec_t _get_workload();

				static txn_result_t MicroRpcScale(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_rpc_scale(yield);
					return r;
				}
				static txn_result_t MicroRdmaScale(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_rdma_scale(yield);
					return r;
				}
				static txn_result_t MicroRdmaDoorbellScale(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_rdma_doorbell_scale(yield);
					return r;
				}
				static txn_result_t MicroRpcStress(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_rpc_stress(yield);
					return r;
				}
				static txn_result_t MicroLoggerFunc(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_logger_func(yield);
					return r;
				}
				static txn_result_t MicroRDMASched(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_rdma_sched(yield);
					return r;
				}
				static txn_result_t MicroFarmOneSided(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_farm_one_sided(yield);
					return r;
				}
				static txn_result_t MicroFarmQpSharing(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_farm_qp_sharing(yield);
					return r;
				}
				static txn_result_t MicroRdmaOneRW(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_rdma_one_op(yield);
					return r;
				}
				static txn_result_t MicroRdmaMulti(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_rdma_read_multi(yield);
					return r;
				}
				static txn_result_t MicroRPCMulti(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_rpc_read_multi(yield);
					return r;
				}
				static txn_result_t MicroLoggerWrite(BenchWorker *w,yield_func_t &yield) {
					assert(false);
					return txn_result_t(true,1);
				}

				static txn_result_t MicroRdmaMultiWrite(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_rdma_write_multi(yield);
					return r;
				}

				static txn_result_t MicroRPCMultiWrite(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_rpc_write_multi(yield);
					return r;
				}

				static txn_result_t MicroRPCRead(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_rpc_read(yield);
					return r;
				}

				static txn_result_t MicroRDMARead(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_rdma_read(yield);
					return r;
				}

				static txn_result_t MicroRDMAAtomic(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_rdma_atomic(yield);
					return r;
				}

				static txn_result_t MicroRDMAMultiAtomic(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_rdma_atomic_multi(yield);
					return r;
				}

				static txn_result_t MicroRDMAWrite(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_rdma_write(yield);
					return r;
				}

				static txn_result_t MicroRPCWrite(BenchWorker *w,yield_func_t &yield) {
					txn_result_t r = static_cast<MicroWorker *>(w)->micro_rpc_write(yield);
					return r;
				}


				char* reply_buf_;   // buf used to receive RPC reply
				char** reply_bufs_; // buf used to receive RPC reply, one per coroutine

				struct ibv_qp **qps;
				struct ibv_cq **cqs;
				struct ibv_mr **mrs;
				uint64_t *addrs;
				uint64_t *rkeys;

				vector<Qp*> qps_;
				vector<bool> need_polls_;
				char* rdma_buf_; // buf used to send RDMA requests

				int per_thread_qp_num_;

				LAT_VARS(post);
			}; // class MicroWorker

		} // namespace micro
	}  // namespace oltp
}  // namespace nocc

#endif
