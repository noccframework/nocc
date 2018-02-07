## Makefile for test files


co:
	g++ -I.. ../oltp/bench_worker.cc routine_test.cc -o co -lboost_coroutine -lboost_system -std=c++0x 
ts:
	g++  ts_manager.cc ts_manager_test.cc -o test

rpc:
	g++ -I.. -pthread ../db/dbrpc.cc ../librdma/src/rdma_msg.cc  ../librdma/src/rdmaio.cc ../librdma/src/rdmarc.cc ../librdma/src/rdmaud.cc rpc_test.cc -o rpc -std=c++0x -lzmq -libverbs -lboost_system -lrt -lssmalloc 
clean:
	rm ts co rpc
