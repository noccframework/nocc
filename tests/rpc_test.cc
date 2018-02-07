#include "../db/dbrpc.h"
#include "../librdma/include/rdmaio.h"

#include "../librdma/include/ralloc.h"

#include <stdio.h>
#include <string.h>

using namespace nocc_framework;

void rpc_test(int id,char *msg,void *arg) {
  //  fprintf(stdout,"receive msg %s\n",msg);
  
  DBRpc *rpc = (DBRpc *)arg;
  char *reply = rpc->get_reply_buf();
  const char *reply_msg = "haha";
  memcpy(reply,reply_msg,strlen(reply_msg) + 1);

  rpc->send_reply(strlen(reply_msg) + 1,id);
}

int main(int argc, char** argv){
  
  if(argc < 2) {
    fprintf(stderr,"need one parmenter for machine id ...\n");
    exit(-1);
  }
  
  int id = atol(argv[1]);

  char *buffer = (char *)malloc(1024 * 1024 * 1024);  
  
  std::vector<std::string> netDef;
  
  netDef.push_back("10.0.0.100");
  netDef.push_back("10.0.0.101");

  int port = 8888;
  RDMAQueues *rdma = bootstrapRDMA(id,port,netDef,1,buffer,1024 * 1024 * 1024);
  uint64_t total = 4 * 1024;
  uint64_t padding = 64;
  uint64_t ringsz = total - padding - MSG_META_SZ;
  RInit(buffer + total * 2 + 1024, 1024 * 1024 * 1024 - 1024 - total);
  RThreadLocalInit();

  RingMessage *msg = new RingMessage( ringsz,padding,0,rdma,buffer);
  nocc_framework::DBRpc *rpc = new nocc_framework::DBRpc(msg);
  rpc->register_callback(rpc_test,0);
  
  if(id == 1) {
    char reply[1024];    
    memset(reply,0,1024);
    int reply_size = 0;
    int i = 0;
    while(1) {
      if(rpc->poll_comps()) {
	reply_size += strlen(reply);	
	i += 1;
	if(i > 12000) {
	  fprintf(stdout,"break %d\n",i);	  
	  break;
	}
	//	fprintf(stdout,"size %d\n",strlen(reply));
	rpc->clear_reqs();
	char *buf = rpc->get_req_buf();
	int msg_size = i;
	//	scanf ("%s",buf);
	//	fprintf(stdout,"start to send msg %s\n",buf);
	rpc->new_req(1);
	rpc->send_reqs(0,strlen(buf) + 1,reply);
      }
    }
    
    fprintf(stdout,"reply size %d\n",reply_size);
  } else {
    while(true) {
      rpc->poll_comps();
    }
  }
}
