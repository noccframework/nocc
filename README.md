**NOCC** framework is aimed to simplify building distributed applications using `RDMA`. 

- We are working hard to make it easy to use.

A snippet of how to use nocc framewirk.

- For one-sided operations

```c++
// for one-sided RDMA reads, as an exmaple
int payload = 64; 
uint64_t remote_addr = 0;        // remote memory address(offset)
char *buffer = Rmalloc(payload); // allocate a local buffer to store the result
auto qp = cm->get_rc_qp(qp_id);  // the the qp handler for the target machine
qp->rc_post_send(IBV_WR_RDMA_READ,
                buffer,
			    payload,
                remote_addr,
                IBV_SEND_SIGNALED,
                cor_id_); // the current execute app id
sched->add_pending(qp,cor_id_); // add the pending request to the scheduler
indirect_yield();   // yield to other coroutine
// when executed, the buffer got the results of remote memory
Rfree(buffer);      // can free the buffer after that
```

- For two-sided(messaging) operations

```c++
int payload = 64;
char *msg_buf = Rmalloc(payload);  // send buffer
char *reply_buf = malloc(payload); // reply buffer
int id = remote_id; // remote server's id
rpc_handler->send_reqs(rpc_id, 
                       payload,
                       reply_buf,
                       &id, // the address of server list to send
                       1,   // number of servers to send
                       cor_id_);
indirect_yield();   // yield to other coroutine
// when executed, the buffer got the results of remote memory
Rfree(msg_buffer);      // can free the buffer after that
free(reply_buffer);     
```



***

**Dependencies:**

- CMake `>= version 2.8` (For compiling)


- Zmq `XX` (May be any version shall be fine, and current version ` >= 4.2.2` has been tested )
- Zmq C++ binding
- Boost `1.61.0` (Only tested)
- [LibRDMA](http://ipads.se.sjtu.edu.cn:1312/Windybeing/rdma_lib)
- [sparsehash](https://github.com/sparsehash/sparsehash-c11)

***

**Build:**

- Building **Ralloc** in [LibRDMA](http://ipads.se.sjtu.edu.cn:1312/Windybeing/rdma_lib)
  - `cd third_party/rdma_lib/`
  - `mkdir lib`
  - `cd ralloc`
  - `make;make install` 
- `cmake CMakeList`
- `make noccocc`

Be sure to add */nocc/third_party/rdma_lib/lib/ to `LD_LIBRARY_PATH`.

***

**Run:**

`cd scripts; ./run2.py config7.xml noccocc "-t 12 -c 10 -r 256" micro`, 

where `t` states for thread used, `c` states for coroutine used and `r` is left for workload.

A template config file:`config_template.xml` is in the root directory.

***

**Detailed config:**

You can modify `src/config.h`, `src/custom_config.h` and `src/app/config.h` to make customization of different running parameters.

We will soon make a detailed description and better configuration tools for NOCC.

***