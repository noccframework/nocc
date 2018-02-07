**NOCC** framework is aimed to simplify building distributed applications using `RDMA`. 

- We are working hard to make it easy to use.

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

`cd scripts; ./run2.py config7.xml noccocc "-t 12 -c 10 -r 256" micro`

A template config file is `config_template.xml` is listed in the root directory.

***

