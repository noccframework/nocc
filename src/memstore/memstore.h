#ifndef DRTMMEMSTORE
#define DRTMMEMSTORE

#include "all.h"
#include "util/spinlock.h"
#include "framework/rdma_sched.h"

#include "rdmaio.h"

#include <stdlib.h>

#define MEMSTORE_MAX_TABLE 16

struct MemNode
{
  volatile uint64_t lock;
  uint64_t seq;

  //  uint64_t counter;
  uint64_t* value; // pointer to the real value. 1: logically delete 2: Node is removed from memstore
  union {
    uint64_t* old_value;
    uint64_t  off;
  };
  SpinLock* read_fallback_lock;
  char padding[24];
  uint64_t read_ts;
  volatile uint64_t read_lock;
  MemNode()
  {
    lock = 0;
    read_ts = 0;
    read_lock = 0;
    seq = 0;
    value = NULL;
    read_fallback_lock = new SpinLock();
  }
} __attribute__ ((aligned (CACHE_LINE_SZ)));

class Memstore {
 public:
  class Iterator {
  public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    Iterator() {}

    virtual bool Valid() = 0;

    // REQUIRES: Valid()
    virtual MemNode* CurNode() = 0;

    virtual uint64_t Key() = 0;

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual bool Next() = 0;

    // Advances to the previous position.
    // REQUIRES: Valid()
    // REQUIRES: Valid()
    virtual bool Prev() = 0;

    // Advance to the first entry with a key >= target
    virtual void Seek(uint64_t key) = 0;

    virtual void SeekPrev(uint64_t key) = 0;

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToFirst() = 0;

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToLast() = 0;

    virtual uint64_t* GetLink() = 0;

    virtual uint64_t GetLinkTarget() = 0;
  };

  virtual Iterator *GetIterator() { return NULL;}
  virtual MemNode* Put(uint64_t k, uint64_t* val) = 0;
  virtual MemNode* Get(uint64_t key) = 0;
  virtual bool     CompareKey(uint64_t k0,uint64_t k1) { return true;}

  MemNode* GetWithInsert(uint64_t key,char *val = NULL) {
    return _GetWithInsert(key,val);
  }
  virtual MemNode* _GetWithInsert(uint64_t key,char *val) = 0;

  // return the remote offset of the true value
  virtual uint64_t RemoteTraverse(uint64_t key,rdmaio::Qp *qp) {
    assert(false); // If supported, then shall re-write
    return 0;
  }
  virtual uint64_t RemoteTraverse(uint64_t key,rdmaio::Qp *qp,
                                  nocc::oltp::RDMA_sched *sched, yield_func_t &yield) {
    assert(false); return 0;
  }
};

#endif
