#include "memdb.h"
#include "rdma_hashext.h"

#include "util/util.h"

using namespace nocc;

void MemDB::AddSchema(int tableid,TABLE_CLASS c,  int klen, int vlen, int meta_len) {

  int total_len = meta_len + vlen;

  // round up to the cache line size
  auto round_size = CACHE_LINE_SZ * 2;
  total_len = nocc::util::Round<int>(total_len,round_size);

  switch(c) {
  case TAB_BTREE:
    stores_[tableid] = new MemstoreBPlusTree();
    break;
  case TAB_BTREE1:
    stores_[tableid] = new MemstoreUint64BPlusTree(klen);
    break;
  case TAB_SBTREE:
    /* This is a secondary index, it does not need to set schema*/
    /* for backward compatability */
    assert(false);
    break;
  case TAB_HASH: {
    auto tabp = new drtm::memstore::RdmaHashExt(1024 * 1024 * 8,store_buffer_); //FIXME!! hard coded
    stores_[tableid] = tabp;
    // update the store buffer
    if(store_buffer_ != NULL) {
      store_buffer_ += tabp->size;
      store_size_ += tabp->size;
      uint64_t M = 1024 * 1024;
      assert(store_size_ < M * STORE_SIZE);
    }
  }
    break;
  default:
    fprintf(stderr,"Unsupported store type! tab %d, type %d\n",tableid,c);
    exit(-1);
  }
  _schemas[tableid].c = c;
  _schemas[tableid].klen = klen;
  _schemas[tableid].vlen = vlen;
  _schemas[tableid].meta_len = meta_len;
  _schemas[tableid].total_len = total_len;
}

void MemDB::EnableRemoteAccess(int tableid,rdmaio::RdmaCtrl *cm) {
  assert(_schemas[tableid].c == TAB_HASH); // now only HashTable support remote accesses
  assert(store_buffer_ != NULL);           // the table shall be allocated on an RDMA region
  drtm::memstore::RdmaHashExt *tab = (drtm::memstore::RdmaHashExt *)(stores_[tableid]);
  tab->enable_remote_accesses(cm);
}

void MemDB::AddSecondIndex(int index_id, TABLE_CLASS c, int klen) {
  _indexs[index_id] = new MemstoreUint64BPlusTree(klen);
}


uint64_t *MemDB::GetIndex(int tableid, uint64_t key) {
  MemNode *mn = _indexs[tableid]->Get(key);
  if(mn == NULL) return NULL;
  return mn->value;
}

uint64_t *MemDB::Get(int tableid,uint64_t key) {
  MemNode *mn = NULL;
  switch(_schemas[tableid].c) {
  case TAB_SBTREE: {
    assert(false);
    mn = _indexs[tableid]->Get(key);
  }
    break;
  default:
    /* otherwise, using store */
    mn = stores_[tableid]->Get(key);
    break;
  }
  if(mn == NULL)
    return NULL;
  return mn->value;
}

void MemDB::PutIndex(int indexid, uint64_t key,uint64_t *value){
  MemNode *mn = _indexs[indexid]->Put(key,value);
  mn->seq = 2;
}

void MemDB::Put(int tableid, uint64_t key, uint64_t *value) {

  switch(_schemas[tableid].c) {
  case TAB_SBTREE: {
    assert(false);
    MemNode *mn = _indexs[tableid]->Put(key,value);
    mn->seq = 2;
  }
    break;
  default:
    MemNode *mn = stores_[tableid]->Put(key,value);
     mn->seq = 2;
    break;
  }
}
