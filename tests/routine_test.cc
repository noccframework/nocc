#include "../oltp/bench_worker.h"

int main() {
  
  nocc_framework::BenchWorker worker(0,true,0xdeadbeaf,1000);
  worker.run();
  return 0;
  
}
