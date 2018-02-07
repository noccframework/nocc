#ifndef NOCC_UTIL_TIMER
#define NOCC_UTIL_TIMER

#include <stdint.h>
#include <vector>


namespace nocc {
  namespace util {

    class Breakdown_Timer {
      const uint64_t max_elems = 1000000;
    public:
      uint64_t sum;
      uint64_t count;
      uint64_t temp;
      std::vector<uint64_t> buffer;
    Breakdown_Timer(): sum(0), count(0) {}
      void start() { temp = rdtsc(); }
      void end() { auto res = (rdtsc() - temp);sum += res; count += 1;
        if(buffer.size() >= max_elems) return;
        buffer.push_back(res);
      }
      double report() {
        if(count == 0) return 0.0; // avoids divided by zero
        double ret =  (double) sum / (double)count;
        // clear the info
        //sum = 0;
        //count = 0;
        return ret;
      }

      void calculate_detailed() {
        if(buffer.size() == 0) return;
        // first erase some items
        int idx = std::floor(buffer.size() * 0.1 / 100.0);
        buffer.erase(buffer.begin(),buffer.begin() + idx + 1);

        // then sort
        std::sort(buffer.begin(),buffer.end());
        int num = std::floor(buffer.size() * 0.01 / 100.0);
        buffer.erase(buffer.begin() + buffer.size() - num,buffer.begin() + buffer.size());
      }

      double report_medium() {
        if(buffer.size() == 0) return 0;
        return buffer[buffer.size() / 2];
      }

      double report_90(){
        if(buffer.size() == 0) return 0;
        int idx = std::floor( buffer.size() * 90 / 100.0);
        return buffer[idx];
      }

      double report_99() {
        if(buffer.size() == 0) return 0;
        int idx = std::floor(buffer.size() * 99 / 100.0);
        return buffer[idx];
      }

      static uint64_t get_one_second_cycle() {
        uint64_t begin = rdtsc();
        sleep(1);
        return rdtsc() - begin;
      }
    };
  } // namespace util
}   // namespace nocc
#endif
