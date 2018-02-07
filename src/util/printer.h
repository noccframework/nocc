#ifndef NOCC_UTIL_DEBUG_H_
#define NOCC_UTIL_DEBUG_H_

#include <stdio.h>
#include <stdarg.h>
#include <string>

extern int verbose;

namespace nocc {
  namespace util {

    const int MAX_PRINT_BUF = 1024;
    // TODO: merge these two class
    class Debugger {
    public:
      static void debug_fprintf(FILE *out,std::string fmt, ...) {
        if(!verbose)return;

        va_list args;
        char buf[MAX_PRINT_BUF];

        va_start(args,fmt);
        vsprintf(buf,fmt.c_str(),args);
        va_end(args);

        fprintf(out,"%s",buf);
      }
    }; // class Debugger

    class SimplePrinter {

    public:
      SimplePrinter() {

      }

      void thread_local_init();

      template <typename... TS>
        static inline void
        assert_printf(bool res,const char *s,TS... args) {

        if(unlikely(!res)) {
          // assertion false
          fprintf(stderr,s,args...);
          assert(false);
        }
        // end
      }

    }; // class Simpleprinter

  }
} // nocc
#endif
