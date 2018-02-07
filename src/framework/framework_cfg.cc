// This file contains the parsing of the framework

#include "framework.h"

#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include <boost/algorithm/string.hpp>


/* global config constants */
extern size_t nthreads;                      // total of worker in used
extern size_t coroutine_num;                 // number of concurrent request per worker

extern size_t backup_nthreads;               // number of log cleaner
extern size_t scale_factor;                  // the scale of the database
extern size_t total_partition;               // total of machines in used
extern size_t current_partition;             // current worker id
extern std::string config_file;              // this file may not exesists

extern size_t distributed_ratio; // the distributed transaction's ratio


namespace nocc {

  namespace oltp {

    void BenchRunner::parse_config(std::string &config_file) {
      /* parsing the config file to git machine mapping*/
      using boost::property_tree::ptree;
      using namespace boost;
      using namespace property_tree;
      ptree pt;

      try {
        read_xml(config_file, pt);

        try {
#if USE_BACKUP_STORE && USE_BACKUP_BENCH_WORKER
          backup_nthreads = pt.get<size_t>("bench.backup_threads");
#endif
        } catch (const ptree_error &e) {
          fprintf(stderr,"parse_config:%s, it may be an error, or not.\n", e.what() );
        }

        //scale_factor = 2 * nthreads; // TODO!! now hard coded
        if(scale_factor == 0) scale_factor = nthreads;
        printf("scale_factor:%lu, nthreads:%lu\n",scale_factor, nthreads);

        BOOST_FOREACH(ptree::value_type &v,
                      pt.get_child("bench.servers.mapping"))
          {
            std::string  s = v.second.data();
            boost::algorithm::trim_right(s);
            boost::algorithm::trim_left(s);
            //    fprintf(stdout,"get config %s\n",s.c_str());
            net_def_.push_back(s);
          }

      } catch (const ptree_error &e) {
        /* using the default settings  */
        fprintf(stdout,"some error happens, using the default setting.\n");
        net_def_.push_back("10.0.0.100");
      }

      try {
        //coroutine_num = pt.get<size_t>("bench.routines");
      } catch (const ptree_error &e) {
        /* pass, using default setting  */
      }
      assert(coroutine_num >= 1);

      total_partition = net_def_.size();

    } // parse config parameter

  }; // namespace oltp

}; // namespace nocc
