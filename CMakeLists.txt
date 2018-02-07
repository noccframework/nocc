cmake_minimum_required(VERSION 2.8)

project(nocc)

set(NOCC_ROOT "." )
#set(CMAKE_BUILD_TYPE Debug)

ADD_DEFINITIONS( -std=c++0x)

#set(CMAKE_CXX_COMPILER g++-4.8)
include_directories(./src)
include_directories(third_party/rdma_lib/include)
include_directories(third_party/rdma_lib/)

## third party libraries
include_directories(third_party/sparsehash-c11)
include_directories(third_party)


#with benchmark profiling
#set(MACRO_FLAGS "-DNDEBUG -DBASE_LINE -DBENCH_PROFILE")

## seperate install
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)

## without benchmark profiling 
#set(MACRO_FLAGS "-DNDEBUG -DBASE_LINE ")  ## ndebug will optimze out assert
set (MACRO_FLAGS "-DBASE_LINE")

## currently drtm in this codebase is not supported, i will fix this later 
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DLEVELDB_PLATFORM_POSIX -pthread -DOS_LINUX -mrtm -pthread -O2 -g ${MACRO_FLAGS}")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DLEVELDB_PLATFORM_POSIX -pthread -DOS_LINUX -mrtm -pthread  -O0 -g2 ${MACRO_FLAGS}")

## TODO, we shall replace it with a pre-complied lib, but since now the lib is not stable, so we just add sources here
file(GLOB RDMA_SOURCES "third_party/rdma_lib/src/*.cc" "third_party/micautil/*.cc")


## add main files
set(EGEN_PREFIX src/app/tpce/egen/)
file(GLOB SOURCES
          "src/app/micro_benches/*.cc" "src/app/tpcc/*.cc"  "src/app/smallbank/*.cc"             # apps
          "src/framework/*.cc"  "src/framework/utils/*.cc"                                       # framework
          "src/memstore/*.cc"                                                                    # memstore
          "src/db/*.cc" "src/db/txs/*.cc"                                                        # framework + CC
          "src/util/*.cc" "src/util/micautil/*.cc" "src/port/*.cc" "src/oltp/utils/*.cc"         # utils
          )

## current it's not well since not all file in *egen* is necessary
file(GLOB TPCE_SOURCES
          "${EGEN_PREFIX}DateTime.cpp" "${EGEN_PREFIX}error.cpp"
          "${EGEN_PREFIX}Random.cpp" "${EGEN_PREFIX}Money.cpp"
          "${EGEN_PREFIX}EGenVersion.cpp" "${EGEN_PREFIX}locking.cpp"
          "${EGEN_PREFIX}threading.cpp" "${EGEN_PREFIX}BaseLogger.cpp" "${EGEN_PREFIX}EGenLogFormatterTab.cpp"
          "${EGEN_PREFIX}MEEPriceBoard.cpp" "${EGEN_PREFIX}MEESecurity.cpp" "${EGEN_PREFIX}MEETickerTape.cpp"
          "${EGEN_PREFIX}MEETradingFloor.cpp" "${EGEN_PREFIX}WheelTime.cpp" "${EGEN_PREFIX}AddressTable.cpp"
          "${EGEN_PREFIX}CustomerSelection.cpp" "${EGEN_PREFIX}CustomerTable.cpp" "${EGEN_PREFIX}InputFlatFilesStructure.cpp"
          "${EGEN_PREFIX}Person.cpp" "${EGEN_PREFIX}ReadRowFunctions.cpp"
          "${EGEN_PREFIX}TradeGen.cpp" "${EGEN_PREFIX}FlatFileLoader.cpp" "${EGEN_PREFIX}CE.cpp" "${EGEN_PREFIX}CETxnInputGenerator.cpp"
          "${EGEN_PREFIX}CETxnMixGenerator.cpp" "${EGEN_PREFIX}DM.cpp" "${EGEN_PREFIX}EGenGenerateAndLoad.cpp" "${EGEN_PREFIX}strutil.cpp"
          "${EGEN_PREFIX}progressmeter.cpp" "${EGEN_PREFIX}progressmeterinterface.cpp" "${EGEN_PREFIX}bucketsimulator.cpp")

add_executable(noccocc ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
target_compile_options(noccocc PRIVATE "-DOCC_TX")

add_executable(noccsi ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
target_compile_options(noccsi PRIVATE "-DSI_TX")


add_executable(noccfarm ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
target_compile_options(noccfarm PRIVATE "-DFARM")

## Micro with one-sided RDMA
add_executable(nn ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
target_compile_options(nn PRIVATE "-DRPC=0" "-DOCC_TX")

# Micro with RDMA RPC
add_executable(nr ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
target_compile_options(nr PRIVATE "-DRPC=1" "-DOCC_TX")



target_link_libraries( noccocc zmq rt ibverbs ssmalloc  boost_system boost_coroutine)
target_link_libraries( noccsi  zmq rt ibverbs ssmalloc  boost_system boost_coroutine)
target_link_libraries( noccfarm  zmq rt ibverbs ssmalloc  boost_system boost_coroutine)
target_link_libraries( nn  zmq rt ibverbs ssmalloc  boost_system boost_coroutine)
target_link_libraries( nr  zmq rt ibverbs ssmalloc  boost_system boost_coroutine)

add_custom_command(TARGET noccocc
                   POST_BUILD
                   COMMAND mv noccocc scripts)

add_custom_command(TARGET nn
                   POST_BUILD
                   COMMAND mv nn scripts)

add_custom_command(TARGET nr
                   POST_BUILD
                   COMMAND mv nr scripts)

add_custom_command(TARGET noccfarm
                   POST_BUILD
                   COMMAND mv noccfarm scripts)


# for the ease of running
set(CMAKE_INSTALL_PREFIX ./)
install(TARGETS noccocc  DESTINATION scripts) 
install(TARGETS noccfarm DESTINATION scripts)
install(TARGETS noccsi   DESTINATION scripts)

