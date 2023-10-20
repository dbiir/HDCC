#ifndef LOGGER_H
#define LOGGER_H

#include "global.h"
#include "helper.h"
#include "concurrentqueue.h"
#include <set>
#include <queue>
#include <fstream>
#include <boost/lockfree/queue.hpp>

enum LogRecType {
  LRT_INVALID = 0,
  LRT_INSERT,
  LRT_UPDATE,
  LRT_DELETE,
  LRT_TRUNCATE
};
enum LogIUD {
  L_INSERT = 0,
  L_UPDATE,
  L_DELETE,
  L_COMMIT,
  L_ABORT,
  L_FLUSH,
  L_C_FLUSH
};

// Command log record (logical logging)
struct CmdLogRecord {
  uint32_t checksum;
  uint64_t lsn;
  LogRecType type;
  uint64_t txn_id; // transaction id
  //uint32_t partid; // partition id
#if WORKLOAD==TPCC
  TPCCTxnType txntype;
#elif WORKLOAD==YCSB
  //YCSBTxnType txntype;
#endif
  uint32_t params_size;
  char * params; // input parameters for this transaction type
};

// ARIES-style log record (physiological logging)
struct AriesLogRecord {
  void init() {
    checksum = 0;
    lsn = UINT64_MAX;
    type = LRT_UPDATE;
    iud = L_UPDATE;
    txn_id = UINT64_MAX;
    table_id = 0;
    key = UINT64_MAX;
#if CC_ALG == HDCC
    max_calvin_tid = UINT64_MAX;
#endif
  }

  uint32_t checksum;
  uint64_t lsn;
  LogRecType type;
  LogIUD iud;
  uint64_t txn_id; // transaction id
  //uint32_t partid; // partition id
  uint32_t table_id; // table being updated
  uint64_t key; // primary key (determines the partition ID)
#if CC_ALG == HDCC
  uint64_t max_calvin_tid;
#endif
  // TODO: column list

  /*lsn
  uint32_t n_cols; //how many columns are being updated
  uint32_t* cols; //ids of modified columns
  uint32_t before_image_size;
  char * before_image; // data buffer for before image
  uint32_t after_image_size;
  char * after_image; // data buffer for after image
  */

};

class LogRecord {
public:
  //LogRecord();
  LogRecType getType() { return rcd.type; }
  void copyRecord( LogRecord * record);
  // TODO: compute a reasonable checksum
  uint64_t computeChecksum() {
    return (uint64_t)rcd.txn_id;
  };
#if LOG_COMMAND
  CmdLogRecord rcd;
#else
  AriesLogRecord rcd;
#endif
private:
  bool isValid;

};

class Logger {
public:
  void init(const char * log_file, const char * txn_file);
  void release();
  LogRecord * createRecord(LogRecord* record);

  LogRecord * createRecord(
    //LogRecType type,
      uint64_t txn_id, LogIUD iud,
    //uint64_t partid,
      uint64_t table_id, uint64_t key);
#if CC_ALG == HDCC
  LogRecord * createRecord(uint64_t txn_id,LogIUD iud,uint64_t table_id,uint64_t key,uint64_t max_calvin_tid);
#endif
  void enqueueRecord(LogRecord* record);
  void processRecord(uint64_t thd_id,uint64_t id);
  void writeToBuffer(uint64_t thd_id,char * data, uint64_t size);
  void writeToBuffer(uint64_t thd_id,LogRecord* record,uint64_t id);
  uint64_t reserveBuffer(uint64_t size);
  void notify_on_sync(uint64_t txn_id);
private:
  pthread_mutex_t mtx;
  uint64_t lsn;

  void flushBuffer(uint64_t thd_id,bool isLog,uint64_t id);
  boost::lockfree::queue<LogRecord *> ** log_queue;
  const char * log_file_name;
  const char * txn_file_name;
  std::ofstream * log_file;
  std::ofstream txn_file;
  uint64_t aries_write_offset;
  std::set<uint64_t> txns_to_notify;
  uint64_t last_flush;
  uint64_t log_buf_cnt;
};


#endif
