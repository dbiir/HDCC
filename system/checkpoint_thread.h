#ifndef _CHECKPOINTTHREAD_H_
#define _CHECKPOINTTHREAD_H_

#include "global.h"
#include "thread.h"

class CheckpointThread : public Thread {
public:
  RC run();
  void setup();
};

#endif