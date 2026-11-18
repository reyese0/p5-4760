#ifndef SHARED_H
#define SHARED_H

#include <sys/types.h>

#define SHM_KEY 1234
#define MSG_KEY 2345

#define MAX_PCBS 18
#define MAX_TOTAL_PROCESSES 40
#define RESOURCE_CLASSES 5
#define RESOURCE_INSTANCES 5
#define BILLION 1000000000U

typedef struct {
    unsigned int seconds;
    unsigned int nanoseconds;
} MyClock;

// Message from oss to a worker
typedef struct {
    long mtype;  // mtype is set to the worker pid
    int pcbIndex;
    int command;
} OssMessage;

// Message from a worker back to oss
// action > 0 requests resource action - 1
// action < 0 releases resource (-action) - 1
typedef struct {
    long mtype;
    int pcbIndex;
    int action;
    int resource;
} UserMessage;

enum {
    OSS_CONTINUE = 1,
    OSS_TERMINATE = 2
};

#endif