#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/shm.h>
#include <time.h>

#include "shared.h"

// Return true once the worker's maximum simulated lifetime has expired
static int time_reached(const MyClock *myClock, unsigned int endSec, unsigned int endNano) {
    return myClock->seconds > endSec || (myClock->seconds == endSec && myClock->nanoseconds >= endNano);
}

// Convert a fractional number of seconds into the clock's format
static void add_seconds(unsigned int sec, unsigned int nano, double add, unsigned int *outSec, unsigned int *outNano) {
    unsigned int whole = (unsigned int)add;
    unsigned int frac = (unsigned int)((add - whole) * BILLION);

    *outSec = sec + whole;
    *outNano = nano + frac;
    if (*outNano >= BILLION) {
        (*outSec)++;
        *outNano -= BILLION;
    }
}

int main(int argc, char *argv[]) {
    int pcbIndex;
    double maxLifetime;
    int shmId;
    int msgId;
    MyClock *myClock;
    int held[RESOURCE_CLASSES] = {0};
    unsigned int endSec;
    unsigned int endNano;

    pcbIndex = atoi(argv[1]);
    maxLifetime = atof(argv[2]);
    if (pcbIndex < 0 || pcbIndex >= MAX_PCBS || maxLifetime <= 0.0) {
        return EXIT_FAILURE;
    }

    shmId = shmget(SHM_KEY, sizeof(MyClock), 0666);
    if (shmId == -1) {
        perror("user shmget");
        return EXIT_FAILURE;
    }

    myClock = (MyClock *)shmat(shmId, NULL, 0);
    if (myClock == (void *)-1) {
        perror("user shmat");
        return EXIT_FAILURE;
    }

    msgId = msgget(MSG_KEY, 0666);
    if (msgId == -1) {
        perror("msgget in child");
        shmdt(myClock);
        return EXIT_FAILURE;
    }

    srand((unsigned int)(time(NULL) ^ getpid() ^ (pcbIndex << 8)));
    add_seconds(myClock->seconds, myClock->nanoseconds, maxLifetime, &endSec, &endNano);

    while (1) {
        OssMessage in;
        UserMessage out;
        int resource;
        int request;

        if (msgrcv(msgId, &in, sizeof(OssMessage) - sizeof(long), (long)getpid(), 0) == -1) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }

        // A worker reports termination
        if (in.command == OSS_TERMINATE || time_reached(myClock, endSec, endNano)) {
            out.mtype = 1;
            out.pcbIndex = pcbIndex;
            out.action = 0;
            out.resource = -1;
            msgsnd(msgId, &out, sizeof(UserMessage) - sizeof(long), 0);
            break;
        }

        request = (rand() % 100) < 70;
        resource = rand() % RESOURCE_CLASSES;

        // Choose only from resources this worker believes it currently holds when releasing
        if (!request) {
            int candidates[RESOURCE_CLASSES];
            int count = 0;

            for (int i = 0; i < RESOURCE_CLASSES; i++) {
                if (held[i] > 0) {
                    candidates[count++] = i;
                }
            }

            if (count > 0) {
                resource = candidates[rand() % count];
                held[resource]--;
                out.action = -(resource + 1);
            } else {
                out.action = resource + 1;
            }
        } else {
            // Do not request more than what exists in the whole system
            int tries = 0;
            while (held[resource] >= RESOURCE_INSTANCES && tries < RESOURCE_CLASSES * 2) {
                resource = rand() % RESOURCE_CLASSES;
                tries++;
            }

            if (held[resource] >= RESOURCE_INSTANCES) {
                out.action = 0;
                out.resource = -1;
                out.mtype = 1;
                out.pcbIndex = pcbIndex;
                msgsnd(msgId, &out, sizeof(UserMessage) - sizeof(long), 0);
                break;
            }

            held[resource]++;
            out.action = resource + 1;
        }

        out.mtype = 1;
        out.pcbIndex = pcbIndex;
        out.resource = resource;
        msgsnd(msgId, &out, sizeof(UserMessage) - sizeof(long), 0);
    }

    shmdt(myClock);
    return EXIT_SUCCESS;
}