#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "shared.h"

#define STEP_NS 10000000U
#define HALF_SECOND_NS 500000000U

typedef struct {
    int occupied;
    pid_t pid;
    unsigned int startSeconds;
    unsigned int startNano;
    int blocked;
    int allocated[RESOURCE_CLASSES];
    int requestedResource;
} PCB;

static FILE *logFile = NULL;
static const char *logFileName = "oss.log";
static MyClock *myClock = NULL;
static int shmId = -1;
static int msgId = -1;
static PCB processTable[MAX_PCBS];
static int available[RESOURCE_CLASSES];
static int running = 1;

static int totalToLaunch = 20;
static int maxSimultaneous = 5;
static double timeLimit = 1.0;
static double launchInterval = 0.1;

static int launched = 0;
static int active = 0;
static int nextScheduleIndex = 0;
static unsigned int nextLaunchSec = 0;
static unsigned int nextLaunchNano = 0;
static unsigned int nextTableSec = 0;
static unsigned int nextTableNano = HALF_SECOND_NS;
static unsigned int nextDeadlockSec = 1;
static unsigned int nextDeadlockNano = 0;

static int totalRequests = 0;
static int immediateGrants = 0;
static int grantedRequests = 0;
static int deadlockRuns = 0;
static int deadlockKills = 0;

// oss output goes to the terminal and the selected log file
static void log_both(const char *fmt, ...) {
    va_list args;
    va_list copy;

    va_start(args, fmt);
    va_copy(copy, args);
    vprintf(fmt, args);
    if (logFile != NULL) {
        vfprintf(logFile, fmt, copy);
        fflush(logFile);
    }
    va_end(copy);
    va_end(args);
    fflush(stdout);
}

static void print_help(void) {
    printf("Usage: oss [-h] [-n proc] [-s simul] [-t timeLimitForChildren] [-i fractionOfSecondToLaunchChildren] [-f logfile]\n");
    printf("  -h        Show help\n");
    printf("  -n proc   Total number of child processes to launch (max %d)\n", MAX_TOTAL_PROCESSES);
    printf("  -s simul  Maximum children allowed simultaneously (max %d)\n", MAX_PCBS);
    printf("  -t value  Maximum simulated seconds a child may exist\n");
    printf("  -i value  Maximum simulated seconds between launch attempts\n");
    printf("  -f file   Log file name\n");
}

static int compare_time(unsigned int leftSec, unsigned int leftNano, unsigned int rightSec, unsigned int rightNano) {
    if (leftSec != rightSec) {
        return leftSec < rightSec ? -1 : 1;
    }
    if (leftNano != rightNano) {
        return leftNano < rightNano ? -1 : 1;
    }
    return 0;
}

static void add_ns(unsigned int *sec, unsigned int *nano, unsigned int ns) {
    *nano += ns;
    while (*nano >= BILLION) {
        (*sec)++;
        *nano -= BILLION;
    }
}

// Used for launch intervals and worker lifetimes that may be fractional seconds
static void add_seconds_to_time(unsigned int sec, unsigned int nano, double add, unsigned int *outSec, unsigned int *outNano) {
    unsigned int whole = (unsigned int)add;
    unsigned int frac = (unsigned int)((add - whole) * BILLION);

    *outSec = sec + whole;
    *outNano = nano + frac;
    if (*outNano >= BILLION) {
        (*outSec)++;
        *outNano -= BILLION;
    }
}

static int current_time_reached(unsigned int sec, unsigned int nano) {
    return compare_time(myClock->seconds, myClock->nanoseconds, sec, nano) >= 0;
}

static int find_free_pcb(void) {
    for (int i = 0; i < MAX_PCBS; i++) {
        if (!processTable[i].occupied) {
            return i;
        }
    }
    return -1;
}

static void release_all_resources(int index) {
    // Termination, deadlock recovery, and abnormal exits all return allocations here
    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        if (processTable[index].allocated[i] > 0) {
            available[i] += processTable[index].allocated[i];
            processTable[index].allocated[i] = 0;
        }
    }
    processTable[index].blocked = 0;
    processTable[index].requestedResource = -1;
}

static void clear_pcb(int index) {
    memset(&processTable[index], 0, sizeof(PCB));
    processTable[index].requestedResource = -1;
}

void signal_handler(int sig) {
    printf("\nReceived SIGALRM (5 second timeout). Cleaning up...\n");
    
    // Send kill signal to all children based on their PIDs in process table
    for (int i = 0; i < MAX_PCBS; i++) {
        if (processTable[i].occupied && processTable[i].pid > 0) {
            printf("Killing child process %d\n", processTable[i].pid);
            kill(processTable[i].pid, SIGTERM);
        }
    }
    running = 0;
    printf("OSS terminated due to 5 second timeout\n");
    exit(1);
}

static void print_allocated_table(void) {
    log_both("Resources allocated:\n    ");
    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        log_both("R%-3d", i);
    }
    log_both("\n");

    for (int i = 0; i < MAX_PCBS; i++) {
        if (!processTable[i].occupied) {
            continue;
        }
        log_both("P%-3d", i);
        for (int j = 0; j < RESOURCE_CLASSES; j++) {
            log_both("%-4d", processTable[i].allocated[j]);
        }
        log_both("\n");
    }
}

static void print_available_table(void) {
    log_both("Total resources available:\n");
    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        log_both("R%-3d", i);
    }
    log_both("\n");

    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        log_both("%-4d", available[i]);
    }
    log_both("\n");
}

static void launch_child(void) {
    int index = find_free_pcb();
    pid_t pid;
    char indexArg[16];
    char limitArg[32];

    if (index < 0 || active >= maxSimultaneous || launched >= totalToLaunch) {
        return;
    }

    pid = fork();
    if (pid == -1) {
        perror("fork");
        return;
    }

    if (pid == 0) {
        // Pass the PCB slot and lifetime
        snprintf(indexArg, sizeof(indexArg), "%d", index);
        snprintf(limitArg, sizeof(limitArg), "%.6f", timeLimit);
        execl("./user", "user", indexArg, limitArg, (char *)NULL);
        perror("execl user");
        _exit(EXIT_FAILURE);
    }

    clear_pcb(index);
    processTable[index].occupied = 1;
    processTable[index].pid = pid;
    processTable[index].startSeconds = myClock->seconds;
    processTable[index].startNano = myClock->nanoseconds;
    launched++;
    active++;

    log_both("OSS launched P%d pid %d at time %u:%09u\n", index, (int)pid, myClock->seconds, myClock->nanoseconds);
}

static int grant_request(int index, int resource, int immediate) {
    if (resource < 0 || resource >= RESOURCE_CLASSES || available[resource] <= 0) {
        return 0;
    }

    // A granted request also wakes a previously blocked process
    available[resource]--;
    processTable[index].allocated[resource]++;
    processTable[index].blocked = 0;
    processTable[index].requestedResource = -1;
    grantedRequests++;
    if (immediate) {
        immediateGrants++;
    }

    log_both("OSS granting P%d request R%d at time %u:%09u\n", index, resource, myClock->seconds, myClock->nanoseconds);
    
    if (grantedRequests % 20 == 0) {
        log_both("Current system resources after %d granted requests\n", grantedRequests);
        print_allocated_table();
    }
    return 1;
}

static void try_unblock_processes(void) {
    int madeProgress = 1;

    // When resources become available, oss grants the saved request and the normal scheduler can choose that worker again on a later pass
    while (madeProgress) {
        madeProgress = 0;
        for (int i = 0; i < MAX_PCBS; i++) {
            if (processTable[i].occupied && processTable[i].blocked &&
                processTable[i].requestedResource >= 0 &&
                available[processTable[i].requestedResource] > 0) {
                grant_request(i, processTable[i].requestedResource, 0);
                madeProgress = 1;
            }
        }
    }
}

static int select_unblocked_process(void) {
    for (int count = 0; count < MAX_PCBS; count++) {
        int index = (nextScheduleIndex + count) % MAX_PCBS;
        if (processTable[index].occupied && !processTable[index].blocked) {
            nextScheduleIndex = (index + 1) % MAX_PCBS;
            return index;
        }
    }
    return -1;
}

static void handle_user_message(const UserMessage *msg) {
    int index = msg->pcbIndex;
    int resource = msg->resource;

    if (index < 0 || index >= MAX_PCBS || !processTable[index].occupied) {
        return;
    }

    if (msg->action == 0) {
        // Worker is done and oss does PCB cleanup and resource release
        log_both("OSS sees P%d terminating at time %u:%09u\n",
                 index, myClock->seconds, myClock->nanoseconds);
        release_all_resources(index);
        waitpid(processTable[index].pid, NULL, 0);
        clear_pcb(index);
        if (active > 0) {
            active--;
        }
        return;
    }

    if (msg->action > 0) {
        resource = msg->action - 1;
        totalRequests++;
        log_both("OSS has detected Process P%d requesting R%d at time %u:%09u\n",
                 index, resource, myClock->seconds, myClock->nanoseconds);
        if (!grant_request(index, resource, 1)) {
            // No reply is sent which leaves the worker blocked in msgrcv
            processTable[index].blocked = 1;
            processTable[index].requestedResource = resource;

            log_both("OSS blocking P%d for R%d at time %u:%09u\n", index, resource, myClock->seconds, myClock->nanoseconds);
        }
        return;
    }

    // Negative action means release resource
    resource = (-msg->action) - 1;
    if (resource >= 0 && resource < RESOURCE_CLASSES &&
        processTable[index].allocated[resource] > 0) {
        processTable[index].allocated[resource]--;
        available[resource]++;
     
        log_both("OSS has acknowledged Process P%d releasing R%d at time %u:%09u\n", index, resource, myClock->seconds, myClock->nanoseconds);
    }
}

static void terminate_deadlocked_process(int index) {
    log_both("Attempting to resolve deadlock...\n");
    log_both("Killing process P%d:\n", index);
    log_both("Resources released are as follows:");
    for (int i = 0; i < RESOURCE_CLASSES; i++) {
        if (processTable[index].allocated[i] > 0) {
            log_both(" R%d:%d", i, processTable[index].allocated[i]);
        }
    }
    log_both("\n");

    // Killing one process per detection pass
    kill(processTable[index].pid, SIGTERM);
    waitpid(processTable[index].pid, NULL, 0);
    release_all_resources(index);
    clear_pcb(index);
    if (active > 0) {
        active--;
    }
    deadlockKills++;
}

static void run_deadlock_detection(void) {
    int work[RESOURCE_CLASSES];
    int finish[MAX_PCBS];
    int deadlocked[MAX_PCBS];
    int deadlockCount = 0;
    int i;
    int r;
    int progress = 1;

    deadlockRuns++;
    log_both("OSS running deadlock detection at time %u:%09u:\n", myClock->seconds, myClock->nanoseconds);

    for (i = 0; i < RESOURCE_CLASSES; i++) {
        work[i] = available[i];
    }

    for (i = 0; i < MAX_PCBS; i++) {
        finish[i] = !processTable[i].occupied || !processTable[i].blocked;
        deadlocked[i] = 0;
        if (processTable[i].occupied && !processTable[i].blocked) {
            for (r = 0; r < RESOURCE_CLASSES; r++) {
                work[r] += processTable[i].allocated[r];
            }
        }
    }

    // Repeatedly mark blocked processes finishable if their requested class is available
    while (progress) {
        progress = 0;
        for (i = 0; i < MAX_PCBS; i++) {
            int req;
            if (finish[i]) {
                continue;
            }
            req = processTable[i].requestedResource;
            if (req >= 0 && req < RESOURCE_CLASSES && work[req] > 0) {
                for (r = 0; r < RESOURCE_CLASSES; r++) {
                    work[r] += processTable[i].allocated[r];
                }
                finish[i] = 1;
                progress = 1;
            }
        }
    }

    // Any blocked process that still can't finish is part of the deadlocked set
    for (i = 0; i < MAX_PCBS; i++) {
        if (processTable[i].occupied && processTable[i].blocked && !finish[i]) {
            deadlocked[i] = 1;
            deadlockCount++;
        }
    }

    if (deadlockCount == 0) {
        log_both("No deadlock detected\n");
        return;
    }

    log_both("Processes");
    for (i = 0; i < MAX_PCBS; i++) {
        if (deadlocked[i]) {
            log_both(" P%d", i);
        }
    }
    log_both(" deadlocked\n");

    for (i = 0; i < MAX_PCBS; i++) {
        if (deadlocked[i]) {
            terminate_deadlocked_process(i);
            break;
        }
    }
}

int main(int argc, char *argv[]) {
    int r;
    int opt;

    while ((opt = getopt(argc, argv, "hn:s:t:i:f:")) != -1) {
        switch (opt) {
            case 'h':
                print_help();
                exit(EXIT_SUCCESS);
            case 'n':
                totalToLaunch = atoi(optarg);
                break;
            case 's':
                maxSimultaneous = atoi(optarg);
                break;
            case 't':
                timeLimit = atof(optarg);
                break;
            case 'i':
                launchInterval = atof(optarg);
                break;
            case 'f':
                logFileName = optarg;
                break;
            default:
                print_help();
                exit(EXIT_FAILURE);
        }
    }

    if (totalToLaunch < 1) {
        totalToLaunch = 1;
    }
    if (totalToLaunch > MAX_TOTAL_PROCESSES) {
        totalToLaunch = MAX_TOTAL_PROCESSES;
    }
    if (maxSimultaneous < 1) {
        maxSimultaneous = 1;
    }
    if (maxSimultaneous > MAX_PCBS) {
        maxSimultaneous = MAX_PCBS;
    }
    if (timeLimit <= 0.0) {
        timeLimit = 1.0;
    }
    if (launchInterval < 0.0) {
        launchInterval = 0.0;
    }

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGALRM, signal_handler);

    logFile = fopen(logFileName, "w");
    if (logFile == NULL) {
        perror("fopen");
        return EXIT_FAILURE;
    }

    // Every resource class starts with the fixed number of configured instances
    for (r = 0; r < RESOURCE_CLASSES; r++) {
        available[r] = RESOURCE_INSTANCES;
    }

    for (r = 0; r < MAX_PCBS; r++) {
        clear_pcb(r);
    }

    shmId = shmget(SHM_KEY, sizeof(MyClock), IPC_CREAT | 0666);
    if (shmId == -1) {
        perror("oss shmget");
        return EXIT_FAILURE;
    }

    myClock = (MyClock *)shmat(shmId, NULL, 0);
    if (myClock == (void *)-1) {
        perror("oss shmat");
        return EXIT_FAILURE;
    }

    myClock->seconds = 0;
    myClock->nanoseconds = 0;

    msgId = msgget(MSG_KEY, IPC_CREAT | 0666);
    if (msgId == -1) {
        perror("msgget in oss");
        return EXIT_FAILURE;
    }

    alarm(5);

    // launch when allowed, wake blocked processes if possible, schedule one
    // unblocked worker, advance the simulated clock, and run periodic reports
    while (running && (launched < totalToLaunch || active > 0)) {
        int status;
        pid_t pid;

        while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
            // If a child exited unexpectedly, free its resources and PCB slot
            for (int i = 0; i < MAX_PCBS; i++) {
                if (processTable[i].occupied && processTable[i].pid == pid) {
                    release_all_resources(i);
                    clear_pcb(i);
                    if (active > 0) {
                        active--;
                    }
                    break;
                }
            }
        }

        if (launched < totalToLaunch && active < maxSimultaneous && current_time_reached(nextLaunchSec, nextLaunchNano)) {
            launch_child();
            add_seconds_to_time(myClock->seconds, myClock->nanoseconds, launchInterval, &nextLaunchSec, &nextLaunchNano);
        }

        try_unblock_processes();
        add_ns(&myClock->seconds, &myClock->nanoseconds, STEP_NS);
        
        //run a process
        int index = select_unblocked_process();
        OssMessage out;
        UserMessage in;

        if (index >= 0) {
            // Wake exactly one worker, then wait for that worker's request/release/exit
            out.mtype = (long)processTable[index].pid;
            out.pcbIndex = index;
            out.command = OSS_CONTINUE;
            if (msgsnd(msgId, &out, sizeof(OssMessage) - sizeof(long), 0) == -1) {
                perror("oss msgsnd");
            } else if (msgrcv(msgId, &in, sizeof(UserMessage) - sizeof(long), 1, 0) == -1) {
                if (errno != EINTR) {
                    perror("oss msgrcv");
                }
            } else {
                handle_user_message(&in);
            }
        }

        try_unblock_processes();
        add_ns(&myClock->seconds, &myClock->nanoseconds, STEP_NS);

        if (current_time_reached(nextTableSec, nextTableNano)) {
            log_both("\nProcess table at time %u:%09u\n", myClock->seconds, myClock->nanoseconds);
            log_both("Entry Occupied PID      StartS StartN     Blocked Request\n");
            for (int i = 0; i < MAX_PCBS; i++) {
                log_both("%-5d %-8d %-8d %-6u %-10u %-7d R%d\n", i,
                        processTable[i].occupied,
                        processTable[i].occupied ? (int)processTable[i].pid : 0,
                        processTable[i].startSeconds,
                        processTable[i].startNano,
                        processTable[i].blocked,
                        processTable[i].requestedResource);
            }

            log_both("Blocked processes:");
            for (int i = 0; i < MAX_PCBS; i++) {
                if (processTable[i].occupied && processTable[i].blocked) {
                    log_both(" P%d(R%d)", i, processTable[i].requestedResource);
                }
            }
            log_both("\n");
            print_available_table();
            print_allocated_table();
            log_both("\n");

            add_ns(&nextTableSec, &nextTableNano, HALF_SECOND_NS);
        }

        if (current_time_reached(nextDeadlockSec, nextDeadlockNano)) {
            run_deadlock_detection();
            nextDeadlockSec++;
        }
    }

    log_both("\nOSS ending report\n");
    log_both("Total resource requests: %d\n", totalRequests);
    log_both("Deadlock detection runs: %d\n", deadlockRuns);

    log_both("Processes terminated by deadlock recovery: %d\n", deadlockKills);
    if (totalRequests > 0) {
        log_both("Immediate grant percentage: %.2f%%\n",
                 (100.0 * immediateGrants) / totalRequests);
    } else {
        log_both("Immediate grant percentage: 0.00%%\n");
    }

    while (waitpid(-1, NULL, WNOHANG) > 0) {
    }

    if (myClock != NULL && myClock != (void *)-1) {
        shmdt(myClock);
        myClock = NULL;
    }
    if (shmId != -1) {
        shmctl(shmId, IPC_RMID, NULL);
        shmId = -1;
    }
    if (msgId != -1) {
        msgctl(msgId, IPC_RMID, NULL);
        msgId = -1;
    }
    if (logFile != NULL) {
        fclose(logFile);
        logFile = NULL;
    }

    return EXIT_SUCCESS;
}