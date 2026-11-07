#include <sys/types.h>
#include <sys/procctl.h>
#include <sys/wait.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc != 3 || procctl(P_PID, 0, PROC_REAP_ACQUIRE, NULL) != 0)
        return 1;
    alarm(60);
    pid_t child = fork();
    if (child < 0)
        return 2;
    if (child == 0) {
        alarm(60);
        if (setsid() < 0)
            return 3;
        FILE *mark = fopen(argv[2], "w");
        if (mark == NULL)
            return 4;
        fprintf(mark, "%d %d\n", (int)getppid(), (int)getpid());
        fclose(mark);
        for (int i = 0; i < 600; ++i) {
            FILE *beat = fopen(argv[1], "a");
            if (beat == NULL)
                return 5;
            fputs("x\n", beat);
            fclose(beat);
            usleep(100000);
        }
        return 0;
    }
    int status;
    while (waitpid(child, &status, 0) < 0) {
        if (errno != EINTR)
            return 6;
    }
    return 0;
}
