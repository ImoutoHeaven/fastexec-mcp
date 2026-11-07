#include <signal.h>
#include <termios.h>
#include <unistd.h>

int main(int argc, char **argv) {
    (void)argv;
    signal(SIGTTOU, SIG_IGN);
    if (setpgid(0, 0) == -1 || tcsetpgrp(STDIN_FILENO, getpid()) == -1)
        return 2;
    if (argc > 1 && tcflow(STDOUT_FILENO, TCOOFF) == -1)
        return 4;
    const char output[] = "foreground-output\n";
    if (write(STDOUT_FILENO, output, sizeof(output) - 1) != sizeof(output) - 1)
        return 3;
    return 19;
}
