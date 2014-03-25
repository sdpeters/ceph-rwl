#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <string.h>
#include <errno.h>

int main(int argc, char **argv)
{
	char *fn = argv[1];
	char buf[655360];
	int buflen = sizeof(buf);
	int i;

	for (i = 0; i < 10000; ++i) {
		int r = listxattr(fn, buf, buflen);
		if (r < 0) {
			perror("listxattr");
			return 1;
		}
		char n[100];
		sprintf(n, "user.foooooooooooooooooooooooooooooooooooooo%d", i);
		r = setxattr(fn, n, "", 0, 0);
		if (r < 0) {
			perror("setxattr");
			return 1;
		}
		printf("set %d\n", i);
	}
	return 0;
}
