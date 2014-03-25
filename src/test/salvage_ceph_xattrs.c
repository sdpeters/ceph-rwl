#include <libgen.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <inttypes.h>

const char *attrs[] = {
	"user.ceph._",
	"user.ceph.snapset",
	"user.cephos.seq",
	"user.cephos.gseq",
	"user.cephos.spill_out",
	0
};

int main(int argc, char **argv)
{
	char *fn = argv[1];
	char buf[65536];
	int buflen = sizeof(buf);
	int i;
	char *tmpdir = argv[2];
	char fn_new[4096];
	sprintf(fn_new, "%s/new.%s", tmpdir, basename(fn));
	char fn_backup[4096];
	sprintf(fn_backup, "%s/orig.%s", tmpdir, basename(fn));

	int r = listxattr(fn, buf, buflen);
	if (r != -1 || errno != E2BIG) {
		perror("listxattr didn't return E2BIG; file is ok");
		return 0;
	}

	struct stat st;
	r = stat(fn, &st);
	if (r < 0) {
		perror("stat");
		return 1;
	}

	int fdin = open(fn, O_RDONLY);
	if (fdin < 0) {
		perror("open read");
		return 1;
	}
	int fdout = open(fn_new, O_WRONLY|O_CREAT|O_EXCL, st.st_mode);
	if (fdout < 0) {
		perror("open write");
		return 1;
	}

	int64_t left = st.st_size;
	while (left > 0) {
		int l = buflen;
		if (l > left)
			l = left;
		r = read(fdin, buf, l);
		if (r != l) {
			perror("read");
			goto fail;
		}
		r = write(fdout, buf, l);
		if (r != l) {
			perror("write");
			goto fail;
		}
		left -= l;			
	}

	for (i=0; attrs[i]; ++i) {
		r = getxattr(fn, attrs[i], buf, buflen);
		if (r < 1) {
			if (errno == ENODATA)
				continue;
			perror("getxattr");
			goto fail;
		}
		r = setxattr(fn_new, attrs[i], buf, r, 0);
		if (r < 0) {
			perror("setxattr");
			goto fail;
		}
	}

	// yay!
	r = rename(fn, fn_backup);
	if (r < 0) {
		perror("rename backup");
		goto fail;
	}
	r = rename(fn_new, fn);
	if (r < 0) {
		perror("final rename to replace");
		r = rename(fn_backup, fn);
		if (r < 0) {
			perror("and failed to roll bakc, bah");
			return 1;
		}
		goto fail;
	}
	printf("done, backup at %s\n", fn_backup);
	return 0;

fail:
	unlink(fn_new);
	return 1;
}
