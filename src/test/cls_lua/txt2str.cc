/*
 * Adapted from linux-kernel/scripts/bin2c.c
 *   Jan 1999 Matt Mackall <mpm@selenic.com>
 *
 * And StackOverflow question
 *   http://stackoverflow.com/questions/2602013/read-whole-ascii-file-into-c-stdstring
 */
#include <string>
#include <fstream>
#include <streambuf>
#include <stdio.h>

int main(int argc, char **argv)
{
  const char *filename = argv[1];

  std::ifstream t(filename);
  std::string str((std::istreambuf_iterator<char>(t)),
      std::istreambuf_iterator<char>());

  int cur = 0;
  int len = str.length();
  const char *buf = str.c_str();

  printf("static const char %s[] =\n", argv[2]);

  do {
    printf("  \"");
    while (cur < len) {
      printf("\\x%02x", buf[cur]);
      if (++cur % 16 == 0)
        break;
    }
    printf("\"");
    if (cur >= len)
      printf(";");
    printf("\n");
  } while (cur < len);

  return 0;
}
