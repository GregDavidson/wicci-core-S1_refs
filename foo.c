#include <stdio.h>
#include <string.h>

int main(void) {
  char str[1000];
  printf("Num args? ");
  int num_args;
  scanf("%d", &num_args);
  int len =snprintf(
  str, sizeof str, "SELECT %s.%s(%.*s)",
    "foo", "bar",
    num_args ? 3 * num_args - 1 : 0, "$1,$2,$3,$4,$5,$6,$7,$8,$9");
  printf("len: %d, str: %s\n", len, str);
}
