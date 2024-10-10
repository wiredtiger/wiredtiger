
int __attribute__((xxx)) func(int a, int b) {
  int x;
  x = a + b;
  return (x);
}

func(5,6);

return func(5,6);

int x;

/* pre */
int *x; /* post */

a;

a, b;

*a;

*a, *b;

a = b;

*a = *b;

x = 5 + 8;

int x = 5 + 8;

__vector unsigned long long __v = {__a, __b};

/* comment */
/*
 * block comment
 */

// Preprocessor

/* pre comment */
#define qwe QWE /* post comment */

#define asd(x, y) ASD \
  ZXC /* post comment */

// typedef

/* pre comment */
typedef aa bbb; /* post comment */

/* pre comment */
typedef TAILQ_HEAD(aa,bb) tqh; /* post comment */

// records

/* pre comment */
struct {
  /* pre comment 2 */
  int a, b; /* post comment 2 */
  char c;
}; /* post comment */

/* pre comment */
struct aaa {
  /* pre comment 2 */
  int a, b; /* post comment 2 */
  char c;
}; /* post comment */

/* pre comment */
struct {
  /* pre comment 2 */
  int a, b; /* post comment 2 */
  char c;
} bbb; /* post comment */

/* pre comment */
struct aaa {
  /* pre comment 2 */
  int a, b; /* post comment 2 */
  char c;
} bbb, ccc; /* post comment */

/* pre comment */
typedef struct aaa {
  /* pre comment 2 */
  int a, b; /* post comment 2 */
  char c;
} bbb, ccc; /* post comment */

/* pre comment */
union aaa {
  /* pre comment 2 */
  int a, b; /* post comment 2 */
  char c;
} bbb, ccc; /* post comment */

/* pre comment */
typedef enum aaa {
  AAA, BBB, CCC
} bbb, ccc; /* post comment */

/* pre comment */
typedef enum aaa {
  AAA, BBB, CCC
} bbb, ccc; /* post comment */

/* pre comment */
typedef union aaa {
  /* pre comment 2 */
  int a, b; /* post comment 2 */
  struct aaabbb {
    int bbb;
    struct aaabbbccc {
      int ccc;
    } yyy;
  } xxx;
  char c;
} bbb, ccc; /* post comment */

/* pre comment */
typedef union aaa {
  /* pre comment 2 */
  int a, b; /* post comment 2 */
  struct {
    int bbb;
    struct {
      int ccc;
    } yyy;
  } xxx;
  char c;
} bbb, ccc; /* post comment */

int func(int a, int b);
void func(int a, int b) __attribute__((__noreturn__));
inline void func(int a, int b) __attribute__((__noreturn__));
WT_INLINE void func(int a, int b) __attribute__((__noreturn__));
void func_of_ptr(int *a[100]) {
}

/*
 * func --
 *      function description
 */
int func(int a, int b) {
  int x;
  x = a + b;
  return (x);
}

/*
 * func --
 *      function description
 */
static const int func2(int a, int b) {
}

do {
} while (0);


extern "C" {
  int func_ext_c(int a, int b);
  struct ext_c_struct; typedef struct ext_c_struct EXT_C_STRUCT;
}

#define AAA
#define BBB 5
#define CCC 5 + 8
#define DDD() 5 + 8
#define EEE(x) x + 8
#define FFF(x, y) x + y
#define GGG(x, y) x + \
 y

qwe asd;
qwe *asd;
qwe* asd;
qwe * asd;
qwe *;

int asd;
int *asd;
int* asd;
int * asd;
int *;

qwe = asd * zxc;
int qwe = asd * zxc;
qwe asd = {123, 456};
struct aaa {
  int a, b;
} bbb = {123, 456};
struct aaa bbb = {123, 456};

qwe *func1(int a, int b) {1}
qwe* func2(int a, int b) {2}
qwe * func3(int a, int b) {3}
qwe*func4(int a, int b) {4}

struct StructWithNested {
    struct {
        int x;
        char y;
    };
};

static const struct {
      int x;
} qwe = {123};

