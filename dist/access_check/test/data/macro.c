#define AAA
#define BBB 5
#define CCC() 5
#define DDD(a, b) a + b
#define EEE(a, b) a ## b ## c
#define FFF(a, b) a ## c #b a b

qwe AAA zxc;
qwe BBB zxc;
qwe CCC() zxc;
qwe DDD(5, 6) zxc;
qwe EEE(5, 6) zxc;
qwe DDD(a b c, d e f) zxc;
BBB;
qwe DDD("ert , dfg", (345,456)) zxc;
qwe FFF(qwe, asd) zxc;

qwe /* AAA */ zxc;

"qwe AAA zxc";


#define VARIADIC(...) func(__VA_ARGS__)
#define PRINTF_LIKE(fmt, ...) printf(fmt, __VA_ARGS__)
#define PRINTF_LIKE2(fmt, ...) printf("%s" fmt, #fmt, __VA_ARGS__)
#define PRINTF_LIKE3(fmt, ...) \
    qwe; \
    printf("%s" fmt, #fmt, __VA_ARGS__); \
    zxc

VARIADIC(5);
VARIADIC(5, 6, 7);
PRINTF_LIKE("%d", 5);
PRINTF_LIKE("%d %s", 5, "qwe");
PRINTF_LIKE2("%d", 5);
PRINTF_LIKE2("%d %s", 5, "qwe");
PRINTF_LIKE2("%d %s %s", 5, "qwe", asd);
PRINTF_LIKE3("%d %s %s", 5, "qwe", asd);

// from https://en.wikipedia.org/wiki/C_preprocessor#Order_of_expansion

#define HE HI
#define LLO _THERE
#define HELLO "HI THERE"
#define CAT(a,b) a##b - CAT: a, b
#define XCAT(a,b) CAT(a,b) - XCAT: a, b
#define CALL(fn) fn(HE,LLO) - CALL: fn

CAT(HE, LLO); // "HI THERE", because concatenation occurs before normal expansion
XCAT(HE, LLO); // HI_THERE, because the tokens originating from parameters ("HE" and "LLO") are expanded first
CALL(CAT); // "HI THERE", because this evaluates to CAT(a,b)
HE; // HI
CAT(AB,HE); // ABHE


#define RECURSE(x) x RECURSE(x)

RECURSE(5);

#define RECURSE_A(x) x RECURSE_B(x)
#define RECURSE_B(x) x RECURSE_A(x)

RECURSE_A(5);
RECURSE_B(5);


/*

"HI THERE" - CAT: HI, _THERE
HI_THERE - CAT: HI, _THERE - XCAT: HI, _THERE
"HI THERE" - CAT: HI, _THERE - CALL: CAT
HI
ABHE - CAT: AB, HI

*/




#define AFTERX(x) AX(X_ ## x = x == #x)
#define XAFTERX(x) XAX(AFTERX(x) = x == #x)
#define TABLESIZE 1024
#define BUFSIZE TABLESIZE

#define AFTERX2(x) AX2(X_ ## x = x == #x)
#define XAFTERX2(x) XAX2(AFTERX(x) = x == #x)
#define X_AFTERX2(x) X AFTERX2
#define X_XAFTERX2(x) X XAFTERX2
#define TABLESIZE2 2048
#define X_TABLESIZE2 4096
#define BUFSIZE2 TABLESIZE2
#define X_BUFSIZE2 TABLESIZE2

int fn() {
    TABLESIZE;
    BUFSIZE;
    AFTERX(BUFSIZE);
    XAFTERX(BUFSIZE);

    AFTERX(AFTERX(BUFSIZE));
    AFTERX(AFTERX(AFTERX(BUFSIZE)));

    XAFTERX(XAFTERX(BUFSIZE));
    XAFTERX(XAFTERX(XAFTERX(BUFSIZE)));

    AFTERX2(BUFSIZE2);
    XAFTERX2(BUFSIZE2);
    AFTERX2(AFTERX2(BUFSIZE2));
    AFTERX2(AFTERX2(AFTERX2(BUFSIZE2)));
    XAFTERX2(XAFTERX2(BUFSIZE2));
    XAFTERX2(XAFTERX2(XAFTERX2(BUFSIZE2)));
}

/*
int fn() {
    1024;
    1024;
    AX(X_BUFSIZE = 1024 == "BUFSIZE");
    XAX(AX(X_1024 = 1024 == "1024") = 1024 == "BUFSIZE");

    AX(X_AFTERX(1024) = AX(X_BUFSIZE = 1024 == "BUFSIZE") == "AFTERX(BUFSIZE)");
    AX(X_AFTERX(AFTERX(1024)) = AX(X_AFTERX(1024) = AX(X_BUFSIZE = 1024 == "BUFSIZE") == "AFTERX(BUFSIZE)") == "AFTERX(AFTERX(BUFSIZE))");

    XAX(AX(X_XAX(AX(X_1024 = 1024 == "1024") = 1024 == "BUFSIZE") = XAX(AX(X_1024 = 1024 == "1024") = 1024 == "BUFSIZE") == "XAX(AX(X_1024 = 1024 == \"1024\") = 1024 == \"BUFSIZE\")") = XAX(AX(X_1024 = 1024 == "1024") = 1024 == "BUFSIZE") == "XAFTERX(BUFSIZE)");
    XAX(AX(X_XAX(AX(X_XAX(AX(X_1024 = 1024 == "1024") = 1024 == "BUFSIZE") = XAX(AX(X_1024 = 1024 == "1024") = 1024 == "BUFSIZE") == "XAX(AX(X_1024 = 1024 == \"1024\") = 1024 == \"BUFSIZE\")") = XAX(AX(X_1024 = 1024 == "1024") = 1024 == "BUFSIZE") == "XAFTERX(BUFSIZE)") = XAX(AX(X_XAX(AX(X_1024 = 1024 == "1024") = 1024 == "BUFSIZE") = XAX(AX(X_1024 = 1024 == "1024") = 1024 == "BUFSIZE") == "XAX(AX(X_1024 = 1024 == \"1024\") = 1024 == \"BUFSIZE\")") = XAX(AX(X_1024 = 1024 == "1024") = 1024 == "BUFSIZE") == "XAFTERX(BUFSIZE)") == "XAX(AX(X_XAX(AX(X_1024 = 1024 == \"1024\") = 1024 == \"BUFSIZE\") = XAX(AX(X_1024 = 1024 == \"1024\") = 1024 == \"BUFSIZE\") == \"XAX(AX(X_1024 = 1024 == \\\"1024\\\") = 1024 == \\\"BUFSIZE\\\")\") = XAX(AX(X_1024 = 1024 == \"1024\") = 1024 == \"BUFSIZE\") == \"XAFTERX(BUFSIZE)\")") = XAX(AX(X_XAX(AX(X_1024 = 1024 == "1024") = 1024 == "BUFSIZE") = XAX(AX(X_1024 = 1024 == "1024") = 1024 == "BUFSIZE") == "XAX(AX(X_1024 = 1024 == \"1024\") = 1024 == \"BUFSIZE\")") = XAX(AX(X_1024 = 1024 == "1024") = 1024 == "BUFSIZE") == "XAFTERX(BUFSIZE)") == "XAFTERX(XAFTERX(BUFSIZE))");

    AX2(2048 = 2048 == "BUFSIZE2");
    XAX2(AX(X_2048 = 2048 == "2048") = 2048 == "BUFSIZE2");
    AX2(X AFTERX2 = AX2(2048 = 2048 == "BUFSIZE2") == "AFTERX2(BUFSIZE2)");
    AX2(X AFTERX2 = AX2(X AFTERX2 = AX2(2048 = 2048 == "BUFSIZE2") == "AFTERX2(BUFSIZE2)") == "AFTERX2(AFTERX2(BUFSIZE2))");
    XAX2(AX(X_XAX2(AX(X_2048 = 2048 == "2048") = 2048 == "BUFSIZE2") = XAX2(AX(X_2048 = 2048 == "2048") = 2048 == "BUFSIZE2") == "XAX2(AX(X_2048 = 2048 == \"2048\") = 2048 == \"BUFSIZE2\")") = XAX2(AX(X_2048 = 2048 == "2048") = 2048 == "BUFSIZE2") == "XAFTERX2(BUFSIZE2)");
    XAX2(AX(X_XAX2(AX(X_XAX2(AX(X_2048 = 2048 == "2048") = 2048 == "BUFSIZE2") = XAX2(AX(X_2048 = 2048 == "2048") = 2048 == "BUFSIZE2") == "XAX2(AX(X_2048 = 2048 == \"2048\") = 2048 == \"BUFSIZE2\")") = XAX2(AX(X_2048 = 2048 == "2048") = 2048 == "BUFSIZE2") == "XAFTERX2(BUFSIZE2)") = XAX2(AX(X_XAX2(AX(X_2048 = 2048 == "2048") = 2048 == "BUFSIZE2") = XAX2(AX(X_2048 = 2048 == "2048") = 2048 == "BUFSIZE2") == "XAX2(AX(X_2048 = 2048 == \"2048\") = 2048 == \"BUFSIZE2\")") = XAX2(AX(X_2048 = 2048 == "2048") = 2048 == "BUFSIZE2") == "XAFTERX2(BUFSIZE2)") == "XAX2(AX(X_XAX2(AX(X_2048 = 2048 == \"2048\") = 2048 == \"BUFSIZE2\") = XAX2(AX(X_2048 = 2048 == \"2048\") = 2048 == \"BUFSIZE2\") == \"XAX2(AX(X_2048 = 2048 == \\\"2048\\\") = 2048 == \\\"BUFSIZE2\\\")\") = XAX2(AX(X_2048 = 2048 == \"2048\") = 2048 == \"BUFSIZE2\") == \"XAFTERX2(BUFSIZE2)\")") = XAX2(AX(X_XAX2(AX(X_2048 = 2048 == "2048") = 2048 == "BUFSIZE2") = XAX2(AX(X_2048 = 2048 == "2048") = 2048 == "BUFSIZE2") == "XAX2(AX(X_2048 = 2048 == \"2048\") = 2048 == \"BUFSIZE2\")") = XAX2(AX(X_2048 = 2048 == "2048") = 2048 == "BUFSIZE2") == "XAFTERX2(BUFSIZE2)") == "XAFTERX2(XAFTERX2(BUFSIZE2))");
}
*/
