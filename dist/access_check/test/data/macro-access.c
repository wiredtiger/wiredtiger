

/* #private(mod1) */
#define M1_QWE(x) x+1
/* #private(mod1) */
#define M1_QWE2(x) M1_QWE(x)+1

#define QWE(x) x+5
/* #public(mod2) */
#define QWE2(x) M1_QWE2(x)+5
#define QWE3(x) QWE2(x)+5

int __wt_mod2_macro_expand_func1(void) {
    QWE(5);
}

int __wt_mod2_macro_expand_func2(void) {
    M1_QWE(5);
}

int __wt_mod2_macro_expand_func3(void) {
    M1_QWE(QWE(5));
}

int __wt_mod2_macro_expand_func33(void) {
    M1_QWE(M1_QWE(5));
}

int __wt_mod2_macro_expand_func4(void) {
    QWE(M1_QWE(5));
}

int __wt_mod2_macro_expand_func5(void) {
    M1_QWE2(10);
    QWE2(10);
    QWE3(10);
    QWE3(QWE2(10));
}

int __wt_mod2_macro_expand_func6(void) {
    QWE(5);
    M1_QWE(5);
    M1_QWE(QWE(5));
    M1_QWE(M1_QWE(5));
    QWE(M1_QWE(5));
}
