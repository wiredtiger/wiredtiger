
struct S0 {
    int z;
};


/* #public(module2) */
struct S1 {
    int x;
    S0 y;
};

/* #private(module1) */
struct S2 {
    S1 s;
};

/* #private(module2) */
S1 *__wti_module2_func1(S2 *s2) {
    return &s2->s;
}

/* #public(module1) */
int func(int num) {
    S2 s2;
    s2;
    s2.s;
    s2.s.x;
    (&s2)->s;
    ((S1*)((S2*)s2)->s)->x;
    ((S1*)((S2*)s2 + 1)->s)->x;
    ((S1*)((S2*)s2 + 1)->s)->x;
    ((S1*)((S2*)s2[10])->s)->x;
    ((S2*)((S1*)((S2*)s2[10])->s)->x)->s;
    ((&(s2->s[i].y)))->z;
    xxx->s2;
    xxx()->s2;
    xxx.s2;
    __wti_module2_func1()->x;
    __wti_module2_func1()[5]->x;
    __wti_module2_func1(s2->s, s5)->x;
    s2->s.x;
    aa->bb->cc->dd;
    (aa->aaa)->bb->cc->dd;
    (5 ? s2 : s3)->s->x;
    (5 ? __wti_module2_func1() : s3)->x;
    ((S1){.x=5}).x;

    return num * num;
}

/* #private(module1) */
static S2 *__wti_module1_func2(void) {
    return 5;
}

/* #private(module2) */
static int __wti_module2_func2(void) {
    return __wti_module1_func2();
}

/* #private(module2) */
int __wti_module2_func3(void) {
    return __wti_module1_func2()->x;
}

static S2 *__wti_module1_funcX(void) {
    return 5;
}

int func_local_struct(int a, char b) {
    struct {
        S1 s1;
        S2 s2;
    } local_str;
    local_str.s1.x = a;
    local_str.s2.s.x = a;
}

