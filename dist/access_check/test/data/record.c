/* #private */
struct S1 {
    int x;
};

/* #private */
struct S2 {
    S1 s;
};

S1 *__wti_module2_func1(S2 *s2) {
    return &s2->s;
}

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
    xxx->s2;
    xxx()->s2;
    xxx.s2;
    __wti_module2_func1()->x;
    __wti_module2_func1(s2->s, s5)->x;
    s2->s.x;
    aa->bb->cc->dd;
    (aa->aaa)->bb->cc->dd;
    (5 ? s2 : s3)->s->x;
    (5 ? __wti_module2_func1() : s3)->x;

    return num * num;
}
