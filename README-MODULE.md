# Modularity by identifier prefix

## Directory structure

```
src/
  ├── include/
  │   ├── module1.h
  │   ├── module1_inline.h
  │   ├── module2.h
  │   ├── module2_inline.h
  │   ├── module3.h
  │   ├── module3_inline.h
  │   ├── wt_internal.h
  │   └── extern.h
  ├── module1/
  │   ├── module1.h
  │   ├── module1.c
  │   ├── module1_file1.c
  │   ├── module1_file2.c
  │   └── module1_file3.c
  ├── module2/
  │   ├── module2.h
  │   ├── module2.c
  │   ├── module2_file1.c
  │   ├── module2_file2.c
  │   └── module2_file3.c
  └── module3/
      ├── module3.h
      ├── module3.c
      ├── module3_file1.c
      ├── module3_file2.c
      └── module3_file3.c
```

## Prefix

module1.h:
```c

struct __wt_module1_struct {  // public
    int a;                    // public
    int wt_b;                 // public
    int wti_module1_d;        // private to module1
    int wtp_e;                // private to the current module - module1
    int wti_module2_f;        // private to module2
    int wti_module3_g;        // private to module3
    struct {
      ...
    } wti_module1_h;          // private to module1
    struct __wti_module2_i {  // private to module2
      ...
      int wt_aaa;             // ?public in a private struct
    } i;
    struct {
      ...
    } wti_module3_j;          // private to module3
};
```


module1.h:
```c

struct __wti_module2_struct {  // The whole type is private to module2
    int field1;                // inherit from parent -> private
    int wt_field2;             // override parent -> public
    int wti_module3_field3;    // override module ownership -> private to module3
};

struct __wt_module2_struct2  { // The whole type is public to module2
};


struct name1 {                               // public
    int field1;                              // ingerit from parent -> public
    int wti_module2_field2;                  // private to module2
    __wti_module2_struct field3;             // private to module2 due to the type name
    __wti_module2_struct wt_field4;          // illegal, the type is private to module2
    __wti_module2_struct wtp_field5;         // illegal, the type is private to module2
    __wti_module2_struct wti_module3_field6; // illegal, the type is private to module2
    __wt_module2_struct2 wti_module3_field;  // private to module3
    struct wti_module2_xxx {                 // private to module2
    } field7;
    struct {
    } wti_module2_field8;                    // private to module2
};

```




module1.h:
```c

struct __wtp_struct1 {                       // private to module1
    struct {
        int field1;                          // private to module1
        struct {
            int field2;                      // private to module1
        } substruct2;
    } substruct1;
};

```





module1.h:
```c

struct __wt_module2_struct1 {                // public to module2
    struct {
        int field1;                          // public to module2
        struct {
            int wtp_field2;                  // private to module2
        } substruct2;
    } substruct1;
};

```



## Comment annotations






module1.h:
```c

/*
 * @public
 * Structure for blah blab blah.
 */
struct __wt_struct {                                   // implicitly belongs to the current module - module1
    int a;                                             // implicitly public
    int b;                    /* @public */
    int d;                    /* @public(module1) */   // private to module1
    int e;                    /* @private */           // private to the current module - module1
    int f;                    /* @private(module2) */  // private to module2
    int g;                    /* @private(module3) */  // private to module3
    struct {
      ...
    } h;                      /* @private(module2) */  // private to module2 via post-comment
    /* @private(module2) */
    struct __wt_struct2 {                              // private to module2 via pre-comment
      ...
      int aaa;                /* @public */            // ?public in a private struct
    } i;                                               // private to module2 via pre-comment
    /* @private */
    struct {
      ...
    } j;                                               // private to the current module - module1
};
```


module1.h:
```c

/*
 * @private(module2)                           // This only marks defaults for contents of the struct.
 * Structure for blah blab blah.               // That's because the following defines type, not value.
 */
struct __wt_struct {                           // The whole struct is private to module2
    /* @private(module2): */
    int field1;                                // inherit from parent -> private
    int field2;       /* @public */            // ?override parent -> public
    int field3;       /* @private(module3) */  // ?override module ownership -> private to module3
};

/*
 * @public(module2)
 */
struct __wt_struct2  {                 // The whole type is public to module2
};


struct name1 {                                         // public, belongs to mpdule1
    int field1;                                        // ingerit from parent -> public
    int field2;               /* @private(module2) */  // private to module2
    __wt_struct field3;                                // private to module2 due to the type name
    __wt_struct field4;       /* @public */            // ???? illegal, the type is private to module2
    __wt_struct field5;       /* @private */           // ???? illegal, the type is private to module2
    __wt_struct field6;       /* @private(module2) */  // ????? illegal, the type is private to module2
    __wt_struct2 field;       /* @private(module3) */  // private to module3
    /* @private(module3) */
    struct xxx {                                       // private to module2
    } field7;
    /* @private(module3) */
    struct {
    } field8;                                          // private to module2
};

```




module1.h:
```c

/* @private */
struct struct1 {                       // private to module1
    struct {
        int field1;                    // private to module1
        struct {
            int field2;                // private to module1
        } substruct2;
    } substruct1;
};

```





module1.h:
```c

/* @public(module2) */
struct struct1 {                                // public to module2
    struct {
        int field1;                             // public to module2
        struct {
            int wtp_field2;     /* @private */  // private to module2
        } substruct2;
    } substruct1;
};

```

