# WiredTiger Coding Conventions

**Principle**: Match existing code patterns. When in doubt, find a similar example and follow it.

## Critical Naming Conventions

### Function Prefixes (MUST follow)
- `wiredtiger_*` - Public API functions
- `__wt_*` - Internal functions used across subsystems  
- `__wti_*` - Internal functions within a single subsystem
- `__<subsystem>_*` - Static functions (e.g., `__btcur_search`, `__log_write`)

### Standard Variable Names
```c
WT_CONNECTION_IMPL *conn;      /* NOT wt_conn */
WT_SESSION_IMPL *session;      /* NOT sess, s */
WT_CURSOR *cursor;
WT_BTREE *btree;
```

## Essential Formatting

- **2 space indent** (no tabs), **100 char lines**
- Return type on separate line, function name at left margin:
  ```c
  int
  __wt_function(WT_SESSION_IMPL *session)
  {
      return (0);  /* Always parenthesize return values */
  }
  ```
- Braces: after functions, NOT after control statements
- Single-statement blocks: no braces (unless ambiguous)

## Error Handling Patterns

**Standard pattern (follow this exactly):**
```c
int
__wt_function(WT_SESSION_IMPL *session)
{
    WT_DECL_RET;
    
    WT_ERR(__wt_operation1(session));
    WT_ERR(__wt_operation2(session));
    
    if (0) {
err:    __cleanup_on_error(session);
    }
    __shared_cleanup(session);
    return (ret);
}
```

**Key macros:**
- `WT_RET(a)` - Return immediately on error
- `WT_ERR(a)` - Set ret and goto err label
- `WT_TRET(a)` - Accumulate error in ret
- `WT_RET_MSG(session, v, ...)` - Return with error message

## Comments (C-style ONLY)

```c
/*
 * __wt_function --
 *     Brief description of what function does.
 */

/* Single-line comment. */

/*
 * Multi-line comment.
 * Each line has an asterisk.
 */

// NEVER use C++ style comments
```

- Describe intent, not mechanics
- Never reference closed tickets or PR numbers
- Use `FIXME-WT-12345` for open tickets

## WiredTiger-Specific Rules

### Variables
- Declare `WT_*` structures in **alphabetical order**:
  ```c
  WT_BTREE *btree;
  WT_CURSOR *cursor;
  WT_SESSION_IMPL *session;
  ```
- Regular structs before `WT_*` structs

### Pointers
- Compare explicitly: `if (p == NULL)` NOT `if (!p)` or `if (p == 0)`

### Loops
- Infinite loops: `for (;;)` NOT `while (true)`

### Memory
- All allocation through session: `__wt_calloc()`, `__wt_malloc()`, etc.
- `__wt_free(session, ptr)` sets ptr to NULL

### Error Label Pattern
```c
if (0) {
err:    /* Error-only cleanup */
}
/* Shared cleanup */
return (ret);
```

## Testing

**Python tests:** `test/suite/test_<name><num>.py`
```python
class test_cursor01(wttest.WiredTigerTestCase):
    scenarios = make_scenarios([
        ('row', dict(key_format='S')),
        ('col', dict(key_format='r')),
    ])
```

## Quick Reference

Look at these files for examples:
- Function style: `src/conn/conn_api.c`, `src/btree/bt_cursor.c`
- Error handling: `src/include/error.h`
- Structures: `src/include/btmem.h`
- Tests: `test/suite/test_cursor01.py`

**See `CONTRIBUTING.rst` and `.clang-format` for full details.**
