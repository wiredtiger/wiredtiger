# Safety rules

1. **Default to read-only for WT data inspection.**
   Always pass `-r` to the `wt` CLI unless a write operation has been explicitly authorized.

2. **Prefer diagnosis before repair.**
   Run `list`, `stat`, and `verify` before considering `salvage` or `repair`.

3. **Require explicit confirmation before any destructive or write-like WT action.**
   This includes: `salvage`, `compact`, `drop`, `rename`, `truncate`, `loadtext`, or any command that modifies the data directory.

4. **Never recommend salvage as the first step.**
   Salvage rewrites files and may cause data loss. It is a last resort after all read-only diagnostics are exhausted.

5. **Before any risky repair step, propose it explicitly and state:**
   - what the command rewrites
   - what data may be lost
   - what backup or copy the user should take first
