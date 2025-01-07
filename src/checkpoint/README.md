# Checkpoint

### Checkpoint Process

The checkpoint process goes through five stages:

- Prepare
- Data files checkpoint
- History store checkpoint
- Flush to disk
- Metadata checkpoint

Each stage is described in details in [arch-checkpoint.dox](../docs/arch-checkpoint.dox).

### APIs for Checkpoint

The checkpoint APIs are declared in [checkpoint.h](./checkpoint.h). Below is a brief description of the functionalities provided by these APIs:

- List the files to checkpoint.
- Take a checkpoint of a file. It is worth noting that there is a distinct API when taking a checkpoint before closing a file.
- Cleanup checkpoint-related structures.
- Log checkpoint progress messages.

There are also APIs dedicated to the checkpoint server to perform the following:

- Create and destroy the thread.
- Signal the thread to start a checkpoint.

> For more information on each API, refer to the comments located above each function definition in [checkpoint.h](./checkpoint.h).
