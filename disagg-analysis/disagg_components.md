* Page deltas  
* Shared storage architecture  
  * Classic Mongo is “shared nothing”. Disagg moves us to “shared storage”. This is a big architectural change  
  * Different WT instances must now agree on the physical structure of tables and pages. Historically, B-Tree construction was non-deterministic.  
* Distributed systems problems  
  * WT used to live in a happy little blinkered world where it could ignore all other nodes. The primary/standby differentiation is a big example of this  
* Layered tables  
  * No specific changes to schema layer, BUT worth talking about creating “layered:foo” vs “create(“...,type=layered”)”  
  * Checkpoint sharing  
  * Precise checkpoints \+ RTS  
* Disagg block manager  
  * Old block manager was, happily, behind a clean interface  
* PALI  
* Cache management (i.e. eviction)  
  * Interacts with materialisation frontier  
* Layered cursors  
  * Everything is a cursor in WT. Backup is a cursor, metadata is a cursor. Cursors are the thing that abstracts away the layered tables \+ PALI implementation  
* Metadata  
  * Shared metadata  
  * New equivalent of turtle file  
  * Metadata service \!= page service  
* History store  
  * Shared now  
* Prepared transactions  
  * Oh boy  
* Version cursors  
* Encryption  
  * The KEK stuff  
    * unrelated to LOL, LMAO, and ROFL  
* Diagnosability  
  * WT tool not working?  
  * Maybe Sean or Etienne can cover this?  
* Testing  
  * Distributed systems strikes again  
* Observability  
  * Not necessarily a disagg change, but parts of the operational changes around disagg