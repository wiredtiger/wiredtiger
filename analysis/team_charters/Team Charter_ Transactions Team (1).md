### **Storage Engines – Transactions Team Charter**

#### **1\. Team Name**

Transactions Team

#### **2\. Purpose and Mission**

* **Purpose:**  
  To provide highly performant, reliable, and consistent data management services, ensuring optimal data integrity and availability across all systems, by expertly managing database transactions, efficient caching, and robust in-memory data structures.

* **Mission Statement:**  
  The Transactions Team is dedicated to designing, developing, and maintaining the foundational components responsible for:  
  * Executing and ensuring the atomicity, consistency, isolation (ACID) of all database transactions.  
  * Optimising system performance and resource utilisation through intelligent cache management and eviction strategies.  
  * Developing and maintaining efficient in-memory B-tree data structures, including their format, indexing, and reconciliation mechanisms.  
  * Ensuring seamless integration and data consistency between various data layers (disk, cache, in-memory).

#### **3\. Goals and Objectives**

* **Customer-Centric Goals:**  
  * Reduce customer operational costs by optimizing storage efficiency and resource utilization (e.g., disk usage, block cache performance).  
  * Make it easier for customers to use the database and improve its reliability, including reduced downtime and streamlined recovery processes.  
  * Deliver solutions that empower customers to manage their data at scale without compromising on performance or integrity.  
* **Team-Specific Goals:**  
  * Maintain and improve functionality for storage-related components, ensuring data durability, integrity, and performance.  
  * Optimize resource utilization (e.g., block cache) for large-scale, diverse workloads.  
  * Provide high-quality implementations for backup, restore, logging, and recovery processes.  
  * Ensure correctness and reliability of WiredTiger's persistence components through rigorous testing and validation.  
  * Collaborate closely with related teams to align on architectural principles and organizational goals.  
  * Respond effectively to customer issues and improve user experiences with persistence-related features.

### **4\. Scope of Work & Responsibilities:**

* **Database Transactions:**  
  * Design and implement transactional logic for core business operations.  
  * Ensure data integrity and consistency across all transactions.  
  * Monitor and optimize transaction performance and concurrency.  
  * Develop rollback and recovery mechanisms.  
  * Collaborate with application teams to define transaction boundaries and requirements.  
* **Cache/Eviction Management:**  
  * Design, implement, and maintain caching layers to reduce latency and improve throughput.  
  * Define and optimise cache eviction policies (e.g., LRU, LFU, FIFO, TTL) based on application needs and data access patterns.  
  * Monitor cache hit rates, eviction rates, and memory usage.  
  * Develop strategies for cache invalidation and consistency with underlying data stores.  
  * Manage cache sizing and scaling.  
* **In-Memory B-tree Format & Reconciliation:**  
  * Design and implement efficient in-memory B-tree data structures for fast data lookup, insertion, and deletion.  
  * Define the B-tree node format, indexing, and key management.  
  * Develop and maintain reconciliation processes to ensure consistency between in-memory B-trees and persistent storage/caches.  
  * Optimize B-tree performance for specific access patterns (e.g., range queries, point lookups).  
  * Handle B-tree rebalancing, splitting, and merging operations.  
* **Cross-cutting Responsibilities:**  
  * Performance tuning and optimization across all components.  
  * Error handling, logging, and monitoring for all data services.  
  * Security considerations for data access and manipulation.  
  * Documentation of designs, APIs, and operational procedures.  
  * Support and troubleshooting for issues related to data core services.  
  * Participation in on-call rotation as required.

### **5\. Out of Scope:**

* Direct application-level data modeling (while we provide guidance, the application teams own their specific models).  
* Front-end UI development.  
* Generic infrastructure management (e.g., network, hardware procurement, OS patching \- unless directly impacting our components).  
* Long-term data archiving strategies (while we ensure data persistence, the long-term archiving policies are usually owned by a separate data governance team).

### **5\. Team Roles and Responsibilities**

* **Team Lead:** [Haribabu Kommi](mailto:haribabu.kommi@mongodb.com)  
* **Engineers:** [Alana Huang](mailto:alana.huang@mongodb.com) [Ayesha Ahmed](mailto:ayesha.ahmed@mongodb.com) [Chenhao Qu](mailto:chenhao.qu@mongodb.com) [Ravi Giri](mailto:ravi.giri@mongodb.com)  [Shoufu Du](mailto:thomas.du@mongodb.com) [Zunyi Liu](mailto:zunyi.liu@mongodb.com)  
* **Aligned staff engineer:** [Vamsi Boyapati](mailto:vamsi.krishna@mongodb.com)

### **9\. Decision-Making Process:**

* **Default:** Consensus where possible.  
* **Technical Decisions:** Key architectural and technical decisions will be made through collaborative discussion, with the Team Lead having final say in case of irreconcilable differences.  
* **Urgent Decisions:** In critical situations requiring immediate action, the Team Lead or a designated senior member can make a decision, with a post-mortem review and communication to the team.  
* **Escalation:** Issues that cannot be resolved within the team will be escalated to \[e.g., Engineering Manager, relevant stakeholders\].

### **10\. Conflict Resolution:**

* Address conflicts directly and respectfully.  
* Focus on the issue, not the person.  
* Seek to understand different perspectives.  
* If direct resolution is not possible, involve the Team Lead to mediate.  
* If still unresolved, escalate to \[e.g., Engineering Manager, HR\].

### **11\. Success Metrics & KPIs:**

* **Database Transactions:**  
  * Transaction throughput (TPS)  
  * Transaction latency (average, 95th percentile, 99th percentile)  
  * Error rates for transactions  
  * Rollback frequency  
* **Cache/Eviction Management:**  
  * Cache hit ratio  
  * Eviction rate  
  * Memory utilisation of caching  
  * Latency reduction due to caching  
* **In-Memory B-tree:**  
  * Lookup/insert/delete performance (latency)  
  * Memory footprint of B-trees  
  * Reconciliation success rate and latency  
  * CPU utilisation for B-tree operations  
* **General:**  
  * System uptime and availability of data core services  
  * Number of production incidents related to data core services  
  * Mean Time To Resolution (MTTR) for incidents  
  * Code quality metrics (e.g., test coverage, static analysis findings)  
  * Stakeholder satisfaction (through feedback mechanisms)

