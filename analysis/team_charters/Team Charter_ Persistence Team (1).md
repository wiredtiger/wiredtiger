### **Team Charter: Persistence Team**

#### **1\. Team Name**

Persistence Team

---

#### **2\. Purpose and Mission**

* **Purpose:**  
  The Persistence Team is responsible for ensuring WiredTiger's storage and data durability functionality is robust, reliable, and performant across all supported use cases.  
* **Mission Statement:**  
  To design, develop, and maintain critical components of WiredTiger's persistence layer, delivering optimized storage and recovery solutions that directly help MongoDB users manage data more effectively, while reducing operational costs and improving ease of use. We aim to empower customers with reliable, efficient, and cost-effective database storage capabilities that align with MongoDB's core mission of enabling data-driven innovation.

---

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

---

#### **4\. Scope of Work**

* Components managed by the Persistence Team:  
  * **Backup:** Developing and optimizing processes for database backups that minimize user errors and operational downtime.  
  * **Block Cache:** Improving caching mechanisms for efficient data access, helping customers reduce latency and resource usage.  
  * **Block Manager:** Managing on-disk data blocks for performance and reliability that lowers costs for users.  
  * **Checkpoints:** Ensuring data consistency and durability with periodic checkpoints to provide peace of mind to customers.  
  * **Compaction:** Managing storage compaction for efficient disk usage, allowing customers to optimize costs and resource utilization.  
  * **Filesystem API:** Implementing abstractions for handling filesystem operations.  
  * **Live Restore:** Enabling recovery of live systems with minimal downtime, reducing operational disruptions for users.  
  * **Logging:** Managing logs for tracking database operations, enabling commit-level durability.  
  * **Overflow Items:** Handling overflowed data in edge-case scenarios. Note that this is not used by MongoDB as of Jun 16, 2025  
  * **Prefetch:** Improving performance through anticipatory data loading.  
  * **RTS (Rollback to Stable):** Rolling back to a stable state to handle uncommitted changes.  
  * **Salvage:** Recovering corrupted databases in failure scenarios, minimizing data loss and recovery times for customers.  
  * **Verify:** Validating data integrity and correctness during operations, giving customers confidence in their data reliability.  
* **Out of Scope:**  
  Any functionality not directly related to WiredTiger persistence operations (e.g., query execution, schema design).

---

#### **5\. Team Roles and Responsibilities**

* **Team Lead:** [Etienne Petrel](mailto:etienne.petrel@mongodb.com)  
* **Engineers:** [Jasmine Bi](mailto:jasmine.bi@mongodb.com) [Yury Ershov](mailto:y.ershov@mongodb.com)  [Peter Macko](mailto:peter.macko@mongodb.com) [Mariam Mojid](mailto:mariam.mojid@mongodb.com) [Sean Watt](mailto:sean.watt@mongodb.com)[Chen Song](mailto:albert.song@mongodb.com) [Luke Pearson](mailto:luke.pearson@mongodb.com) [Dylan Liang](mailto:dylan.liang@mongodb.com)

---

#### **6\. Key Metrics and Success Criteria**

* **Key Metrics:**  
  * Improved customer outcomes:  
    * Reduction in disk usage through improved storage allocation and efficiency.  
    * Improved latency and throughput for customer workloads, enabling faster query and operational performance.  
    * Reduction in customer incidents and support escalations caused by persistence layer issues and bottlenecks.  
  * Performance benchmarks (e.g., latency, IOPS) for persistence components.  
  * Reliability metrics (e.g., backup success rates).  
* **Success Criteria:**  
  * Meeting quarterly goals for customer-impacting component optimizations (e.g., reducing operational costs, improving reliability).  
  * Delivering high-quality features that improve customer satisfaction.  
  * Positive feedback from internal teams and external users regarding ease of use, cost-efficiency, and reliability of persistence-related features.

