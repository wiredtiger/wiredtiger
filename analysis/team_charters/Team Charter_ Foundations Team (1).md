### **Team Charter: Foundations Team**

#### **1\. Team Name**

Foundations Team

---

#### **2\. Purpose and Mission**

**Purpose:**  
To ensure the WiredTiger API delivers integrity, reliability, and evolvability, providing MongoDB and its customers with a robust, secure, and performant foundation for data management.

**Mission Statement:**

We uphold the reliability of WiredTiger’s guarantees to our customers (MongoDB) by solving systemic issues, enhancing tools and test frameworks, and refining our toolchain for future development needs. By strengthening WiredTiger’s foundational capabilities, we enable MongoDB to confidently meet the demands of data-driven applications at scale.

---

#### **3\. Impacts and Objectives**

**Customer-Centric Objectives:**

* **\[FND-API\] Evolve WiredTiger’s API to Meet Customer Needs:** Continuously improve and adapt WiredTiger’s API, tooling, and testing frameworks to align with MongoDB’s evolving customer requirements and workloads.  
* **\[FND-Rel\] Enable Reliable Releases:** Deliver reliable, secure, and high-quality code deployments and releases, empowering customers with confidence to build and scale their critical applications.  
* **\[FND-Wide\] Strengthen WiredTiger Foundations:** Continuously optimize WiredTiger as a unified system by addressing inefficiencies spanning its core components and creating systemic frameworks that enable future evolution.  
* **\[FND-Infra\] Enable Consistency and Integrity:** Provide tools and infrastructure that enable teams to uphold and improve WiredTiger’s durability, reliability, and performance story.

**Team-Specific Impact:**

* **\[FND-Rel\] Support Delivering Reliable Code:** Ensure secure and reliable code deployments by maintaining rigorous standards for releasability and building the tooling necessary to assist in this goal.  
* **\[FND-Wide\] Seeking and Solving Systemic Issues:** Identify and address systemic issues spanning WiredTiger by improving the effectiveness of test frameworks, debugging tools, and foundational capabilities. Further empower teams to solve customer concerns and drive WiredTiger’s durability, reliability, and performance story.  
* **\[FND-Wide\] System-Wide Improvements**: Proactively optimize WiredTiger as a cohesive system by identifying and addressing cross-component inefficiencies. Strengthen its foundational performance, reliability, and scalability across diverse workloads.  
* **\[FND-UX\] Optimize the User Experience:** Continuously improve the customer experience when interacting with WiredTiger API by addressing usability issues, refining abstractions, and aligning the API with MongoDB’s broader goals. Collaborate across teams to ensure consistency in API expectations, testing standards, and operational needs.

---

#### **4\. Areas of Ownership** 

**WiredTiger Core Functionality**

* **API (sessions, connections, configuration):**

  *Goals mapping: \[FND-API\] \[FND-UX\]*

  Ensure WiredTiger can keep its durability, reliability, and performance promises by guiding users to correct and efficient utilization of WiredTiger. This involves refining API usage patterns, evolving the API design, and enhancing the underlying implementation that backs the API.

* **Data Handle Management:**

  *Goals mapping: \[FND-Wide\]*

  Optimize efficient caching mechanism for tables, addressing distinct requirements for disaggregated storage (3x handles) and classic WiredTiger implementations.

* **Schema management:**

  *Goals mapping: \[FND-Wide\]*

  Maintain all WiredTiger schema type operations (e.g. create, drop, alter) and add support where needed in disaggregated storage.

* **Language bindings (SWIG, Python):** 

  *Goals mapping: \[FND-API\] \[FND-UX\]*

  Preserve all language bindings of the WiredTiger API. Support for PALR and PALite in disaggregated storage.

* **WiredTiger as a System:**

  *Goals mapping: \[FND-Wide\] \[FND-Infra\]*

  Ensure WiredTiger operates as a unified and efficient system.

* **Cursors:**

  *Goals mapping: \[FND-API\] \[FND-UX\]*

  Refine and preserve the layer of CRUD operations between MongoDB and WiredTiger btree functionality.

* **Metadata:**

  *Goals mapping: \[FND-Wide\]*

  Maintain WiredTiger’s schema table and ensure that it accurately depicts correct information of all tables in the system

* **Layered Tables:**

  *Goals mapping: \[FND-API\] \[FND-Wide\]*

  Continuously add support and evolve a new type of table called layered tables. The new table introduces the management of two tables sharing ownership called the ingest and the stable table

**Build, Test, and Deploy Infrastructure**

* **Build/Compile/Lint:** 

  *Goals mapping: \[FND-Infra\] \[FND-Rel\]*

  Maintain WiredTiger's build system, linting, and development tools to enforce best coding practices while refining the toolchain to support future development needs.

* **Release Management:** 

  *Goals mapping: \[FND-Rel\]*

  Ensure secure and reliable code deployments by maintaining rigorous standards for development, testing, and release processes.

* **CI/CD Infrastructure:** 

  *Goals mapping: \[FND-Infra\] \[FND-Rel\]*

  Advance WiredTiger’s CI/CD infrastructure and build scalable pipelines to improve testing standards for code reliability and integrity.

* **Performance and benchmarking:** 

  *Goals mapping: \[FND-UX\] \[FND-Wide\]*

  Accurately measure WiredTiger’s performance through designing benchmark tools or testing frameworks to ensure WiredTiger’s efficiency in real-world customer cases.

* **Correctness frameworks:** 

  *Goals mapping: \[FND-Infra\]*

  Develop comprehensive correctness testing frameworks that simulate production environments, edge cases, and failure scenarios.

* **WiredTiger Memory models:** 

  *Goals mapping: \[FND-Wide\]*

  Progressively evolve WiredTiger’s memory model to ensure reliability, integrity, and correctness in customer data.

**Out of Scope:**  
Any functionality not directly related to WiredTiger's API or similar cross-cutting functionality (e.g. reconciliation, timestamps, block manager). A component being out of scope does not mean we won't work on it \-- just that we don't own it.

---

#### **5\. Team Roles and Responsibilities**

* **Team Lead:** [Radoslav Kardum](mailto:radoslav.kardum@mongodb.com)  
* **Engineers:** [Donald Anderson](mailto:donald.anderson@mongodb.com)[Jie Chen](mailto:jie.chen@mongodb.com)[Ivan Kochin](mailto:ivan.kochin@mongodb.com)[Sid Mahajan](mailto:siddhartha.mahajan@10gen.com)[Luke Chen](mailto:luke.chen@mongodb.com)[Will Korteland](mailto:will.korteland@mongodb.com)[Alex Blekhman](mailto:alexander.blekhman@mongodb.com)[Alexander Pullen](mailto:alex.pullen@mongodb.com)[Salman Javed](mailto:salman.javed@mongodb.com)

---

#### **6\. Key Metrics and Success Criteria**

* **Key Metrics:**  
  * Increased effectiveness of debugging tools and test frameworks.  
  * Resolution of cross-cutting bottlenecks or misalignments.  
  * Code releases meeting MongoDB’s quality testing standards.  
  * Reduction in post-release critical issues (BFs) and regressions (Perf BFs).  
* **Success Criteria:**  
  * Continuously achieving quarterly targets for systemic and API improvements.  
  * Increased MongoDB confidence through highly reliable, and performant releases.  
  * Delivering impactful tooling and frameworks that enable future evolvability.  
  * Positive feedback from internal MongoDB teams on system coherence and scalability.