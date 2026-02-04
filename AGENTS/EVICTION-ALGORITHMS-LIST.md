* [Cache eviction algorithms list](https://github.com/ershov/cache-eviction-info/wiki)

# Cache eviction algorithms info

## Cache eviction algorithms list

| Name | Description | Details |
|:-:|:-|:-|
|<h3><BR>Simple<BR>Eviction Policies<BR></h3>
| **FIFO**<BR>First-In, First-Out | Evicts the oldest entry in the cache first. | <li>[wiki](https://en.wikipedia.org/wiki/Cache_replacement_policies#FIFO)
| **CLOCK**<BR>Second Chance | Uses a circular buffer to manage pages, giving each page a second chance before eviction. | <li>[wiki](https://en.wikipedia.org/wiki/Page_replacement_algorithm#Clock)
| **Double Clock** | An extension of the CLOCK algorithm with two clock hands for improved performance. | <li>[wiki](https://en.wikipedia.org/wiki/Page_replacement_algorithm#Clock_pro)<li>[caffeine](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/irr/DClockPolicy.java#L37)
| **n-bit clock** | Enhances the CLOCK algorithm by using multiple reference bits. | <li>[wiki](https://en.wikipedia.org/wiki/Page_replacement_algorithm#N-bit_clock)
| **LP-FIFO**<BR>Least Priority FIFO | Combines FIFO with priority levels to manage cache entries.
| **QD-LP-FIFO**<BR>Queue and Priority-Based FIFO | Enhances LP-FIFO with additional queue management for better performance.
| **S3FIFO** | An advanced FIFO variant with additional sampling strategies. | <li>[s3fifo.com](https://s3fifo.com/)<li>[s3fifo : lazy promotion and quick demotion](https://s3fifo.com/blog/2023/06/01/fifo-is-better-than-lru-the-power-of-lazy-promotion-and-quick-demotion/)<li>[caffeine](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/two_queue/S3FifoPolicy.java#L37)
|<h3><BR>Frequency-Based<BR>Policies<BR></h3>
| **LFU**<BR>Least Frequently Used | Evicts the least frequently accessed item. | <li>[wiki](https://en.wikipedia.org/wiki/Cache_replacement_policies#LFU)<li>[caffeine](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/linked/FrequentlyUsedPolicy.java#L39)
| **TinyLFU** | An optimized version of LFU for small cache sizes.
| **W-TinyLFU** | A weighted version of TinyLFU for better performance.
| **W-TinyLFU-S**<BR>TinyLFU with Segmentation | Combines TinyLFU with segmentation to improve performance in specific workload scenarios.
|<h3><BR>Recency-Based<BR>Policies<BR></h3>
| **LRU**<BR>Least Recently Used | Evicts the least recently accessed item first. | <li>[wiki](https://en.wikipedia.org/wiki/Cache_replacement_policies#LRU)
| **Segmented LRU** | Divides the cache into segments to optimize replacement decisions. | <li>[wiki](https://en.wikipedia.org/wiki/Cache_replacement_policies#Segmented_LRU)
| **SIEVE
| **Multi-generational LRU** | An extension of LRU to manage items across multiple generations. | <li>[wiki](https://en.wikipedia.org/wiki/Cache_replacement_policies#Multi-generational_LRU)
|<h3><BR>Combined<BR>Recency and Frequency<BR>Policies<BR></h3>
| **LRFU**<BR>Least Recently/Frequently Used | Combines LRU and LFU to balance recency and frequency.
| **LRU-K** | A generalization of LRU that keeps track of the last K references to each item.
| **LFUDA**<BR>Least Frequently Used with Dynamic Aging | Adjusts frequency counts over time to favor more recently used items. | <li>[wiki](https://en.wikipedia.org/wiki/Cache_replacement_policies#LFUDA)
|<h3><BR>Adaptive and Learning-Based<BR>Policies<BR></h3>
| **2Q**<BR>Two Queues | A two-level cache replacement algorithm to improve hit rates. | <li>[wiki](https://en.wikipedia.org/wiki/2Q)<li>[OpenBSD 2Q](https://flak.tedunangst.com/post/2Q-buffer-cache-algorithm)<li>[memcached 2Q](https://github.com/memcached/memcached/pull/97)<li>[caffeine 2Q](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/two_queue/TwoQueuePolicy.java#L29)<li>[caffeine TuQ](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/two_queue/TuQueuePolicy.java#L31)
| **Quadruply-segmented LRU** | | <li>[caffeine](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/linked/S4LruPolicy.java#L38)
| **SIEVE** | A low overhead cache replacement algorithm. | <li>[caffeine](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/linked/SievePolicy.java#L33)
| **ARC**<BR>Adaptive Replacement Cache | Balances between recency and frequency by maintaining two lists for recent and frequently accessed items. | <li>[wiki](https://en.wikipedia.org/wiki/Adaptive_replacement_cache)<li>[caffeine](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/adaptive/ArcPolicy.java#L31)
| **CAR**<BR>Clock with Adaptive Replacement policy | An extension of the CLOCK algorithm combined with ARC principles. | <li>[wiki](https://en.wikipedia.org/wiki/Clock_with_adaptive_replacement)<li>[caffeine](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/adaptive/CarPolicy.java#L31)<li>[caffeine, CART](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/adaptive/CartPolicy.java#L31)
| **CLOCK-Pro** | An enhancement of the CLOCK algorithm that considers both recency and frequency. | <li>[caffeine](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/irr/ClockProPolicy.java#L31)<li>[caffeine clock pro simple](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/irr/ClockProSimplePolicy.java#L32)<li>[caffeine clock pro +](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/irr/ClockProPlusPolicy.java#L31)
| **LeCaR**<BR>Learning Cache Replacement | Uses reinforcement learning to improve cache replacement decisions.
| **Dueling Clock** | An adaptive algorithm that dynamically selects between multiple eviction policies based on their performance.
| **GD-Wheel** | Generalized Dueling Wheels combine multiple cache eviction policies in a hierarchical structure to adapt dynamically.
| **LHD**<BR>Learning Hierarchical Distribution | Uses machine learning to adaptively manage cache replacement.
| **Q-LRU** | A hybrid of LRU and LFU that uses Q-learning to dynamically adapt the cache replacement policy.
|<h3><BR>Hierarchical and Segmented<BR>Policies<BR></h3>
| **SLRU**<BR>Segmented LRU | A variant of LRU that segments the cache for better performance. | <li>[wiki](https://en.wikipedia.org/wiki/Cache_replacement_policies#SLRU)<li>[caffeine](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/linked/SegmentedLruPolicy.java#L36)
| **MQ**<BR>Multi-Queue | Maintains multiple queues to manage cache entries based on access patterns. | <li>[caffeine](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/linked/MultiQueuePolicy.java#L35)
| **CACHEUS** | A hierarchical web cache architecture designed to minimize latency.
|<h3><BR>Energy Efficient<BR>Policies<BR></h3>
| **EE-LRU**<BR>Energy Efficient LRU | Optimizes LRU for energy efficiency in cache replacement.
|<h3><BR>Specialized Policies<BR></h3>
| **FRD**<BR>Filtering-based Buffer Cache | Aims to improve buffer cache performance by filtering data based on access patterns. | <li>[caffeine](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/irr/FrdPolicy.java#L31)
| **LIRS**<BR>Low Inter-reference Recency Set | Improves cache hit rates by managing inter-reference recency. | <li>[caffeine](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/irr/LirsPolicy.java#L34)<li>[caffeine hill climber](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/irr/HillClimberFrdPolicy.java#L31)
| **LIRS2** | An enhanced version of LIRS for better performance.
| **ARC++** | An enhanced version of ARC with additional adaptive mechanisms.
| **MicroARC** | A micro-optimized version of ARC designed for smaller cache sizes.
| **Sampled** | Uses sampling techniques to improve cache eviction decisions. | <li>[caffeine](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/sampled/SampledPolicy.java#L44)
|<h3><BR>Ideal</h3>
| **Belady’s Algorithm**<BR>Optimal Page Replacement | An ideal algorithm that evicts the page that will not be used for the longest period of time in the future. | <li>[wiki](https://en.wikipedia.org/wiki/Belady%27s_minimal_page_replacement_policy)<li>[caffeine](https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/opt/ClairvoyantPolicy.java#L37)

## Links

* https://s3fifo.com/
* https://s3fifo.com/blog/2023/06/01/fifo-is-better-than-lru-the-power-of-lazy-promotion-and-quick-demotion/
* Caffeine comparison of algorithms: https://github.com/ben-manes/caffeine/wiki/Efficiency
* https://github.com/ben-manes/caffeine/blob/master/simulator/src/main/java/com/github/benmanes/caffeine/cache/simulator/policy/adaptive/ArcPolicy.java (and other files in this and adjacent directories)
* https://github.com/memcached/memcached/pull/97
* https://flak.tedunangst.com/post/2Q-buffer-cache-algorithm
* Cache simulator: https://github.com/cacheMon/libCacheSim
* [A Buffer Cache Design for Global Ordering and Parallel Processing in the WAFL File System (pdf)](https://drive.google.com/file/d/1TQvogbONY-QUhrp9Ztom-lB6VjJiyFc7/view)

