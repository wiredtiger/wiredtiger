#ifndef COMPONENT_H
#define COMPONENT_H

namespace test_harness {
/*
 * A component is a class that defines 3 unique stages in its life-cycle.
 *  - Load: In this stage the component should be setting up its members, and creating anything it
 *  needs as part of the run stage. An example would be populating a database.
 *  - Run: This is the primary stage of the component, most if not all of the workload occurs at
 *  this point.
 */
class component {
    public:
    /*
     * The load function should perform all tasked required to setup the component for the main
     * phase of the test.
     */
    virtual void load() {
        _running = true;
    }

    /*
     * The run phase encompases all operations that occur during the primary phase of the workload.
     */
    virtual void run() = 0;

    /*
     * The finish phase is a cleanup phase, created objects are destroyed here and any final testing
     * requirements can be performed in this phase. An example could be the verifcation of the
     * database. Or checking some relevant statistics.
     */
    virtual void finish() {
        _running = false;
    }

protected:
    volatile bool _running;
};
}
#endif
