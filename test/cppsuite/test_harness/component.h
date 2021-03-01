#ifndef COMPONENT_H
#define COMPONENT_H

namespace test_harness {
/*
 * This is an interface intended to be implemented by most test framework classes. Defining some
 * standard functions which can then be called in sequence by the top level test class.
 */
class component {
    public:
    /*
     * The load function should perform all tasked required to setup the component for the main
     * phase of the test.
     */
    virtual void load() = 0;

    /*
     * The run phase encompases all operations that occur during the primary phase of the workload.
     */
    virtual void run() = 0;

    /*
     * The finish phase is a cleanup phase, created objects are destroyed here and any final testing
     * requirements can be performed in this phase. An example could be the verifcation of the
     * database. Or checking some relevant statistics.
     */
    virtual void finish() = 0;
};
}
#endif