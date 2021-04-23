#include "thread_context.h"
namespace test_framework {
class workload {
    virtual void read_operation(thread_context t) = 0;
    virtual void update_operation() = 0;
    virtual void remove_operation() = 0;
};

} // namespace test_framework
