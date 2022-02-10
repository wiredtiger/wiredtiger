#ifndef WT_ERRORHANDLER_H
#define WT_ERRORHANDLER_H

class ErrorHandler {
    public:
    static void throwIfNonZero(int result);

    private:
    ErrorHandler() = delete;
};

#endif // WT_ERRORHANDLER_H
