inline std::ostream&
operator<<(std::ostream &os, const timespec &ts)
{
    double secs = ts.tv_sec + (double)ts.tv_nsec / (double)1000000000;
    os << std::fixed << std::setprecision(2) << secs;
    return os;
}

inline timespec
operator-(const timespec &lhs, const timespec &rhs)
{
    timespec ts;

    if (lhs.tv_nsec < rhs.tv_nsec) {
        ts.tv_sec = lhs.tv_sec - rhs.tv_sec - 1;
        ts.tv_nsec = lhs.tv_nsec - rhs.tv_nsec + 1000000000;
    } else {
        ts.tv_sec = lhs.tv_sec - rhs.tv_sec;
        ts.tv_nsec = lhs.tv_nsec - rhs.tv_nsec;
    }
    return ts;
}

inline timespec
operator+(const timespec &lhs, const int n)
{
    timespec ts = lhs;
    ts.tv_sec += n;
    return ts;
}

inline bool
operator<(const timespec &lhs, const timespec &rhs)
{
    if (lhs.tv_sec == rhs.tv_sec)
        return (lhs.tv_nsec < rhs.tv_nsec);
    else
        return (lhs.tv_sec < rhs.tv_sec);
}

inline bool
operator>=(const timespec &lhs, const timespec &rhs)
{
    return (!(lhs < rhs));
}

inline bool
operator==(const timespec &lhs, int n)
{
    return (lhs.tv_sec == n && lhs.tv_nsec == 0);
}

inline bool
operator!=(const timespec &lhs, int n)
{
    return (lhs.tv_sec != n || lhs.tv_nsec != 0);
}

inline timespec &
operator+=(timespec &lhs, const int n)
{
    lhs.tv_sec += n;
    return lhs;
}

inline bool
operator==(const timespec &lhs, const timespec &rhs)
{
    return (lhs.tv_sec == rhs.tv_sec && lhs.tv_nsec == rhs.tv_nsec);
}

inline timespec &
operator-=(timespec &lhs, const timespec &rhs)
{
    lhs.tv_sec -= rhs.tv_sec;
    lhs.tv_nsec -= rhs.tv_nsec;
    if (lhs.tv_nsec < 0) {
        lhs.tv_nsec += 1000000000;
        lhs.tv_sec -= 1;
    }
    return lhs;
}
