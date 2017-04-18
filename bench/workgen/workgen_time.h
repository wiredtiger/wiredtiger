inline std::ostream&
operator<<(std::ostream &os, const timespec &ts)
{
    char oldfill;
    std::streamsize oldwidth;

    os << ts.tv_sec << ".";
    oldfill = os.fill('0');
    oldwidth = os.width(3);
    os << (int)(ts.tv_nsec / 1000000);
    os.fill(oldfill);
    os.width(oldwidth);
    return (os);
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
    return (ts);
}

inline timespec
operator+(const timespec &lhs, const int n)
{
    timespec ts = lhs;
    ts.tv_sec += n;
    return (ts);
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
operator>(const timespec &lhs, const timespec &rhs)
{
    if (lhs.tv_sec == rhs.tv_sec)
	return (lhs.tv_nsec > rhs.tv_nsec);
    else
	return (lhs.tv_sec > rhs.tv_sec);
}

inline bool
operator>=(const timespec &lhs, const timespec &rhs)
{
    return (!(lhs < rhs));
}

inline bool
operator<=(const timespec &lhs, const timespec &rhs)
{
    return (!(lhs > rhs));
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
    return (lhs);
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
    return (lhs);
}

inline timespec
ts_add_ms(const timespec &lhs, const uint64_t n)
{
    timespec ts;

    ts.tv_sec = lhs.tv_sec + (n / 1000);
    ts.tv_nsec = lhs.tv_nsec + 1000000 * (n % 1000);
    while (ts.tv_nsec > 1000000000) {
	ts.tv_nsec -= 1000000000;
	ts.tv_sec++;
    }
    return (ts);
}

inline void
ts_assign(timespec &lhs, const timespec &rhs)
{
    lhs.tv_sec = rhs.tv_sec;
    lhs.tv_nsec = rhs.tv_nsec;
}

inline void
ts_clear(timespec &ts)
{
    ts.tv_sec = 0;
    ts.tv_nsec = 0;
}

inline uint64_t
ts_ms(const timespec &ts)
{
    return (ts.tv_nsec / 1000000) + (ts.tv_sec * 1000);
}

inline uint64_t
ts_us(const timespec &ts)
{
    return (ts.tv_nsec / 1000) + (ts.tv_sec * 1000000);
}
