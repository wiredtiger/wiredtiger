
   0. Building

$ cd build; cmake -G ninja ..; ninja

   1. Creating a database

$ bench/simple_read/simple_read -w

   2. Running the workload

$ WIREDTIGER_CONFIG="statistics=[all],statistics_log=(wait=1,json=false,on_close=true),verbose=[evictserver]" bench/simple_read/simple_read -r --cache_size=800000 > q

   3. Processing logs:

$ perl -nE 'if (/^search (.{12})/) { say $1 }' q | sort | uniq -c > qq2 & perl -nE 'if (/consider.*key (.{12})/) { say $1 }' q | sort | uniq -c > qq & perl -nE 'if (/consider.*key (.{12})/) { say "$. $1" }' q > qq-time & wait ; perl -nE 'print if (110000 ... 145000)' qq-time  > qq-time-zoom

   4. Plotting:

$ gnuplot
gnuplot> plot 'qq' using 2:1, 'qq2' using 2:1
gnuplot> plot 'qq-time' using 1:2 with dots
gnuplot> plot 'qq-time-zoom' using 1:2 with dots

