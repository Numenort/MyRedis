QPS:
====== PING_INLINE ======
  100000 requests completed in 2.35 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

0.03% <= 0.1 milliseconds
0.43% <= 0.2 milliseconds
2.14% <= 0.3 milliseconds
8.10% <= 0.4 milliseconds
22.24% <= 0.5 milliseconds
42.07% <= 0.6 milliseconds
60.52% <= 0.7 milliseconds
74.35% <= 0.8 milliseconds
83.71% <= 0.9 milliseconds
89.50% <= 1.0 milliseconds
92.91% <= 1.1 milliseconds
95.03% <= 1.2 milliseconds
96.39% <= 1.3 milliseconds
97.29% <= 1.4 milliseconds
97.95% <= 1.5 milliseconds
98.40% <= 1.6 milliseconds
98.74% <= 1.7 milliseconds
98.99% <= 1.8 milliseconds
99.17% <= 1.9 milliseconds
99.33% <= 2 milliseconds
99.87% <= 3 milliseconds
99.94% <= 4 milliseconds
99.95% <= 5 milliseconds
99.96% <= 6 milliseconds
99.98% <= 7 milliseconds
100.00% <= 8 milliseconds
42553.19 requests per second

====== PING_BULK ======
  100000 requests completed in 2.18 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

92.54% <= 1 milliseconds
99.66% <= 2 milliseconds
99.96% <= 3 milliseconds
100.00% <= 3 milliseconds
45766.59 requests per second

====== SET ======
  100000 requests completed in 2.44 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

79.81% <= 1 milliseconds
98.99% <= 2 milliseconds
99.90% <= 3 milliseconds
99.95% <= 4 milliseconds
99.95% <= 6 milliseconds
99.97% <= 7 milliseconds
99.99% <= 8 milliseconds
100.00% <= 8 milliseconds
41000.41 requests per second

====== GET ======
  100000 requests completed in 2.23 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

91.58% <= 1 milliseconds
99.67% <= 2 milliseconds
99.97% <= 3 milliseconds
100.00% <= 4 milliseconds
44742.73 requests per second

====== INCR ======
  100000 requests completed in 2.32 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

88.71% <= 1 milliseconds
99.64% <= 2 milliseconds
99.97% <= 3 milliseconds
100.00% <= 3 milliseconds
43140.64 requests per second

====== LPUSH ======
  100000 requests completed in 2.23 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

91.07% <= 1 milliseconds
99.38% <= 2 milliseconds
99.73% <= 3 milliseconds
99.86% <= 4 milliseconds
99.93% <= 5 milliseconds
99.96% <= 6 milliseconds
99.96% <= 7 milliseconds
100.00% <= 11 milliseconds
100.00% <= 11 milliseconds
44923.63 requests per second

====== RPUSH ======
  100000 requests completed in 2.21 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

90.57% <= 1 milliseconds
99.42% <= 2 milliseconds
99.92% <= 3 milliseconds
99.96% <= 4 milliseconds
99.97% <= 5 milliseconds
99.97% <= 6 milliseconds
99.99% <= 7 milliseconds
100.00% <= 8 milliseconds
45330.91 requests per second

====== LPOP ======
  100000 requests completed in 2.16 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

92.22% <= 1 milliseconds
99.67% <= 2 milliseconds
99.97% <= 3 milliseconds
100.00% <= 3 milliseconds
46274.87 requests per second

====== RPOP ======
  100000 requests completed in 2.14 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

92.84% <= 1 milliseconds
99.67% <= 2 milliseconds
99.97% <= 3 milliseconds
100.00% <= 4 milliseconds
100.00% <= 4 milliseconds
46685.34 requests per second

====== SADD ======
  100000 requests completed in 2.20 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

91.21% <= 1 milliseconds
99.60% <= 2 milliseconds
99.82% <= 3 milliseconds
99.92% <= 4 milliseconds
99.95% <= 5 milliseconds
99.96% <= 6 milliseconds
100.00% <= 6 milliseconds
45433.89 requests per second

====== HSET ======
  100000 requests completed in 2.17 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

92.15% <= 1 milliseconds
99.67% <= 2 milliseconds
99.96% <= 3 milliseconds
100.00% <= 4 milliseconds
100.00% <= 4 milliseconds
46040.52 requests per second

====== SPOP ======
  100000 requests completed in 2.16 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

92.36% <= 1 milliseconds
99.69% <= 2 milliseconds
99.97% <= 3 milliseconds
100.00% <= 4 milliseconds
46360.68 requests per second

====== ZADD ======
  100000 requests completed in 2.17 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

92.42% <= 1 milliseconds
99.79% <= 2 milliseconds
99.98% <= 3 milliseconds
100.00% <= 4 milliseconds
100.00% <= 4 milliseconds
46146.75 requests per second

====== ZPOPMIN ======
  100000 requests completed in 2.16 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

92.37% <= 1 milliseconds
99.54% <= 2 milliseconds
99.90% <= 3 milliseconds
99.97% <= 4 milliseconds
99.98% <= 5 milliseconds
100.00% <= 5 milliseconds
46232.08 requests per second

====== LPUSH (needed to benchmark LRANGE) ======
  100000 requests completed in 2.15 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

92.08% <= 1 milliseconds
99.63% <= 2 milliseconds
99.96% <= 3 milliseconds
99.99% <= 4 milliseconds
100.00% <= 4 milliseconds
46403.71 requests per second

====== LRANGE_100 (first 100 elements) ======
  100000 requests completed in 2.93 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

79.86% <= 1 milliseconds
99.21% <= 2 milliseconds
99.69% <= 3 milliseconds
99.81% <= 4 milliseconds
99.87% <= 5 milliseconds
99.88% <= 6 milliseconds
99.93% <= 7 milliseconds
99.97% <= 8 milliseconds
99.99% <= 9 milliseconds
99.99% <= 10 milliseconds
100.00% <= 13 milliseconds
34153.00 requests per second

====== LRANGE_300 (first 300 elements) ======
  100000 requests completed in 5.43 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

3.73% <= 1 milliseconds
96.26% <= 2 milliseconds
99.06% <= 3 milliseconds
99.37% <= 4 milliseconds
99.62% <= 5 milliseconds
99.80% <= 6 milliseconds
99.83% <= 7 milliseconds
99.88% <= 8 milliseconds
99.90% <= 9 milliseconds
99.93% <= 10 milliseconds
99.97% <= 11 milliseconds
99.99% <= 12 milliseconds
100.00% <= 13 milliseconds
100.00% <= 13 milliseconds
18426.39 requests per second

====== LRANGE_500 (first 450 elements) ======
  100000 requests completed in 6.95 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

0.06% <= 1 milliseconds
78.54% <= 2 milliseconds
98.52% <= 3 milliseconds
99.16% <= 4 milliseconds
99.33% <= 5 milliseconds
99.44% <= 6 milliseconds
99.62% <= 7 milliseconds
99.74% <= 8 milliseconds
99.84% <= 9 milliseconds
99.87% <= 10 milliseconds
99.88% <= 11 milliseconds
99.92% <= 12 milliseconds
99.95% <= 13 milliseconds
99.97% <= 14 milliseconds
99.97% <= 15 milliseconds
99.99% <= 16 milliseconds
99.99% <= 17 milliseconds
100.00% <= 18 milliseconds
100.00% <= 19 milliseconds
14390.56 requests per second

====== LRANGE_600 (first 600 elements) ======
  100000 requests completed in 8.54 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

0.08% <= 1 milliseconds
41.24% <= 2 milliseconds
94.90% <= 3 milliseconds
98.24% <= 4 milliseconds
98.73% <= 5 milliseconds
98.99% <= 6 milliseconds
99.26% <= 7 milliseconds
99.47% <= 8 milliseconds
99.65% <= 9 milliseconds
99.84% <= 10 milliseconds
99.92% <= 11 milliseconds
99.95% <= 12 milliseconds
99.96% <= 13 milliseconds
99.97% <= 14 milliseconds
99.98% <= 15 milliseconds
100.00% <= 16 milliseconds
100.00% <= 18 milliseconds
100.00% <= 21 milliseconds
11709.60 requests per second

====== MSET (10 keys) ======
  100000 requests completed in 2.45 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

85.89% <= 1 milliseconds
99.22% <= 2 milliseconds
99.75% <= 3 milliseconds
99.89% <= 4 milliseconds
99.96% <= 5 milliseconds
99.96% <= 6 milliseconds
99.96% <= 7 milliseconds
99.97% <= 8 milliseconds
99.97% <= 11 milliseconds
100.00% <= 12 milliseconds
100.00% <= 21 milliseconds
40783.04 requests per second