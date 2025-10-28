# tree_man
Tool for Big Data group and filter.
You can build a tree of groups and subgroups of unlimited nesting levels with the ability to filter parallel to each group with the ability to rollback.

# What to use for
- Real-time analytics
- Interactive dashboards
- OLAP queries
- Big Data processing

# Benchmarks

Benchmarks were tested on a MacBook Pro M2 with 32 GB of RAM.

```matlab

group_creation/1000000  time:   [69.617 ms 70.301 ms 71.122 ms]
                        change: [−0.9812% +0.1466% +1.5020%] (p = 0.82 > 0.05)
                        No change in performance detected.
Found 11 outliers among 100 measurements (11.00%)
  6 (6.00%) high mild
  5 (5.00%) high severe

group_by/10             time:   [36.479 µs 38.026 µs 39.899 µs]
                        change: [−5.9138% −0.0057% +5.9125%] (p = 1.00 > 0.05)
                        No change in performance detected.
Found 6 outliers among 100 measurements (6.00%)
  3 (3.00%) high mild
  3 (3.00%) high severe
group_by/100            time:   [65.828 µs 68.474 µs 72.334 µs]
                        change: [+0.0051% +9.2984% +20.540%] (p = 0.07 > 0.05)
                        No change in performance detected.
Found 10 outliers among 100 measurements (10.00%)
  2 (2.00%) high mild
  8 (8.00%) high severe

group_by/1000           time:   [177.78 µs 190.40 µs 205.12 µs]
Found 6 outliers among 100 measurements (6.00%)
  1 (1.00%) high mild
  5 (5.00%) high severe

group_by/10000          time:   [589.39 µs 612.54 µs 639.57 µs]
Found 9 outliers among 100 measurements (9.00%)
  4 (4.00%) high mild
  5 (5.00%) high severe

group_by/1000000        time:   [17.528 ms 18.489 ms 19.640 ms]
Found 8 outliers among 100 measurements (8.00%)
  8 (8.00%) high severe

get_subgroup            time:   [31.068 ns 31.497 ns 32.126 ns]
Found 18 outliers among 100 measurements (18.00%)
  5 (5.00%) low mild
  3 (3.00%) high mild
  10 (10.00%) high severe

go_to_next_relative     time:   [10.107 ns 10.238 ns 10.434 ns]
Found 10 outliers among 100 measurements (10.00%)
  1 (1.00%) low mild
  3 (3.00%) high mild
  6 (6.00%) high severe

has_next_relative       time:   [3.6735 ns 3.7688 ns 3.8968 ns]
Found 16 outliers among 100 measurements (16.00%)
  8 (8.00%) high mild
  8 (8.00%) high severe

filter/10               time:   [15.598 µs 16.675 µs 17.980 µs]
Found 6 outliers among 100 measurements (6.00%)
  3 (3.00%) high mild
  3 (3.00%) high severe

filter/100              time:   [50.681 µs 53.299 µs 56.490 µs]
Found 12 outliers among 100 measurements (12.00%)
  5 (5.00%) high mild
  7 (7.00%) high severe

filter/1000             time:   [98.215 µs 101.84 µs 106.69 µs]
Found 7 outliers among 100 measurements (7.00%)
  1 (1.00%) high mild
  6 (6.00%) high severe

filter/10000            time:   [322.09 µs 333.36 µs 347.33 µs]
Found 7 outliers among 100 measurements (7.00%)
  1 (1.00%) low mild
  2 (2.00%) high mild
  4 (4.00%) high severe
filter/1000000          time:   [5.5357 ms 5.8989 ms 6.3865 ms]

Found 7 outliers among 100 measurements (7.00%)
  1 (1.00%) high mild
  6 (6.00%) high severe

clear_subgroups         time:   [64.327 µs 66.691 µs 69.825 µs]
Found 17 outliers among 100 measurements (17.00%)
  7 (7.00%) high mild
  10 (10.00%) high severe

collect_all_groups      time:   [2.1572 µs 2.2162 µs 2.2803 µs]
Found 15 outliers among 100 measurements (15.00%)
  10 (10.00%) high mild
  5 (5.00%) high severe

concurrent_reads/2      time:   [63.874 µs 70.545 µs 78.438 µs]
Found 11 outliers among 100 measurements (11.00%)
  5 (5.00%) high mild
  6 (6.00%) high severe

concurrent_reads/4      time:   [88.194 µs 95.785 µs 104.95 µs]
Found 8 outliers among 100 measurements (8.00%)
  4 (4.00%) high mild
  4 (4.00%) high severe

concurrent_reads/8      time:   [174.80 µs 184.31 µs 196.30 µs]
Found 5 outliers among 100 measurements (5.00%)
  5 (5.00%) high severe

concurrent_reads/16     time:   [288.95 µs 305.11 µs 328.51 µs]
Found 10 outliers among 100 measurements (10.00%)
  1 (1.00%) high mild
  9 (9.00%) high severe

concurrent_relative_navigation
                        time:   [161.94 µs 173.38 µs 187.53 µs]
Found 12 outliers among 100 measurements (12.00%)
  4 (4.00%) high mild
  8 (8.00%) high severe

parallel_filter         time:   [1.8345 ms 1.9246 ms 2.0046 ms]
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high severe

memory_allocation/100   time:   [86.831 µs 90.020 µs 94.331 µs]
Found 10 outliers among 100 measurements (10.00%)
  6 (6.00%) high mild
  4 (4.00%) high severe

memory_allocation/1000  time:   [351.82 µs 363.50 µs 377.95 µs]
Found 5 outliers among 100 measurements (5.00%)
  1 (1.00%) high mild
  4 (4.00%) high severe

memory_allocation/10000 time:   [1.2853 ms 1.3262 ms 1.3829 ms]
Found 8 outliers among 100 measurements (8.00%)
  2 (2.00%) high mild
  6 (6.00%) high severe

deep_hierarchy_creation time:   [567.94 µs 579.39 µs 595.03 µs]
Found 5 outliers among 100 measurements (5.00%)
  2 (2.00%) high mild
  3 (3.00%) high severe

```
