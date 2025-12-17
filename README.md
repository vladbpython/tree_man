# tree_man
Tool for Big Data group and filter.
You can build a tree of groups and subgroups of unlimited nesting levels with the ability to filter parallel to each group with the ability to rollback.

# Indexes
- Support field index (based in bit index)
- Support text index for full search text

# Structs
Supports only single structs with types:
- u128
- u64
- u32
- u16
- u8
- usize
- i128
- i64
- i32
- i16
- i8
- isize
- f64 (via ordered-float)
- f32 (via ordered-float)
- bool
- string

## In development
- Option<T>
- vec<T>
- *map<T>

# What to use for
- Real-time analytics
- Interactive dashboards
- OLAP queries
- Big Data processing

# Benchmarks
Benchmarks were tested on a MacBook Pro M2 with 32 GB of RAM.

## General include drill-down

```matlab

group_creation/10       time:   [1.7051 µs 1.7094 µs 1.7144 µs]
                        thrpt:  [5.8330 Melem/s 5.8501 Melem/s 5.8649 Melem/s]
Found 12 outliers among 100 measurements (12.00%)
  6 (6.00%) high mild
  6 (6.00%) high severe
group_creation/100      time:   [7.6516 µs 7.6684 µs 7.6878 µs]
                        thrpt:  [13.008 Melem/s 13.041 Melem/s 13.069 Melem/s]
Found 10 outliers among 100 measurements (10.00%)
  4 (4.00%) high mild
  6 (6.00%) high severe
group_creation/1000     time:   [184.33 µs 185.35 µs 186.41 µs]
                        thrpt:  [5.3647 Melem/s 5.3951 Melem/s 5.4250 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  3 (3.00%) low mild
  3 (3.00%) high mild
group_creation/10000    time:   [834.05 µs 836.82 µs 840.27 µs]
                        thrpt:  [11.901 Melem/s 11.950 Melem/s 11.990 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  2 (2.00%) high mild
  4 (4.00%) high severe
group_creation/100000   time:   [6.3228 ms 6.3460 ms 6.3720 ms]
                        thrpt:  [15.694 Melem/s 15.758 Melem/s 15.816 Melem/s]
Found 10 outliers among 100 measurements (10.00%)
  6 (6.00%) high mild
  4 (4.00%) high severe

group_by/10             time:   [70.281 µs 70.670 µs 71.046 µs]
                        thrpt:  [140.75 Kelem/s 141.50 Kelem/s 142.29 Kelem/s]
Found 6 outliers among 100 measurements (6.00%)
  1 (1.00%) low mild
  5 (5.00%) high mild
group_by/100            time:   [89.538 µs 90.469 µs 91.277 µs]
                        thrpt:  [1.0956 Melem/s 1.1054 Melem/s 1.1168 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  6 (6.00%) low mild
group_by/1000           time:   [134.92 µs 136.56 µs 138.36 µs]
                        thrpt:  [7.2275 Melem/s 7.3228 Melem/s 7.4118 Melem/s]
group_by/10000          time:   [242.50 µs 247.03 µs 252.23 µs]
                        thrpt:  [39.647 Melem/s 40.480 Melem/s 41.237 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  3 (3.00%) high mild
  5 (5.00%) high severe
group_by/100000         time:   [940.69 µs 954.88 µs 971.45 µs]
                        thrpt:  [102.94 Melem/s 104.72 Melem/s 106.31 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  3 (3.00%) high mild
  3 (3.00%) high severe

get_subgroup            time:   [27.098 ns 27.194 ns 27.348 ns]
Found 9 outliers among 100 measurements (9.00%)
  6 (6.00%) high mild
  3 (3.00%) high severe

filter/10               time:   [1.5262 µs 1.5333 µs 1.5400 µs]
                        thrpt:  [6.4936 Melem/s 6.5219 Melem/s 6.5524 Melem/s]
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high severe
filter/100              time:   [2.1494 µs 2.1589 µs 2.1682 µs]
                        thrpt:  [46.121 Melem/s 46.320 Melem/s 46.526 Melem/s]
Found 4 outliers among 100 measurements (4.00%)
  4 (4.00%) high mild
filter/1000             time:   [6.2455 µs 6.2613 µs 6.2770 µs]
                        thrpt:  [159.31 Melem/s 159.71 Melem/s 160.11 Melem/s]
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high severe
filter/10000            time:   [377.13 µs 379.72 µs 382.56 µs]
                        thrpt:  [26.140 Melem/s 26.335 Melem/s 26.516 Melem/s]
Found 5 outliers among 100 measurements (5.00%)
  1 (1.00%) low severe
  1 (1.00%) low mild
  2 (2.00%) high mild
  1 (1.00%) high severe

filter/100000           time:   [1.2358 ms 1.2437 ms 1.2530 ms]
                        thrpt:  [79.807 Melem/s 80.406 Melem/s 80.918 Melem/s]
Found 12 outliers among 100 measurements (12.00%)
  1 (1.00%) low severe
  7 (7.00%) low mild
  2 (2.00%) high mild
  2 (2.00%) high severe

clear_subgroups         time:   [93.683 µs 94.899 µs 95.982 µs]
Found 3 outliers among 100 measurements (3.00%)
  2 (2.00%) low mild
  1 (1.00%) high severe

collect_all_groups      time:   [329.94 ns 336.11 ns 342.14 ns]

create_single_field_index/100
                        time:   [10.964 µs 10.983 µs 11.009 µs]
                        thrpt:  [9.0837 Melem/s 9.1046 Melem/s 9.1207 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  7 (7.00%) high mild
  1 (1.00%) high severe
create_single_field_index/1000
                        time:   [411.40 µs 413.21 µs 415.09 µs]
                        thrpt:  [2.4091 Melem/s 2.4201 Melem/s 2.4307 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  1 (1.00%) low mild
  3 (3.00%) high mild
  2 (2.00%) high severe

create_single_field_index/10000
                        time:   [1.9090 ms 1.9213 ms 1.9419 ms]
                        thrpt:  [5.1495 Melem/s 5.2049 Melem/s 5.2384 Melem/s]
Found 11 outliers among 100 measurements (11.00%)
  5 (5.00%) low severe
  1 (1.00%) low mild
  3 (3.00%) high mild
  2 (2.00%) high severe
create_single_field_index/100000
                        time:   [16.567 ms 16.653 ms 16.737 ms]
                        thrpt:  [5.9747 Melem/s 6.0051 Melem/s 6.0361 Melem/s]
Found 5 outliers among 100 measurements (5.00%)
  3 (3.00%) low mild
  2 (2.00%) high mild

create_multiple_field_indexes/100
                        time:   [39.975 µs 40.062 µs 40.171 µs]
                        thrpt:  [2.4893 Melem/s 2.4961 Melem/s 2.5016 Melem/s]
Found 10 outliers among 100 measurements (10.00%)
  4 (4.00%) high mild
  6 (6.00%) high severe

create_multiple_field_indexes/1000
                        time:   [1.1177 ms 1.1222 ms 1.1269 ms]
                        thrpt:  [887.38 Kelem/s 891.07 Kelem/s 894.71 Kelem/s]
Found 9 outliers among 100 measurements (9.00%)
  4 (4.00%) low mild
  3 (3.00%) high mild
  2 (2.00%) high severe
create_multiple_field_indexes/10000
                        time:   [5.4990 ms 5.5201 ms 5.5409 ms]
                        thrpt:  [1.8048 Melem/s 1.8116 Melem/s 1.8185 Melem/s]
create_multiple_field_indexes/100000
                        time:   [49.326 ms 49.712 ms 50.213 ms]
                        thrpt:  [1.9915 Melem/s 2.0116 Melem/s 2.0273 Melem/s]
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high severe

create_field_index/100  time:   [2.6011 µs 2.6165 µs 2.6341 µs]
                        thrpt:  [37.963 Melem/s 38.219 Melem/s 38.445 Melem/s]
Found 3 outliers among 100 measurements (3.00%)
  2 (2.00%) high mild
  1 (1.00%) high severe
create_field_index/1000 time:   [16.330 µs 16.388 µs 16.455 µs]
                        thrpt:  [60.771 Melem/s 61.019 Melem/s 61.237 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  5 (5.00%) high mild
  2 (2.00%) high severe
create_field_index/10000
                        time:   [153.09 µs 153.42 µs 153.88 µs]
                        thrpt:  [64.985 Melem/s 65.180 Melem/s 65.321 Melem/s]
Found 10 outliers among 100 measurements (10.00%)
  6 (6.00%) high mild
  4 (4.00%) high severe

create_field_index/100000
                        time:   [1.9346 ms 1.9457 ms 1.9581 ms]
                        thrpt:  [51.070 Melem/s 51.396 Melem/s 51.690 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  4 (4.00%) high mild
  2 (2.00%) high severe

group_creation_with_field_indexes/100
                        time:   [33.149 µs 33.212 µs 33.288 µs]
                        thrpt:  [3.0041 Melem/s 3.0110 Melem/s 3.0166 Melem/s]
Found 23 outliers among 100 measurements (23.00%)
  7 (7.00%) high mild
  16 (16.00%) high severe

group_creation_with_field_indexes/1000
                        time:   [1.0649 ms 1.0714 ms 1.0792 ms]
                        thrpt:  [926.61 Kelem/s 933.34 Kelem/s 939.07 Kelem/s]
Found 5 outliers among 100 measurements (5.00%)
  3 (3.00%) low mild
  1 (1.00%) high mild
  1 (1.00%) high severe
group_creation_with_field_indexes/10000
                        time:   [4.8957 ms 4.9083 ms 4.9217 ms]
                        thrpt:  [2.0318 Melem/s 2.0374 Melem/s 2.0426 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  5 (5.00%) high mild
  1 (1.00%) high severe
group_creation_with_field_indexes/100000
                        time:   [44.362 ms 44.739 ms 45.167 ms]
                        thrpt:  [2.2140 Melem/s 2.2352 Melem/s 2.2542 Melem/s]
Found 2 outliers among 100 measurements (2.00%)
  1 (1.00%) high mild
  1 (1.00%) high severe

field_index_operation_and/1000
                        time:   [34.689 µs 34.739 µs 34.804 µs]
                        thrpt:  [28.733 Melem/s 28.786 Melem/s 28.827 Melem/s]
Found 14 outliers among 100 measurements (14.00%)
  6 (6.00%) high mild
  8 (8.00%) high severe
field_index_operation_and/10000
                        time:   [484.79 µs 487.46 µs 490.63 µs]
                        thrpt:  [20.382 Melem/s 20.514 Melem/s 20.627 Melem/s]
Found 9 outliers among 100 measurements (9.00%)
  2 (2.00%) low severe
  4 (4.00%) high mild
  3 (3.00%) high severe
field_index_operation_and/100000
                        time:   [2.3359 ms 2.3732 ms 2.4139 ms]
                        thrpt:  [41.427 Melem/s 42.137 Melem/s 42.810 Melem/s]
Found 2 outliers among 100 measurements (2.00%)
  1 (1.00%) high mild
  1 (1.00%) high severe

field_index_operation_vs_normal_filter/normal_filter/1000000
                        time:   [22.143 ms 22.415 ms 22.739 ms]
                        thrpt:  [43.977 Melem/s 44.614 Melem/s 45.161 Melem/s]
Found 12 outliers among 100 measurements (12.00%)
  8 (8.00%) high mild
  4 (4.00%) high severe

field_index_operation_vs_normal_filter/field_index_operation/1000000
                        time:   [13.832 ms 13.931 ms 14.057 ms]
                        thrpt:  [71.140 Melem/s 71.783 Melem/s 72.299 Melem/s]
Found 2 outliers among 100 measurements (2.00%)
  1 (1.00%) low mild
  1 (1.00%) high severe

field_index_operation_vs_normal_filter/normal_filter/2000000
                        time:   [46.279 ms 46.615 ms 46.990 ms]
                        thrpt:  [42.562 Melem/s 42.904 Melem/s 43.216 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  6 (6.00%) high mild
  2 (2.00%) high severe

field_index_operation_vs_normal_filter/field_index_operation/2000000
                        time:   [28.688 ms 28.997 ms 29.370 ms]
                        thrpt:  [68.098 Melem/s 68.972 Melem/s 69.716 Melem/s]
Found 4 outliers among 100 measurements (4.00%)
  2 (2.00%) high mild
  2 (2.00%) high severe

field_index_operation_vs_normal_filter/normal_filter/3000000
                        time:   [65.583 ms 66.110 ms 66.797 ms]
                        thrpt:  [44.912 Melem/s 45.379 Melem/s 45.744 Melem/s]
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high severe

field_index_operation_vs_normal_filter/field_index_operation/3000000
                        time:   [39.263 ms 39.541 ms 39.852 ms]
                        thrpt:  [75.279 Melem/s 75.871 Melem/s 76.407 Melem/s]
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high severe

complex_field_index_operations
                        time:   [3.2385 ms 3.2714 ms 3.3059 ms]
Found 2 outliers among 100 measurements (2.00%)
  1 (1.00%) high mild
  1 (1.00%) high severe

create_field_index_in_subgroups/1000
                        time:   [290.25 µs 292.69 µs 295.54 µs]
                        thrpt:  [3.3836 Melem/s 3.4166 Melem/s 3.4453 Melem/s]
Found 4 outliers among 100 measurements (4.00%)
  1 (1.00%) low mild
  2 (2.00%) high mild
  1 (1.00%) high severe

create_field_index_in_subgroups/10000
                        time:   [1.2960 ms 1.3105 ms 1.3286 ms]
                        thrpt:  [7.5266 Melem/s 7.6308 Melem/s 7.7163 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  2 (2.00%) low mild
  4 (4.00%) high mild
  2 (2.00%) high severe
create_field_index_in_subgroups/100000
                        time:   [8.7655 ms 8.8206 ms 8.8875 ms]
                        thrpt:  [11.252 Melem/s 11.337 Melem/s 11.408 Melem/s]
Found 5 outliers among 100 measurements (5.00%)
  2 (2.00%) high mild
  3 (3.00%) high severe

create_field_index_recursive
                        time:   [3.8576 ms 3.8726 ms 3.8883 ms]
Found 2 outliers among 100 measurements (2.00%)
  1 (1.00%) high mild
  1 (1.00%) high severe

group_by_with_field_indexes/1000
                        time:   [767.88 µs 772.51 µs 776.68 µs]
                        thrpt:  [1.2875 Melem/s 1.2945 Melem/s 1.3023 Melem/s]
Found 3 outliers among 100 measurements (3.00%)
  2 (2.00%) low mild
  1 (1.00%) high mild
group_by_with_field_indexes/10000
                        time:   [3.1929 ms 3.2112 ms 3.2298 ms]
                        thrpt:  [3.0961 Melem/s 3.1141 Melem/s 3.1320 Melem/s]
group_by_with_field_indexes/100000
                        time:   [20.286 ms 20.392 ms 20.499 ms]
                        thrpt:  [4.8782 Melem/s 4.9040 Melem/s 4.9294 Melem/s]
Found 3 outliers among 100 measurements (3.00%)
  1 (1.00%) low mild
  2 (2.00%) high mild

btree_subgroup_access/get_subgroup
                        time:   [33.659 ns 36.640 ns 39.880 ns]
Found 20 outliers among 100 measurements (20.00%)
  19 (19.00%) low severe
  1 (1.00%) low mild
btree_subgroup_access/first_last_subgroup
                        time:   [53.055 ns 53.544 ns 54.034 ns]
btree_subgroup_access/subgroups_range
                        time:   [123.23 ns 125.97 ns 128.71 ns]
Found 5 outliers among 100 measurements (5.00%)
  4 (4.00%) high mild
  1 (1.00%) high severe

parallel_filter         time:   [120.62 µs 121.64 µs 122.66 µs]
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) low mild

memory_allocation/100   time:   [111.91 µs 113.60 µs 115.65 µs]
                        thrpt:  [864.71 Kelem/s 880.24 Kelem/s 893.54 Kelem/s]
memory_allocation/1000  time:   [340.85 µs 343.76 µs 346.87 µs]
                        thrpt:  [2.8829 Melem/s 2.9090 Melem/s 2.9339 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  7 (7.00%) high mild
  1 (1.00%) high severe

memory_allocation/10000 time:   [1.1199 ms 1.1233 ms 1.1272 ms]
                        thrpt:  [8.8713 Melem/s 8.9020 Melem/s 8.9290 Melem/s]
Found 11 outliers among 100 measurements (11.00%)
  2 (2.00%) low mild
  5 (5.00%) high mild
  4 (4.00%) high severe

memory_with_field_indexes/100
                        time:   [47.817 µs 47.913 µs 48.018 µs]
                        thrpt:  [2.0826 Melem/s 2.0871 Melem/s 2.0913 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  4 (4.00%) high mild
  2 (2.00%) high severe

memory_with_field_indexes/1000
                        time:   [1.1895 ms 1.1938 ms 1.1984 ms]
                        thrpt:  [834.48 Kelem/s 837.69 Kelem/s 840.69 Kelem/s]
Found 6 outliers among 100 measurements (6.00%)
  1 (1.00%) low severe
  4 (4.00%) high mild
  1 (1.00%) high severe
memory_with_field_indexes/10000
                        time:   [5.9668 ms 5.9980 ms 6.0319 ms]
                        thrpt:  [1.6579 Melem/s 1.6672 Melem/s 1.6759 Melem/s]
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high severe

deep_hierarchy_creation time:   [813.80 µs 819.65 µs 824.73 µs]
Found 7 outliers among 100 measurements (7.00%)
  5 (5.00%) low severe
  2 (2.00%) low mild

```

## Try to compare include drill-down

```matlab


bench_query/FilterData_with_field_index_eq_price_100q_high_cardinality
                        time:   [176.31 µs 177.23 µs 178.33 µs]
                        thrpt:  [560.74 Kelem/s 564.24 Kelem/s 567.17 Kelem/s]
Found 5 outliers among 100 measurements (5.00%)
  2 (2.00%) high mild
  3 (3.00%) high severe
  
bench_query/FilterData_no_index_eq_price_100q_high_cardinality
                        time:   [338.44 ms 340.37 ms 342.35 ms]
                        thrpt:  [292.10  elem/s 293.80  elem/s 295.47  elem/s]
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild
  
bench_query/Vec_baseline_100q_eq_price_high_cardinality
                        time:   [489.18 ms 491.68 ms 494.29 ms]
                        thrpt:  [202.31  elem/s 203.39  elem/s 204.43  elem/s]
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild

bench_query/Vec_parallel_100q_eq_price_high_cardinality
                        time:   [233.38 ms 235.71 ms 238.43 ms]
                        thrpt:  [419.40  elem/s 424.26  elem/s 428.48  elem/s]
Found 2 outliers among 100 measurements (2.00%)
  1 (1.00%) high mild
  1 (1.00%) high severe

bench_query/FilterData_with_field_index_eq_price_100q_low_cardinality
                        time:   [1.4125 ms 1.4481 ms 1.4939 ms]
                        thrpt:  [66.939 Kelem/s 69.058 Kelem/s 70.798 Kelem/s]
Found 12 outliers among 100 measurements (12.00%)
  4 (4.00%) high mild
  8 (8.00%) high severe

bench_query/FilterData_no_index_eq_price_100q_low_cardinality
                        time:   [325.19 ms 329.07 ms 333.08 ms]
                        thrpt:  [300.22  elem/s 303.89  elem/s 307.51  elem/s]
Found 5 outliers among 100 measurements (5.00%)
  5 (5.00%) high mild

bench_query/Vec_baseline_100q_eq_price_low_cardinality
                        time:   [263.20 ms 264.34 ms 265.56 ms]
                        thrpt:  [376.56  elem/s 378.30  elem/s 379.93  elem/s]
Found 5 outliers among 100 measurements (5.00%)
  5 (5.00%) high mild

bench_query/Vec_parallel_100q_eq_price_low_cardinality
                        time:   [158.82 ms 160.54 ms 162.33 ms]
                        thrpt:  [616.02  elem/s 622.91  elem/s 629.66  elem/s]
Found 10 outliers among 100 measurements (10.00%)
  1 (1.00%) low mild
  9 (9.00%) high mild

bench_query/FilterData_with_field_index_eq_bool_100q_high_cardinality
                        time:   [426.21 ms 430.16 ms 434.42 ms]
                        thrpt:  [230.19  elem/s 232.47  elem/s 234.63  elem/s]
Found 6 outliers among 100 measurements (6.00%)
  4 (4.00%) high mild
  2 (2.00%) high severe

bench_query/FilterData_no_index_eq_bool_100q_high_cardinality
                        time:   [431.66 ms 439.30 ms 450.29 ms]
                        thrpt:  [222.08  elem/s 227.64  elem/s 231.67  elem/s]
Found 7 outliers among 100 measurements (7.00%)
  3 (3.00%) high mild
  4 (4.00%) high severe

bench_query/Vec_baseline_100q_eq_bool_high_cardinality
                        time:   [655.92 ms 665.36 ms 675.63 ms]
                        thrpt:  [148.01  elem/s 150.30  elem/s 152.46  elem/s]
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high mild

bench_query/Vec_parallel_100q_eq_bool_high_cardinality
                        time:   [323.18 ms 325.91 ms 329.00 ms]
                        thrpt:  [303.95  elem/s 306.83  elem/s 309.42  elem/s]
Found 5 outliers among 100 measurements (5.00%)
  4 (4.00%) high mild
  1 (1.00%) high severe

bench_query/FilterData_with_field_index_eq_bool_100q_low_cardinality
                        time:   [451.81 ms 461.66 ms 473.95 ms]
                        thrpt:  [210.99  elem/s 216.61  elem/s 221.33  elem/s]
Found 6 outliers among 100 measurements (6.00%)
  4 (4.00%) high mild
  2 (2.00%) high severe
 
bench_query/FilterData_no_index_eq_bool_100q_low_cardinality
                        time:   [433.10 ms 437.00 ms 440.92 ms]
                        thrpt:  [226.80  elem/s 228.83  elem/s 230.90  elem/s]
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild
 
bench_query/Vec_baseline_100q_eq_bool_low_cardinality
                        time:   [673.23 ms 680.25 ms 687.85 ms]
                        thrpt:  [145.38  elem/s 147.01  elem/s 148.54  elem/s]
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high mild
 
bench_query/Vec_parallel_100q_eq_bool_low_cardinality
                        time:   [320.52 ms 324.21 ms 328.04 ms]
                        thrpt:  [304.84  elem/s 308.44  elem/s 311.99  elem/s]
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high mild

bench_query/FilterData_with_field_index_not_eq_100q_high_cardinality
                        time:   [459.68 ms 464.62 ms 469.71 ms]
                        thrpt:  [212.90  elem/s 215.23  elem/s 217.54  elem/s]
Found 4 outliers among 100 measurements (4.00%)
  4 (4.00%) high mild

bench_query/FilterData_no_index_not_eq_100q_high_cardinality
                        time:   [437.03 ms 440.15 ms 443.89 ms]
                        thrpt:  [225.28  elem/s 227.20  elem/s 228.82  elem/s]
Found 6 outliers among 100 measurements (6.00%)
  3 (3.00%) high mild
  3 (3.00%) high severe

bench_query/Vec_baseline_100q_not_eq_high_cardinality
                        time:   [811.05 ms 816.61 ms 822.42 ms]
                        thrpt:  [121.59  elem/s 122.46  elem/s 123.30  elem/s]
Found 8 outliers among 100 measurements (8.00%)
  8 (8.00%) high mild

bench_query/Vec_parallel_100q_not_eq_high_cardinality
                        time:   [354.14 ms 356.54 ms 359.05 ms]
                        thrpt:  [278.51  elem/s 280.48  elem/s 282.37  elem/s]
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild

bench_query/FilterData_with_field_index_not_eq_100q_low_cardinality
                        time:   [1.3849 ms 1.4006 ms 1.4210 ms]
                        thrpt:  [70.375 Kelem/s 71.398 Kelem/s 72.206 Kelem/s]
Found 14 outliers among 100 measurements (14.00%)
  5 (5.00%) high mild
  9 (9.00%) high severe

bench_query/FilterData_no_index_not_eq_100q_low_cardinality
                        time:   [270.71 ms 273.95 ms 277.34 ms]
                        thrpt:  [360.57  elem/s 365.02  elem/s 369.40  elem/s]
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high mild

bench_query/Vec_baseline_100q_not_eq_low_cardinality
                        time:   [258.29 ms 258.97 ms 259.74 ms]
                        thrpt:  [385.00  elem/s 386.14  elem/s 387.16  elem/s]
Found 8 outliers among 100 measurements (8.00%)
  6 (6.00%) high mild
  2 (2.00%) high severe

bench_query/Vec_parallel_100q_not_eq_low_cardinality
                        time:   [153.75 ms 155.19 ms 156.74 ms]
                        thrpt:  [637.98  elem/s 644.37  elem/s 650.41  elem/s]
Found 3 outliers among 100 measurements (3.00%)
  2 (2.00%) high mild
  1 (1.00%) high severe

bench_query/FilterData_with_field_in_value_index_100q_high_cadinality
                        time:   [219.37 µs 220.56 µs 221.68 µs]
                        thrpt:  [451.11 Kelem/s 453.40 Kelem/s 455.84 Kelem/s]

bench_query/FilterData_in_value_no_index_100q_high_cadinality
                        time:   [314.25 ms 317.76 ms 321.53 ms]
                        thrpt:  [311.01  elem/s 314.70  elem/s 318.22  elem/s]
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high mild

bench_query/Vec_in_value_baseline_100q_high_cadinality
                        time:   [656.40 ms 658.65 ms 660.98 ms]
                        thrpt:  [151.29  elem/s 151.83  elem/s 152.35  elem/s]
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high mild

bench_query/Vec_in_value_parallel_100q_high_cadinality
                        time:   [225.48 ms 227.85 ms 230.67 ms]
                        thrpt:  [433.52  elem/s 438.89  elem/s 443.50  elem/s]
Found 6 outliers among 100 measurements (6.00%)
  2 (2.00%) high mild
  4 (4.00%) high severe

bench_query/FilterData_with_field_in_value_index_100q_low_cardinality
                        time:   [6.7237 ms 6.8643 ms 7.0189 ms]
                        thrpt:  [14.247 Kelem/s 14.568 Kelem/s 14.873 Kelem/s]
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high mild
  
bench_query/FilterData_in_value_no_index_100q_low_cardinality
                        time:   [293.42 ms 296.77 ms 300.33 ms]
                        thrpt:  [332.96  elem/s 336.96  elem/s 340.81  elem/s]
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild

bench_query/Vec_in_value_baseline_100q_low_cardinality
                        time:   [276.61 ms 278.41 ms 280.30 ms]
                        thrpt:  [356.75  elem/s 359.18  elem/s 361.52  elem/s]
Found 10 outliers among 100 measurements (10.00%)
  10 (10.00%) high mild

bench_query/Vec_in_value_parallel_100q_low_cardinality
                        time:   [153.08 ms 154.18 ms 155.34 ms]
                        thrpt:  [643.76  elem/s 648.60  elem/s 653.25  elem/s]
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild

Benchmarking bench_query/FilterData_with_field_in_range_index_100q_high_cardinality: Collecting 10 samples in estimate
bench_query/FilterData_with_field_in_range_index_100q_high_cardinality
                        time:   [3.6359 ms 3.6601 ms 3.6928 ms]
                        thrpt:  [27.079 Kelem/s 27.322 Kelem/s 27.504 Kelem/s]
Found 2 outliers among 10 measurements (20.00%)
  2 (20.00%) high severe

bench_query/FilterData_in_range_no_index_100q_high_cardinality
                        time:   [311.95 ms 317.33 ms 326.24 ms]
                        thrpt:  [306.53  elem/s 315.13  elem/s 320.57  elem/s]
Found 2 outliers among 10 measurements (20.00%)
  1 (10.00%) low mild
  1 (10.00%) high severe
Benchmarking bench_query/Vec_in_range_baseline_100q_high_cardinality: Collecting 10 samples in estimated 12.170 s (20 
bench_query/Vec_in_range_baseline_100q_high_cardinality
                        time:   [607.10 ms 609.99 ms 612.77 ms]
                        thrpt:  [163.19  elem/s 163.94  elem/s 164.72  elem/s]

bench_query/Vec_in_range_parallel_100q_high_cardinality
                        time:   [218.72 ms 221.51 ms 224.57 ms]
                        thrpt:  [445.29  elem/s 451.44  elem/s 457.20  elem/s]
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe

bench_query/FilterData_with_field_in_range_index_100q_low_cardinality
                        time:   [412.83 ms 418.77 ms 424.75 ms]
                        thrpt:  [235.43  elem/s 238.79  elem/s 242.23  elem/s]
                        
bench_query/FilterData_in_range_no_index_100q_low_cardinality
                        time:   [397.69 ms 410.81 ms 423.46 ms]
                        thrpt:  [236.15  elem/s 243.42  elem/s 251.45  elem/s]
Found 2 outliers among 10 measurements (20.00%)
  1 (10.00%) low mild
  1 (10.00%) high mild

bench_query/Vec_in_range_baseline_100q_low_cardinality
                        time:   [466.60 ms 507.36 ms 568.11 ms]
                        thrpt:  [176.02  elem/s 197.10  elem/s 214.32  elem/s]
Found 2 outliers among 10 measurements (20.00%)
  1 (10.00%) high mild
  1 (10.00%) high severe

bench_query/Vec_in_range_parallel_100q_low_cardinality
                        time:   [270.31 ms 273.48 ms 278.91 ms]
                        thrpt:  [358.54  elem/s 365.66  elem/s 369.94  elem/s]

```
