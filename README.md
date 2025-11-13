# tree_man
Tool for Big Data group and filter.
You can build a tree of groups and subgroups of unlimited nesting levels with the ability to filter parallel to each group with the ability to rollback.

# Indexes
Add support Indexes

# What to use for
- Real-time analytics
- Interactive dashboards
- OLAP queries
- Big Data processing

# Benchmarks


Benchmarks were tested on a MacBook Pro M2 with 32 GB of RAM.

## General

```matlab

group_creation/10       time:   [1.4515 µs 1.4561 µs 1.4612 µs]
                        thrpt:  [6.8436 Melem/s 6.8677 Melem/s 6.8894 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  4 (4.00%) high mild
  3 (3.00%) high severe
group_creation/100      time:   [7.9127 µs 7.9511 µs 8.0025 µs]
                        thrpt:  [12.496 Melem/s 12.577 Melem/s 12.638 Melem/s]
Found 9 outliers among 100 measurements (9.00%)
  3 (3.00%) high mild
  6 (6.00%) high severe
group_creation/1000     time:   [198.02 µs 202.21 µs 207.50 µs]
                        thrpt:  [4.8193 Melem/s 4.9454 Melem/s 5.0499 Melem/s]
Found 11 outliers among 100 measurements (11.00%)
  1 (1.00%) low mild
  1 (1.00%) high mild
  9 (9.00%) high severe
group_creation/10000    time:   [885.93 µs 897.76 µs 911.61 µs]
                        thrpt:  [10.970 Melem/s 11.139 Melem/s 11.288 Melem/s]
Found 11 outliers among 100 measurements (11.00%)
  1 (1.00%) high mild
  10 (10.00%) high severe
group_creation/100000   time:   [7.0105 ms 7.0552 ms 7.1084 ms]
                        thrpt:  [14.068 Melem/s 14.174 Melem/s 14.264 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  2 (2.00%) high mild
  5 (5.00%) high severe

group_by/10             time:   [60.975 µs 62.513 µs 64.085 µs]
                        thrpt:  [156.04 Kelem/s 159.97 Kelem/s 164.00 Kelem/s]
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high severe
group_by/100            time:   [82.676 µs 84.852 µs 87.365 µs]
                        thrpt:  [1.1446 Melem/s 1.1785 Melem/s 1.2095 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  3 (3.00%) high mild
  4 (4.00%) high severe
group_by/1000           time:   [132.34 µs 136.37 µs 140.64 µs]
                        thrpt:  [7.1106 Melem/s 7.3328 Melem/s 7.5560 Melem/s]
Found 4 outliers among 100 measurements (4.00%)
  2 (2.00%) high mild
  2 (2.00%) high severe
group_by/10000          time:   [252.16 µs 258.81 µs 266.38 µs]
                        thrpt:  [37.540 Melem/s 38.638 Melem/s 39.657 Melem/s]
Found 5 outliers among 100 measurements (5.00%)
  2 (2.00%) high mild
  3 (3.00%) high severe

group_by/100000         time:   [1.1077 ms 1.1377 ms 1.1735 ms]
                        thrpt:  [85.216 Melem/s 87.900 Melem/s 90.274 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  1 (1.00%) high mild
  6 (6.00%) high severe

get_subgroup            time:   [30.769 ns 30.912 ns 31.076 ns]
Found 6 outliers among 100 measurements (6.00%)
  4 (4.00%) high mild
  2 (2.00%) high severe

filter/10               time:   [21.974 µs 22.529 µs 23.058 µs]
                        thrpt:  [433.69 Kelem/s 443.88 Kelem/s 455.09 Kelem/s]
Found 3 outliers among 100 measurements (3.00%)
  1 (1.00%) low mild
  1 (1.00%) high mild
  1 (1.00%) high severe
filter/100              time:   [51.445 µs 52.393 µs 53.547 µs]
                        thrpt:  [1.8675 Melem/s 1.9087 Melem/s 1.9438 Melem/s]
Found 2 outliers among 100 measurements (2.00%)
  1 (1.00%) high mild
  1 (1.00%) high severe
filter/1000             time:   [115.65 µs 118.26 µs 121.21 µs]
                        thrpt:  [8.2502 Melem/s 8.4560 Melem/s 8.6468 Melem/s]
Found 3 outliers among 100 measurements (3.00%)
  1 (1.00%) high mild
  2 (2.00%) high severe
filter/10000            time:   [356.56 µs 361.83 µs 368.57 µs]
                        thrpt:  [27.132 Melem/s 27.637 Melem/s 28.046 Melem/s]
Found 15 outliers among 100 measurements (15.00%)
  2 (2.00%) low severe
  1 (1.00%) low mild
  2 (2.00%) high mild
  10 (10.00%) high severe

filter/100000           time:   [1.0583 ms 1.0763 ms 1.0979 ms]
                        thrpt:  [91.082 Melem/s 92.914 Melem/s 94.495 Melem/s]
Found 12 outliers among 100 measurements (12.00%)
  3 (3.00%) low severe
  3 (3.00%) low mild
  1 (1.00%) high mild
  5 (5.00%) high severe

clear_subgroups         time:   [78.838 µs 81.779 µs 85.718 µs]
Found 8 outliers among 100 measurements (8.00%)
  3 (3.00%) high mild
  5 (5.00%) high severe

collect_all_groups      time:   [258.18 ns 259.30 ns 260.62 ns]
Found 4 outliers among 100 measurements (4.00%)
  3 (3.00%) low mild
  1 (1.00%) high severe

create_single_index/100 time:   [87.407 µs 89.549 µs 92.143 µs]
                        thrpt:  [1.0853 Melem/s 1.1167 Melem/s 1.1441 Melem/s]
Found 3 outliers among 100 measurements (3.00%)
  1 (1.00%) high mild
  2 (2.00%) high severe
create_single_index/1000
                        time:   [381.20 µs 385.24 µs 389.74 µs]
                        thrpt:  [2.5658 Melem/s 2.5958 Melem/s 2.6233 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  2 (2.00%) low mild
  2 (2.00%) high mild
  2 (2.00%) high severe

create_single_index/10000
                        time:   [1.8323 ms 1.8498 ms 1.8708 ms]
                        thrpt:  [5.3454 Melem/s 5.4061 Melem/s 5.4577 Melem/s]
Found 12 outliers among 100 measurements (12.00%)
  1 (1.00%) low severe
  2 (2.00%) low mild
  4 (4.00%) high mild
  5 (5.00%) high severe
create_single_index/100000
                        time:   [15.337 ms 15.418 ms 15.516 ms]
                        thrpt:  [6.4451 Melem/s 6.4860 Melem/s 6.5203 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  4 (4.00%) high mild
  3 (3.00%) high severe

create_multiple_indexes/100
                        time:   [256.51 µs 260.92 µs 265.46 µs]
                        thrpt:  [376.71 Kelem/s 383.26 Kelem/s 389.85 Kelem/s]
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild
create_multiple_indexes/1000
                        time:   [967.70 µs 982.51 µs 998.31 µs]
                        thrpt:  [1.0017 Melem/s 1.0178 Melem/s 1.0334 Melem/s]
Found 4 outliers among 100 measurements (4.00%)
  3 (3.00%) high mild
  1 (1.00%) high severe
create_multiple_indexes/10000
                        time:   [4.9199 ms 4.9608 ms 5.0082 ms]
                        thrpt:  [1.9967 Melem/s 2.0158 Melem/s 2.0326 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  1 (1.00%) high mild
  7 (7.00%) high severe
create_multiple_indexes/100000
                        time:   [42.225 ms 42.391 ms 42.570 ms]
                        thrpt:  [2.3491 Melem/s 2.3590 Melem/s 2.3683 Melem/s]
Found 4 outliers among 100 measurements (4.00%)
  3 (3.00%) high mild
  1 (1.00%) high severe

create_bit_index/100    time:   [1.4250 µs 1.4299 µs 1.4356 µs]
                        thrpt:  [69.655 Melem/s 69.933 Melem/s 70.176 Melem/s]
Found 9 outliers among 100 measurements (9.00%)
  2 (2.00%) low mild
  1 (1.00%) high mild
  6 (6.00%) high severe
create_bit_index/1000   time:   [6.6406 µs 6.6723 µs 6.7134 µs]
                        thrpt:  [148.96 Melem/s 149.87 Melem/s 150.59 Melem/s]
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high severe
create_bit_index/10000  time:   [61.933 µs 62.740 µs 64.090 µs]
                        thrpt:  [156.03 Melem/s 159.39 Melem/s 161.46 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  1 (1.00%) high mild
  6 (6.00%) high severe
create_bit_index/100000 time:   [381.08 µs 392.75 µs 407.41 µs]
                        thrpt:  [245.45 Melem/s 254.62 Melem/s 262.41 Melem/s]
Found 9 outliers among 100 measurements (9.00%)
  3 (3.00%) low mild
  6 (6.00%) high severe

group_creation_with_indexes/100
                        time:   [207.49 µs 213.61 µs 221.18 µs]
                        thrpt:  [452.13 Kelem/s 468.13 Kelem/s 481.95 Kelem/s]
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high severe
group_creation_with_indexes/1000
                        time:   [865.58 µs 879.61 µs 897.03 µs]
                        thrpt:  [1.1148 Melem/s 1.1369 Melem/s 1.1553 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  1 (1.00%) low mild
  4 (4.00%) high mild
  3 (3.00%) high severe
group_creation_with_indexes/10000
                        time:   [4.6764 ms 4.7540 ms 4.8536 ms]
                        thrpt:  [2.0603 Melem/s 2.1035 Melem/s 2.1384 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  1 (1.00%) low mild
  4 (4.00%) high mild
  3 (3.00%) high severe
group_creation_with_indexes/100000
                        time:   [40.760 ms 41.207 ms 41.803 ms]
                        thrpt:  [2.3922 Melem/s 2.4268 Melem/s 2.4534 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  6 (6.00%) high severe

filter_by_index_vs_normal/normal_filter/1000
                        time:   [1.7009 µs 1.7085 µs 1.7173 µs]
                        thrpt:  [582.30 Melem/s 585.30 Melem/s 587.92 Melem/s]
Found 4 outliers among 100 measurements (4.00%)
  3 (3.00%) high mild
  1 (1.00%) high severe
filter_by_index_vs_normal/index_filter/1000
                        time:   [1.3921 µs 1.3977 µs 1.4040 µs]
                        thrpt:  [712.26 Melem/s 715.45 Melem/s 718.34 Melem/s]
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high severe
filter_by_index_vs_normal/normal_filter/10000
                        time:   [17.432 µs 17.578 µs 17.769 µs]
                        thrpt:  [562.77 Melem/s 568.91 Melem/s 573.67 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  1 (1.00%) high mild
  5 (5.00%) high severe
filter_by_index_vs_normal/index_filter/10000
                        time:   [12.315 µs 12.394 µs 12.486 µs]
                        thrpt:  [800.91 Melem/s 806.83 Melem/s 812.04 Melem/s]
Found 5 outliers among 100 measurements (5.00%)
  1 (1.00%) high mild
  4 (4.00%) high severe
filter_by_index_vs_normal/normal_filter/100000
                        time:   [172.05 µs 175.43 µs 179.47 µs]
                        thrpt:  [557.20 Melem/s 570.04 Melem/s 581.23 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  2 (2.00%) high mild
  6 (6.00%) high severe
filter_by_index_vs_normal/index_filter/100000
                        time:   [109.89 µs 111.40 µs 113.26 µs]
                        thrpt:  [882.89 Melem/s 897.68 Melem/s 910.04 Melem/s]
Found 9 outliers among 100 measurements (9.00%)
  3 (3.00%) high mild
  6 (6.00%) high severe

filter_by_index_range/1000
                        time:   [1.4761 µs 1.4940 µs 1.5147 µs]
                        thrpt:  [660.19 Melem/s 669.34 Melem/s 677.48 Melem/s]
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high mild
filter_by_index_range/10000
                        time:   [14.574 µs 14.798 µs 15.072 µs]
                        thrpt:  [663.48 Melem/s 675.76 Melem/s 686.17 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  2 (2.00%) high mild
  6 (6.00%) high severe
filter_by_index_range/100000
                        time:   [133.60 µs 135.55 µs 138.04 µs]
                        thrpt:  [724.43 Melem/s 737.73 Melem/s 748.48 Melem/s]
Found 11 outliers among 100 measurements (11.00%)
  2 (2.00%) high mild
  9 (9.00%) high severe

get_sorted_by_index/100 time:   [82.711 µs 84.117 µs 85.914 µs]
                        thrpt:  [1.1640 Melem/s 1.1888 Melem/s 1.2090 Melem/s]
Found 4 outliers among 100 measurements (4.00%)
  4 (4.00%) high severe
get_sorted_by_index/1000
                        time:   [502.94 µs 508.26 µs 514.40 µs]
                        thrpt:  [1.9440 Melem/s 1.9675 Melem/s 1.9883 Melem/s]
Found 9 outliers among 100 measurements (9.00%)
  1 (1.00%) low severe
  2 (2.00%) high mild
  6 (6.00%) high severe
get_sorted_by_index/10000
                        time:   [4.2613 ms 4.3200 ms 4.3863 ms]
                        thrpt:  [2.2798 Melem/s 2.3148 Melem/s 2.3467 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  4 (4.00%) low severe
  2 (2.00%) low mild
  1 (1.00%) high severe

get_top_n_by_index/10   time:   [360.92 µs 364.27 µs 368.87 µs]
Found 6 outliers among 100 measurements (6.00%)
  2 (2.00%) low severe
  1 (1.00%) high mild
  3 (3.00%) high severe
get_top_n_by_index/100  time:   [411.54 µs 415.84 µs 421.52 µs]
Found 6 outliers among 100 measurements (6.00%)
  1 (1.00%) low mild
  1 (1.00%) high mild
  4 (4.00%) high severe
get_top_n_by_index/1000 time:   [779.94 µs 790.82 µs 804.32 µs]
Found 7 outliers among 100 measurements (7.00%)
  2 (2.00%) high mild
  5 (5.00%) high severe

bit_operation_and/1000  time:   [3.6023 µs 3.6470 µs 3.7044 µs]
                        thrpt:  [269.95 Melem/s 274.20 Melem/s 277.60 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  3 (3.00%) high mild
  5 (5.00%) high severe
bit_operation_and/10000 time:   [26.881 µs 27.069 µs 27.331 µs]
                        thrpt:  [365.88 Melem/s 369.43 Melem/s 372.01 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  3 (3.00%) high mild
  4 (4.00%) high severe
bit_operation_and/100000
                        time:   [276.73 µs 280.83 µs 286.32 µs]
                        thrpt:  [349.25 Melem/s 356.08 Melem/s 361.37 Melem/s]
Found 10 outliers among 100 measurements (10.00%)
  5 (5.00%) high mild
  5 (5.00%) high severe

bit_operation_vs_normal_filter/normal_filter/1000
                        time:   [2.2801 µs 2.3340 µs 2.4015 µs]
                        thrpt:  [416.40 Melem/s 428.45 Melem/s 438.57 Melem/s]
Found 9 outliers among 100 measurements (9.00%)
  3 (3.00%) high mild
  6 (6.00%) high severe
bit_operation_vs_normal_filter/bit_operation/1000
                        time:   [3.5142 µs 3.5637 µs 3.6251 µs]
                        thrpt:  [275.85 Melem/s 280.61 Melem/s 284.56 Melem/s]
Found 17 outliers among 100 measurements (17.00%)
  9 (9.00%) high mild
  8 (8.00%) high severe
bit_operation_vs_normal_filter/normal_filter/10000
                        time:   [22.782 µs 23.201 µs 23.761 µs]
                        thrpt:  [420.86 Melem/s 431.01 Melem/s 438.93 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  2 (2.00%) high mild
  6 (6.00%) high severe
bit_operation_vs_normal_filter/bit_operation/10000
                        time:   [26.326 µs 26.692 µs 27.235 µs]
                        thrpt:  [367.18 Melem/s 374.64 Melem/s 379.86 Melem/s]
Found 10 outliers among 100 measurements (10.00%)
  5 (5.00%) high mild
  5 (5.00%) high severe
bit_operation_vs_normal_filter/normal_filter/100000
                        time:   [211.29 µs 213.64 µs 216.41 µs]
                        thrpt:  [462.08 Melem/s 468.08 Melem/s 473.29 Melem/s]
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high severe
bit_operation_vs_normal_filter/bit_operation/100000
                        time:   [255.00 µs 259.16 µs 264.80 µs]
                        thrpt:  [377.65 Melem/s 385.87 Melem/s 392.15 Melem/s]
Found 3 outliers among 100 measurements (3.00%)
  1 (1.00%) high mild
  2 (2.00%) high severe

complex_bit_operations  time:   [407.68 µs 411.63 µs 416.12 µs]
Found 6 outliers among 100 measurements (6.00%)
  4 (4.00%) high mild
  2 (2.00%) high severe

create_index_in_subgroups/1000
                        time:   [275.53 µs 278.91 µs 282.59 µs]
                        thrpt:  [3.5387 Melem/s 3.5854 Melem/s 3.6293 Melem/s]
Found 9 outliers among 100 measurements (9.00%)
  1 (1.00%) low severe
  1 (1.00%) low mild
  5 (5.00%) high mild
  2 (2.00%) high severe

create_index_in_subgroups/10000
                        time:   [1.1581 ms 1.1704 ms 1.1863 ms]
                        thrpt:  [8.4298 Melem/s 8.5437 Melem/s 8.6346 Melem/s]
Found 9 outliers among 100 measurements (9.00%)
  3 (3.00%) high mild
  6 (6.00%) high severe
create_index_in_subgroups/100000
                        time:   [7.8751 ms 7.9583 ms 8.0637 ms]
                        thrpt:  [12.401 Melem/s 12.565 Melem/s 12.698 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  5 (5.00%) high mild
  2 (2.00%) high severe

create_index_recursive  time:   [3.7561 ms 3.7945 ms 3.8420 ms]
Found 10 outliers among 100 measurements (10.00%)
  4 (4.00%) high mild
  6 (6.00%) high severe

group_by_with_indexes/1000
                        time:   [667.67 µs 675.98 µs 685.12 µs]
                        thrpt:  [1.4596 Melem/s 1.4793 Melem/s 1.4977 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  5 (5.00%) high mild
  2 (2.00%) high severe
group_by_with_indexes/10000
                        time:   [3.0441 ms 3.0776 ms 3.1193 ms]
                        thrpt:  [3.2059 Melem/s 3.2493 Melem/s 3.2850 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  2 (2.00%) high mild
  5 (5.00%) high severe
group_by_with_indexes/100000
                        time:   [19.061 ms 19.182 ms 19.311 ms]
                        thrpt:  [5.1784 Melem/s 5.2133 Melem/s 5.2464 Melem/s]
Found 3 outliers among 100 measurements (3.00%)
  2 (2.00%) high mild
  1 (1.00%) high severe

btree_subgroup_access/get_subgroup
                        time:   [26.980 ns 27.090 ns 27.220 ns]
Found 8 outliers among 100 measurements (8.00%)
  1 (1.00%) low mild
  3 (3.00%) high mild
  4 (4.00%) high severe
btree_subgroup_access/first_last_subgroup
                        time:   [62.401 ns 62.655 ns 62.930 ns]
Found 8 outliers among 100 measurements (8.00%)
  1 (1.00%) low mild
  6 (6.00%) high mild
  1 (1.00%) high severe
btree_subgroup_access/subgroups_range
                        time:   [108.19 ns 108.91 ns 109.66 ns]
Found 4 outliers among 100 measurements (4.00%)
  4 (4.00%) high mild

hierarchical_filtering_with_indexes
                        time:   [1.9661 µs 1.9852 µs 2.0035 µs]
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high mild

complex_query_workflow  time:   [55.398 ms 55.890 ms 56.511 ms]
Found 6 outliers among 100 measurements (6.00%)
  1 (1.00%) high mild
  5 (5.00%) high severe

parallel_filter         time:   [146.68 µs 149.43 µs 152.52 µs]
Found 11 outliers among 100 measurements (11.00%)
  1 (1.00%) low severe
  5 (5.00%) low mild
  3 (3.00%) high mild
  2 (2.00%) high severe

memory_allocation/100   time:   [104.14 µs 105.91 µs 108.02 µs]
                        thrpt:  [925.78 Kelem/s 944.18 Kelem/s 960.25 Kelem/s]
Found 3 outliers among 100 measurements (3.00%)
  1 (1.00%) high mild
  2 (2.00%) high severe
memory_allocation/1000  time:   [343.58 µs 349.32 µs 356.42 µs]
                        thrpt:  [2.8057 Melem/s 2.8627 Melem/s 2.9106 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  2 (2.00%) high mild
  4 (4.00%) high severe

memory_allocation/10000 time:   [1.1649 ms 1.1806 ms 1.2018 ms]
                        thrpt:  [8.3210 Melem/s 8.4704 Melem/s 8.5847 Melem/s]
Found 18 outliers among 100 measurements (18.00%)
  1 (1.00%) low severe
  3 (3.00%) low mild
  4 (4.00%) high mild
  10 (10.00%) high severe

memory_with_indexes/100 time:   [261.02 µs 264.58 µs 268.60 µs]
                        thrpt:  [372.29 Kelem/s 377.96 Kelem/s 383.12 Kelem/s]
Found 10 outliers among 100 measurements (10.00%)
  1 (1.00%) low mild
  5 (5.00%) high mild
  4 (4.00%) high severe
memory_with_indexes/1000
                        time:   [971.38 µs 995.40 µs 1.0264 ms]
                        thrpt:  [974.24 Kelem/s 1.0046 Melem/s 1.0295 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  2 (2.00%) high mild
  6 (6.00%) high severe
memory_with_indexes/10000
                        time:   [5.0441 ms 5.1373 ms 5.2584 ms]
                        thrpt:  [1.9017 Melem/s 1.9465 Melem/s 1.9825 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  6 (6.00%) high severe

deep_hierarchy_creation time:   [683.08 µs 697.87 µs 716.71 µs]
Found 10 outliers among 100 measurements (10.00%)
  6 (6.00%) low mild
  4 (4.00%) high severe

```

## Try to compare

```matlab

02_multi_threaded/creation/TreeMan_parallel/1000
                        time:   [118.27 µs 122.08 µs 126.52 µs]
                        thrpt:  [7.9039 Melem/s 8.1915 Melem/s 8.4551 Melem/s]
Found 10 outliers among 100 measurements (10.00%)
  1 (1.00%) low mild
  4 (4.00%) high mild
  5 (5.00%) high severe
02_multi_threaded/creation/im::Vector_parallel/1000
                        time:   [134.18 µs 136.79 µs 139.77 µs]
                        thrpt:  [7.1546 Melem/s 7.3104 Melem/s 7.4524 Melem/s]
Found 20 outliers among 100 measurements (20.00%)
  14 (14.00%) low mild
  2 (2.00%) high mild
  4 (4.00%) high severe
02_multi_threaded/creation/rpds::Vector_parallel/1000
                        time:   [166.69 µs 171.94 µs 177.44 µs]
                        thrpt:  [5.6358 Melem/s 5.8159 Melem/s 5.9993 Melem/s]
Found 17 outliers among 100 measurements (17.00%)
  9 (9.00%) low severe
  1 (1.00%) low mild
  1 (1.00%) high mild
  6 (6.00%) high severe

02_multi_threaded/creation/im::HashMap_parallel/1000
                        time:   [225.67 µs 233.23 µs 241.62 µs]
                        thrpt:  [4.1386 Melem/s 4.2877 Melem/s 4.4313 Melem/s]
Found 11 outliers among 100 measurements (11.00%)
  11 (11.00%) high severe

02_multi_threaded/creation/im::OrdMap_parallel/1000
                        time:   [225.94 µs 231.89 µs 238.80 µs]
                        thrpt:  [4.1876 Melem/s 4.3124 Melem/s 4.4260 Melem/s]
Found 14 outliers among 100 measurements (14.00%)
  2 (2.00%) high mild
  12 (12.00%) high severe

02_multi_threaded/creation/TreeMan_parallel/10000
                        time:   [227.63 µs 242.37 µs 260.69 µs]
                        thrpt:  [38.359 Melem/s 41.259 Melem/s 43.931 Melem/s]
Found 9 outliers among 100 measurements (9.00%)
  2 (2.00%) high mild
  7 (7.00%) high severe

02_multi_threaded/creation/im::Vector_parallel/10000
                        time:   [338.54 µs 347.73 µs 359.49 µs]
                        thrpt:  [27.818 Melem/s 28.758 Melem/s 29.539 Melem/s]
Found 16 outliers among 100 measurements (16.00%)
  2 (2.00%) low mild
  7 (7.00%) high mild
  7 (7.00%) high severe

02_multi_threaded/creation/rpds::Vector_parallel/10000
                        time:   [498.68 µs 508.16 µs 520.05 µs]
                        thrpt:  [19.229 Melem/s 19.679 Melem/s 20.053 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  7 (7.00%) high severe

02_multi_threaded/creation/im::HashMap_parallel/10000
                        time:   [646.93 µs 665.17 µs 687.31 µs]
                        thrpt:  [14.549 Melem/s 15.034 Melem/s 15.458 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  3 (3.00%) high mild
  5 (5.00%) high severe

02_multi_threaded/creation/im::OrdMap_parallel/10000
                        time:   [619.84 µs 641.93 µs 667.67 µs]
                        thrpt:  [14.978 Melem/s 15.578 Melem/s 16.133 Melem/s]
Found 11 outliers among 100 measurements (11.00%)
  4 (4.00%) high mild
  7 (7.00%) high severe

02_multi_threaded/creation/TreeMan_parallel/100000
                        time:   [591.49 µs 614.96 µs 641.98 µs]
                        thrpt:  [155.77 Melem/s 162.61 Melem/s 169.06 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  1 (1.00%) high mild
  5 (5.00%) high severe

02_multi_threaded/creation/im::Vector_parallel/100000
                        time:   [1.9417 ms 1.9758 ms 2.0195 ms]
                        thrpt:  [49.517 Melem/s 50.613 Melem/s 51.500 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  1 (1.00%) high mild
  6 (6.00%) high severe

02_multi_threaded/creation/rpds::Vector_parallel/100000
                        time:   [3.5286 ms 3.5757 ms 3.6334 ms]
                        thrpt:  [27.522 Melem/s 27.967 Melem/s 28.340 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  6 (6.00%) high severe

02_multi_threaded/creation/im::HashMap_parallel/100000
                        time:   [2.5672 ms 2.6564 ms 2.7707 ms]
                        thrpt:  [36.092 Melem/s 37.645 Melem/s 38.953 Melem/s]
Found 5 outliers among 100 measurements (5.00%)
  1 (1.00%) high mild
  4 (4.00%) high severe

02_multi_threaded/creation/im::OrdMap_parallel/100000
                        time:   [2.6410 ms 2.7284 ms 2.8292 ms]
                        thrpt:  [35.346 Melem/s 36.652 Melem/s 37.864 Melem/s]
Found 9 outliers among 100 measurements (9.00%)
  3 (3.00%) high mild
  6 (6.00%) high severe


02_multi_threaded/filtering/TreeMan_parallel/50
                        time:   [92.619 µs 94.717 µs 97.250 µs]
                        thrpt:  [514.14 Kelem/s 527.89 Kelem/s 539.84 Kelem/s]
Found 6 outliers among 100 measurements (6.00%)
  2 (2.00%) high mild
  4 (4.00%) high severe

02_multi_threaded/filtering/im::Vector_parallel/50
                        time:   [95.670 µs 97.843 µs 100.30 µs]
                        thrpt:  [498.50 Kelem/s 511.02 Kelem/s 522.63 Kelem/s]
Found 4 outliers among 100 measurements (4.00%)
  2 (2.00%) high mild
  2 (2.00%) high severe

02_multi_threaded/filtering/TreeMan_parallel/500
                        time:   [202.26 µs 207.53 µs 213.53 µs]
                        thrpt:  [2.3416 Melem/s 2.4093 Melem/s 2.4721 Melem/s]
Found 4 outliers among 100 measurements (4.00%)
  2 (2.00%) high mild
  2 (2.00%) high severe

02_multi_threaded/filtering/im::Vector_parallel/500
                        time:   [286.95 µs 294.55 µs 303.16 µs]
                        thrpt:  [1.6493 Melem/s 1.6975 Melem/s 1.7425 Melem/s]
Found 5 outliers among 100 measurements (5.00%)
  2 (2.00%) high mild
  3 (3.00%) high severe

02_multi_threaded/filtering/TreeMan_parallel/5000
                        time:   [481.82 µs 494.19 µs 507.49 µs]
                        thrpt:  [9.8524 Melem/s 10.118 Melem/s 10.377 Melem/s]
Found 11 outliers among 100 measurements (11.00%)
  4 (4.00%) low mild
  5 (5.00%) high mild
  2 (2.00%) high severe

02_multi_threaded/filtering/im::Vector_parallel/5000
                        time:   [1.0201 ms 1.0383 ms 1.0588 ms]
                        thrpt:  [4.7222 Melem/s 4.8154 Melem/s 4.9016 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  4 (4.00%) high mild
  2 (2.00%) high severe


03_parallelism_impact/TreeMan_sequential
                        time:   [1.2974 ms 1.3081 ms 1.3215 ms]
                        thrpt:  [75.670 Melem/s 76.446 Melem/s 77.075 Melem/s]
Found 5 outliers among 100 measurements (5.00%)
  2 (2.00%) high mild
  3 (3.00%) high severe
03_parallelism_impact/TreeMan_parallel
                        time:   [583.02 µs 608.12 µs 637.37 µs]
                        thrpt:  [156.89 Melem/s 164.44 Melem/s 171.52 Melem/s]
Found 9 outliers among 100 measurements (9.00%)
  2 (2.00%) low mild
  1 (1.00%) high mild
  6 (6.00%) high severe

03_parallelism_impact/im::Vector_sequential
                        time:   [1.5700 ms 1.5842 ms 1.5996 ms]
                        thrpt:  [62.515 Melem/s 63.123 Melem/s 63.692 Melem/s]
Found 4 outliers among 100 measurements (4.00%)
  3 (3.00%) high mild
  1 (1.00%) high severe

03_parallelism_impact/im::Vector_parallel
                        time:   [1.9662 ms 1.9957 ms 2.0317 ms]
                        thrpt:  [49.219 Melem/s 50.109 Melem/s 50.860 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  1 (1.00%) high mild
  5 (5.00%) high severe

03_parallelism_impact/im::HashMap_sequential
                        time:   [5.1247 ms 5.1604 ms 5.2012 ms]
                        thrpt:  [19.226 Melem/s 19.378 Melem/s 19.513 Melem/s]
Found 5 outliers among 100 measurements (5.00%)
  1 (1.00%) high mild
  4 (4.00%) high severe

03_parallelism_impact/im::HashMap_parallel
                        time:   [2.5933 ms 2.7057 ms 2.8504 ms]
                        thrpt:  [35.083 Melem/s 36.958 Melem/s 38.561 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  2 (2.00%) high mild
  5 (5.00%) high severe


04_unique_features/hierarchical/TreeMan/100000
                        time:   [3.1842 ms 3.2715 ms 3.3662 ms]
                        thrpt:  [29.707 Melem/s 30.567 Melem/s 31.405 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  3 (3.00%) high mild
  5 (5.00%) high severe

04_unique_features/hierarchical/im::HashMap_nested/100000
                        time:   [14.959 ms 15.162 ms 15.414 ms]
                        thrpt:  [6.4877 Melem/s 6.5953 Melem/s 6.6849 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  4 (4.00%) high mild
  3 (3.00%) high severe

04_unique_features/hierarchical/TreeMan/1000000
                        time:   [24.315 ms 24.963 ms 25.710 ms]
                        thrpt:  [38.896 Melem/s 40.059 Melem/s 41.127 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  4 (4.00%) high mild
  4 (4.00%) high severe


04_unique_features/hierarchical/im::HashMap_nested/1000000
                        time:   [206.18 ms 209.21 ms 212.58 ms]
                        thrpt:  [4.7041 Melem/s 4.7799 Melem/s 4.8502 Melem/s]
Found 5 outliers among 100 measurements (5.00%)
  1 (1.00%) high mild
  4 (4.00%) high severe


04_unique_features/parallel_groups/TreeMan_builtin/10000
                        time:   [660.47 µs 668.21 µs 678.33 µs]
                        thrpt:  [14.742 Melem/s 14.965 Melem/s 15.141 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  1 (1.00%) low mild
  1 (1.00%) high mild
  4 (4.00%) high severe


04_unique_features/parallel_groups/TreeMan_sequential/10000
                        time:   [1.3039 ms 1.3209 ms 1.3380 ms]
                        thrpt:  [7.4737 Melem/s 7.5708 Melem/s 7.6693 Melem/s]
Found 4 outliers among 100 measurements (4.00%)
  3 (3.00%) high mild
  1 (1.00%) high severe

04_unique_features/parallel_groups/TreeMan_builtin/100000
                        time:   [3.8184 ms 3.8757 ms 3.9298 ms]
                        thrpt:  [25.446 Melem/s 25.802 Melem/s 26.189 Melem/s]
Found 3 outliers among 100 measurements (3.00%)
  2 (2.00%) low mild
  1 (1.00%) high mild

04_unique_features/parallel_groups/TreeMan_sequential/100000
                        time:   [5.5491 ms 5.6152 ms 5.6864 ms]
                        thrpt:  [17.586 Melem/s 17.809 Melem/s 18.021 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  3 (3.00%) low mild
  2 (2.00%) high mild
  3 (3.00%) high severe

```

## Backtest

```matlab

candles_groupby_optimized/group_by_symbol
                        time:   [50.734 ms 51.295 ms 51.842 ms]
                        thrpt:  [19.289 Melem/s 19.495 Melem/s 19.711 Melem/s]

candles_filtering/filter_bullish
                        time:   [8.2537 ms 8.3605 ms 8.4385 ms]
                        thrpt:  [118.50 Melem/s 119.61 Melem/s 121.16 Melem/s]
candles_filtering/filter_complex
                        time:   [2.4943 ms 2.5434 ms 2.6032 ms]
                        thrpt:  [384.14 Melem/s 393.17 Melem/s 400.91 Melem/s]
Found 1 outliers among 20 measurements (5.00%)
  1 (5.00%) high severe
candles_filtering/filter_price_range
                        time:   [2.8728 ms 2.9472 ms 3.0422 ms]
                        thrpt:  [328.71 Melem/s 339.31 Melem/s 348.09 Melem/s]
Found 3 outliers among 20 measurements (15.00%)
  3 (15.00%) high mild


candles_bit_indexes/create_bit_indexes
                        time:   [84.828 ms 85.594 ms 86.456 ms]
                        thrpt:  [11.567 Melem/s 11.683 Melem/s 11.789 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  3 (3.00%) high mild
  3 (3.00%) high severe
candles_bit_indexes/filter_bullish_bit
                        time:   [8.2582 ms 8.3133 ms 8.3682 ms]
                        thrpt:  [119.50 Melem/s 120.29 Melem/s 121.09 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  2 (2.00%) low severe
  3 (3.00%) low mild
  1 (1.00%) high mild
  1 (1.00%) high severe

candles_bit_indexes/bit_and_bullish_high_volume
                        time:   [7.1745 ms 7.2577 ms 7.3457 ms]
                        thrpt:  [136.13 Melem/s 137.78 Melem/s 139.38 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  1 (1.00%) low mild
  4 (4.00%) high mild
  2 (2.00%) high severe

candles_bit_indexes/bit_complex_btc_or_eth_and_bullish
                        time:   [2.3987 ms 2.4596 ms 2.5377 ms]
                        thrpt:  [394.05 Melem/s 406.57 Melem/s 416.89 Melem/s]
Found 4 outliers among 100 measurements (4.00%)
  1 (1.00%) high mild
  3 (3.00%) high severe


candles_indexes/create_indexes
                        time:   [201.17 ms 202.45 ms 203.88 ms]
                        thrpt:  [4.9049 Melem/s 4.9396 Melem/s 4.9710 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  4 (4.00%) high mild
  3 (3.00%) high severe
candles_indexes/filter_by_symbol_index
                        time:   [5.2495 ms 5.3877 ms 5.5248 ms]
                        thrpt:  [181.00 Melem/s 185.61 Melem/s 190.50 Melem/s]
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high mild
candles_indexes/filter_by_price_range
                        time:   [3.5166 ms 3.6866 ms 3.8651 ms]
                        thrpt:  [258.73 Melem/s 271.26 Melem/s 284.37 Melem/s]
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high mild
candles_indexes/get_top_100_by_price
                        time:   [326.74 µs 336.84 µs 349.96 µs]
                        thrpt:  [2.8574 Gelem/s 2.9688 Gelem/s 3.0605 Gelem/s]
Found 9 outliers among 100 measurements (9.00%)
  2 (2.00%) high mild
  7 (7.00%) high severe


candles_group_with_indexes/group_by_symbol_with_indexes
                        time:   [121.57 ms 123.74 ms 126.38 ms]
                        thrpt:  [7.9127 Melem/s 8.0817 Melem/s 8.2256 Melem/s]
Found 10 outliers among 100 measurements (10.00%)
  4 (4.00%) high mild
  6 (6.00%) high severe

candles_memory/create_and_group/100000
                        time:   [15.300 ms 15.574 ms 15.940 ms]
                        thrpt:  [6.2737 Melem/s 6.4210 Melem/s 6.5361 Melem/s]
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high severe

candles_memory/create_and_group/500000
                        time:   [75.409 ms 76.006 ms 76.716 ms]
                        thrpt:  [6.5175 Melem/s 6.5784 Melem/s 6.6305 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  8 (8.00%) high severe

candles_memory/create_and_group/1000000
                        time:   [148.90 ms 150.15 ms 151.56 ms]
                        thrpt:  [6.5981 Melem/s 6.6600 Melem/s 6.7159 Melem/s]
Found 13 outliers among 100 measurements (13.00%)
  3 (3.00%) low mild
  1 (1.00%) high mild
  9 (9.00%) high severe

```

## Backtest Parallel

```matlab

candles_concurrent_optimized/2
                        time:   [17.390 ms 17.437 ms 17.494 ms]
                        thrpt:  [57.163 Melem/s 57.349 Melem/s 57.503 Melem/s]
Found 9 outliers among 100 measurements (9.00%)
  1 (1.00%) low mild
  2 (2.00%) high mild
  6 (6.00%) high severe
candles_concurrent_optimized/4
                        time:   [24.009 ms 24.096 ms 24.209 ms]
                        thrpt:  [41.306 Melem/s 41.501 Melem/s 41.651 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  3 (3.00%) high mild
  5 (5.00%) high severe
candles_concurrent_optimized/8
                        time:   [24.827 ms 25.196 ms 25.699 ms]
                        thrpt:  [38.911 Melem/s 39.689 Melem/s 40.278 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  2 (2.00%) high mild
  5 (5.00%) high severe
candles_concurrent_optimized/16
                        time:   [36.082 ms 36.781 ms 37.589 ms]
                        thrpt:  [26.604 Melem/s 27.188 Melem/s 27.715 Melem/s]
Found 3 outliers among 100 measurements (3.00%)
  1 (1.00%) high mild
  2 (2.00%) high severe

candles_concurrent_batched/2
                        time:   [15.215 ms 15.325 ms 15.476 ms]
                        thrpt:  [64.617 Melem/s 65.254 Melem/s 65.726 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  2 (2.00%) high mild
  5 (5.00%) high severe
candles_concurrent_batched/4
                        time:   [21.308 ms 21.440 ms 21.600 ms]
                        thrpt:  [46.297 Melem/s 46.641 Melem/s 46.930 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  1 (1.00%) high mild
  7 (7.00%) high severe
candles_concurrent_batched/8
                        time:   [22.044 ms 22.415 ms 22.864 ms]
                        thrpt:  [43.736 Melem/s 44.614 Melem/s 45.364 Melem/s]
Found 10 outliers among 100 measurements (10.00%)
  2 (2.00%) high mild
  8 (8.00%) high severe
candles_concurrent_batched/16
                        time:   [32.074 ms 32.809 ms 33.694 ms]
                        thrpt:  [29.679 Melem/s 30.479 Melem/s 31.178 Melem/s]
Found 6 outliers among 100 measurements (6.00%)
  3 (3.00%) high mild
  3 (3.00%) high severe

sequential_vs_parallel/sequential_ops
                        time:   [1.1761 ms 1.1836 ms 1.1936 ms]
                        thrpt:  [837.80 Melem/s 844.89 Melem/s 850.26 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  3 (3.00%) high mild
  5 (5.00%) high severe
sequential_vs_parallel/parallel_8threads_1m_ops
                        time:   [23.585 ms 24.007 ms 24.516 ms]
                        thrpt:  [40.790 Melem/s 41.654 Melem/s 42.400 Melem/s]
Found 13 outliers among 100 measurements (13.00%)
  1 (1.00%) high mild
  12 (12.00%) high severe

```
