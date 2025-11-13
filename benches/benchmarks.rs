use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use tree_man::{
    group_filter_parallel,
    group::GroupData,
    bit_index::BitOp,
};
use std::hint::black_box;

#[derive(Clone, Debug)]
struct Product {
    id: u32,
    category: String,
    brand: String,
    price: f64,
    stock: u32,
    is_available: bool,
}

fn create_products(count: usize) -> Vec<Product> {
    (0..count).map(|i| Product {
        id: i as u32,
        category: ["Phones", "Laptops", "Tablets"][i % 3].to_string(),
        brand: ["Apple", "Samsung", "Dell", "Lenovo"][i % 4].to_string(),
        price: 500.0 + (i as f64) * 10.0,
        stock: (i % 100) as u32,
        is_available: i % 3 != 0,
    }).collect()
}


// SINGLE THREAD BENCHMARKS

fn bench_group_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_creation");
    for size in [10, 100, 1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let products = create_products(size);
                let root = GroupData::new_root(
                    "Root".to_string(),
                    black_box(products),
                    "All"
                );
                black_box(root);
            });
        });
    }
    group.finish();
}

fn bench_group_by(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_by");
    for size in [10, 100, 1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let products = create_products(size);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            b.iter(|| {
                root.clear_subgroups();
                root.group_by(|p| black_box(p.category.clone()), "Categories");
            });
        });
    }
    group.finish();
}

fn bench_get_subgroup(c: &mut Criterion) {
    let products = create_products(100);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    root.group_by(|p| p.category.clone(), "Categories");
    c.bench_function("get_subgroup", |b| {
        b.iter(|| {
            black_box(root.get_subgroup(&"Phones".to_string()));
        });
    });
}

fn bench_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter");
    for size in [10, 100, 1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let products = create_products(size);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            b.iter(|| {
                root.reset_filters();
                root.filter(|p| black_box(p.price > 700.0));
            });
        });
    }
    group.finish();
}

fn bench_clear_subgroups(c: &mut Criterion) {
    let products = create_products(100);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    c.bench_function("clear_subgroups", |b| {
        b.iter(|| {
            root.group_by(|p| p.category.clone(), "Categories");
            black_box(root.clear_subgroups());
        });
    });
}

fn bench_collect_all_groups(c: &mut Criterion) {
    let products = create_products(100);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    root.group_by(|p| p.category.clone(), "Categories");
    let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
    phones.group_by(|p| p.brand.clone(), "Brands");
    c.bench_function("collect_all_groups", |b| {
        b.iter(|| {
            black_box(root.collect_all_groups());
        });
    });
}

// INDEX CREATION BENCHMARKS

fn bench_create_single_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_single_index");
    for size in [100, 1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let products = create_products(size);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            
            b.iter(|| {
                root.create_index("id", |p: &Product| black_box(p.id));
            });
        });
    }
    group.finish();
}

fn bench_create_multiple_indexes(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_multiple_indexes");
    for size in [100, 1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let products = create_products(size);
            b.iter(|| {
                let root = GroupData::new_root("Root".to_string(), products.clone(), "All");
                root.create_index("id", |p: &Product| p.id)
                    .create_index("category", |p: &Product| p.category.clone())
                    .create_index("price", |p: &Product| (p.price * 100.0) as i64);
                black_box(root);
            });
        });
    }
    group.finish();
}

fn bench_create_bit_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_bit_index");
    for size in [100, 1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let products = create_products(size);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            
            b.iter(|| {
                root.create_bit_index("is_available", |p: &Product| black_box(p.is_available));
            });
        });
    }
    group.finish();
}

fn bench_group_creation_with_indexes(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_creation_with_indexes");
    for size in [100, 1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let products = create_products(size);
            b.iter(|| {
                let root = GroupData::new_root_with_indexes(
                    "Root".to_string(),
                    black_box(products.clone()),
                    "All",
                    |fd| {
                        fd.create_index("id", |p: &Product| p.id)
                            .create_index("price", |p: &Product| (p.price * 100.0) as i64)
                            .create_bit_index("is_available", |p: &Product| p.is_available);
                        fd
                    },
                );
                black_box(root);
            });
        });
    }
    group.finish();
}


// INDEX SEARCH BENCHMARKS

fn bench_filter_by_index_vs_normal(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_by_index_vs_normal");
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        let products = create_products(*size);
        let root = GroupData::new_root("Root".to_string(), products, "All");
        root.create_index("category", |p: &Product| p.category.clone());
        // Normal filter
        group.bench_with_input(
            BenchmarkId::new("normal_filter", size),
            size,
            |b, _| {
                b.iter(|| {
                    let result: Vec<_> = root.data.items()
                        .iter()
                        .filter(|p| p.category == "Phones")
                        .cloned()
                        .collect();
                    black_box(result);
                });
            }
        );
        // Index filter
        group.bench_with_input(
            BenchmarkId::new("index_filter", size),
            size,
            |b, _| {
                b.iter(|| {
                    let result = root.filter_by_index("category", &"Phones".to_string());
                    black_box(result);
                });
            }
        );
    }
    
    group.finish();
}

fn bench_filter_by_index_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_by_index_range");
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let products = create_products(size);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            root.create_index("id", |p: &Product| p.id);
            let range_start = (size / 4) as u32;
            let range_end = (size / 2) as u32;
            b.iter(|| {
                let result = root.filter_by_index_range("id", black_box(range_start)..black_box(range_end));
                black_box(result);
            });
        });
    }
    group.finish();
}

fn bench_get_sorted_by_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_sorted_by_index");
    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let products = create_products(size);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            root.create_index("price", |p: &Product| (p.price * 100.0) as i64);
            
            b.iter(|| {
                let result = root.get_sorted_by_index::<i64>("price");
                black_box(result);
            });
        });
    }
    group.finish();
}

fn bench_get_top_n_by_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_top_n_by_index");
    let products = create_products(100000);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    root.create_index("price", |p: &Product| (p.price * 100.0) as i64);
    for n in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, &n| {
            b.iter(|| {
                let result = root.get_top_n_by_index::<i64>("price", black_box(n));
                black_box(result);
            });
        });
    }
    group.finish();
}


// BIT INDEX BENCHMARKS

fn bench_bit_operation_and(c: &mut Criterion) {
    let mut group = c.benchmark_group("bit_operation_and");
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let products = create_products(size);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            root.create_bit_index("is_available", |p: &Product| p.is_available)
                .create_bit_index("in_stock", |p: &Product| p.stock > 10);
            b.iter(|| {
                let result = root.filter_by_bit_operation(&[
                    ("is_available", BitOp::And),
                    ("in_stock", BitOp::And),
                ]);
                black_box(result);
            });
        });
    }
    
    group.finish();
}

fn bench_bit_operation_vs_normal_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("bit_operation_vs_normal_filter");
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        let products = create_products(*size);
        let root = GroupData::new_root("Root".to_string(), products, "All");
        root.create_bit_index("is_available", |p: &Product| p.is_available)
            .create_bit_index("in_stock", |p: &Product| p.stock > 10);
        // Normal filter
        group.bench_with_input(
            BenchmarkId::new("normal_filter", size),
            size,
            |b, _| {
                b.iter(|| {
                    let result: Vec<_> = root.data.items()
                        .iter()
                        .filter(|p| p.is_available && p.stock > 10)
                        .cloned()
                        .collect();
                    black_box(result);
                });
            }
        );
        // Bit operation
        group.bench_with_input(
            BenchmarkId::new("bit_operation", size),
            size,
            |b, _| {
                b.iter(|| {
                    let result = root.filter_by_bit_operation(&[
                        ("is_available", BitOp::And),
                        ("in_stock", BitOp::And),
                    ]);
                    black_box(result);
                });
            }
        );
    }
    group.finish();
}

fn bench_complex_bit_operations(c: &mut Criterion) {
    let products = create_products(100000);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    root.create_bit_index("is_available", |p: &Product| p.is_available)
        .create_bit_index("in_stock", |p: &Product| p.stock > 10)
        .create_bit_index("expensive", |p: &Product| p.price > 800.0)
        .create_bit_index("cheap", |p: &Product| p.price < 600.0);
    c.bench_function("complex_bit_operations", |b| {
        b.iter(|| {
            // (available AND in_stock) OR (expensive AND NOT cheap)
            let result1 = root.filter_by_bit_operation(&[
                ("is_available", BitOp::And),
                ("in_stock", BitOp::And),
                ("expensive", BitOp::Or),
            ]);
            black_box(result1);
        });
    });
}


// INDEX IN SUBGROUPS BENCHMARKS

fn bench_create_index_in_subgroups(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_index_in_subgroups");
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let products = create_products(size);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            root.group_by(|p| p.category.clone(), "Categories");
            
            b.iter(|| {
                root.create_index_in_subgroups("price", |p: &Product| black_box((p.price * 100.0) as i64));
            });
        });
    }
    
    group.finish();
}

fn bench_create_index_recursive(c: &mut Criterion) {
    let products = create_products(10000);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    root.group_by(|p| p.category.clone(), "Categories");
    for subgroup in root.get_all_subgroups() {
        subgroup.group_by(|p| p.brand.clone(), "Brands");
    }
    c.bench_function("create_index_recursive", |b| {
        b.iter(|| {
            root.create_index_recursive("id", |p: &Product| black_box(p.id));
        });
    });
}

fn bench_group_by_with_indexes(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_by_with_indexes");
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let products = create_products(size);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            b.iter(|| {
                root.clear_subgroups();
                root.group_by_with_indexes(
                    |p| black_box(p.category.clone()),
                    "Categories",
                    |fd| {
                        fd.create_index("id", |p: &Product| p.id)
                            .create_index("price", |p: &Product| (p.price * 100.0) as i64);
                    },
                );
            });
        });
    }
    group.finish();
}


// BTREE SUBGROUPS BENCHMARKS

fn bench_btree_subgroup_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_subgroup_access");
    let products = create_products(10000);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    root.group_by(|p| p.brand.clone(), "Brands");
    group.bench_function("get_subgroup", |b| {
        b.iter(|| {
            black_box(root.get_subgroup(&"Apple".to_string()));
        });
    });
    group.bench_function("first_last_subgroup", |b| {
        b.iter(|| {
            black_box(root.first_subgroup_key());
            black_box(root.last_subgroup_key());
        });
    });
    group.bench_function("subgroups_range", |b| {
        b.iter(|| {
            let result = root.get_subgroups_range(
                black_box("Apple".to_string())..=black_box("Lenovo".to_string())
            );
            black_box(result);
        });
    });
    
    group.finish();
}


// COMBINED OPERATIONS BENCHMARKS

fn bench_hierarchical_filtering_with_indexes(c: &mut Criterion) {
    let products = create_products(100000);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    root.create_index("category", |p: &Product| p.category.clone())
        .create_index("price", |p: &Product| (p.price * 100.0) as i64)
        .create_bit_index("is_available", |p: &Product| p.is_available)
        .create_bit_index("in_stock", |p: &Product| p.stock > 10);
    c.bench_function("hierarchical_filtering_with_indexes", |b| {
        b.iter(|| {
            // Level 1: Category filter
            root.apply_index_filter("category", &black_box("Phones".to_string()));
            // Level 2: Price range
            root.apply_index_range("price", black_box(60000i64)..black_box(80000i64));
            // Level 3: Bit operations
            root.apply_bit_operation(&[
                ("is_available", BitOp::And),
                ("in_stock", BitOp::And),
            ]);
            let count = root.data.len();
            // Reset
            root.reset_filters();
            black_box(count);
        });
    });
}

fn bench_complex_query_workflow(c: &mut Criterion) {
    let products = create_products(100000);
    c.bench_function("complex_query_workflow", |b| {
        b.iter(|| {
            // Create root with indexes
            let root = GroupData::new_root_with_indexes(
                "Store".to_string(),
                products.clone(),
                "All",
                |fd| {
                    fd.create_index("id", |p: &Product| p.id)
                        .create_index("category", |p: &Product| p.category.clone())
                        .create_index("price", |p: &Product| (p.price * 100.0) as i64)
                        .create_bit_index("available", |p: &Product| p.is_available);
                    fd
                },
            );
            // Group by category with indexes
            root.group_by_with_indexes(
                |p| p.category.clone(),
                "Categories",
                |fd| {
                    fd.create_index("price", |p: &Product| (p.price * 100.0) as i64);
                },
            );
            // Query specific group
            if let Some(phones) = root.get_subgroup(&"Phones".to_string()) {
                let top_10 = phones.get_top_n_by_index::<i64>("price", 10);
                black_box(top_10);
            }
            black_box(root);
        });
    });
}


// CONCURRENT BENCHMARKS (Original + with indexes)
fn bench_parallel_filter(c: &mut Criterion) {
    let products = create_products(1000);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    root.group_by(|p| p.category.clone(), "Categories");
    let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
    let laptops = root.get_subgroup(&"Laptops".to_string()).unwrap();
    let tablets = root.get_subgroup(&"Tablets".to_string()).unwrap();
    c.bench_function("parallel_filter", |b| {
        b.iter(|| {
            group_filter_parallel!(
                phones => |p: &Product| black_box(p.price > 800.0),
                laptops => |p: &Product| black_box(p.price > 1500.0),
                tablets => |p: &Product| black_box(p.price > 600.0),
            );
        });
    });
}



// MEMORY BENCHMARKS (Original + with indexes)


fn bench_memory_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_allocation");
    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let products = create_products(size);
                let root = GroupData::new_root("Root".to_string(), products, "All");
                root.group_by(|p| p.category.clone(), "Categories");
                black_box(root);
            });
        });
    }
    group.finish();
}

fn bench_memory_with_indexes(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_with_indexes");
    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let products = create_products(size);
                let root = GroupData::new_root_with_indexes(
                    "Root".to_string(),
                    products,
                    "All",
                    |fd| {
                        fd.create_index("id", |p: &Product| p.id)
                            .create_index("category", |p: &Product| p.category.clone())
                            .create_index("price", |p: &Product| (p.price * 100.0) as i64)
                            .create_bit_index("is_available", |p: &Product| p.is_available)
                            .create_bit_index("in_stock", |p: &Product| p.stock > 10);
                        fd
                    },
                );
                black_box(root);
            });
        });
    }
    group.finish();
}

fn bench_deep_hierarchy(c: &mut Criterion) {
    let products = create_products(100);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    c.bench_function("deep_hierarchy_creation", |b| {
        b.iter(|| {
            root.clear_subgroups();
            root.group_by(|p| p.category.clone(), "Level1");
            
            for cat in ["Phones", "Laptops", "Tablets"].iter() {
                if let Some(group) = root.get_subgroup(&cat.to_string()) {
                    group.group_by(|p| p.brand.clone(), "Level2");
                    
                    for brand in ["Apple", "Samsung"].iter() {
                        if let Some(brand_group) = group.get_subgroup(&brand.to_string()) {
                            brand_group.group_by(|p| {
                                if p.price > 800.0 { "Premium".to_string() }
                                else { "Budget".to_string() }
                            }, "Level3");
                        }
                    }
                }
            }
        });
    });
}


criterion_group!(
    benches,
    // Original benchmarks
    bench_group_creation,
    bench_group_by,
    bench_get_subgroup,
    bench_filter,
    bench_clear_subgroups,
    bench_collect_all_groups,
    // Index creation
    bench_create_single_index,
    bench_create_multiple_indexes,
    bench_create_bit_index,
    bench_group_creation_with_indexes,
    // Index search
    bench_filter_by_index_vs_normal,
    bench_filter_by_index_range,
    bench_get_sorted_by_index,
    bench_get_top_n_by_index,
    // Bit operations
    bench_bit_operation_and,
    bench_bit_operation_vs_normal_filter,
    bench_complex_bit_operations,
    // Indexes in subgroups
    bench_create_index_in_subgroups,
    bench_create_index_recursive,
    bench_group_by_with_indexes,
    // BTree operations
    bench_btree_subgroup_access,
    // Combined operations
    bench_hierarchical_filtering_with_indexes,
    bench_complex_query_workflow,
    // Concurrent
    bench_parallel_filter,
    // Memory
    bench_memory_allocation,
    bench_memory_with_indexes,
    bench_deep_hierarchy,
);

criterion_main!(benches);