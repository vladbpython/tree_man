use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use tree_man::{
    Op,
    FieldOperation,
    OrderedFloat,
    group_filter_parallel,
    group::GroupData,
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
                root.group_by(|p| black_box(p.category.clone()), "Categories").unwrap();
            });
        });
    }
    group.finish();
}

fn bench_get_subgroup(c: &mut Criterion) {
    let products = create_products(100);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    root.group_by(|p| p.category.clone(), "Categories").unwrap();
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
                root.filter(|p| black_box(p.price > 100.0)).unwrap();
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
            root.group_by(|p| p.category.clone(), "Categories").unwrap();
            black_box(root.clear_subgroups());
        });
    });
}

fn bench_collect_all_groups(c: &mut Criterion) {
    let products = create_products(100);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    root.group_by(|p| p.category.clone(), "Categories").unwrap();
    let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
    phones.group_by(|p| p.brand.clone(), "Brands").unwrap();
    c.bench_function("collect_all_groups", |b| {
        b.iter(|| {
            black_box(root.collect_all_groups());
        });
    });
}

// INDEX CREATION BENCHMARKS

fn bench_create_single_field_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_single_field_index");
    for size in [100, 1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let products = create_products(size);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            
            b.iter(|| {
                root.create_field_index("id", |p: &Product| black_box(p.id)).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_create_multiple_field_indexes(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_multiple_field_indexes");
    for size in [100, 1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let products = create_products(size);
            b.iter(|| {
                let root = GroupData::new_root("Root".to_string(), products.clone(), "All");
                root.create_field_index("id", |p: &Product| p.id).unwrap();
                root.create_field_index("category", |p: &Product| p.category.clone()).unwrap();
                root.create_field_index("price", |p: &Product| (p.price * 100.0) as i64).unwrap();
                black_box(root);
            });
        });
    }
    group.finish();
}

fn bench_create_field_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_field_index");
    for size in [100, 1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let products = create_products(size);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            
            b.iter(|| {
                root.create_field_index("is_available", |p: &Product| black_box(p.is_available)).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_group_creation_with_field_indexes(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_creation_with_field_indexes");
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
                        fd.create_field_index("id", |p: &Product| p.id).unwrap();
                        fd.create_field_index("price", |p: &Product| (p.price * 100.0) as i64).unwrap();
                        fd.create_field_index("is_available", |p: &Product| p.is_available).unwrap();
                        Ok(fd)
                    },
                ).unwrap();
                black_box(root);
            });
        });
    }
    group.finish();
}

// INDEX BENCHMARKS

fn bench_field_index_operation_and(c: &mut Criterion) {
    let mut group = c.benchmark_group("field_index_operation_and");
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let products = create_products(size);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            root.create_field_index("is_avaible", |p: &Product| p.is_available).unwrap();
            root.create_field_index("in_stock", |p: &Product| p.stock).unwrap();
            b.iter(|| {
                root.reset_filters();
                let result = root.filter_by_fields_ops(
                    &[
                        (
                            "is_avaible",
                            &[(FieldOperation::eq(true),Op::And)]
                        ),
                        (
                            "in_stock",
                            &[(FieldOperation::gt(10),Op::And)]
                        )
                    ]
                ).unwrap();
                black_box(result.len());
            });
        });
    }
    
    group.finish();
}

fn bench_field_index_operation_vs_normal_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("field_index_operation_vs_normal_filter");
    for size in [1_000_000,2_000_000,3_000_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        let products = create_products(*size);
        let root = GroupData::new_root("Root".to_string(), products, "All");
        root.create_field_index("is_available", |p: &Product| p.is_available).unwrap();
        root.create_field_index("in_stock", |p: &Product| p.stock).unwrap();
        // Normal filter
        group.bench_with_input(
            BenchmarkId::new("normal_filter", size),
            size,
            |b, _| {
                b.iter(|| {
                    root.data.reset_to_source();
                    root.filter(|p| p.is_available && p.stock > 10).unwrap();
                    black_box(root.data.items().len());
                });
            }
        );
        // Bit operation
        group.bench_with_input(
            BenchmarkId::new("field_index_operation", size),
            size,
            |b, _| {
                b.iter(|| {
                    root.reset_filters();
                    root.data.filter_by_fields_ops(&[
                        (
                            "is_available",
                            &[(FieldOperation::eq(true),Op::And)],
                        ),
                        (
                            "in_stock",
                            &[(FieldOperation::gt(10),Op::And)]
                        )
                        ]
                    ).unwrap();
                    black_box(root.data.items().len());
                });
            }
        );
    }
    group.finish();
}

fn bench_complex_field_index_operations(c: &mut Criterion) {
    let products = create_products(100000);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    root.create_field_index("is_available", |p: &Product| p.is_available).unwrap();
    //.create_bit_index("in_stock", |p: &Product| p.stock > 10)
    root.create_field_index("in_stock", |p: &Product| p.stock).unwrap();
    //root.create_field_index("expensive", |p: &Product| p.price > 800.0)
    root.create_field_index("expensive", |p| OrderedFloat(p.price)).unwrap();
       // .create_bit_index("cheap", |p: &Product| p.price < 600.0);
    c.bench_function("complex_field_index_operations", |b| {
        b.iter(|| {
            root.reset_filters();
            let result = root.filter_by_fields_ops
            (
                &[
                    (
                        "is_available",
                        &[(FieldOperation::eq(true),Op::And)],
                    ),
                    (
                        "in_stock",
                        &[(FieldOperation::gt(10),Op::And)],
                    ),
                    (
                        "expensive",
                        &[(FieldOperation::gt(800.0),Op::Or)]
                    )
                ]
            ).unwrap();
            black_box(result.len());
        });
    });
}


// INDEX IN SUBGROUPS BENCHMARKS

fn bench_create_field_index_in_subgroups(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_field_index_in_subgroups");
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let products = create_products(size);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            root.group_by(|p| p.category.clone(), "Categories").unwrap();
            
            b.iter(|| {
                root.create_field_index_in_subgroups("price", |p: &Product| black_box((p.price * 100.0) as i64)).unwrap();
            });
        });
    }
    
    group.finish();
}

fn bench_create_field_index_recursive(c: &mut Criterion) {
    let products = create_products(10000);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    root.group_by(|p| p.category.clone(), "Categories").unwrap();
    for subgroup in root.get_all_subgroups() {
        subgroup.group_by(|p| p.brand.clone(), "Brands").unwrap();
    }
    c.bench_function("create_field_index_recursive", |b| {
        b.iter(|| {
            root.create_field_index_recursive("id", |p: &Product| black_box(p.id)).unwrap();
        });
    });
}

fn bench_group_by_with_field_indexes(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_by_with_field_indexes");
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
                        fd.create_field_index("id", |p: &Product| p.id).unwrap();
                        fd.create_field_index("price", |p: &Product| (p.price * 100.0) as i64).unwrap();
                        Ok(())
                    },
                ).unwrap();
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
    root.group_by(|p| p.brand.clone(), "Brands").unwrap();
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


// CONCURRENT BENCHMARKS (Original)

fn bench_parallel_filter(c: &mut Criterion) {
    let products = create_products(1000);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    root.group_by(|p| p.category.clone(), "Categories").unwrap();
    let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
    let laptops = root.get_subgroup(&"Laptops".to_string()).unwrap();
    let tablets = root.get_subgroup(&"Tablets".to_string()).unwrap();
    c.bench_function("parallel_filter", |b| {
        b.iter(|| {
            phones.reset_filters();
            laptops.reset_filters();
            tablets.reset_filters();
            group_filter_parallel!(
                phones => |p: &Product| black_box(p.price > 800.0),
                laptops => |p: &Product| black_box(p.price > 1500.0),
                tablets => |p: &Product| black_box(p.price > 600.0),
            ).unwrap();
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
                root.group_by(|p| p.category.clone(), "Categories").unwrap();
                black_box(root);
            });
        });
    }
    group.finish();
}

fn bench_memory_with_field_indexes(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_with_field_indexes");
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
                        fd.create_field_index("id", |p: &Product| p.id).unwrap();
                        fd.create_field_index("category", |p: &Product| p.category.clone()).unwrap();
                        fd.create_field_index("price", |p: &Product| (p.price * 100.0) as i64).unwrap();
                        fd.create_field_index("is_available", |p: &Product| p.is_available).unwrap();
                        fd.create_field_index("in_stock", |p: &Product| p.stock > 10).unwrap();
                        Ok(fd)
                    },
                ).unwrap();
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
            root.group_by(|p| p.category.clone(), "Level1").unwrap();
            
            for cat in ["Phones", "Laptops", "Tablets"].iter() {
                if let Some(group) = root.get_subgroup(&cat.to_string()) {
                    group.group_by(|p| p.brand.clone(), "Level2").unwrap();
                    
                    for brand in ["Apple", "Samsung"].iter() {
                        if let Some(brand_group) = group.get_subgroup(&brand.to_string()) {
                            brand_group.group_by(|p| {
                                if p.price > 800.0 { "Premium".to_string() }
                                else { "Budget".to_string() }
                            }, "Level3").unwrap();
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
    bench_create_single_field_index,
    bench_create_multiple_field_indexes,
    bench_create_field_index,
    bench_group_creation_with_field_indexes,
    // Index operations
    bench_field_index_operation_and,
    bench_field_index_operation_vs_normal_filter,
    bench_complex_field_index_operations,
    // Indexes in subgroups
    bench_create_field_index_in_subgroups,
    bench_create_field_index_recursive,
    bench_group_by_with_field_indexes,
    // BTree operations
    bench_btree_subgroup_access,
    // Concurrent
    bench_parallel_filter,
    // Memory
    bench_memory_allocation,
    bench_memory_with_field_indexes,
    bench_deep_hierarchy,
);

criterion_main!(benches);