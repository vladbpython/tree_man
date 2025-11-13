use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, BatchSize,Throughput};
use tree_man::group::GroupData;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use im::{Vector as ImVector, HashMap as ImHashMap, OrdMap as ImOrdMap};
use rpds::Vector as RpdsVector;
use rayon::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Product {
    id: u64,
    category: String,
    brand: String,
    price: u64,  // Changed to u64 for Eq/Hash
    rating: u64, // Changed to u64 (rating * 100)
    in_stock: bool,
}

fn create_products(count: usize) -> Vec<Product> {
    (0..count).map(|i| Product {
        id: i as u64,
        category: ["Phones", "Laptops", "Tablets", "Accessories"][i % 4].to_string(),
        brand: ["Apple", "Samsung", "Dell", "Lenovo", "HP"][i % 5].to_string(),
        price: (100.0 + (i as f64 * 23.7) % 2000.0) as u64,
        rating: ((1.0 + (i as f64 * 0.123) % 4.0) * 100.0) as u64,
        in_stock: i % 3 != 0,
    }).collect()
}


//  MULTI-THREADED (все используют Rayon)

fn bench_multi_threaded_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("02_multi_threaded/creation");
    group.measurement_time(Duration::from_secs(10));
    for size in [1_000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        // TreeMan С Rayon
        group.bench_with_input(
            BenchmarkId::new("TreeMan_parallel", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || create_products(size),
                    |products| {
                        let root = GroupData::new_root(
                            "Root".to_string(),
                            products,
                            "All Products"
                        );
                        black_box(root)
                    },
                    BatchSize::LargeInput
                );
            }
        );

        // im::Vector С Rayon (через Vec)
        group.bench_with_input(
            BenchmarkId::new("im::Vector_parallel", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || create_products(size),
                    |products| {
                        let vec_data: Vec<Product> = products
                            .into_par_iter()
                            .collect();
                        let data: ImVector<Product> = vec_data
                            .into_iter()
                            .collect();
                        black_box(data)
                    },
                    BatchSize::LargeInput
                );
            }
        );
        
        // rpds::Vector С Rayon
        group.bench_with_input(
            BenchmarkId::new("rpds::Vector_parallel", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || create_products(size),
                    |products| {
                        let vec_data: Vec<Product> = products
                            .into_par_iter()
                            .collect();
                        
                        let mut data = RpdsVector::new();
                        for p in vec_data {
                            data.push_back_mut(p);
                        }
                        black_box(data)
                    },
                    BatchSize::LargeInput
                );
            }
        );
        
        // im::HashMap С Rayon (через промежуточный Vec)
        group.bench_with_input(
            BenchmarkId::new("im::HashMap_parallel", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || create_products(size),
                    |products| {
                        // Параллельная группировка через Vec<(K, V)>
                        use std::collections::HashMap as StdHashMap;
                        
                        let groups: Vec<(String, Vec<Product>)> = products
                            .into_par_iter()
                            .fold(
                                || StdHashMap::new(),
                                |mut acc, p| {
                                    acc.entry(p.category.clone())
                                        .or_insert_with(Vec::new)
                                        .push(p);
                                    acc
                                }
                            )
                            .reduce(
                                || StdHashMap::new(),
                                |mut a, b| {
                                    for (k, mut v) in b {
                                        a.entry(k).or_insert_with(Vec::new).append(&mut v);
                                    }
                                    a
                                }
                            )
                            .into_iter()
                            .collect();
                        
                        let map: ImHashMap<String, Vec<Product>> = groups.into_iter().collect();
                        black_box(map)
                    },
                    BatchSize::LargeInput
                );
            }
        );
        
        // im::OrdMap С Rayon
        group.bench_with_input(
            BenchmarkId::new("im::OrdMap_parallel", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || create_products(size),
                    |products| {
                        use std::collections::HashMap as StdHashMap;
                        
                        let groups: Vec<(String, Vec<Product>)> = products
                            .into_par_iter()
                            .fold(
                                || StdHashMap::new(),
                                |mut acc, p| {
                                    acc.entry(p.category.clone())
                                        .or_insert_with(Vec::new)
                                        .push(p);
                                    acc
                                }
                            )
                            .reduce(
                                || StdHashMap::new(),
                                |mut a, b| {
                                    for (k, mut v) in b {
                                        a.entry(k).or_insert_with(Vec::new).append(&mut v);
                                    }
                                    a
                                }
                            )
                            .into_iter()
                            .collect();
                        
                        let map: ImOrdMap<String, Vec<Product>> = groups.into_iter().collect();
                        black_box(map)
                    },
                    BatchSize::LargeInput
                );
            }
        );
    }
    
    group.finish();
}

fn bench_multi_threaded_filtering(c: &mut Criterion) {
    let mut group = c.benchmark_group("02_multi_threaded/filtering");
    group.measurement_time(Duration::from_secs(10));
    for size in [50,500,5000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        // TreeMan С Rayon
        group.bench_with_input(
            BenchmarkId::new("TreeMan_parallel", size),
            size,
            |b, &size| {
                let products = create_products(size);
                let root = GroupData::new_root(
                    "Root".to_string(),
                    products,
                    "All Products"
                );
                
                b.iter(|| {
                    root.reset_filters();
                    root.filter(|p| p.price > 700);
                    root.filter(|p| p.rating > 300);
                    root.filter(|p| p.in_stock);
                    black_box(root.data.len())
                });
            }
        );
        
        // im::Vector С Rayon (через Vec)
        group.bench_with_input(
            BenchmarkId::new("im::Vector_parallel", size),
            size,
            |b, &size| {
                let products = create_products(size);
                let data: ImVector<Product> = products.into_iter().collect();
                b.iter(|| {
                    let vec: Vec<Product> = data.iter().cloned().collect();
                    let filtered1: Vec<Product> = vec
                        .par_iter()
                        .filter(|p| p.price > 700)
                        .cloned()
                        .collect();
                    let filtered2: Vec<Product> = filtered1
                        .par_iter()
                        .filter(|p| p.rating > 300)
                        .cloned()
                        .collect();
                    let filtered3: Vec<Product> = filtered2
                        .par_iter()
                        .filter(|p| p.in_stock)
                        .cloned()
                        .collect();
                    let result: ImVector<Product> = filtered3.into_iter().collect();
                    black_box(result.len())
                });
            }
        );
    }
    
    group.finish();
}


// PARALLELISM IMPACT

fn bench_parallelism_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("03_parallelism_impact");
    group.measurement_time(Duration::from_secs(10));
    let size = 100_000;
    group.throughput(Throughput::Elements(size as u64));
    // TreeMan: Sequential vs Parallel
    group.bench_function("TreeMan_sequential", |b| {
        b.iter_batched(
            || create_products(size),
            |products| {
                let arc_items: Vec<Arc<Product>> = products
                    .into_iter()
                    .map(Arc::new)
                    .collect();
                black_box(arc_items)
            },
            BatchSize::LargeInput
        );
    });
    
    group.bench_function("TreeMan_parallel", |b| {
        b.iter_batched(
            || create_products(size),
            |products| {
                let root = GroupData::new_root(
                    "Root".to_string(),
                    products,
                    "All Products"
                );
                black_box(root)
            },
            BatchSize::LargeInput
        );
    });
    
    // im::Vector: Sequential vs Parallel
    group.bench_function("im::Vector_sequential", |b| {
        b.iter_batched(
            || create_products(size),
            |products| {
                let data: ImVector<Product> = products
                    .into_iter()
                    .collect();
                black_box(data)
            },
            BatchSize::LargeInput
        );
    });
    
    group.bench_function("im::Vector_parallel", |b| {
        b.iter_batched(
            || create_products(size),
            |products| {
                let vec_data: Vec<Product> = products
                    .into_par_iter()
                    .collect();
                let data: ImVector<Product> = vec_data
                    .into_iter()
                    .collect();
                black_box(data)
            },
            BatchSize::LargeInput
        );
    });
    
    // im::HashMap: Sequential vs Parallel
    group.bench_function("im::HashMap_sequential", |b| {
        b.iter_batched(
            || create_products(size),
            |products| {
                let mut map = ImHashMap::new();
                for p in products {
                    map.entry(p.category.clone())
                        .or_insert_with(Vec::new)
                        .push(p);
                }
                black_box(map)
            },
            BatchSize::LargeInput
        );
    });
    
    group.bench_function("im::HashMap_parallel", |b| {
        b.iter_batched(
            || create_products(size),
            |products| {
                use std::collections::HashMap as StdHashMap;
                let groups: Vec<(String, Vec<Product>)> = products
                    .into_par_iter()
                    .fold(
                        || StdHashMap::new(),
                        |mut acc, p| {
                            acc.entry(p.category.clone())
                                .or_insert_with(Vec::new)
                                .push(p);
                            acc
                        }
                    )
                    .reduce(
                        || StdHashMap::new(),
                        |mut a, b| {
                            for (k, mut v) in b {
                                a.entry(k).or_insert_with(Vec::new).append(&mut v);
                            }
                            a
                        }
                    )
                    .into_iter()
                    .collect();
                
                let map: ImHashMap<String, Vec<Product>> = groups.into_iter().collect();
                black_box(map)
            },
            BatchSize::LargeInput
        );
    });
    group.finish();
}


// UNIQUE FEATURES

fn bench_unique_hierarchical_grouping(c: &mut Criterion) {
    let mut group = c.benchmark_group("04_unique_features/hierarchical");
    group.measurement_time(Duration::from_secs(10));
    for size in [100_000, 1_000_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(
            BenchmarkId::new("TreeMan", size),
            size,
            |b, &size| {
                let products = create_products(size);
                let root = GroupData::new_root(
                    "Root".to_string(),
                    products,
                    "All Products"
                );
                b.iter(|| {
                    root.clear_subgroups();
                    // Level 1: By category
                    root.group_by(|p| p.category.clone(), "By Category");
                    // Level 2: By brand (nested!)
                    let categories = root.subgroups_keys();
                    for cat_key in categories {
                        if let Some(cat_group) = root.get_subgroup(&cat_key) {
                            cat_group.group_by(|p| p.brand.clone(), "By Brand");
                        }
                    }
                    black_box(root.collect_all_groups().len())
                });
            }
        );
        
        // im::HashMap: эмуляция иерархии (вложенные Maps)
        group.bench_with_input(
            BenchmarkId::new("im::HashMap_nested", size),
            size,
            |b, &size| {
                let products = create_products(size);
                b.iter(|| {
                    // Level 1: By category
                    let mut cat_map: ImHashMap<String, Vec<Product>> = ImHashMap::new();
                    for p in products.clone() {
                        cat_map.entry(p.category.clone())
                            .or_insert_with(Vec::new)
                            .push(p);
                    }
                    // Level 2: By brand (nested)
                    let mut nested: ImHashMap<String, ImHashMap<String, Vec<Product>>> = ImHashMap::new();
                    for (category, prods) in cat_map {
                        let mut brand_map: ImHashMap<String, Vec<Product>> = ImHashMap::new();
                        for p in prods {
                            brand_map.entry(p.brand.clone())
                                .or_insert_with(Vec::new)
                                .push(p);
                        }
                        nested.insert(category, brand_map);
                    }
                    black_box(nested.len())
                });
            }
        );
    }
    
    group.finish();
}

fn bench_unique_parallel_group_filtering(c: &mut Criterion) {
    let mut group = c.benchmark_group("04_unique_features/parallel_groups");
    group.measurement_time(Duration::from_secs(10));
    for size in [10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        let products = create_products(*size);
        let root = GroupData::new_root(
            "Root".to_string(),
            products,
            "All Products"
        );
        root.group_by(|p| p.category.clone(), "By Category");
        let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
        let laptops = root.get_subgroup(&"Laptops".to_string()).unwrap();
        let tablets = root.get_subgroup(&"Tablets".to_string()).unwrap();
        let accessories = root.get_subgroup(&"Accessories".to_string()).unwrap();
        // TreeMan: built-in parallel group filtering (УНИКАЛЬНО!)
        group.bench_with_input(
            BenchmarkId::new("TreeMan_builtin", size),
            size,
            |b, _| {
                b.iter(|| {
                    phones.reset_filters();
                    laptops.reset_filters();
                    tablets.reset_filters();
                    accessories.reset_filters();
                    tree_man::group_filter_parallel!(
                        phones => |p: &Product| p.price > 800,
                        laptops => |p: &Product| p.price > 1500,
                        tablets => |p: &Product| p.price > 600,
                        accessories => |p: &Product| p.price > 50,
                    );
                    black_box((
                        phones.data.len(),
                        laptops.data.len()
                    ))
                });
            }
        );
        
        // Sequential для сравнения
        group.bench_with_input(
            BenchmarkId::new("TreeMan_sequential", size),
            size,
            |b, _| {
                b.iter(|| {
                    phones.reset_filters();
                    laptops.reset_filters();
                    tablets.reset_filters();
                    accessories.reset_filters();
                    
                    phones.filter(|p| p.price > 800);
                    laptops.filter(|p| p.price > 1500);
                    tablets.filter(|p| p.price > 600);
                    accessories.filter(|p| p.price > 50);
                    black_box((
                        phones.data.len(),
                        laptops.data.len()
                    ))
                });
            }
        );
    }
    
    group.finish();
}

// CRITERION GROUPS

criterion_group!(
    benches,
    // сравнение: multi-threaded
    bench_multi_threaded_creation,
    bench_multi_threaded_filtering,
    // parallelism
    bench_parallelism_impact,
    // Уникальные фичи
    bench_unique_hierarchical_grouping,
    bench_unique_parallel_group_filtering,
);

criterion_main!(benches);