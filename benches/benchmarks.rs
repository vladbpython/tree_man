use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use tree_man::{
    group_filter_parallel,
    group::GroupData
};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;

#[derive(Clone, Debug)]
struct Product {
    category: String,
    brand: String,
    price: f64,
}

fn create_products(count: usize) -> Vec<Product> {
    (0..count).map(|i| Product {
        category: ["Phones", "Laptops", "Tablets"][i % 3].to_string(),
        brand: ["Apple", "Samsung", "Dell", "Lenovo"][i % 4].to_string(),
        price: 500.0 + (i as f64) * 10.0,
    }).collect()
}


// SINGLE THREAD BENCHMARKS


fn bench_group_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_creation");
    
    for size in [10, 100, 1000,10000,1000000].iter() {
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
    
    for size in [10, 100, 1000,10000,1000000].iter() {
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

fn bench_relatives_navigation(c: &mut Criterion) {
    let products = create_products(100);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    root.group_by(|p| p.brand.clone(), "Brands");
    
    let keys = root.subgroups_keys();
    let first = root.get_subgroup(&keys[0]).unwrap();
    
    c.bench_function("go_to_next_relative", |b| {
        b.iter(|| {
            black_box(first.go_to_next_relative());
        });
    });
}

fn bench_has_relatives(c: &mut Criterion) {
    let products = create_products(100);
    let root = GroupData::new_root("Root".to_string(), products, "All");
    root.group_by(|p| p.brand.clone(), "Brands");
    
    let keys = root.subgroups_keys();
    let first = root.get_subgroup(&keys[0]).unwrap();
    
    c.bench_function("has_next_relative", |b| {
        b.iter(|| {
            black_box(first.has_next_relative());
        });
    });
}

fn bench_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter");
    
    for size in [10, 100, 1000,10000,1000000].iter() {
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


// CONCURENT BENCHMARKS

fn bench_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_reads");
    
    let products = create_products(100);
    let root = Arc::new(GroupData::new_root("Root".to_string(), products, "All"));
    root.group_by(|p| p.category.clone(), "Categories");
    
    for num_threads in [2, 4, 8, 16].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_threads),
            num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    let handles: Vec<_> = (0..num_threads).map(|_| {
                        let root_clone = Arc::clone(&root);
                        thread::spawn(move || {
                            for _ in 0..100 {
                                black_box(root_clone.get_subgroup(&"Phones".to_string()));
                                black_box(root_clone.subgroups_count());
                                black_box(root_clone.subgroups_keys());
                            }
                        })
                    }).collect();
                    
                    for handle in handles {
                        handle.join().unwrap();
                    }
                });
            }
        );
    }
    
    group.finish();
}

fn bench_concurrent_relative_navigation(c: &mut Criterion) {
    let products = create_products(100);
    let root = Arc::new(GroupData::new_root("Root".to_string(), products, "All"));
    root.group_by(|p| p.brand.clone(), "Brands");
    
    let keys = root.subgroups_keys();
    let first = root.get_subgroup(&keys[0]).unwrap();
    
    c.bench_function("concurrent_relative_navigation", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..10).map(|_| {
                let group = Arc::clone(&first);
                thread::spawn(move || {
                    for _ in 0..100 {
                        black_box(group.has_next_relative());
                        black_box(group.has_prev_relative());
                        black_box(group.go_to_next_relative());
                    }
                })
            }).collect();
            
            for handle in handles {
                handle.join().unwrap();
            }
        });
    });
}

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

// MEMORY BENCHMARKS

fn bench_memory_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_allocation");
    
    for size in [100, 1000, 10000].iter() {
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
    bench_group_creation,
    bench_group_by,
    bench_get_subgroup,
    bench_relatives_navigation,
    bench_has_relatives,
    bench_filter,
    bench_clear_subgroups,
    bench_collect_all_groups,
    bench_concurrent_reads,
    bench_concurrent_relative_navigation,
    bench_parallel_filter,
    bench_memory_allocation,
    bench_deep_hierarchy,
);

criterion_main!(benches);