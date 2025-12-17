use criterion::{criterion_group, criterion_main, Criterion,Throughput};
use tree_man::{
    FilterData,
    FieldOperation,
    Op,
};
use rayon::prelude::*;
use std::{
    hint::black_box,
    sync::Arc,
    time::Duration,
};


#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Product {
    id: u64,
    category: String,
    brand: String,
    price: u64,
    rating: u64,
    in_stock: bool,
}

fn create_products_low_cardinality(count: usize) -> Vec<Product> {
    (0..count).map(|i| Product {
        id: i as u64,
        category: ["Phones", "Laptops", "Tablets", "Accessories"][i % 4].to_string(),
        brand: ["Apple", "Samsung", "Dell", "Lenovo", "HP"][i % 5].to_string(),
        price: (100.0 + (i as f64 * 23.7) % 2000.0) as u64,
        rating: ((1.0 + (i as f64 * 0.123) % 4.0) * 100.0) as u64,
        in_stock: i % 3 != 0,
    }).collect()
}

fn create_products_high_cardinality(size: usize) -> Vec<Arc<Product>> {
    (0..size)
        .map(|i| {
            Arc::new(Product {
                id: i as u64,
                category: format!("Category_{}", i % 100),
                brand: format!("Brand_{}", i % 50),
                price: i as u64, // уникальные цены
                rating: ((i % 5) + 1) as u64 ,
                in_stock: i % 2 == 0,
            })
        })
        .collect()
}

fn bench_query_with_indexes_eq_price_high_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_query");
    group.measurement_time(Duration::from_secs(10));
    
    let size = 2_000_000;
    group.throughput(Throughput::Elements(100)); // 100 queries

    // FilterData WITH field index - мгновенные lookup'ы!
    group.bench_function("FilterData_with_field_index_eq_price_100q_high_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        let data = FilterData::from_vec(products);
        data.create_field_index("price", |p| p.price).unwrap();
        
        b.iter(|| {
            for price in 100..200 {
                data.reset_to_source();
                data.filter_by_field_ops("price", &[(FieldOperation::eq(price),Op::And)]).unwrap();
                assert!(data.len() > 0);
                black_box(data.len());
            }
        });
    });
    
    // FilterData WITHOUT index - full scan каждый раз
    group.bench_function("FilterData_no_index_eq_price_100q_high_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        let data = FilterData::from_vec(products);
        
        b.iter(|| {
            for price in 100..200 {
                data.reset_to_source();
                data.filter(|p| p.price == price as u64).unwrap();
                black_box(data.len());
            }
        });
    });
    
    // Vec baseline - full scan каждый раз
    group.bench_function("Vec_baseline_100q_eq_price_high_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        
        b.iter(|| {
            for price in 100..200 {
                let filtered: Vec<_> = products
                    .iter()
                    .filter(|p| p.price == price as u64)
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    // Vec parallel - full scan каждый раз
    group.bench_function("Vec_parallel_100q_eq_price_high_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        
        b.iter(|| {
            for price in 100..200 {
                let filtered: Vec<_> = products
                    .par_iter()
                    .filter(|p| p.price == price as u64)
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    group.finish();
}

fn bench_query_with_indexes_eq_price_low_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_query");
    group.measurement_time(Duration::from_secs(10));
    
    let size = 2_000_000;
    group.throughput(Throughput::Elements(100)); // 100 queries

    // FilterData WITH field index - мгновенные lookup'ы!
    group.bench_function("FilterData_with_field_index_eq_price_100q_low_cardinality", |b| {
        let products = create_products_low_cardinality(size);
        let data = FilterData::from_vec(products);
        data.create_field_index("price", |p| p.price).unwrap();
        
        b.iter(|| {
            for price in 100..200 {
                data.reset_to_source();
                data.filter_by_field_ops("price", &[(FieldOperation::eq(price),Op::And)]).unwrap();
                assert!(data.len() > 0);
                black_box(data.len());
            }
        });
    });
    
    // FilterData WITHOUT index - full scan каждый раз
    group.bench_function("FilterData_no_index_eq_price_100q_low_cardinality", |b| {
        let products = create_products_low_cardinality(size);
        let data = FilterData::from_vec(products);
        
        b.iter(|| {
            for price in 100..200 {
                data.reset_to_source();
                data.filter(|p| p.price == price as u64).unwrap();
                black_box(data.len());
            }
        });
    });
    
    // Vec baseline - full scan каждый раз
    group.bench_function("Vec_baseline_100q_eq_price_low_cardinality", |b| {
        let products = create_products_low_cardinality(size);
        
        b.iter(|| {
            for price in 100..200 {
                let filtered: Vec<_> = products
                    .iter()
                    .filter(|p| p.price == price as u64)
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    // Vec parallel - full scan каждый раз
    group.bench_function("Vec_parallel_100q_eq_price_low_cardinality", |b| {
        let products = create_products_low_cardinality(size);
        
        b.iter(|| {
            for price in 100..200 {
                let filtered: Vec<_> = products
                    .par_iter()
                    .filter(|p| p.price == price as u64)
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    group.finish();
}

fn bench_query_with_indexes_eq_bool_high_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_query");
    group.measurement_time(Duration::from_secs(10));
    
    let size = 2_000_000;
    group.throughput(Throughput::Elements(100)); // 100 queries

    // FilterData WITH field index - мгновенные lookup'ы!
    group.bench_function("FilterData_with_field_index_eq_bool_100q_high_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        let data = FilterData::from_vec(products);
        data.create_field_index("in_stock", |p| p.in_stock).unwrap();
        
        b.iter(|| {
            for _ in 100..200 {
                data.reset_to_source();
                data.filter_by_field_ops("in_stock", &[(FieldOperation::eq(true),Op::And)]).unwrap();
                assert!(data.len() > 0);
                black_box(data.len());
            }
        });
    });
    
    // FilterData WITHOUT index - full scan каждый раз
    group.bench_function("FilterData_no_index_eq_bool_100q_high_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        let data = FilterData::from_vec(products);
        
        b.iter(|| {
            for _ in 100..200 {
                data.reset_to_source();
                data.filter(|p| p.in_stock == true).unwrap();
                black_box(data.len());
            }
        });
    });
    
    // Vec baseline - full scan каждый раз
    group.bench_function("Vec_baseline_100q_eq_bool_high_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        
        b.iter(|| {
            for _ in 100..200 {
                let filtered: Vec<_> = products
                    .iter()
                    .filter(|p| p.in_stock == true)
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    // Vec parallel - full scan каждый раз
    group.bench_function("Vec_parallel_100q_eq_bool_high_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        
        b.iter(|| {
            for _ in 100..200 {
                let filtered: Vec<_> = products
                    .par_iter()
                    .filter(|p| p.in_stock == true)
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    group.finish();
}

fn bench_query_with_indexes_eq_bool_low_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_query");
    group.measurement_time(Duration::from_secs(10));
    
    let size = 2_000_000;
    group.throughput(Throughput::Elements(100)); // 100 queries

    // FilterData WITH field index - мгновенные lookup'ы!
    group.bench_function("FilterData_with_field_index_eq_bool_100q_low_cardinality", |b| {
        let products = create_products_low_cardinality(size);
        let data = FilterData::from_vec(products);
        data.create_field_index("in_stock", |p| p.in_stock).unwrap();
        
        b.iter(|| {
            for _ in 100..200 {
                data.reset_to_source();
                data.filter_by_field_ops("in_stock", &[(FieldOperation::eq(true),Op::And)]).unwrap();
                assert!(data.len() > 0);
                black_box(data.len());
            }
        });
    });
    
    // FilterData WITHOUT index - full scan каждый раз
    group.bench_function("FilterData_no_index_eq_bool_100q_low_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        let data = FilterData::from_vec(products);
        
        b.iter(|| {
            for _ in 100..200 {
                data.reset_to_source();
                data.filter(|p| p.in_stock == true).unwrap();
                black_box(data.len());
            }
        });
    });
    
    // Vec baseline - full scan каждый раз
    group.bench_function("Vec_baseline_100q_eq_bool_low_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        
        b.iter(|| {
            for _ in 100..200 {
                let filtered: Vec<_> = products
                    .iter()
                    .filter(|p| p.in_stock == true)
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    // Vec parallel - full scan каждый раз
    group.bench_function("Vec_parallel_100q_eq_bool_low_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        
        b.iter(|| {
            for _ in 100..200 {
                let filtered: Vec<_> = products
                    .par_iter()
                    .filter(|p| p.in_stock == true)
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    group.finish();
}

fn bench_query_with_indexes_not_eq_high_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_query");
    group.measurement_time(Duration::from_secs(10));
    
    let size = 2_000_000;
    group.throughput(Throughput::Elements(100)); // 100 queries

    // FilterData WITH field index - мгновенные lookup'ы!
    group.bench_function("FilterData_with_field_index_not_eq_100q_high_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        let data = FilterData::from_vec(products);
        data.create_field_index("price", |p| p.price).unwrap();
        
        b.iter(|| {
            for price in 100..200 {
                data.reset_to_source();
                data.filter_by_field_ops("price", &[(FieldOperation::not_eq(price),Op::And)]).unwrap();
                assert!(data.len() > 0);
                black_box(data.len());
            }
        });
    });
    
    // FilterData WITHOUT index - full scan каждый раз
    group.bench_function("FilterData_no_index_not_eq_100q_high_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        let data = FilterData::from_vec(products);
        
        b.iter(|| {
            for price in 100..200 {
                data.reset_to_source();
                data.filter(|p| p.price != price as u64).unwrap();
                black_box(data.len());
            }
        });
    });
    
    // Vec baseline - full scan каждый раз
    group.bench_function("Vec_baseline_100q_not_eq_high_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        
        b.iter(|| {
            for price in 100..200 {
                let filtered: Vec<_> = products
                    .iter()
                    .filter(|p| p.price != price as u64)
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    // Vec parallel - full scan каждый раз
    group.bench_function("Vec_parallel_100q_not_eq_high_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        
        b.iter(|| {
            for price in 100..200 {
                let filtered: Vec<_> = products
                    .par_iter()
                    .filter(|p| p.price != price as u64)
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    group.finish();
}


fn bench_query_with_indexes_not_eq_low_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_query");
    group.measurement_time(Duration::from_secs(10));
    
    let size = 2_000_000;
    group.throughput(Throughput::Elements(100)); // 100 queries

    // FilterData WITH field index - мгновенные lookup'ы!
    group.bench_function("FilterData_with_field_index_not_eq_100q_low_cardinality", |b| {
        let products = create_products_low_cardinality(size);
        let data = FilterData::from_vec(products);
        data.create_field_index("price", |p| p.price).unwrap();
        
        b.iter(|| {
            for price in 100..200 {
                data.reset_to_source();
                data.filter_by_field_ops("price", &[(FieldOperation::eq(price),Op::And)]).unwrap();
                assert!(data.len() > 0);
                black_box(data.len());
            }
        });
    });
    
    // FilterData WITHOUT index - full scan каждый раз
    group.bench_function("FilterData_no_index_not_eq_100q_low_cardinality", |b| {
        let products = create_products_low_cardinality(size);
        let data = FilterData::from_vec(products);
        
        b.iter(|| {
            for price in 100..200 {
                data.reset_to_source();
                data.filter(|p| p.price == price as u64).unwrap();
                black_box(data.len());
            }
        });
    });
    
    // Vec baseline - full scan каждый раз
    group.bench_function("Vec_baseline_100q_not_eq_low_cardinality", |b| {
        let products = create_products_low_cardinality(size);
        
        b.iter(|| {
            for price in 100..200 {
                let filtered: Vec<_> = products
                    .iter()
                    .filter(|p| p.price == price as u64)
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    // Vec parallel - full scan каждый раз
    group.bench_function("Vec_parallel_100q_not_eq_low_cardinality", |b| {
        let products = create_products_low_cardinality(size);
        
        b.iter(|| {
            for price in 100..200 {
                let filtered: Vec<_> = products
                    .par_iter()
                    .filter(|p| p.price == price as u64)
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    group.finish();
}

fn bench_query_in_values_with_indexes_high_cadinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_query");
    group.measurement_time(Duration::from_secs(10));
    
    let size = 2_000_000;
    group.throughput(Throughput::Elements(100)); // 100 queries

    // FilterData WITH field index - мгновенные lookup'ы!
    group.bench_function("FilterData_with_field_in_value_index_100q_high_cadinality", |b| {
        let products = create_products_high_cardinality(size);
        let data = FilterData::from_vec(products);
        data.create_field_index("price", |p| p.price).unwrap();
        
        b.iter(|| {
            for price in 100..200 {
                let prices = vec![price,price+100,price+200];
                data.reset_to_source();
                data.filter_by_field_ops("price", &[(FieldOperation::in_values(prices),Op::And)]).unwrap();
                assert!(data.len() > 0);
                black_box(data.len());
            }
        });
    });
    
    // FilterData WITHOUT index - full scan каждый раз
    group.bench_function("FilterData_in_value_no_index_100q_high_cadinality", |b| {
        let products = create_products_high_cardinality(size);
        let data = FilterData::from_vec(products);
        
        b.iter(|| {
            for price in 100..200 {
                let prices = vec![price,price+100,price+200];
                data.reset_to_source();
                data.filter(|p| prices.contains(&p.price)).unwrap();
                black_box(data.len());
            }
        });
    });
    
    // Vec baseline - full scan каждый раз
    group.bench_function("Vec_in_value_baseline_100q_high_cadinality", |b| {
        let products = create_products_high_cardinality(size);
        
        b.iter(|| {
            for price in 100..200 {
                let prices = vec![price,price+100,price+200];
                let filtered: Vec<_> = products
                    .iter()
                    .filter(|p| prices.contains(&p.price))
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    // Vec parallel - full scan каждый раз
    group.bench_function("Vec_in_value_parallel_100q_high_cadinality", |b| {
        let products = create_products_high_cardinality(size);
        
        b.iter(|| {
            for price in 100..200 {
                let prices = vec![price,price+100,price+200];
                let filtered: Vec<_> = products
                    .par_iter()
                    .filter(|p| prices.contains(&p.price))
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    group.finish();
}

fn bench_query_in_values_with_indexes_low_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_query");
    group.measurement_time(Duration::from_secs(10));
    
    let size = 2_000_000;
    group.throughput(Throughput::Elements(100)); // 100 queries

    // FilterData WITH field index - мгновенные lookup'ы!
    group.bench_function("FilterData_with_field_in_value_index_100q_low_cardinality", |b| {
        let products = create_products_low_cardinality(size);
        let data = FilterData::from_vec(products);
        data.create_field_index("price", |p| p.price).unwrap();
        
        b.iter(|| {
            for price in 100..200 {
                let prices = vec![price,price+100,price+200];
                data.reset_to_source();
                data.filter_by_field_ops("price", &[(FieldOperation::in_values(prices),Op::And)]).unwrap();
                assert!(data.len() > 0);
                black_box(data.len());
            }
        });
    });
    
    // FilterData WITHOUT index - full scan каждый раз
    group.bench_function("FilterData_in_value_no_index_100q_low_cardinality", |b| {
        let products = create_products_low_cardinality(size);
        let data = FilterData::from_vec(products);
        
        b.iter(|| {
            for price in 100..200 {
                let prices = vec![price,price+100,price+200];
                data.reset_to_source();
                data.filter(|p| prices.contains(&p.price)).unwrap();
                black_box(data.len());
            }
        });
    });
    
    // Vec baseline - full scan каждый раз
    group.bench_function("Vec_in_value_baseline_100q_low_cardinality", |b| {
        let products = create_products_low_cardinality(size);
        
        b.iter(|| {
            for price in 100..200 {
                let prices = vec![price,price+100,price+200];
                let filtered: Vec<_> = products
                    .iter()
                    .filter(|p| prices.contains(&p.price))
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    // Vec parallel - full scan каждый раз
    group.bench_function("Vec_in_value_parallel_100q_low_cardinality", |b| {
        let products = create_products_low_cardinality(size);
        
        b.iter(|| {
            for price in 100..200 {
                let prices = vec![price,price+100,price+200];
                let filtered: Vec<_> = products
                    .par_iter()
                    .filter(|p| prices.contains(&p.price))
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    group.finish();
}

fn bench_query_in_range_with_indexes_high_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_query");
    group.measurement_time(Duration::from_secs(10));
    
    let size = 2_000_000;
    group.sample_size(10);
    group.throughput(Throughput::Elements(100)); // 100 queries

    // FilterData WITH field index - мгновенные lookup'ы!
    group.bench_function("FilterData_with_field_in_range_index_100q_high_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        let data = FilterData::from_vec(products);
        data.create_field_index("price", |p| p.price).unwrap();
        
        b.iter(|| {
            for price in 100..200 {
                data.reset_to_source();
                data.filter_by_field_ops("price", &[(FieldOperation::range(price, price * 15),Op::And)]).unwrap();
                black_box(data.len());
            }
        });
    });

        // FilterData WITHOUT index - full scan каждый раз
    group.bench_function("FilterData_in_range_no_index_100q_high_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        let data = FilterData::from_vec(products);
        
        b.iter(|| {
            for price in 100..200 {
                data.reset_to_source();
                data.filter(|p| p.price >= price && p.price <= price * 15).unwrap();
                black_box(data.len());
            }
        });
    });
    
    // Vec baseline - full scan каждый раз
    group.bench_function("Vec_in_range_baseline_100q_high_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        
        b.iter(|| {
            for price in 100..200 {
                let filtered: Vec<_> = products
                    .iter()
                    .filter(|p| p.price >= price && p.price <= price * 15)
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    // Vec parallel - full scan каждый раз
    group.bench_function("Vec_in_range_parallel_100q_high_cardinality", |b| {
        let products = create_products_high_cardinality(size);
        
        b.iter(|| {
            for price in 100..200 {
                let filtered: Vec<_> = products
                    .par_iter()
                    .filter(|p| p.price >= price && p.price <= price * 15)
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    group.finish();
}


fn bench_query_in_range_with_indexes_low_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_query");
    group.measurement_time(Duration::from_secs(10));
    
    let size = 2_000_000;
    group.sample_size(10);
    group.throughput(Throughput::Elements(100)); // 100 queries
    let init_products = create_products_low_cardinality(size);

    // FilterData WITH field index - мгновенные lookup'ы!
    group.bench_function("FilterData_with_field_in_range_index_100q_low_cardinality", |b| {
        let data = FilterData::from_vec(init_products.clone());
        data.create_field_index("price", |p| p.price).unwrap();
        
        b.iter(|| {
            for price in 100..200 {
                data.reset_to_source();
                data.filter_by_field_ops("price", &[(FieldOperation::range(price, price * 15),Op::And)]).unwrap();
                black_box(data.len());
            }
        });
    });

        // FilterData WITHOUT index - full scan каждый раз
    group.bench_function("FilterData_in_range_no_index_100q_low_cardinality", |b| {
        let data = FilterData::from_vec(init_products.clone());
        
        b.iter(|| {
            for price in 100..200 {
                data.reset_to_source();
                data.filter(|p| p.price >= price && p.price <= price * 15).unwrap();
                
                black_box(data.len());
            }
        });
    });
    
    // Vec baseline - full scan каждый раз
    group.bench_function("Vec_in_range_baseline_100q_low_cardinality", |b| {
        let products = init_products.clone();
        b.iter(|| {
            for price in 100..200 {
                let filtered: Vec<_> = products
                    .iter()
                    .filter(|p| p.price >= price && p.price <= price * 15)
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    // Vec parallel - full scan каждый раз
    group.bench_function("Vec_in_range_parallel_100q_low_cardinality", |b| {
        let products = init_products.clone();
        
        b.iter(|| {
            for price in 100..200 {
                let filtered: Vec<_> = products
                    .par_iter()
                    .filter(|p| p.price >= price && p.price <= price * 15)
                    .collect();
                black_box(filtered.len());
            }
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_query_with_indexes_eq_price_high_cardinality,
    bench_query_with_indexes_eq_price_low_cardinality,
    bench_query_with_indexes_eq_bool_high_cardinality,
    bench_query_with_indexes_eq_bool_low_cardinality,
    bench_query_with_indexes_not_eq_high_cardinality,
    bench_query_with_indexes_not_eq_low_cardinality,
    bench_query_in_values_with_indexes_high_cadinality,
    bench_query_in_values_with_indexes_low_cardinality,
    bench_query_in_range_with_indexes_high_cardinality,
    bench_query_in_range_with_indexes_low_cardinality,
);

criterion_main!(benches);
