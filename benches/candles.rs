use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId,Throughput};
use rust_decimal::prelude::FromPrimitive;
use tree_man::{group::GroupData, bit_index::BitOp};
use std::{
    hint::black_box,
    sync::Arc
};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

// Candle Structure

#[derive(Clone, Debug)]
struct Candle {
    _timestamp: i64,         
    symbol: String,           // BTC, ETH, etc
    _timeframe: String,        // 1m, 5m, 15m, 1h, etc
    _open: Decimal,
    _high: Decimal,
    _low: Decimal,
    close: Decimal,
    volume: Decimal,
    is_bullish: bool,         // close > open
    is_high_volume: bool,     // volume > average
}

impl Candle {
    fn new(
        timestamp: i64,
        symbol: String,
        timeframe: String,
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: Decimal,
    ) -> Self {
        Self {
            _timestamp: timestamp,
            symbol,
            _timeframe: timeframe,
            _open: open,
            _high: high,
            _low: low,
            close,
            volume,
            is_bullish: close > open,
            is_high_volume: volume > dec!(1000),
        }
    }
}

// ============================================================================
// Data Generation
// ============================================================================
fn generate_candles(count: usize) -> Vec<Candle> {
    let mut rng = StdRng::seed_from_u64(42);
    let symbols = vec!["BTC", "ETH", "SOL", "MATIC", "AVAX"];
    let timeframes = vec!["1m", "5m", "15m", "1h", "4h", "1d"];
    let base_timestamp = 1704067200; // 2024-01-01 00:00:00
    (0..count)
        .map(|i| {
            let symbol = symbols[i % symbols.len()].to_string();
            let timeframe = timeframes[i % timeframes.len()].to_string();
            // Базовые цены для символов
            let base_price: Decimal = match symbol.as_str() {
                "BTC" => dec!(45000),
                "ETH" => dec!(2500),
                "SOL" => dec!(100),
                "MATIC" => dec!(1),
                "AVAX" => dec!(40),
                _ => dec!(100),
            };
            // Генерируем случайные значения с Decimal
            let open = base_price
                + Decimal::from(rng.random_range(-500..=500)) * dec!(0.1);
            let close = open
                + Decimal::from(rng.random_range(-300..=300)) * dec!(0.1);
            let high = open.max(close) + Decimal::from(rng.random_range(0..=200)) * dec!(0.1);
            let low = open.min(close) - Decimal::from(rng.random_range(0..=200)) * dec!(0.1);
            let volume = Decimal::from(rng.random_range(100..=5000));
            let timestamp = base_timestamp + (i as i64 * 60);
            Candle::new(timestamp, symbol, timeframe, open, high, low, close, volume)
        })
        .collect()
}

// Benchmarks

fn bench_candles_group_by(c: &mut Criterion) {
    let mut group = c.benchmark_group("candles_groupby_optimized");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(15));
    // Создаем Arc ОДИН раз
    let candles = Arc::new(generate_candles(1_000_000));
    group.throughput(Throughput::Elements(1_000_000));
    //Group by symbol (почти zero-copy)
    group.bench_function("group_by_symbol", |b| {
        let candles_ref = Arc::clone(&candles);
        b.iter_batched(
            || {
                // Конвертируем в Vec<Arc<Candle>> БЕЗ клонирования самих Candle
                let arc_candles: Vec<_> = candles_ref
                    .iter()
                    .map(|c| Arc::new(c.clone()))  // Клонируем Candle, но это быстрее
                    .collect();
                
                Arc::new(GroupData::new_root(
                    "Root".to_string(),
                    arc_candles,
                    "All Candles"
                ))
            },
            |root| {
                root.group_by(|c| c.symbol.clone(), "By Symbol");
                black_box(&root);
            },
            criterion::BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn bench_candles_filtering(c: &mut Criterion) {
    let mut group = c.benchmark_group("candles_filtering");
    group.sample_size(20);
    group.throughput(Throughput::Elements(1_000_000));
    group.measurement_time(std::time::Duration::from_secs(15));
    let candles = generate_candles(1_000_000);
    let root = Arc::new(GroupData::new_root(
        "Root".to_string(),
        candles.clone(),
        "All Candles"
    ));
    root.data
    .create_bit_index("filter_bullish",|c|c.is_bullish)
    .create_bit_index("filter_complex", |c|{
        //c.is_bullish 
        c.volume > dec!(2000.0) &&  c.volume < dec!(3000)
        //&& c.price_change_pct() > dec!(1.0)
    })
   .create_decimal_index("filter_price_range", |c|c.close);
    group.throughput(Throughput::Elements(1_000_000));
    // Simple filter
    group.bench_function("filter_bullish", |b| {
        b.iter(|| {
            let result = root.data.filter_by_bit_index("filter_bullish");
            black_box(result.len());
        });
    });
    // Complex filter
    group.bench_function("filter_complex", |b| {
        b.iter(|| {
            let result = root.data.filter_by_bit_index("filter_complex");
            black_box(result.len());
        });
    });
    // Range filter on price
    group.bench_function("filter_price_range", |b| {
        b.iter(|| {
            let result = root.data.filter_by_index_range("filter_price_range",dec!(2000.0)..dec!(3000));
            black_box(result.len());
        });
    });
    group.finish();
}

fn bench_candles_indexes(c: &mut Criterion) {
    let mut group = c.benchmark_group("candles_indexes");
    group.throughput(Throughput::Elements(1_000_000));
    let candles = generate_candles(1_000_000);
    // Create indexes
    group.bench_function("create_indexes", |b| {
        b.iter(|| {
            let root = Arc::new(GroupData::new_root(
                "Root".to_string(),
                candles.clone(),
                "All Candles"
            ));
            root
                .create_index("symbol", |c: &Candle| c.symbol.clone())
                .create_index("close", |c: &Candle| c.close);
            
            black_box(&root);
        });
    });
    
    // Benchmark with indexes created
    let root = Arc::new(GroupData::new_root(
        "Root".to_string(),
        candles.clone(),
        "All Candles"
    ));
    root
        .create_index("symbol", |c: &Candle| c.symbol.clone())
        .create_index("close", |c: &Candle| c.close * dec!(100.0));

    // Filter by index - exact match
    group.bench_function("filter_by_symbol_index", |b| {
        b.iter(|| {
            let result = root.filter_by_index("symbol", &"BTC".to_string());
            black_box(result.len());
        });
    });
    // Range query on price
    group.bench_function("filter_by_price_range", |b| {
        let min_price = Decimal::from_f64(2000.0 * 100.0).unwrap();
        let max_price = Decimal::from_f64(3000.0 * 100.0).unwrap();
        
        b.iter(|| {
            let result = root.filter_by_index_range("close", min_price..max_price);
            black_box(result.len());
        });
    });
    // Get top N by price
    group.bench_function("get_top_100_by_price", |b| {
        b.iter(|| {
            let result = root.get_top_n_by_index::<Decimal>("close", 100);
            black_box(result.len());
        });
    });
    group.finish();
}

fn bench_candles_bit_indexes(c: &mut Criterion) {
    let mut group = c.benchmark_group("candles_bit_indexes");
    group.throughput(Throughput::Elements(1_000_000));
    let candles = generate_candles(1_000_000);
    // Create bit indexes
    group.bench_function("create_bit_indexes", |b| {
        b.iter(|| {
            let root = Arc::new(GroupData::new_root(
                "Root".to_string(),
                candles.clone(),
                "All Candles"
            ));
            root.create_bit_index("is_bullish", |c: &Candle| c.is_bullish)
                .create_bit_index("is_high_volume", |c: &Candle| c.is_high_volume)
                .create_bit_index("is_btc", |c: &Candle| c.symbol == "BTC");
            
            black_box(&root);
        });
    });
    // Benchmark with bit indexes
    let root = Arc::new(GroupData::new_root(
        "Root".to_string(),
        candles.clone(),
        "All Candles"
    ));
    root.create_bit_index("is_bullish", |c: &Candle| c.is_bullish)
        .create_bit_index("is_high_volume", |c: &Candle| c.is_high_volume)
        .create_bit_index("is_btc", |c: &Candle| c.symbol == "BTC")
        .create_bit_index("is_eth", |c: &Candle| c.symbol == "ETH");
    
    // Single bit filter
    group.bench_function("filter_bullish_bit", |b| {
        b.iter(|| {
            let result = root.filter_by_bit_operation(&[("is_bullish", BitOp::And)]);
            black_box(result.len());
        });
    });
    // AND operation
    group.bench_function("bit_and_bullish_high_volume", |b| {
        b.iter(|| {
            let result = root.filter_by_bit_operation(&[
                ("is_bullish", BitOp::And),
                ("is_high_volume", BitOp::And),
            ]);
            black_box(result.len());
        });
    });
    // Complex bit operations
    group.bench_function("bit_complex_btc_or_eth_and_bullish", |b| {
        b.iter(|| {
            let result = root.filter_by_bit_operation(&[
                ("is_btc", BitOp::Or),
                ("is_eth", BitOp::Or),
                ("is_bullish", BitOp::And),
            ]);
            black_box(result.len());
        });
    });
    group.finish();
}

fn bench_candles_group_with_indexes(c: &mut Criterion) {
    let mut group = c.benchmark_group("candles_group_with_indexes");
    group.throughput(Throughput::Elements(1_000_000));
    let candles = generate_candles(1_000_000);
    // Group by with automatic index creation
    group.bench_function("group_by_symbol_with_indexes", |b| {
        b.iter(|| {
            let root = Arc::new(GroupData::new_root(
                "Root".to_string(),
                candles.clone(),
                "All Candles"
            ));
            root.group_by_with_indexes(
                |c| c.symbol.clone(),
                "By Symbol",
                |filter_data| {
                    filter_data.create_index("close", |c: &Candle| c.close * dec!(100.0));
                    filter_data.create_bit_index("is_bullish", |c: &Candle| c.is_bullish);
                }
            );
            
            black_box(&root);
        });
    });
    group.finish();
}

fn bench_candles_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("candles_memory");
    // Benchmark memory allocation patterns
    for size in [100_000, 500_000, 1_000_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(
            BenchmarkId::new("create_and_group", size),
            size,
            |b, &size| {
                b.iter(|| {
                    let candles = generate_candles(size);
                    let root = Arc::new(GroupData::new_root(
                        "Root".to_string(),
                        candles,
                        "All Candles"
                    ));
                    
                    root.group_by(|c| c.symbol.clone(), "By Symbol");
                    
                    black_box(&root);
                });
            }
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_candles_group_by,
    bench_candles_filtering,
    bench_candles_bit_indexes,
    bench_candles_indexes,
    bench_candles_group_with_indexes,
    bench_candles_memory_usage,
);

criterion_main!(benches);
