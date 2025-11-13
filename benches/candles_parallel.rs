use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId,Throughput};
#[cfg(target_os = "macos")]
use pprof::criterion::{PProfProfiler, Output};
use rayon::prelude::*;
use tree_man::group::GroupData;
use std::{
    hint::black_box,
    sync::Arc
};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

// ============================================================================
// Candle Structure (Финансовая свеча)
// ============================================================================

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct Candle {
    timestamp: i64,           // Unix timestamp
    symbol: String,           // BTC, ETH, etc
    timeframe: String,        // 1m, 5m, 15m, 1h, etc
    open: Decimal,
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
            timestamp,
            symbol,
            timeframe,
            open,
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

fn bench_candles_concurrent_rayon(c: &mut Criterion) {
    let mut group = c.benchmark_group("candles_concurrent_optimized");
    let candles = generate_candles(1_000_000);
    group.throughput(Throughput::Elements(1_000_000));
    let root = Arc::new(GroupData::new_root(
        "Root".to_string(),
        candles.clone(),
        "All Candles"
    ));
    root.group_by(|c| c.symbol.clone(), "By Symbol");
    let subgroups = root.get_subgroups();
    let symbols = vec!["BTC", "ETH", "SOL", "MATIC", "AVAX"];
    for &num_threads in [2, 4, 8, 16].iter() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .unwrap();
        group.bench_with_input(
            BenchmarkId::from_parameter(num_threads),
            &num_threads,
            |b, _| {
                b.iter(|| {
                    pool.install(|| {
                        (0..num_threads).into_par_iter().for_each(|i| {
                            let symbol = symbols[i % symbols.len()];
                            for _ in 0..1_000_000 {
                                if let Some(group) = subgroups.get(symbol) {
                                    black_box(group.data.len());
                                }
                            }
                        });
                    });
                });
            },
        );
    }
    group.finish();
}

fn bench_candles_concurrent_batched(c: &mut Criterion) {
    let mut group = c.benchmark_group("candles_concurrent_batched");
    group.throughput(Throughput::Elements(1_000_000));
    let candles = generate_candles(1_000_000);
    let root = Arc::new(GroupData::new_root(
        "Root".to_string(),
        candles.clone(),
        "All Candles"
    ));
    root.group_by(|c| c.symbol.clone(), "By Symbol");
    let subgroups = root.get_subgroups();
    let symbols = vec!["BTC", "ETH", "SOL", "MATIC", "AVAX"];
    for &num_threads in [2, 4, 8, 16].iter() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .unwrap();
        group.bench_with_input(
            BenchmarkId::from_parameter(num_threads),
            &num_threads,
            |b, _| {
                b.iter(|| {
                    pool.install(|| {
                        // ✅ Создаем батчи по 100K операций
                        let batches: Vec<_> = (0..num_threads)
                            .map(|i| {
                                let symbol = symbols[i % symbols.len()];
                                (0..100).map(|_| symbol).collect::<Vec<_>>()
                            })
                            .collect();
                        
                        batches.into_par_iter().for_each(|batch| {
                            for &symbol in &batch {
                                for _ in 0..10_000 {
                                    if let Some(group) = subgroups.get(symbol) {
                                        black_box(group.data.len());
                                    }
                                }
                            }
                        });
                    });
                });
            },
        );
    }
    group.finish();
}

fn bench_candles_sequential_vs_parallel(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_vs_parallel");
    let candles = generate_candles(1_000_000);
    group.throughput(Throughput::Elements(1_000_000));
    let root = Arc::new(GroupData::new_root(
        "Root".to_string(),
        candles.clone(),
        "All Candles"
    ));
    root.group_by(|c| c.symbol.clone(), "By Symbol");
    let subgroups = root.get_subgroups();
    let symbols = vec!["BTC", "ETH", "SOL", "MATIC", "AVAX"];
    group.bench_function("sequential_ops", |b| {
        b.iter(|| {
            for i in 0..8 {
                let symbol = symbols[i % symbols.len()];
                for _ in 0..10_000 {
                    if let Some(group) = subgroups.get(symbol) {
                        black_box(group.data.len());
                    }
                }
            }
        });
    });
    group.bench_function("parallel_8threads_1m_ops", |b| {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(8)
            .build()
            .unwrap();
        b.iter(|| {
            pool.install(|| {
                (0..8).into_par_iter().for_each(|i| {
                    let symbol = symbols[i % symbols.len()];
                    for _ in 0..1_000_000 {
                        if let Some(group) = subgroups.get(symbol) {
                            black_box(group.data.len());
                        }
                    }
                });
            });
        });
    });
    group.finish();
}

#[cfg(target_os = "macos")]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = 
        bench_candles_concurrent_rayon,
        bench_candles_concurrent_batched,
        bench_candles_sequential_vs_parallel,

}
criterion_main!(benches);