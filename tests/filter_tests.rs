#[cfg(test)]
mod filter_data_tests{
    use rust_decimal::{Decimal, prelude::FromPrimitive};
    use tree_man::{
        Op, FieldOperation,
        filter::{
            IntoFilterData,
            FilterData,
        }
    };
    use std::{
        sync::{
            Arc,
            atomic::{
                AtomicUsize,
                Ordering
            }
        },
        time::Instant,
        thread,
    };

    #[derive(Clone)]
    #[allow(dead_code)]
    struct Product {
        id: u64,
        price: u64,
        category: String,
        in_stock: bool,
    }

    #[test]
    fn test_filtered_collection_from_vec() {
        let data = vec![1, 2, 3, 4, 5];
        let filtered = data.into_filtered();
        assert_eq!(filtered.len(), 5);
        assert_eq!(filtered.current_level(), 0);
    }

    #[test]
    fn test_filtered_collection_filter() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let filtered = data.into_filtered();
        filtered.filter(|x| *x > 5).unwrap();
        assert_eq!(filtered.len(), 5);
        assert_eq!(filtered.current_level(), 1);
    }

    #[test]
    fn test_filtered_decimal_collection_filter() {
        let data = vec![
            Decimal::from_u8(1).unwrap_or(Decimal::ZERO), 
            Decimal::from_u8(2).unwrap_or(Decimal::ZERO), 
            Decimal::from_u8(3).unwrap_or(Decimal::ZERO), 
            Decimal::from_u8(4).unwrap_or(Decimal::ZERO), 
            Decimal::from_u8(5).unwrap_or(Decimal::ZERO), 
            Decimal::from_u8(6).unwrap_or(Decimal::ZERO), 
            Decimal::from_u8(7).unwrap_or(Decimal::ZERO), 
            Decimal::from_u8(8).unwrap_or(Decimal::ZERO), 
            Decimal::from_u8(9).unwrap_or(Decimal::ZERO), 
            Decimal::from_u8(10).unwrap_or(Decimal::ZERO)
        ];
        let filtered = data.into_filtered();
        filtered.filter(|x| *x > Decimal::from_u8(5).unwrap_or(Decimal::ZERO)).unwrap();
        assert_eq!(filtered.len(), 5);
        assert_eq!(filtered.current_level(), 1);
    }

    #[test]
    fn test_field_index_eq() {
        let products = vec![
            Product { id: 1, price: 100, category: "A".into(), in_stock: true },
            Product { id: 2, price: 200, category: "B".into(), in_stock: true },
            Product { id: 3, price: 300, category: "A".into(), in_stock: false },
        ];
        
        let data = FilterData::from_vec(products);
        data.create_field_index("price", |p| p.price).unwrap();
        
        data.filter_by_field_ops("price", &[
            (FieldOperation::eq(200), Op::And)
        ]).unwrap();
        assert_eq!(data.len(), 1);
    }

    #[test]
    fn test_field_index_range() {
        let products = vec![
            Product { id: 1, price: 100, category: "A".into(), in_stock: true },
            Product { id: 2, price: 200, category: "B".into(), in_stock: true },
            Product { id: 3, price: 300, category: "A".into(), in_stock: false },
            Product { id: 4, price: 400, category: "B".into(), in_stock: true },
        ];
        
        let data = FilterData::from_vec(products);
        data.create_field_index("price", |p| p.price).unwrap();
        
        data.filter_by_field_ops("price", &[
            (FieldOperation::gte(200), Op::And),
            (FieldOperation::lte(300), Op::And),
        ]).unwrap();
        assert_eq!(data.len(), 2); // 200, 300
    }

    #[test]
    fn test_filtered_collection_multiple_filters() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let filtered = data.into_filtered();
        filtered.filter(|x| *x > 3).unwrap();
        assert_eq!(filtered.len(), 7);
        filtered.filter(|x| *x % 2 == 0).unwrap();
        assert_eq!(filtered.len(), 4); // 4, 6, 8, 10
        assert_eq!(filtered.current_level(), 2);
    }

    #[test]
    fn test_filtered_collection_navigation() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let filtered = data.into_filtered();
        filtered.filter(|x| *x > 5).unwrap();
        filtered.filter(|x| *x % 2 == 0).unwrap();
        assert_eq!(filtered.current_level(), 2);
        filtered.up();
        assert_eq!(filtered.current_level(), 1);
        assert_eq!(filtered.len(), 5);
        filtered.reset_to_source();
        assert_eq!(filtered.current_level(), 0);
        assert_eq!(filtered.len(), 10);
    }

    #[test]
    fn test_filtered_collection_memory_leak() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);
        
        struct DropCounter {
            _id: usize,
        }
        
        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        DROP_COUNT.store(0, Ordering::SeqCst);
        
        {
            let data: Vec<DropCounter> = (0..1000)
                .map(|i| DropCounter { _id: i })
                .collect();
            let filtered = data.into_filtered();
            filtered.filter(|_| true).unwrap();
            filtered.filter(|_| true).unwrap();
            filtered.filter(|_| true).unwrap();
        }
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1000);
    }

    #[test]
    fn test_cleanup_on_reset_to_source() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);
        struct DropCounter {
            _id: usize,
        }
        
        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }
        DROP_COUNT.store(0, Ordering::SeqCst);
        let data: Vec<DropCounter> = (0..1000)
            .map(|i| DropCounter { _id: i })
            .collect();
        let filtered = data.into_filtered();
        filtered.filter(|d| d._id > 100).unwrap();
        filtered.filter(|d| d._id > 500).unwrap();
        filtered.filter(|d| d._id > 800).unwrap();
        assert_eq!(filtered.stored_levels_count(), 4);
        filtered.reset_to_source();
        assert_eq!(filtered.stored_levels_count(), 1);
        assert_eq!(filtered.current_level(), 0);
        let stats = filtered.memory_stats();
        println!("After reset_to_source: {:?}", stats);
        assert!(stats.is_clean());
        assert_eq!(stats.wasted_items, 0);
    }

    #[test]
    fn test_cleanup_on_goto_level() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);
        
        struct DropCounter {
            _id: usize,
        }
        
        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }
        
        DROP_COUNT.store(0, Ordering::SeqCst);

        let data: Vec<DropCounter> = (0..1000)
            .map(|i| DropCounter { _id: i })
            .collect();
        let filtered = data.into_filtered();
        filtered.filter(|_| true).unwrap();
        filtered.filter(|_| true).unwrap();
        filtered.filter(|_| true).unwrap();
        filtered.filter(|_| true).unwrap();
        filtered.filter(|_| true).unwrap();
        assert_eq!(filtered.stored_levels_count(), 6);
        filtered.up();
        assert_eq!(filtered.stored_levels_count(), 5);
        assert_eq!(filtered.current_level(), 4);
        let stats = filtered.memory_stats();
        assert!(stats.is_clean());
    }

    #[test]
    fn test_cleanup_on_up() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        filtered.filter(|x| *x > 100).unwrap();
        filtered.filter(|x| *x > 500).unwrap();
        filtered.filter(|x| *x > 800).unwrap();
        assert_eq!(filtered.stored_levels_count(), 4);
        assert_eq!(filtered.current_level(), 3);
        filtered.up();
        assert_eq!(filtered.current_level(), 2);
        assert_eq!(filtered.stored_levels_count(), 3);
        let stats = filtered.memory_stats();
        assert!(stats.is_clean());
        assert_eq!(stats.wasted_items, 0);
    }

    #[test]
    fn test_no_cleanup_on_down() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        filtered.filter(|x| *x > 100).unwrap();
        filtered.filter(|x| *x > 500).unwrap();
        assert_eq!(filtered.current_level(), 2);
        filtered.up();
        assert_eq!(filtered.current_level(), 1);
        assert_eq!(filtered.stored_levels_count(), 2);
        assert_eq!(filtered.current_level(), 1);
    }

    #[test]
    fn test_memory_stats_after_operations() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        let stats = filtered.memory_stats();
        assert_eq!(stats.stored_levels, 1);
        assert_eq!(stats.current_level_items, 1000);
        assert_eq!(stats.total_stored_items, 1000);
        assert!(stats.is_clean());
        assert_eq!(stats.efficiency(), 1.0);
        filtered.filter(|x| *x > 500).unwrap();
        filtered.filter(|x| *x > 800).unwrap();
        let stats = filtered.memory_stats();
        assert_eq!(stats.stored_levels, 3);
        assert!(stats.current_level_items < 1000);
        assert!(stats.is_clean());
        filtered.up();
        let stats = filtered.memory_stats();
        assert_eq!(stats.stored_levels, 2);
        assert!(stats.is_clean());
        assert_eq!(stats.wasted_items, 0);
    }

    #[test]
    fn test_filter_after_navigation_cleans_forward_levels() {
        let data: Vec<i32> = (0..100).collect();
        let filtered = data.into_filtered();
        filtered.filter(|x| *x > 20).unwrap();
        filtered.filter(|x| *x > 50).unwrap();
        filtered.filter(|x| *x > 70).unwrap();
        assert_eq!(filtered.stored_levels_count(), 4);
        filtered.up();
        filtered.up();
        assert_eq!(filtered.stored_levels_count(), 2);
        filtered.filter(|x| *x < 40).unwrap();
        assert_eq!(filtered.stored_levels_count(), 3);
        assert_eq!(filtered.current_level(), 2);
        let stats = filtered.memory_stats();
        assert!(stats.is_clean());
    }

    #[test]
    fn test_complex_navigation_scenario() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);
        
        struct DropCounter {
            _id: usize,
        }
        
        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }
        
        DROP_COUNT.store(0, Ordering::SeqCst);
        
        let data: Vec<DropCounter> = (0..1000)
            .map(|i| DropCounter { _id: i })
            .collect();
        let filtered = data.into_filtered();
        filtered.filter(|d| d._id > 200).unwrap();
        filtered.filter(|d| d._id > 400).unwrap();
        filtered.filter(|d| d._id > 600).unwrap();
        filtered.up();
        filtered.up();
        assert_eq!(filtered.stored_levels_count(), 2);
        filtered.filter(|d| d._id < 500).unwrap();
        filtered.filter(|d| d._id % 2 == 0).unwrap();
        assert_eq!(filtered.stored_levels_count(), 4);
        filtered.reset_to_source();
        assert_eq!(filtered.stored_levels_count(), 1);
        let stats = filtered.memory_stats();
        assert!(stats.is_clean());
        assert_eq!(stats.wasted_items, 0);
    }

    #[test]
    fn test_efficiency_metric() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        let stats = filtered.memory_stats();
        println!("Initial: {:?}", stats);
        assert_eq!(stats.efficiency(), 1.0);
        assert!(stats.is_clean());
        filtered.filter(|x| *x > 500).unwrap();
        let stats = filtered.memory_stats();
        println!("After filter 1: {:?}", stats);
        assert_eq!(stats.efficiency(), 1.0);
        assert!(stats.is_clean());
        filtered.filter(|x| *x > 800).unwrap();
        let stats = filtered.memory_stats();
        println!("After filter 2: {:?}", stats);
        assert_eq!(stats.efficiency(), 1.0);
        assert!(stats.is_clean());
    }

    #[test]
    fn test_efficiency_with_navigation() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        filtered.filter(|x| *x > 200).unwrap();
        filtered.filter(|x| *x > 500).unwrap();
        filtered.filter(|x| *x > 800).unwrap();
        let stats = filtered.memory_stats();
        println!("At level 3: {:?}", stats);
        assert_eq!(stats.current_level, 3);
        assert_eq!(stats.efficiency(), 1.0);
        assert!(stats.is_clean());
        filtered.up();
        filtered.up();
        let stats = filtered.memory_stats();
        println!("After goto level 1: {:?}", stats);
        assert_eq!(stats.current_level, 1);
        assert_eq!(stats.stored_levels, 2);
        assert_eq!(stats.efficiency(), 1.0);
        assert!(stats.is_clean());
    }

    #[test]
    fn test_success_vs_wasted() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        filtered.filter(|x| *x > 500).unwrap();
        filtered.filter(|x| *x > 800).unwrap();
        let stats = filtered.memory_stats();
        assert_eq!(stats.useful_items, stats.total_stored_items);
        assert_eq!(stats.wasted_items, 0);
        assert_eq!(stats.efficiency(), 1.0);
    }

    #[test]
    fn test_current_level_ratio() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        let stats = filtered.memory_stats();
        assert_eq!(stats.current_level_ratio(), 1.0);
        filtered.filter(|x| *x > 500).unwrap();
        let stats = filtered.memory_stats();
        println!("Current level ratio: {:.2}", stats.current_level_ratio());
        assert!(stats.current_level_ratio() > 0.3);
        assert!(stats.current_level_ratio() < 0.4);
        assert_eq!(stats.efficiency(), 1.0);
    }

    #[test]
    fn test_no_memory_leak_with_cleanup() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);
        
        struct LargeData {
            _data: Vec<u8>,
            _id: usize,
        }
        
        impl Drop for LargeData {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }
        
        DROP_COUNT.store(0, Ordering::SeqCst);
        
        {
            let data: Vec<LargeData> = (0..500)
                .map(|i| LargeData {
                    _data: vec![0u8; 1024],
                    _id: i,
                })
                .collect();
            let filtered = data.into_filtered();
            for _ in 0..10 {
                filtered.filter(|_| true).unwrap();
            }
            filtered.reset_to_source();
            filtered.filter(|_| true).unwrap();
            filtered.up();
            let stats = filtered.memory_stats();
            assert!(stats.is_clean());
        }
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 500);
    }

    #[test]
    fn test_concurrent_cleanup() {
        let data: Vec<i32> = (0..10000).collect();
        let filtered = Arc::new(data.into_filtered());
        let mut handles = vec![];
        for t in 0..4 {
            let f = Arc::clone(&filtered);
            let handle = thread::spawn(move || {
                for i in 0..100 {
                    if i % 2 == 0 {
                        let _ = f.filter(|x| *x > (t * 1000 + i));
                    } else {
                        f.reset_to_source();
                    }
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let stats = filtered.memory_stats();
        println!("Concurrent cleanup stats: {:?}", stats);
        assert!(stats.wasted_items < 100);
    }

    #[test]
    fn allocation_overhead() {
        let data: Vec<i32> = (0..100_000).collect();
        let filtered = Arc::new(data.into_filtered());
        println!("=== Benchmarking allocations ===");
        let start = Instant::now();
        for i in 0..100 {
            let _ = filtered.filter(|x| *x > i * 100);
        }
        let duration = start.elapsed();
        println!("100 sequential filters: {:?}", duration);
        println!("Avg per filter: {:?}", duration / 100);
        filtered.reset_to_source();
        let start = Instant::now();
        let mut handles = vec![];
        for _ in 0..8 {
            let f = Arc::clone(&filtered);
            let handle = thread::spawn(move || {
                for _ in 0..10000 {
                    let _ = f.len();
                    let _ = f.memory_stats();
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let duration = start.elapsed();
        println!("80k parallel reads: {:?}", duration);
        println!("Avg per read: ~{} ns", duration.as_nanos() / 80000);
    }

    #[test]
    fn test_cleanup_functionality() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        filtered.filter(|x| *x > 200).unwrap();
        filtered.filter(|x| *x > 500).unwrap();
        filtered.filter(|x| *x > 800).unwrap();
        assert_eq!(filtered.stored_levels_count(), 4);
        filtered.up();
        filtered.up();
        assert_eq!(filtered.stored_levels_count(), 2);
        let stats = filtered.memory_stats();
        assert!(stats.is_clean());
        assert_eq!(stats.efficiency(), 1.0);
    }

    #[test]
    fn test_concurrent_operations() {
        let data: Vec<i32> = (0..10000).collect();
        let filtered = Arc::new(data.into_filtered());
        let mut handles = vec![];
        for t in 0..4 {
            let f = Arc::clone(&filtered);
            let handle = thread::spawn(move || {
                for i in 0..100 {
                    if i % 2 == 0 {
                        let _ = f.filter(|x| *x > (t * 1000 + i));
                    } else {
                        f.reset_to_source();
                    }
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let stats = filtered.memory_stats();
        println!("Final stats: {:?}", stats);
        assert!(stats.is_clean());
    }

    #[test]
    fn test_filter_error_handling() {
        let data = vec![1, 2, 3, 4, 5];
        let filtered = data.into_filtered();
        let result = filtered.filter(|x| *x > 100);
        assert!(result.is_err());
        match result {
            Err(e) => {
                println!("Got expected error: {:?}", e);
            }
            Ok(_) => panic!("Should have returned error"),
        }
        assert_eq!(filtered.len(), 5);
        assert_eq!(filtered.current_level(), 0);
    }

    #[test]
    fn test_chainable_with_errors() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let filtered = data.into_filtered();
        let result = filtered
            .filter(|x| *x > 3)
            .and_then(|f| f.filter(|x| *x < 8))
            .and_then(|f| f.filter(|x| *x % 2 == 0));
        assert!(result.is_ok());
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_field_index_boolean_basic() {
        let data: Vec<i32> = (0..100).collect();
        let filtered = data.into_filtered();
        assert!(filtered.create_field_index("even", |x| *x % 2 == 0).is_ok());
        assert!(filtered.create_field_index("gt_50", |x| *x > 50).is_ok());
        assert!(filtered.create_field_index("divisible_by_10", |x| *x % 10 == 0).is_ok());
        assert!(filtered.has_index("even"));
        assert!(filtered.has_index("gt_50"));
        assert!(filtered.has_index("divisible_by_10"));
        let result = filtered.filter_by_field_ops("even", &[
            (FieldOperation::eq(true), Op::And)
        ]).unwrap();
        assert_eq!(result.len(), 50); // 0, 2, 4, ..., 98
        filtered.reset_to_source();
        
        let result = filtered.filter_by_field_ops("gt_50", &[
            (FieldOperation::eq(true), Op::And)
        ]).unwrap();
        assert_eq!(result.len(), 49); // 51-99
        filtered.reset_to_source();
        
        let result = filtered.filter_by_field_ops("divisible_by_10", &[
            (FieldOperation::eq(true), Op::And)
        ]).unwrap();
        assert_eq!(result.len(), 10); // 0, 10, 20, ..., 90
    }

    #[test]
    fn test_field_index_multiple_operations() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        filtered.create_field_index("even", |x| *x % 2 == 0).unwrap();
        filtered.create_field_index("value", |x| *x as u64).unwrap();
        filtered.create_field_index("divisible_by_5", |x| *x % 5 == 0).unwrap();
        let result = filtered.filter_by_field_ops("even", &[
            (FieldOperation::eq(true), Op::And),
        ]).unwrap();
        result.filter_by_field_ops("value", &[
            (FieldOperation::gt(500), Op::And),
            (FieldOperation::lt(800), Op::And),
        ]).unwrap();
        
        // Должны быть: 502, 504, 506, ..., 798
        assert_eq!(result.len(), 149);
        filtered.reset_to_source();
        
        let result = filtered.filter_by_field_ops("value", &[
            (FieldOperation::gt(500), Op::And),
            (FieldOperation::lt(800), Op::And),
        ]).unwrap();
        result.filter_by_field_ops("divisible_by_5", &[
            (FieldOperation::eq(false), Op::And), // NOT divisible by 5
        ]).unwrap();
        
        // Диапазон 501-799: 299 чисел
        // Из них делятся на 5: 505, 510, ..., 795 = 59 чисел
        // Результат: 299 - 59 = 240
        assert_eq!(result.len(), 240);
    }

    #[test]
    fn test_field_index_chainable() {
        use ordered_float::OrderedFloat;
        
        #[derive(Clone)]
        #[allow(dead_code)]
        struct Product {
            id: u32,
            price: f64,
            in_stock: bool,
            is_featured: bool,
            is_on_sale: bool,
        }
        
        let products: Vec<Product> = (0..1000)
            .map(|i| Product {
                id: i,
                price: (i as f64) * 10.0,
                in_stock: i % 3 != 0,      // ~667 items
                is_featured: i % 5 == 0,   // 200 items
                is_on_sale: i % 7 == 0,    // ~143 items
            })
            .collect();
        let filtered = products.into_filtered();
        
        // Создаем field индексы
        filtered.create_field_index("in_stock", |p| p.in_stock).unwrap();
        filtered.create_field_index("featured", |p| p.is_featured).unwrap();
        filtered.create_field_index("is_on_sale", |p| p.is_on_sale).unwrap();
        filtered.create_field_index("price", |p| OrderedFloat(p.price)).unwrap();
        
        // Сценарий 1: Цена в диапазоне + на распродаже
        let result = filtered
            .filter_by_field_ops("price", &[
                (FieldOperation::gte(1000.0), Op::And),
                (FieldOperation::lt(5000.0), Op::And),
            ])
            .and_then(|f| f.filter_by_field_ops("is_on_sale", &[
                (FieldOperation::eq(true), Op::And)
            ]))
            .unwrap();
        println!("Scenario 1 (price 1000-5000 + is_on_sale): {} items", result.len());
        assert!(result.len() > 0);
        for item in result.items().iter() {
            assert!(item.price >= 1000.0 && item.price < 5000.0);
            assert!(item.is_on_sale);
        }
        filtered.reset_to_source();
        
        // Сценарий 2: В наличии + премиум + дорогие
        let result = filtered
            .filter_by_field_ops("in_stock", &[
                (FieldOperation::eq(true), Op::And)
            ])
            .and_then(|f| f.filter_by_field_ops("featured", &[
                (FieldOperation::eq(true), Op::And)
            ]))
            .and_then(|f| f.filter_by_field_ops("price", &[
                (FieldOperation::gte(2000.0), Op::And)
            ]))
            .unwrap();
        println!("Scenario 2 (in_stock + featured + price >= 2000): {} items", result.len());
        assert!(result.len() > 0);
        for item in result.items().iter() {
            assert!(item.in_stock);
            assert!(item.is_featured);
            assert!(item.price >= 2000.0);
        }
        filtered.reset_to_source();
    
        // Сценарий 3: Бюджетные товары на распродаже в наличии
        let result = filtered
            .filter_by_field_ops("price", &[
                (FieldOperation::lt(1000.0), Op::And)
            ])
            .and_then(|f| f.filter_by_field_ops("is_on_sale", &[
                (FieldOperation::eq(true), Op::And)
            ]))
            .and_then(|f| f.filter_by_field_ops("in_stock", &[
                (FieldOperation::eq(true), Op::And)
            ]))
            .unwrap();
        println!("Scenario 3 (price < 1000 + is_on_sale + in_stock): {} items", result.len());
        assert!(result.len() > 0);
        for item in result.items().iter() {
            assert!(item.price < 1000.0);
            assert!(item.is_on_sale);
            assert!(item.in_stock);
        }
    }

    #[test]
    fn test_field_index_performance() {
        let data: Vec<i32> = (0..100_000).collect();
        let filtered = data.into_filtered();
        
        // Создаем field индексы
        filtered.create_field_index("even", |x| *x % 2 == 0).unwrap();
        filtered.create_field_index("mod3", |x| *x % 3 == 0).unwrap();
        filtered.create_field_index("mod5", |x| *x % 5 == 0).unwrap();
        filtered.create_field_index("value", |x| *x as u64).unwrap();
        println!("=== Field Index Performance ===");
        // Тест 1: Простая фильтрация
        let start = Instant::now();
        filtered.filter_by_field_ops("even", &[
            (FieldOperation::eq(true), Op::And)
        ]).unwrap();
        filtered.reset_to_source();
        println!("simple filters: {:?}", start.elapsed());

        // Тест 2: Комплексные операции
        let start = Instant::now();
        let result = filtered.filter_by_field_ops("even", &[
            (FieldOperation::eq(true), Op::And),
        ]).unwrap();
        result.filter_by_field_ops("mod3", &[
            (FieldOperation::eq(true), Op::And),
        ]).unwrap();
        result.filter_by_field_ops("value", &[
            (FieldOperation::gt(50000), Op::And),
        ]).unwrap();
        filtered.reset_to_source();
        println!("complex operations: {:?}", start.elapsed());

        // Тест 3: Создание новых FilterData для параллельной работы
        println!("Parallel test with independent FilterData:");
        let start = Instant::now();
        let handles: Vec<_> = (0..4)
            .map(|_| {
                thread::spawn(|| {
                    let thread_data: Vec<i32> = (0..100_000).collect();
                    let thread_filtered = thread_data.into_filtered();
                    thread_filtered.create_field_index("even", |x| *x % 2 == 0).unwrap();
                    for _ in 0..100 {
                        let _ = thread_filtered.filter_by_field_ops("even", &[
                            (FieldOperation::eq(true), Op::And)
                        ]);
                        thread_filtered.reset_to_source();
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        println!("100 parallel operations (4 threads): {:?}", start.elapsed());
    }

    #[test]
    fn test_field_index_error_handling() {
        let data: Vec<i32> = (0..100).collect();
        let filtered = data.into_filtered();
        
        // Создаем field индекс
        filtered.create_field_index("even", |x| *x % 2 == 0).unwrap();
        // Ошибка: индекс не существует
        let result = filtered.filter_by_field_ops("nonexistent", &[
            (FieldOperation::eq(true), Op::And)
        ]);
        assert!(result.is_err());
        // Создаем индекс который не найдет ничего
        filtered.create_field_index("impossible", |x| *x > 1000).unwrap();
        let result = filtered.filter_by_field_ops("impossible", &[
            (FieldOperation::eq(true), Op::And)
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_field_index_with_string_index() {
        #[derive(Clone)]
        #[allow(dead_code)]
        struct Log {
            id: u32,
            level: String,
            has_error: bool,
            is_critical: bool,
            response_time_ms: u32,
        }
        
        let logs: Vec<Log> = (0..10_000)
            .map(|i| Log {
                id: i,
                level: match i % 4 {
                    0 => "INFO".to_string(),
                    1 => "WARN".to_string(),
                    2 => "ERROR".to_string(),
                    _ => "DEBUG".to_string(),
                },
                has_error: i % 10 == 0,
                is_critical: i % 50 == 0,
                response_time_ms: (i % 1000) as u32,
            })
            .collect();
        
        let filtered = logs.into_filtered();
        filtered.create_field_index("level", |l| l.level.clone()).unwrap();
        filtered.create_field_index("has_error", |l| l.has_error).unwrap();
        filtered.create_field_index("is_critical", |l| l.is_critical).unwrap();
        let result = filtered
            .filter_by_field_ops("level", &[
                (FieldOperation::eq("ERROR".to_string()), Op::And)
            ])
            .and_then(|f| f.filter_by_field_ops("has_error", &[
                (FieldOperation::eq(true), Op::And)
            ]))
            .unwrap();
        println!("Mixed filter result: {} items", result.len());
        // Проверяем результат
        for item in result.items().iter() {
            assert_eq!(item.level, "ERROR");
            assert!(item.has_error);
        }
        filtered.reset_to_source();
        
        // Фильтрация: (WARN OR ERROR) AND is_critical
        let result = filtered
            .filter(|l| l.level == "WARN" || l.level == "ERROR")
            .and_then(|f| f.filter_by_field_ops("is_critical", &[
                (FieldOperation::eq(true), Op::And)
            ]))
            .unwrap();
        println!("WARN/ERROR with critical: {} items", result.len());
        assert!(result.len() > 0);
    }

    #[test]
    fn test_field_index_memory_efficiency() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);
        
        #[allow(dead_code)]
        struct Counter {
            id: usize,
            flag1: bool,
            flag2: bool,
            flag3: bool,
        }
        
        impl Drop for Counter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }
        
        DROP_COUNT.store(0, Ordering::SeqCst);
        
        {
            let data: Vec<Counter> = (0..10_000)
                .map(|i| Counter {
                    id: i,
                    flag1: i % 2 == 0,
                    flag2: i % 3 == 0,
                    flag3: i % 5 == 0,
                })
                .collect();
            
            let filtered = data.into_filtered();
            
            // Создаем field индексы для boolean
            filtered.create_field_index("flag1", |c| c.flag1).unwrap();
            filtered.create_field_index("flag2", |c| c.flag2).unwrap();
            filtered.create_field_index("flag3", |c| c.flag3).unwrap();
            // Множество операций
            for _ in 0..100 {
                let _ = filtered.filter_by_field_ops("flag1", &[
                    (FieldOperation::eq(true), Op::And),
                ]).and_then(|f| f.filter_by_field_ops("flag2", &[
                    (FieldOperation::eq(true), Op::And),
                ]));
                filtered.reset_to_source();
            }
            // Проверяем что индексы не создают утечек
            let indexes = filtered.list_indexes();
            assert_eq!(indexes.len(), 3);
        }
        // Все объекты должны быть удалены
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 10_000);
    }

    #[test]
    fn test_field_index_in_values() {
        let data: Vec<i32> = (0..100).collect();
        let filtered = data.into_filtered();
        filtered.create_field_index("value", |x| *x as u64).unwrap();
        
        // IN operation через field index
        let result = filtered.filter_by_field_ops("value", &[
            (FieldOperation::in_values(vec![10, 20, 30, 40, 50]), Op::And),
        ]).unwrap();
        assert_eq!(result.len(), 5);
        let items = result.items();
        let values: Vec<i32> = items.iter().map(|n| **n).collect();
        assert_eq!(values, vec![10, 20, 30, 40, 50]);
    }

    #[test]
    fn test_field_index_not_in_values() {
        let data: Vec<i32> = (0..100).collect();
        let filtered = data.into_filtered();
        filtered.create_field_index("value", |x| *x as u64).unwrap();
        // NOT IN operation через field index
        let result = filtered.filter_by_field_ops("value", &[
            (FieldOperation::not_in_values(vec![10, 20, 30, 40, 50]), Op::And),
        ]).unwrap();
        assert_eq!(result.len(), 95); // 100 - 5
        let items = result.items();
        let excluded = [10, 20, 30, 40, 50];
        assert!(items.iter().all(|n| !excluded.contains(&**n)));
    }
}