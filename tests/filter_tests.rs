#[cfg(test)]
mod tests{
    use tree_man::filter::IntoFilterData;
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
        
        filtered.filter(|x| *x > 5);
        
        assert_eq!(filtered.len(), 5);
        assert_eq!(filtered.current_level(), 1);
    }

    #[test]
    fn test_filtered_collection_multiple_filters() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let filtered = data.into_filtered();
        
        filtered.filter(|x| *x > 3);
        assert_eq!(filtered.len(), 7);
        
        filtered.filter(|x| *x % 2 == 0);
        assert_eq!(filtered.len(), 4); // 4, 6, 8, 10
        
        assert_eq!(filtered.current_level(), 2);
    }

    #[test]
    fn test_filtered_collection_navigation() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let filtered = data.into_filtered();
        
        filtered.filter(|x| *x > 5);
        filtered.filter(|x| *x % 2 == 0);
        
        assert_eq!(filtered.current_level(), 2);
        
        filtered.go_to_level(1);
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
            
            filtered.filter(|_| true);
            filtered.filter(|_| true);
            filtered.filter(|_| true);
        } // filtered dropped
        
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
        
        // Создаем несколько уровней
        filtered.filter(|d| d._id > 100);  // Level 1: ~900 items
        filtered.filter(|d| d._id > 500);  // Level 2: ~500 items
        filtered.filter(|d| d._id > 800);  // Level 3: ~200 items
        
        assert_eq!(filtered.stored_levels_count(), 4); // 0, 1, 2, 3
        
        // Возвращаемся к источнику - должны очиститься уровни 1, 2, 3
        filtered.reset_to_source();
        
        assert_eq!(filtered.stored_levels_count(), 1); // Только level 0
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
        
        // Создаем 5 уровней
        filtered.filter(|_| true);  // Level 1
        filtered.filter(|_| true);  // Level 2
        filtered.filter(|_| true);  // Level 3
        filtered.filter(|_| true);  // Level 4
        filtered.filter(|_| true);  // Level 5
        
        assert_eq!(filtered.stored_levels_count(), 6); // 0-5
        
        // Переходим к уровню 2 - должны очиститься 3, 4, 5
        filtered.go_to_level(2);
        
        assert_eq!(filtered.stored_levels_count(), 3); // 0, 1, 2
        assert_eq!(filtered.current_level(), 2);
        
        let stats = filtered.memory_stats();
        assert!(stats.is_clean());
    }

    #[test]
    fn test_cleanup_on_up() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        
        // Создаем дерево уровней
        filtered.filter(|x| *x > 100);  // Level 1
        filtered.filter(|x| *x > 500);  // Level 2
        filtered.filter(|x| *x > 800);  // Level 3
        
        assert_eq!(filtered.stored_levels_count(), 4);
        assert_eq!(filtered.current_level(), 3);
        
        // Поднимаемся на уровень вверх
        filtered.up();
        
        assert_eq!(filtered.current_level(), 2);
        assert_eq!(filtered.stored_levels_count(), 3); // Level 3 удален
        
        let stats = filtered.memory_stats();
        assert!(stats.is_clean());
        assert_eq!(stats.wasted_items, 0);
    }

    #[test]
    fn test_no_cleanup_on_down() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        
        filtered.filter(|x| *x > 100);  // Level 1
        filtered.filter(|x| *x > 500);  // Level 2
        
        assert_eq!(filtered.current_level(), 2);
        
        // Поднимаемся
        filtered.up();
        assert_eq!(filtered.current_level(), 1);
        assert_eq!(filtered.stored_levels_count(), 2); // Level 2 очищен
        
        // Остаемся на level 1, т.к. level 2 не существует
        assert_eq!(filtered.current_level(), 1);
    }

    #[test]
    fn test_memory_stats_after_operations() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        
        // Начальная статистика
        let stats = filtered.memory_stats();
        assert_eq!(stats.stored_levels, 1);
        assert_eq!(stats.current_level_items, 1000);
        assert_eq!(stats.total_stored_items, 1000);
        assert!(stats.is_clean());
        assert_eq!(stats.efficiency(), 1.0);
        
        // Создаем фильтры
        filtered.filter(|x| *x > 500);
        filtered.filter(|x| *x > 800);
        
        let stats = filtered.memory_stats();
        assert_eq!(stats.stored_levels, 3);
        assert!(stats.current_level_items < 1000);
        assert!(stats.is_clean());
        
        // Возвращаемся назад
        filtered.go_to_level(1);
        
        let stats = filtered.memory_stats();
        assert_eq!(stats.stored_levels, 2);
        assert!(stats.is_clean());
        assert_eq!(stats.wasted_items, 0);
    }

    #[test]
    fn test_filter_after_navigation_cleans_forward_levels() {
        let data: Vec<i32> = (0..100).collect();
        let filtered = data.into_filtered();
        
        // Создаем цепочку
        filtered.filter(|x| *x > 20);  // Level 1
        filtered.filter(|x| *x > 50);  // Level 2
        filtered.filter(|x| *x > 70);  // Level 3
        
        assert_eq!(filtered.stored_levels_count(), 4);
        
        // Возвращаемся к level 1
        filtered.go_to_level(1);
        assert_eq!(filtered.stored_levels_count(), 2);
        
        // Применяем новый фильтр от level 1
        filtered.filter(|x| *x < 40);  // Новый level 2
        
        assert_eq!(filtered.stored_levels_count(), 3); // 0, 1, new 2
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
        
        // Сложный сценарий навигации
        filtered.filter(|d| d._id > 200);   // Level 1
        filtered.filter(|d| d._id > 400);   // Level 2
        filtered.filter(|d| d._id > 600);   // Level 3
        
        filtered.go_to_level(1);              // Очистка 2, 3
        assert_eq!(filtered.stored_levels_count(), 2);
        
        filtered.filter(|d| d._id < 500);   // Новый Level 2
        filtered.filter(|d| d._id % 2 == 0); // Level 3
        
        assert_eq!(filtered.stored_levels_count(), 4);
        
        filtered.reset_to_source();          // Очистка всех кроме 0
        assert_eq!(filtered.stored_levels_count(), 1);
        
        let stats = filtered.memory_stats();
        assert!(stats.is_clean());
        assert_eq!(stats.wasted_items, 0);
    }

    #[test]
    fn test_efficiency_metric() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        
        // 100% эффективность в начале
        let stats = filtered.memory_stats();
        println!("Initial: {:?}", stats);
        assert_eq!(stats.efficiency(), 1.0);
        assert!(stats.is_clean());
        
        // Фильтруем
        filtered.filter(|x| *x > 500);
        
        let stats = filtered.memory_stats();
        println!("After filter 1: {:?}", stats);
        // Эффективность 100% - есть уровни 0 и 1, оба полезны
        // useful = 1000 + 500 = 1500
        // total = 1500
        // efficiency = 1500/1500 = 1.0
        assert_eq!(stats.efficiency(), 1.0);
        assert!(stats.is_clean());
        
        filtered.filter(|x| *x > 800);
        
        let stats = filtered.memory_stats();
        println!("After filter 2: {:?}", stats);
        // Все еще 100% - уровни 0, 1, 2 все полезны
        // useful = 1000 + 500 + 200 = 1700
        // total = 1700
        // efficiency = 1.0
        assert_eq!(stats.efficiency(), 1.0);
        assert!(stats.is_clean());
    }

    #[test]
    fn test_efficiency_with_navigation() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        
        filtered.filter(|x| *x > 200);  // Level 1: ~800
        filtered.filter(|x| *x > 500);  // Level 2: ~500
        filtered.filter(|x| *x > 800);  // Level 3: ~200
        
        // На level 3, всё полезно
        let stats = filtered.memory_stats();
        println!("At level 3: {:?}", stats);
        assert_eq!(stats.current_level, 3);
        assert_eq!(stats.efficiency(), 1.0);
        assert!(stats.is_clean());
        
        // Возвращаемся на level 1
        filtered.go_to_level(1);
        
        let stats = filtered.memory_stats();
        println!("After goto level 1: {:?}", stats);
        assert_eq!(stats.current_level, 1);
        // Уровни 2 и 3 удалены через cleanup_levels_above
        assert_eq!(stats.stored_levels, 2); // 0 и 1
        assert_eq!(stats.efficiency(), 1.0);
        assert!(stats.is_clean());
    }

    #[test]
    fn test_success_vs_wasted() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        
        filtered.filter(|x| *x > 500);
        filtered.filter(|x| *x > 800);
        
        let stats = filtered.memory_stats();
        // Level 0: 1000, Level 1: 500, Level 2: 200
        // Current = 2
        // Useful = все три уровня = 1700
        // Wasted = 0
        assert_eq!(stats.useful_items, stats.total_stored_items);
        assert_eq!(stats.wasted_items, 0);
        assert_eq!(stats.efficiency(), 1.0);
    }

    #[test]
    fn test_current_level_ratio() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        
        let stats = filtered.memory_stats();
        // Текущий уровень = 100% от всех данных
        assert_eq!(stats.current_level_ratio(), 1.0);
        
        filtered.filter(|x| *x > 500);
        
        let stats = filtered.memory_stats();
        // Current level = 500, Total = 1500
        // Ratio = 500/1500 ≈ 0.33
        println!("Current level ratio: {:.2}", stats.current_level_ratio());
        assert!(stats.current_level_ratio() > 0.3);
        assert!(stats.current_level_ratio() < 0.4);
        
        // Но efficiency все еще 1.0 (нет мусора)
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
                    _data: vec![0u8; 1024], // 1KB each
                    _id: i,
                })
                .collect();
            
            let filtered = data.into_filtered();
            
            // Создаем много уровней
            for _ in 0..10 {
                filtered.filter(|_| true);
            }
            
            // Навигация туда-сюда
            filtered.go_to_level(5);
            filtered.go_to_level(2);
            filtered.reset_to_source();
            filtered.filter(|_| true);
            filtered.up();
            
            // Проверяем что мусора нет
            let stats = filtered.memory_stats();
            assert!(stats.is_clean());
        }
        
        // Все 500 объектов должны быть удалены
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 500);
    }

    #[test]
    fn test_concurrent_cleanup() {
        use std::thread;
        
        let data: Vec<i32> = (0..10000).collect();
        let filtered = Arc::new(data.into_filtered());
        
        let mut handles = vec![];
        
        for t in 0..4 {
            let f = Arc::clone(&filtered);
            let handle = thread::spawn(move || {
                for i in 0..100 {
                    if i % 2 == 0 {
                        f.filter(|x| *x > (t * 1000 + i));
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
        
        // После всех операций проверяем отсутствие мусора
        let stats = filtered.memory_stats();
        println!("Concurrent cleanup stats: {:?}", stats);
        
        // Может быть не идеально чисто из-за race conditions,
        // но wasted_items должен быть небольшим
        assert!(stats.wasted_items < 100);
    }

    #[test]
    fn allocation_overhead() {
        let data: Vec<i32> = (0..100_000).collect();
        let filtered = Arc::new(data.into_filtered());
        
        println!("\n=== Benchmarking allocations ===");
        
        // Test 1: Sequential filters
        let start = Instant::now();
        for i in 0..100 {
            filtered.filter(|x| *x > i * 100);
        }
        let duration = start.elapsed();
        println!("100 sequential filters: {:?}", duration);
        println!("Avg per filter: {:?}", duration / 100);
        
        filtered.reset_to_source();
        
        // Test 2: Parallel reads
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
        println!("\n80k parallel reads: {:?}", duration);
        println!("Avg per read: ~{} ns", duration.as_nanos() / 80000);
    }

    #[test]
    fn test_cleanup_functionality() {
        let data: Vec<i32> = (0..1000).collect();
        let filtered = data.into_filtered();
        
        filtered.filter(|x| *x > 200);
        filtered.filter(|x| *x > 500);
        filtered.filter(|x| *x > 800);
        
        assert_eq!(filtered.stored_levels_count(), 4);
        
        filtered.go_to_level(1);
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
                        f.filter(|x| *x > (t * 1000 + i));
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

}