#[cfg(test)]
mod memory_leak_tests{
    use memory_stats::memory_stats;
    use tree_man::{
        group::GroupData,
        filter::FilterData,
        Op, FieldOperation,
    };
    use serial_test::serial;
    use std::{
        sync::{Arc, atomic::{AtomicUsize, Ordering}},
        thread,
        time,
    };
    
    static DROP_COUNTER: AtomicUsize = AtomicUsize::new(0);
    static CREATE_COUNTER: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug,Clone)]
    struct Product {
        category: String,
        brand: String,
        _price: f64,
    }

    fn create_test_products(count: usize) -> Vec<Product> {
        (0..count).map(|i| Product {
            category: ["Phones", "Laptops", "Tablets"][i % 3].to_string(),
            brand: ["Apple", "Samsung", "Dell", "Lenovo"][i % 4].to_string(),
            _price: 500.0 + (i as f64) * 10.0,
        }).collect()
    }

    #[derive(Debug,Clone)]
    struct TrackedProduct {
        category: String,
        brand: String,
        price: f64,
        _id: usize,
    }

    impl TrackedProduct {
        fn new(category: String, brand: String, price: f64) -> Self {
            let id = CREATE_COUNTER.fetch_add(1, Ordering::SeqCst);
            Self {
                category,
                brand,
                price,
                _id: id + 1,
            }
        }
    }

    impl Drop for TrackedProduct {
        fn drop(&mut self) {
            DROP_COUNTER.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn create_tracked_products(count: usize) -> Vec<TrackedProduct> {
        (0..count).map(|i| TrackedProduct::new(
            ["Phones", "Laptops", "Tablets"][i % 3].to_string(),
            ["Apple", "Samsung"][i % 2].to_string(),
            500.0 + (i as f64) * 10.0,
        )).collect()
    }

    #[test]
    fn test_no_circular_references() {
        let items: Vec<i32> = (0..1000).collect();
        let root = Arc::new(GroupData::new_root(
            "Root".to_string(),
            items,
            "Root"
        ));
        root.group_by(|n| (n % 5).to_string(), "By Mod 5").unwrap();
        let initial_count = Arc::strong_count(&root);
        println!("Initial Arc count: {}", initial_count);
        let children = root.get_all_subgroups();
        println!("Children count: {}", children.len());
        drop(children);
        let after_drop = Arc::strong_count(&root);
        println!("After drop count: {}", after_drop);
        assert_eq!(initial_count, after_drop, "Arc references leaked!");
    }

    #[test]
    fn test_index_weak_references() {
        let items: Vec<String> = (0..1000)
            .map(|i| format!("item_{}", i))
            .collect();
        let data = FilterData::from_vec(items);
        
        // ✅ Используем field_index
        data.create_field_index("test", |s| s.len()).unwrap();
        
        assert!(data.validate_indexes());
        drop(data);
    }

    #[test]
    #[serial]
    fn test_many_operations_no_leak() {
        println!("== Many Operations No Leak (macOS) ==");
        let start_mem = memory_stats().map(|m| m.physical_mem);
        
        for i in 0..100 {
            let items: Vec<i32> = (0..10_000).collect();
            let data = FilterData::from_vec(items);
            // Используем field_index для boolean
            data.create_field_index("even", |&n| n % 2 == 0).unwrap();
            // Фильтрация через field operations
            let _ = data.filter_by_field_ops("even", &[
                (FieldOperation::eq(true), Op::And)
            ]).unwrap();
            
            let _ = data.filter(|&n| n > 5000).unwrap();
            data.reset_to_source();
            data.clear_all_indexes();

            if i % 20 == 0 {
                thread::sleep(time::Duration::from_millis(100));
            }
        }
        
        thread::sleep(time::Duration::from_secs(3));
        let end_mem = memory_stats().map(|m| m.physical_mem);
        if let (Some(start), Some(end)) = (start_mem, end_mem) {
            let diff = end.saturating_sub(start);
            println!("Memory growth: {} MB", diff / 1024 / 1024);
            assert!(diff < 50_000_000, 
                    "Memory leak detected! Growth: {} MB (expected <100 MB)", 
                    diff / 1024 / 1024);
        }
        println!("No memory leak");
    }

    #[test]
    fn test_drop_order() {
        struct DropChecker {
            id: i32,
        }

        impl Drop for DropChecker {
            fn drop(&mut self) {
                println!("Dropping {}", self.id);
            }
        }
        
        let items: Vec<DropChecker> = (0..10)
            .map(|id| DropChecker { id })
            .collect();
        let data = FilterData::from_vec(items);
        println!("Created FilterData");
        drop(data);
        println!("Dropped FilterData");
    }

    #[test]
    #[serial]
    fn test_no_memory_leak_simple() {
        println!("== No Memory Leak Simple ==");
        
        DROP_COUNTER.store(0, Ordering::SeqCst);
        CREATE_COUNTER.store(0, Ordering::SeqCst);
        {
            let products = create_tracked_products(10);
            let root: Arc<GroupData<String, TrackedProduct>> = 
                GroupData::new_root("Root".to_string(), products, "All");
            assert_eq!(root.data.len(), 10);
        }
        let created = CREATE_COUNTER.load(Ordering::SeqCst);
        let dropped = DROP_COUNTER.load(Ordering::SeqCst);
        println!("Created: {}, Dropped: {}", created, dropped);
        assert_eq!(created, dropped, "Memory leak detected!");
        println!("No memory leak!");
    }

    #[test]
    #[serial]
    fn test_no_memory_leak_with_grouping() {
        println!("== No Memory Leak With Grouping ==");
        DROP_COUNTER.store(0, Ordering::SeqCst);
        CREATE_COUNTER.store(0, Ordering::SeqCst);
        
        {
            let products = create_tracked_products(10);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            root.group_by(|p| p.category.clone(), "Categories").unwrap();
            assert_eq!(root.subgroups_count(), 3);
        }
        let created = CREATE_COUNTER.load(Ordering::SeqCst);
        let dropped = DROP_COUNTER.load(Ordering::SeqCst);
        println!("Created: {}, Dropped: {}", created, dropped);
        assert_eq!(created, dropped, "Memory leak detected!");
        println!("No memory leak!");
    }

    #[test]
    #[serial]
    fn test_no_memory_leak_deep_hierarchy() {
        println!("== No Memory Leak Deep Hierarchy ==");
        DROP_COUNTER.store(0, Ordering::SeqCst);
        CREATE_COUNTER.store(0, Ordering::SeqCst);
        
        {
            let products = create_tracked_products(20);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            root.group_by(|p| p.category.clone(), "Categories").unwrap();
            let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
            phones.group_by(|p| p.brand.clone(), "Brands").unwrap();
            let keys = phones.subgroups_keys();
            if !keys.is_empty() {
                let brand = phones.get_subgroup(&keys[0]).unwrap();
                brand.group_by(|p| {
                    if p.price > 600.0 { "Expensive".to_string() }
                    else { "Cheap".to_string() }
                }, "Price").unwrap();
            }
        }
        let created = CREATE_COUNTER.load(Ordering::SeqCst);
        let dropped = DROP_COUNTER.load(Ordering::SeqCst);
        println!("Created: {}, Dropped: {}", created, dropped);
        assert_eq!(created, dropped, "Memory leak detected!");
        println!("No memory leak!");
    }

    #[test]
    #[serial]
    fn test_parent_navigation_cleanup() {
        println!("== Parent Navigation Cleanup Test ==");
        let products = create_test_products(12);
        let root = Arc::new(GroupData::new_root("Root".to_string(), products, "All"));
        root.group_by(|p| p.category.clone(), "Categories").unwrap();
        let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands").unwrap();
        let keys = phones.subgroups_keys();
        let brand = phones.get_subgroup(&keys[0]).unwrap();
        println!("Before: Phones have {} subgroups", phones.subgroups_count());
        let back = brand.go_to_parent().unwrap();
        println!("✓ After: Phones have {} subgroups", back.subgroups_count());
        assert_eq!(back.subgroups_count(), 0, "Parent subgroups not cleared!");
        println!("No memory leak!");
    }

    #[test]
    #[serial]
    fn test_multiple_cycles_no_leak() {
        println!("== Multiple Cycles No Leak ==");
        DROP_COUNTER.store(0, Ordering::SeqCst);
        CREATE_COUNTER.store(0, Ordering::SeqCst);
        for cycle in 0..10 {
            let products = create_tracked_products(10);
            let root = GroupData::new_root(
                format!("Root_{}", cycle),
                products,
                "All"
            );
            root.group_by(|p| p.category.clone(), "Categories").unwrap();
            root.clear_subgroups();
        }
        let created = CREATE_COUNTER.load(Ordering::SeqCst);
        let dropped = DROP_COUNTER.load(Ordering::SeqCst);
        println!("Created: {}, Dropped: {}", created, dropped);
        assert_eq!(created, dropped, "Memory leak detected in cycles!");
        println!("No memory leak!");
    }

    #[test]
    #[serial]
    fn test_weak_references_dont_leak() {
        println!("== Weak References Don't Leak ==");
        let products = create_test_products(12);
        let root = Arc::new(GroupData::new_root("Root".to_string(), products, "All"));
        root.group_by(|p| p.category.clone(), "Categories").unwrap();
        let _phones = root.get_subgroup(&"Phones".to_string()).unwrap();
        let weak_root = Arc::downgrade(&root);
        assert!(weak_root.upgrade().is_some());
        drop(root);
        assert!(weak_root.upgrade().is_none());
        println!("No memory leak!");
    }

    #[test]
    #[serial]
    fn test_circular_reference_prevention() {
        println!("== Circular Reference Prevention ==");
        let products = create_test_products(12);
        let root = Arc::new(GroupData::new_root("Root".to_string(), products, "All"));
        root.group_by(|p| p.category.clone(), "Categories").unwrap();
        let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
        let parent = phones.go_to_parent();
        assert!(parent.is_some());
        drop(phones);
        drop(root);
        println!("No memory leak!");
    }

    #[test]
    #[serial]
    fn test_filter_no_leak() {
        println!("== Filter No Leak ==");
        DROP_COUNTER.store(0, Ordering::SeqCst);
        CREATE_COUNTER.store(0, Ordering::SeqCst);
        
        {
            let products = create_tracked_products(20);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            let _ = root.filter(|p| p.price > 600.0).unwrap();
            let _ = root.filter(|p| p.brand == "Apple").unwrap();
            root.reset_filters();
        }
        let created = CREATE_COUNTER.load(Ordering::SeqCst);
        let dropped = DROP_COUNTER.load(Ordering::SeqCst);
        println!("Created: {}, Dropped: {}", created, dropped);
        assert_eq!(created, dropped, "Memory leak in filter!");
        println!("No memory leak!");
    }

    #[test]
    #[serial]
    fn test_arc_count() {
        println!("== Arc Reference Count Test ==");
        let products = create_test_products(10);
        let root = Arc::new(GroupData::new_root("Root".to_string(), products, "All"));
        let initial_count = Arc::strong_count(&root);
        println!("Initial Arc count: {}", initial_count);
        root.group_by(|p| p.category.clone(), "Categories").unwrap();
        let after_grouping = Arc::strong_count(&root);
        println!("After group Arc count: {}", after_grouping);
        assert_eq!(initial_count, after_grouping, "Oops Arc count increase!");
        println!("No memory leak!");
    }

    #[test]
    #[serial]
    fn test_build_with_mapping_no_leak() {
        println!("== Build With Mapping No Leak ==");
        DROP_COUNTER.store(0, Ordering::SeqCst);
        CREATE_COUNTER.store(0, Ordering::SeqCst);
        
        {
            let products = create_tracked_products(100);
            let root = GroupData::new_root("Root".to_string(), products, "All");

            // Группируем с field индексами
            root.group_by_with_indexes(
                |p| p.category.clone(),
                "Categories",
                |fd| {
                    // Используем field_index
                    fd.create_field_index("brand", |p: &TrackedProduct| p.brand.clone())?;
                    fd.create_field_index("price", |p: &TrackedProduct| (p.price * 100.0) as i64)?;
                    Ok(())
                },
            ).unwrap();

            // Проверяем что индексы созданы
            let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
            assert!(phones.data.has_index("brand"));
            assert!(phones.data.has_index("price"));
            
            // Пересоздаем индексы многократно
            for i in 0..10 {
                phones.data.create_field_index("test", move |p: &TrackedProduct| {
                    (p.price as i64) % (i + 1)
                }).unwrap();
            }
        }
        thread::sleep(time::Duration::from_millis(100));
        let created = CREATE_COUNTER.load(Ordering::SeqCst);
        let dropped = DROP_COUNTER.load(Ordering::SeqCst);
        println!("Created: {}, Dropped: {}", created, dropped);
        assert_eq!(created, dropped, "Memory leak in build_with_mapping!");
        println!("No memory leak!");
    }

    #[test]
    #[serial]
    fn test_index_mapping_parent_independence() {
        println!("== Index Mapping Parent Independence ==");
        DROP_COUNTER.store(0, Ordering::SeqCst);
        CREATE_COUNTER.store(0, Ordering::SeqCst);
        
        {
            let products = create_tracked_products(50);
            let parent_data = Arc::new(
                products.into_iter().map(Arc::new).collect::<Vec<_>>()
            );
            
            // Создаем Indexed FilterData
            let indices = vec![0, 5, 10, 15, 20, 25, 30, 35, 40, 45];
            let data = FilterData::from_indices(&parent_data, indices);
            
            // Создаем field индекс с маппингом
            data.create_field_index("category", |p: &TrackedProduct| {
                p.category.clone()
            }).unwrap();
            
            // Дропаем parent_data - индекс должен стать невалидным
            drop(parent_data);
            
            // Но данные не должны протечь
            assert!(!data.is_valid());
        }
        thread::sleep(time::Duration::from_millis(100));
        let created = CREATE_COUNTER.load(Ordering::SeqCst);
        let dropped = DROP_COUNTER.load(Ordering::SeqCst);
        println!("Created: {}, Dropped: {}", created, dropped);
        assert_eq!(created, dropped, "Memory leak with parent drop!");
        println!("No memory leak!");
    }

    #[test]
    #[serial]
    fn test_index_only_stores_positions() {
        println!("== Index Only Stores Positions ==");
        let start_mem = memory_stats().map(|m| m.physical_mem);
        // Создаем большие данные
        let large_products: Vec<String> = (0..100_000)
            .map(|i| format!("Product with very long description number {}", i).repeat(10))
            .collect();
        let data = FilterData::from_vec(large_products);
        let after_data = memory_stats().map(|m| m.physical_mem);

        // Создаем field индекс - должен хранить только usize, не клонировать данные
        data.create_field_index("len", |s: &String| s.len()).unwrap();
        let after_index = memory_stats().map(|m| m.physical_mem);

        if let (Some(start), Some(after_d), Some(after_i)) = (start_mem, after_data, after_index) {
            let data_size = after_d.saturating_sub(start);
            let index_size = after_i.saturating_sub(after_d);
            println!("Data size: {} MB", data_size / 1024 / 1024);
            println!("Index size: {} MB", index_size / 1024 / 1024);

            // Индекс должен быть НАМНОГО меньше данных
            assert!(index_size < data_size / 10,
                "Index too large! Should only store positions, not data. Index: {} MB, Data: {} MB",
                index_size / 1024 / 1024, data_size / 1024 / 1024);
        }
        println!("✓ Index stores only positions!");
    }
}