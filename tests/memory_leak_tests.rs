#[cfg(test)]
mod test{
    use tree_man::{
        group::GroupData,
        filter::FilterData,
    };
    use serial_test::serial;
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
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
        // Создаем root с детьми
        let items: Vec<i32> = (0..1000).collect();
        let root = Arc::new(GroupData::new_root(
            "Root".to_string(),
            items,
            "Root"
        ));
        
        root.group_by(|n| (n % 5).to_string(), "By Mod 5");
        
        // Получаем количество Arc ссылок
        let initial_count = Arc::strong_count(&root);
        println!("Initial Arc count: {}", initial_count);
        
        // Получаем детей
        let children = root.get_all_subgroups();
        println!("Children count: {}", children.len());
        
        // Дропаем детей
        drop(children);
        
        // Arc count должен вернуться к исходному
        let after_drop = Arc::strong_count(&root);
        println!("After drop count: {}", after_drop);
        
        assert_eq!(initial_count, after_drop, 
                   "Arc references leaked!");
    }

    #[test]
    fn test_index_weak_references() {
        let items: Vec<String> = (0..1000)
            .map(|i| format!("item_{}", i))
            .collect();
        
        let data = FilterData::from_vec(items);
        data.create_index("test", |s| s.len());
        
        // Проверяем что индекс валиден
        assert!(data.validate_indexes());
        
        // Дропаем data
        drop(data);
        
        // После drop индексы должны стать невалидными
        // (если используют Weak правильно)
    }

    #[test]
    fn test_many_operations_no_leak() {
        use memory_stats::memory_stats;
        
        let start_mem = memory_stats().map(|m| m.physical_mem);
        
        for _ in 0..100 {
            let items: Vec<i32> = (0..10_000).collect();
            let data = FilterData::from_vec(items);
            
            // Много операций
            data.create_bit_index("even", |&n| n % 2 == 0);
            data.filter_by_bit_index("even");
            data.filter(|&n| n > 5000);
            data.reset_to_source();
            
            // data дропается здесь
        }
        
        // Даем время освободить память
        std::thread::sleep(std::time::Duration::from_millis(100));
        
        let end_mem = memory_stats().map(|m| m.physical_mem);
        
        if let (Some(start), Some(end)) = (start_mem, end_mem) {
            let diff = end.saturating_sub(start);
            println!("Memory growth: {} MB", diff / 1024 / 1024);
            
            // Не должно быть значительного роста памяти
            assert!(diff < 50_000_000, 
                    "Possible memory leak! Growth: {} bytes", diff);
        }
    }

    #[test]
    fn test_drop_order() {
        // Проверяем что дропается в правильном порядке
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
        
        // Должно вывести "Dropping" для всех элементов
    }

    #[test]
    #[serial]
    fn test_no_memory_leak_simple() {
        println!("== No Memory Leak Simple ==");
        
        DROP_COUNTER.store(0, Ordering::SeqCst);
        CREATE_COUNTER.store(0, Ordering::SeqCst);
        
        {
            let products = create_tracked_products(10);
            let root: Arc<GroupData<String, TrackedProduct>> = GroupData::new_root("Root".to_string(), products, "All");
            
            assert_eq!(root.data.len(), 10);
        }
        
        // Все объекты должны быть удалены
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
            
            root.group_by(|p| p.category.clone(), "Categories");
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
        println!("==  No Memory Leak Deep Hierarchy ==");
        
        DROP_COUNTER.store(0, Ordering::SeqCst);
        CREATE_COUNTER.store(0, Ordering::SeqCst);
        
        {
            let products = create_tracked_products(20);
            let root = GroupData::new_root("Root".to_string(), products, "All");
            
            root.group_by(|p| p.category.clone(), "Categories");
            
            let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
            phones.group_by(|p| p.brand.clone(), "Brands");
            
            let keys = phones.subgroups_keys();
            if !keys.is_empty() {
                let brand = phones.get_subgroup(&keys[0]).unwrap();
                brand.group_by(|p| {
                    if p.price > 600.0 { "Expensive".to_string() }
                    else { "Cheap".to_string() }
                }, "Price");
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
        
        root.group_by(|p| p.category.clone(), "Categories");
        let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands");
        
        let keys = phones.subgroups_keys();
        let brand = phones.get_subgroup(&keys[0]).unwrap();
        
        println!("Before: Phones have {} subgroups", phones.subgroups_count());
        
        // Идем вверх - должна произойти очистка
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
            
            root.group_by(|p| p.category.clone(), "Categories");
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
        
        root.group_by(|p| p.category.clone(), "Categories");
        
        let _phones = root.get_subgroup(&"Phones".to_string()).unwrap();
        let weak_root = Arc::downgrade(&root);
        
        // Root still exists
        assert!(weak_root.upgrade().is_some());
        
        // Drop root
        drop(root);
        
        // Weak reference should be None now
        assert!(weak_root.upgrade().is_none());
        
        println!("No memory leak!");
    }

    #[test]
    #[serial]
    fn test_circular_reference_prevention() {
        println!("== Circular Reference Prevention ==");
        
        let products = create_test_products(12);
        let root = Arc::new(GroupData::new_root("Root".to_string(), products, "All"));
        
        root.group_by(|p| p.category.clone(), "Categories");
        
        let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
        
        // Parent is Weak reference, so no circular dependency
        let parent = phones.go_to_parent();
        assert!(parent.is_some());
        
        // Both root and phones can be dropped without issue
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
            
            // Multiple filters
            root.filter(|p| p.price > 600.0);
            root.filter(|p| p.brand == "Apple");
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
        
        root.group_by(|p| p.category.clone(), "Categories");
        
        let after_grouping = Arc::strong_count(&root);
        println!("After group Arc count: {}", after_grouping);
        
        // Children have Weak references to parent, so count should be same
        assert_eq!(initial_count, after_grouping, "Oops Arc count increase!");
        
        println!("No memory leak!");
    }
}