#[cfg(test)]
mod unit_tests {
    use tree_man::{
        group::GroupData,
        bit_index::BitOp,
    };
    use std::sync::Arc;

    #[derive(Debug, Clone)]
    struct Product {
        id: u32,
        category: String,
        brand: String,
        price: f64,
        stock: u32,
        is_available: bool,
    }

    fn create_test_products(count: usize) -> Vec<Product> {
        (0..count)
            .map(|i| Product {
                id: i as u32,
                category: ["Phones", "Laptops", "Tablets"][i % 3].to_string(),
                brand: ["Apple", "Samsung", "Dell", "Lenovo"][i % 4].to_string(),
                price: 500.0 + (i as f64) * 10.0,
                stock: (i % 50) as u32,
                is_available: i % 3 != 0,
            })
            .collect()
    }

    #[test]
    fn test_group_creation() {
        println!("== Group Creation ==");

        let products = create_test_products(10);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        assert_eq!(root.key, "Root");
        assert_eq!(root.data.len(), 10);
        assert_eq!(root.depth(), 0);
        assert!(root.is_root());
        assert_eq!(root.subgroups_count(), 0);

        println!("== Group Creation == success");
    }

    #[test]
    fn test_group_creation_with_indexes() {
        println!("== Group Creation with Indexes ==");

        let products = create_test_products(100);
        let root = GroupData::new_root_with_indexes(
            "Root".to_string(),
            products,
            "All Products",
            |fd| {
                fd.create_index("id", |p: &Product| p.id)
                    .create_index("price", |p: &Product| (p.price * 100.0) as i64)
                    .create_index("stock", |p: &Product| p.stock);
                fd
            },
        );

        assert_eq!(root.data.len(), 100);
        assert!(root.data.has_index("id"));
        assert!(root.data.has_index("price"));
        assert!(root.data.has_index("stock"));

        let indexes = root.data.list_indexes();
        println!("Created indexes: {:?}", indexes);

        println!("== Group Creation with Indexes == success");
    }

    #[test]
    fn test_group_by() {
        println!("== Group By ==");

        let products = create_test_products(12);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        root.group_by(|p| p.category.clone(), "Categories");

        assert_eq!(root.subgroups_count(), 3); // Phones, Laptops, Tablets

        let keys = root.subgroups_keys();
        assert!(keys.contains(&"Phones".to_string()));
        assert!(keys.contains(&"Laptops".to_string()));
        assert!(keys.contains(&"Tablets".to_string()));

        println!("== Group By == work correct");
    }

    #[test]
    fn test_group_by_with_indexes() {
        println!("== Group By with Indexes ==");

        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        // Группируем с автоматическим созданием индексов в подгруппах
        root.group_by_with_indexes(
            |p| p.category.clone(),
            "Categories",
            |fd| {
                fd.create_index("id", |p: &Product| p.id)
                    .create_index("price", |p: &Product| (p.price * 100.0) as i64)
                    .create_bit_index("is_available", |p: &Product| p.is_available);
            },
        );

        assert_eq!(root.subgroups_count(), 3);

        // Проверяем что все подгруппы имеют индексы
        for subgroup in root.get_all_subgroups() {
            assert!(subgroup.data.has_index("id"));
            assert!(subgroup.data.has_index("price"));
            assert!(subgroup.data.has_index("bit:is_available"));
            println!("Subgroup '{}' has all indexes", subgroup.key);
        }

        println!("== Group By with Indexes == success");
    }

    #[test]
    fn test_create_indexes() {
        println!("== Create Indexes ==");

        let products = create_test_products(50);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        // Создаём обычные индексы
        root.create_index("id", |p: &Product| p.id)
            .create_index("category", |p: &Product| p.category.clone())
            .create_index("price", |p: &Product| (p.price * 100.0) as i64);

        assert!(root.data.has_index("id"));
        assert!(root.data.has_index("category"));
        assert!(root.data.has_index("price"));

        // Создаём битовые индексы
        root.create_bit_index("is_available", |p: &Product| p.is_available)
            .create_bit_index("in_stock", |p: &Product| p.stock > 0)
            .create_bit_index("expensive", |p: &Product| p.price > 700.0);

        assert!(root.data.has_index("bit:is_available"));
        assert!(root.data.has_index("bit:in_stock"));
        assert!(root.data.has_index("bit:expensive"));

        println!("All indexes created successfully");
        println!("== Create Indexes == success");
    }

    #[test]
    fn test_filter_by_index() {
        println!("== Filter by Index ==");

        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        root.create_index("category", |p: &Product| p.category.clone());

        // Фильтрация через индекс (read-only)
        let phones = root.filter_by_index("category", &"Phones".to_string());
        println!("Found {} phones", phones.len());
        assert!(!phones.is_empty());

        for phone in &phones {
            assert_eq!(phone.category, "Phones");
        }

        // Состояние группы не изменилось
        assert_eq!(root.data.len(), 100);
        assert_eq!(root.data.current_level(), 0);

        println!("== Filter by Index == success");
    }

    #[test]
    fn test_apply_index_filter() {
        println!("== Apply Index Filter ==");

        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        root.create_index("category", |p: &Product| p.category.clone());

        assert_eq!(root.data.current_level(), 0);

        // Применяем фильтр как новый уровень
        root.apply_index_filter("category", &"Laptops".to_string());

        assert_eq!(root.data.current_level(), 1);
        assert!(root.data.len() < 100);

        // Все элементы должны быть ноутбуками
        for item in root.data.items().iter() {
            assert_eq!(item.category, "Laptops");
        }

        println!("Filtered to {} laptops", root.data.len());
        println!("== Apply Index Filter == success");
    }

    #[test]
    fn test_filter_by_index_range() {
        println!("== Filter by Index Range ==");

        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        root.create_index("id", |p: &Product| p.id);

        // Range query: id от 20 до 40
        let range_products = root.filter_by_index_range::<u32,_>("id", 20..40);

        println!("Found {} products in range 20..40", range_products.len());
        assert_eq!(range_products.len(), 20);

        for product in &range_products {
            assert!(product.id >= 20 && product.id < 40);
        }

        println!("== Filter by Index Range == success");
    }

    #[test]
    fn test_apply_index_range() {
        println!("== Apply Index Range ==");

        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        root.create_index("price", |p: &Product| (p.price * 100.0) as i64);

        // Применяем range как новый уровень
        let min_price = 60000; // $600
        let max_price = 80000; // $800
        root.apply_index_range("price", min_price..=max_price);

        assert_eq!(root.data.current_level(), 1);

        for item in root.data.items().iter() {
            let price_cents = (item.price * 100.0) as i64;
            assert!(price_cents >= min_price && price_cents <= max_price);
        }

        println!(
            "Filtered to {} products in price range $600-$800",
            root.data.len()
        );
        println!("== Apply Index Range == success");
    }

    #[test]
    fn test_get_sorted_by_index() {
        println!("== Get Sorted by Index ==");

        let products = create_test_products(50);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        root.create_index("price", |p: &Product| (p.price * 100.0) as i64);

        let sorted = root.get_sorted_by_index::<i64>("price");

        assert_eq!(sorted.len(), 50);

        // Проверяем что отсортировано
        for i in 1..sorted.len() {
            assert!(sorted[i - 1].price <= sorted[i].price);
        }

        println!("Products sorted by price:");
        println!("  First: ${:.2}", sorted[0].price);
        println!("  Last: ${:.2}", sorted[sorted.len() - 1].price);

        println!("== Get Sorted by Index == success");
    }

    #[test]
    fn test_get_top_n_by_index() {
        println!("== Get Top N by Index ==");

        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        root.create_index("price", |p: &Product| (p.price * 100.0) as i64);

        let top_10 = root.get_top_n_by_index::<i64>("price", 10);

        assert_eq!(top_10.len(), 10);

        println!("Top 10 most expensive products:");
        for (i, product) in top_10.iter().enumerate() {
            println!(
                "  {}. {} {} - ${:.2}",
                i + 1,
                product.brand,
                product.category,
                product.price
            );
        }

        // Проверяем что первый - самый дорогой
        assert_eq!(top_10[0].price, 1490.0);
        
        // Проверяем что последний из топ-10
        assert_eq!(top_10[9].price, 1400.0);
        
        // Проверяем порядок (убывание)
        for i in 1..top_10.len() {
            assert!(top_10[i - 1].price >= top_10[i].price);
        }

        println!("== Get Top N by Index == success");
    }

    #[test]
    fn test_bit_index_operations() {
        println!("== Bit Index Operations ==");

        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        // Создаём битовые индексы
        root.create_bit_index("is_available", |p: &Product| p.is_available)
            .create_bit_index("in_stock", |p: &Product| p.stock > 10)
            .create_bit_index("expensive", |p: &Product| p.price > 700.0);

        // AND операция: доступные И в наличии И дорогие
        let premium = root.filter_by_bit_operation(&[
            ("is_available", BitOp::And),
            ("in_stock", BitOp::And),
            ("expensive", BitOp::And),
        ]);

        println!("Premium products (available AND in_stock AND expensive): {}", premium.len());

        for product in &premium {
            assert!(product.is_available);
            assert!(product.stock > 10);
            assert!(product.price > 700.0);
        }

        // OR операция: дорогие ИЛИ много на складе
        let popular_or_expensive = root.filter_by_bit_operation(&[
            ("expensive", BitOp::Or),
            ("in_stock", BitOp::Or),
        ]);

        println!(
            "Popular or expensive: {}",
            popular_or_expensive.len()
        );

        println!("== Bit Index Operations == success");
    }

    #[test]
    fn test_apply_bit_operation() {
        println!("== Apply Bit Operation ==");

        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        root.create_bit_index("is_available", |p: &Product| p.is_available)
            .create_bit_index("in_stock", |p: &Product| p.stock > 5);

        assert_eq!(root.data.current_level(), 0);

        // Применяем битовую операцию как фильтр
        root.apply_bit_operation(&[
            ("is_available", BitOp::And),
            ("in_stock", BitOp::And),
        ]);

        assert_eq!(root.data.current_level(), 1);

        for item in root.data.items().iter() {
            assert!(item.is_available);
            assert!(item.stock > 5);
        }

        println!(
            "Filtered to {} available products with stock > 5",
            root.data.len()
        );
        println!("== Apply Bit Operation == success");
    }

    #[test]
    fn test_create_index_in_subgroups() {
        println!("== Create Index in Subgroups ==");

        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        root.group_by(|p| p.category.clone(), "Categories");

        // Создаём индексы во всех подгруппах параллельно
        root.create_index_in_subgroups("price", |p: &Product| (p.price * 100.0) as i64);

        // Проверяем что все подгруппы имеют индекс
        for subgroup in root.get_all_subgroups() {
            assert!(subgroup.data.has_index("price"));
            println!("Subgroup '{}' has price index", subgroup.key);
        }

        println!("== Create Index in Subgroups == success");
    }

    #[test]
    fn test_create_index_recursive() {
        println!("== Create Index Recursive ==");

        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        // Первый уровень группировки
        root.group_by(|p| p.category.clone(), "Categories");

        // Второй уровень группировки
        for subgroup in root.get_all_subgroups() {
            subgroup.group_by(|p| p.brand.clone(), "Brands");
        }

        // Рекурсивно создаём индекс во всём дереве
        root.create_index_recursive("id", |p: &Product| p.id);

        // Проверяем root
        assert!(root.data.has_index("id"));

        // Проверяем первый уровень
        for cat_group in root.get_all_subgroups() {
            assert!(cat_group.data.has_index("id"));

            // Проверяем второй уровень
            for brand_group in cat_group.get_all_subgroups() {
                assert!(brand_group.data.has_index("id"));
            }
        }

        println!("Index created recursively in all groups");
        println!("== Create Index Recursive == success");
    }

    #[test]
    fn test_btree_sorted_subgroups() {
        println!("== BTree Sorted Subgroups ==");

        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        root.group_by(|p| p.brand.clone(), "Brands");

        let keys = root.subgroups_keys();
        println!("Subgroups (sorted): {:?}", keys);

        // Проверяем что ключи отсортированы
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        assert_eq!(keys, sorted_keys);

        // Первый и последний ключи
        let first = root.first_subgroup_key();
        let last = root.last_subgroup_key();
        println!("First: {:?}, Last: {:?}", first, last);

        assert_eq!(first, Some("Apple".to_string()));
        assert_eq!(last, Some("Samsung".to_string()));

        println!("== BTree Sorted Subgroups == success");
    }

    #[test]
    fn test_subgroups_range() {
        println!("== Subgroups Range ==");

        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        root.group_by(|p| p.brand.clone(), "Brands");

        // Range query по подгруппам
        let range = root.get_subgroups_range("Apple".to_string()..="Lenovo".to_string());

        println!("Brands in range Apple..=Lenovo:");
        for group in &range {
            println!("  - {} ({} products)", group.key, group.data.len());
        }

        // Проверяем что диапазон корректный
        for group in &range {
            assert!(group.key >= "Apple".to_string() && group.key <= "Lenovo".to_string());
        }

        println!("== Subgroups Range == success");
    }

    #[test]
    fn test_top_bottom_subgroups() {
        println!("== Top/Bottom Subgroups ==");

        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        root.group_by(|p| p.brand.clone(), "Brands");

        // Топ-2 по ключу
        let top_2 = root.get_top_n_subgroups(2);
        println!("Top 2 brands:");
        for group in &top_2 {
            println!("  - {} ({} products)", group.key, group.data.len());
        }

        // Bottom-2 по ключу
        let bottom_2 = root.get_bottom_n_subgroups(2);
        println!("Bottom 2 brands:");
        for group in &bottom_2 {
            println!("  - {} ({} products)", group.key, group.data.len());
        }

        assert_eq!(top_2.len(), 2);
        assert_eq!(bottom_2.len(), 2);

        println!("== Top/Bottom Subgroups == success");
    }

    #[test]
    fn test_navigation_down() {
        println!("== Navigation Down ==");

        let products = create_test_products(12);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        root.group_by(|p| p.category.clone(), "Categories");

        let phones = root.go_to_subgroup(&"Phones".to_string());
        assert!(phones.is_some());

        let phones = phones.unwrap();
        assert_eq!(phones.key, "Phones");
        assert_eq!(phones.depth(), 1);
        assert!(!phones.is_root());

        println!("== Navigation Down === work");
    }

    #[test]
    fn test_navigation_up() {
        println!("== Navigation Up ==");

        let products = create_test_products(12);
        let root = Arc::new(GroupData::new_root(
            "Root".to_string(),
            products,
            "All",
        ));

        root.group_by(|p| p.category.clone(), "Categories");

        let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
        let back = phones.go_to_parent();

        assert!(back.is_some());
        let back = back.unwrap();
        assert_eq!(back.key, "Root");

        println!("== Navigation Up == work");
    }

    #[test]
    fn test_get_path() {
        println!("== Get Path ==");

        let products = create_test_products(12);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        root.group_by(|p| p.category.clone(), "Categories");

        let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands");

        let keys = phones.subgroups_keys();
        let brand = phones.get_subgroup(&keys[0]).unwrap();

        let path = brand.get_path();
        assert_eq!(path.len(), 3); // Root -> Phones -> Brand
        assert_eq!(path[0], "Root");
        assert_eq!(path[1], "Phones");

        println!("== Get Path == works correct");
    }

    #[test]
    fn test_filter_group() {
        println!("== Filter Group ==");

        let products = create_test_products(20);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        let before = root.data.len();
        root.filter(|p| p.price > 600.0);
        let after = root.data.len();

        assert!(after < before);

        println!("== Filter Group == works correct");
    }

    #[test]
    fn test_combined_filtering() {
        println!("== Combined Filtering ==");

        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        // Создаём все типы индексов
        root.create_index("category", |p: &Product| p.category.clone())
            .create_index("price", |p: &Product| (p.price * 100.0) as i64)
            .create_bit_index("is_available", |p: &Product| p.is_available)
            .create_bit_index("in_stock", |p: &Product| p.stock > 5);

        println!("Initial: {} products", root.data.len());

        // Уровень 1: фильтр по категории через индекс
        root.apply_index_filter("category", &"Phones".to_string());
        println!("After category filter: {} products", root.data.len());

        // Уровень 2: range по цене
        root.apply_index_range("price", 60000..80000);
        println!("After price range: {} products", root.data.len());

        // Уровень 3: битовые операции
        root.apply_bit_operation(&[
            ("is_available", BitOp::And),
            ("in_stock", BitOp::And),
        ]);
        println!("After bit operations: {} products", root.data.len());

        // Откатываемся назад
        root.data.up();
        println!("After up(): {} products", root.data.len());

        // Сброс к началу
        root.reset_filters();
        println!("After reset: {} products", root.data.len());
        assert_eq!(root.data.len(), 100);

        println!("== Combined Filtering == success");
    }

    #[test]
    fn test_clear_subgroups() {
        println!("== Clear Subgroups ==");

        let products = create_test_products(12);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        root.group_by(|p| p.category.clone(), "Categories");
        assert_eq!(root.subgroups_count(), 3);

        root.clear_subgroups();
        assert_eq!(root.subgroups_count(), 0);

        println!("== Clear Subgroups == works");
    }

    #[test]
    fn test_depth_calculation() {
        println!("== Depth Calculation ==");

        let products = create_test_products(12);
        let root = GroupData::new_root("Root".to_string(), products, "All");

        assert_eq!(root.depth(), 0);

        root.group_by(|p| p.category.clone(), "Categories");
        let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
        assert_eq!(phones.depth(), 1);

        phones.group_by(|p| p.brand.clone(), "Brands");
        let keys = phones.subgroups_keys();
        let brand = phones.get_subgroup(&keys[0]).unwrap();
        assert_eq!(brand.depth(), 2);

        println!("== Depth Calculation == works correct");
    }

    #[test]
    fn test_complex_workflow() {
        println!("== Complex Workflow ==");

        let products = create_test_products(200);

        // Шаг 1: Создаём root с индексами
        let root = GroupData::new_root_with_indexes(
            "Store".to_string(),
            products,
            "All Products",
            |fd| {
                fd.create_index("id", |p: &Product| p.id)
                    .create_index("category", |p: &Product| p.category.clone())
                    .create_index("price", |p: &Product| (p.price * 100.0) as i64)
                    .create_bit_index("is_available", |p: &Product| p.is_available)
                    .create_bit_index("in_stock", |p: &Product| p.stock > 10);
                fd
            },
        );

        println!("Step 1: Created root with 5 indexes");

        // Шаг 2: Группируем по категориям с индексами
        root.group_by_with_indexes(
            |p| p.category.clone(),
            "By Category",
            |fd| {
                fd.create_index("brand", |p: &Product| p.brand.clone())
                    .create_index("price", |p: &Product| (p.price * 100.0) as i64)
                    .create_bit_index("premium", |p: &Product| p.price > 800.0);
            },
        );

        println!("Step 2: Grouped by category with indexes in subgroups");

        // Шаг 3: Работаем с конкретной категорией
        let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
        println!("Step 3: Selected Phones group ({} items)", phones.data.len());

        // Шаг 4: Range query по цене
        let mid_range = phones.filter_by_index_range("price", 60000..80000);
        println!("Step 4: Found {} mid-range phones", mid_range.len());

        // Шаг 5: Битовые операции
        let premium_available =
            phones.filter_by_bit_operation(&[("premium", BitOp::And), ("is_available", BitOp::And)]);
        println!(
            "Step 5: Found {} premium available phones",
            premium_available.len()
        );

        // Шаг 6: Сортировка по цене
        let sorted_phones = phones.get_sorted_by_index::<i64>("price");
        println!(
            "Step 6: Sorted phones, cheapest: ${:.2}, most expensive: ${:.2}",
            sorted_phones.first().unwrap().price,
            sorted_phones.last().unwrap().price
        );

        // Шаг 7: Топ-5 самых дорогих
        let top_5 = phones.get_top_n_by_index::<i64>("price", 5);
        println!("Step 7: Top 5 most expensive phones:");
        for (i, phone) in top_5.iter().enumerate() {
            println!("  {}. {} - ${:.2}", i + 1, phone.brand, phone.price);
        }

        // Шаг 8: Применяем фильтры ко всем категориям
        root.filter_subgroups(|p| p.price > 700.0);
        println!("Step 8: Filtered all categories (price > $700)");

        // Шаг 9: Статистика
        println!("\nFinal Statistics:");
        println!("  Total groups: {}", root.total_groups_count());
        println!("  Max depth: {}", root.max_depth());
        for cat_group in root.get_all_subgroups() {
            println!(
                "  {}: {} products",
                cat_group.key,
                cat_group.data.len()
            );
        }

        println!("\n== Complex Workflow == success");
    }

    #[test]
    fn test_performance_indicators() {
        use std::time::Instant;

        println!("== Performance Indicators ==");

        let products = create_test_products(10_000);

        // Создание с индексами
        let start = Instant::now();
        let root = GroupData::new_root_with_indexes(
            "Store".to_string(),
            products,
            "All",
            |fd| {
                fd.create_index("id", |p: &Product| p.id)
                    .create_index("price", |p: &Product| (p.price * 100.0) as i64)
                    .create_bit_index("available", |p: &Product| p.is_available);
                fd
            },
        );
        println!("Creating 10K products with 3 indexes: {:?}", start.elapsed());

        // Группировка с индексами
        let start = Instant::now();
        root.group_by_with_indexes(
            |p| p.category.clone(),
            "Categories",
            |fd| {
                fd.create_index("price", |p: &Product| (p.price * 100.0) as i64);
            },
        );
        println!("Grouping with indexes: {:?}", start.elapsed());

        // Индексный поиск
        let start = Instant::now();
        let _result = root.filter_by_index("id", &5000u32);
        println!("Index search (1 item from 10K): {:?}", start.elapsed());

        // Range query
        let start = Instant::now();
        let _range = root.filter_by_index_range("id", 1000..2000);
        println!("Range query (1000 items): {:?}", start.elapsed());

        // Битовая операция
        let start = Instant::now();
        let _bits = root.filter_by_bit_operation(&[("available", BitOp::And)]);
        println!("Bit operation: {:?}", start.elapsed());

        println!("== Performance Indicators == complete");
    }
}