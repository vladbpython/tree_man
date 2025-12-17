#[cfg(test)]
mod group_data_tests {
    use tree_man::{
        group::GroupData,
        filter::FilterData,
        Op, FieldOperation,
        result::GlobalResult,
    };
    use std::{
        sync::Arc,
        time::Instant,
    };

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
        
        let result_root = GroupData::new_root_with_indexes(
            "Root".to_string(),
            products,
            "All Products",
            |fd| -> GlobalResult<FilterData<Product>> {
                // Используем field_index вместо regular_index
                fd.create_field_index("id", |p: &Product| p.id)?;
                fd.create_field_index("price", |p: &Product| (p.price * 100.0) as i64)?;
                fd.create_field_index("stock", |p: &Product| p.stock)?;
                Ok(fd)
            },
        );

        assert!(result_root.is_ok());
        let root = result_root.unwrap();
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
        root.group_by(|p| p.category.clone(), "Categories").unwrap();
        assert_eq!(root.subgroups_count(), 3);
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
        let result_group_by_with_indexes = root.group_by_with_indexes(
            |p| p.category.clone(),
            "Categories",
            |fd| -> GlobalResult<()> {
                // Используем field_index для всех типов
                fd.create_field_index("id", |p: &Product| p.id)?;
                fd.create_field_index("price", |p: &Product| (p.price * 100.0) as i64)?;
                fd.create_field_index("is_available", |p: &Product| p.is_available)?;
                Ok(())
            },
        );
        assert!(result_group_by_with_indexes.is_ok());
        assert_eq!(root.subgroups_count(), 3);
        
        for subgroup in root.get_all_subgroups() {
            assert!(subgroup.data.has_index("id"));
            assert!(subgroup.data.has_index("price"));
            assert!(subgroup.data.has_index("is_available"));
            println!("Subgroup '{}' has all indexes", subgroup.key);
        }
        println!("== Group By with Indexes == success");
    }

    #[test]
    fn test_create_indexes() {
        println!("== Create Indexes ==");
        let products = create_test_products(50);
        let root = GroupData::new_root("Root".to_string(), products, "All");
        // Используем create_field_index вместо create_regular_index
        root.create_field_index("id", |p: &Product| p.id).unwrap();
        root.create_field_index("category", |p: &Product| p.category.clone()).unwrap();
        root.create_field_index("price", |p: &Product| (p.price * 100.0) as i64).unwrap();
        assert!(root.data.has_index("id"));
        assert!(root.data.has_index("category"));
        assert!(root.data.has_index("price"));
        // Создаём field индексы для boolean значений
        root.create_field_index("is_available", |p: &Product| p.is_available).unwrap();
        root.create_field_index("in_stock", |p: &Product| p.stock > 0).unwrap();
        root.create_field_index("expensive", |p: &Product| p.price > 700.0).unwrap();
        assert!(root.data.has_index("is_available"));
        assert!(root.data.has_index("in_stock"));
        assert!(root.data.has_index("expensive"));
        println!("All indexes created successfully");
        println!("== Create Indexes == success");
    }

    #[test]
    fn test_filter_by_index() {
        println!("== Filter by Index ==");
        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");
        root.create_field_index("category", |p: &Product| p.category.clone()).unwrap();
        // Используем filter_by_field_ops вместо filter_by_regular_index
        let phones = root.filter_by_field_ops("category", &[
            (FieldOperation::eq("Phones".to_string()), Op::And)
        ]).unwrap();
        println!("Found {} phones", phones.len());
        assert!(!phones.is_empty());
        for phone in phones.iter() {
            assert_eq!(phone.category, "Phones");
        }
        // Состояние группы ИЗМЕНИЛОСЬ!
        assert_eq!(root.data.len(), 34); // Отфильтровано до Phones
        assert_eq!(root.data.current_level(), 1); // Новый уровень
        // Если нужно вернуться к исходному состоянию
        root.data.reset_to_source();
        assert_eq!(root.data.len(), 100);
        assert_eq!(root.data.current_level(), 0);
        println!("== Filter by Index == success");
    }

    #[test]
    fn test_filter_by_index_range() {
        println!("== Filter by Index Range ==");
        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");
        root.create_field_index("id", |p: &Product| p.id).unwrap();
        // Используем filter_by_field_ops с range вместо filter_by_regular_index_range
        let range_products = root.filter_by_field_ops("id", &[
            (FieldOperation::gte(20u32), Op::And),
            (FieldOperation::lt(40u32), Op::And),
        ]).unwrap();
        println!("Found {} products in range 20..40", range_products.len());
        assert_eq!(range_products.len(), 20);
        for product in range_products.iter() {
            assert!(product.id >= 20 && product.id < 40);
        }
        println!("== Filter by Index Range == success");
    }

    #[test]
    fn test_field_index_boolean_operations() {
        println!("== Field Index Boolean Operations ==");
        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");
        // Создаем field индексы для boolean значений
        root.create_field_index("is_available", |p: &Product| p.is_available).unwrap();
        root.create_field_index("in_stock", |p: &Product| p.stock > 10).unwrap();
        root.create_field_index("expensive", |p: &Product| p.price > 700.0).unwrap();
        // Фильтруем через field operations (цепочка)
        _ = root.filter_by_field_ops("is_available", &[
            (FieldOperation::eq(true), Op::And)
        ]).unwrap();
        _ = root.filter_by_field_ops("in_stock", &[
            (FieldOperation::eq(true), Op::And)
        ]).unwrap();
        _ = root.filter_by_field_ops("expensive",   &[
            (FieldOperation::eq(true), Op::And)
        ]).unwrap();
        println!("Premium products: {}",root.data.items().len());
        for product in root.data.items().iter() {
            assert!(product.is_available);
            assert!(product.stock > 10);
            assert!(product.price > 700.0);
        }
        println!("== Field Index Boolean Operations == success");
    }

    #[test]
    fn test_create_index_in_subgroups() {
        println!("== Create Index in Subgroups ==");
        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");
        root.group_by(|p| p.category.clone(), "Categories").unwrap();
        //Используем create_field_index_in_subgroups
        root.create_field_index_in_subgroups("price", |p: &Product| (p.price * 100.0) as i64).unwrap();
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
        root.group_by(|p| p.category.clone(), "Categories").unwrap();
        for subgroup in root.get_all_subgroups() {
            subgroup.group_by(|p| p.brand.clone(), "Brands").unwrap();
        }
        // Используем create_field_index_recursive
        root.create_field_index_recursive("id", |p: &Product| p.id).unwrap();
        assert!(root.data.has_index("id"));
        for cat_group in root.get_all_subgroups() {
            assert!(cat_group.data.has_index("id"));
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
        root.group_by(|p| p.brand.clone(), "Brands").unwrap();
        let keys = root.subgroups_keys();
        println!("Subgroups (sorted): {:?}", keys);
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        assert_eq!(keys, sorted_keys);
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
        root.group_by(|p| p.brand.clone(), "Brands").unwrap();
        let range = root.get_subgroups_range("Apple".to_string()..="Lenovo".to_string());
        println!("Brands in range Apple..=Lenovo:");
        for group in &range {
            println!("  - {} ({} products)", group.key, group.data.len());
        }
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
        root.group_by(|p| p.brand.clone(), "Brands").unwrap();
        let top_2 = root.get_top_n_subgroups(2);
        println!("Top 2 brands:");
        for group in &top_2 {
            println!("  - {} ({} products)", group.key, group.data.len());
        }
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
        root.group_by(|p| p.category.clone(), "Categories").unwrap();
        
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
        root.group_by(|p| p.category.clone(), "Categories").unwrap();
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
        root.group_by(|p| p.category.clone(), "Categories").unwrap();
        let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands").unwrap();
        let keys = phones.subgroups_keys();
        let brand = phones.get_subgroup(&keys[0]).unwrap();
        let path = brand.get_path();
        assert_eq!(path.len(), 3);
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
        root.filter(|p| p.price > 600.0).unwrap();
        let after = root.data.len();
        assert!(after < before);
        println!("== Filter Group == works correct");
    }

    #[test]
    fn test_filter_subgroups() {
        println!("== Filter Subgroups ==");
        let products = create_test_products(100);
        let root = GroupData::new_root("Root".to_string(), products, "All");
        root.group_by(|p| p.category.clone(), "Categories").unwrap();
        let results = root.filter_subgroups(|p| p.price > 700.0).unwrap();
        println!("\nFilter results:");
        for (category, items) in &results {
            println!("  {}: {} products", category, items.len());
            for item in items.iter() {
                assert!(item.price > 700.0);
            }
        }
        
        // Подгруппы также остались отфильтрованными
        for subgroup in root.get_all_subgroups() {
            println!(
                "Subgroup '{}': {} items at level {}",
                subgroup.key,
                subgroup.data.len(),
                subgroup.data.current_level()
            );
            assert_eq!(subgroup.data.current_level(), 1);
            for item in subgroup.data.items().iter() {
                assert!(item.price > 700.0);
            }
        }
        
        println!("== Filter Subgroups == success");
    }

    #[test]
    fn test_clear_subgroups() {
        println!("== Clear Subgroups ==");
        let products = create_test_products(12);
        let root = GroupData::new_root("Root".to_string(), products, "All");
        root.group_by(|p| p.category.clone(), "Categories").unwrap();
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
        root.group_by(|p| p.category.clone(), "Categories").unwrap();
        let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
        assert_eq!(phones.depth(), 1);
        phones.group_by(|p| p.brand.clone(), "Brands").unwrap();
        let keys = phones.subgroups_keys();
        let brand = phones.get_subgroup(&keys[0]).unwrap();
        assert_eq!(brand.depth(), 2);
        println!("== Depth Calculation == works correct");
    }

    #[test]
    fn test_performance_indicators() {
        println!("== Performance Indicators ==");
        let products = create_test_products(10_000);
        let start = Instant::now();
        let root = GroupData::new_root_with_indexes(
            "Store".to_string(),
            products,
            "All",
            |fd| {
                // Используем field_index
                fd.create_field_index("id", |p: &Product| p.id)?;
                fd.create_field_index("price", |p: &Product| (p.price * 100.0) as i64)?;
                fd.create_field_index("available", |p: &Product| p.is_available)?;
                Ok(fd)
            },
        ).unwrap();
        println!("Creating 10K products with 3 indexes: {:?}", start.elapsed());
        let start = Instant::now();
        root.group_by_with_indexes(
            |p| p.category.clone(),
            "Categories",
            |fd| {
                fd.create_field_index("price", |p: &Product| (p.price * 100.0) as i64)?;
                Ok(())
            },
        ).unwrap();
        println!("Grouping with indexes: {:?}", start.elapsed());

        // Index search через field operations
        let start = Instant::now();
        let _result = root.filter_by_field_ops("id", &[
            (FieldOperation::eq(5000u32), Op::And)
        ]);
        println!("Index search (1 item from 10K): {:?}", start.elapsed());
        
        // Range query через field operations
        root.data.reset_to_source();
        let start = Instant::now();
        let _range = root.filter_by_field_ops("id", &[
            (FieldOperation::gte(1000u32), Op::And),
            (FieldOperation::lt(2000u32), Op::And),
        ]);
        println!("Range query (1000 items): {:?}", start.elapsed());
        
        // Boolean operation через field operations
        root.data.reset_to_source();
        let start = Instant::now();
        let _bits = root.filter_by_field_ops("available", &[
            (FieldOperation::eq(true), Op::And)
        ]);
        println!("Boolean operation: {:?}", start.elapsed());
        println!("== Performance Indicators == complete");
    }
}