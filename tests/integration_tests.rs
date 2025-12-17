#[cfg(test)]
mod integration_tests {
    use tree_man::{
        FieldOperation,
        Op,
        group_filter_parallel,
        group::{GroupData}
    };
    use std::sync::{
        Arc,
        atomic::{
            AtomicUsize,
            Ordering
        },
    };

    #[derive(Clone, Debug)]
    struct Product {
        category: String,
        brand: String,
        _model: String,
        price: f64,
        in_stock: bool,
    }

    fn create_realistic_products() -> Vec<Product> {
        vec![
            // Phones
            Product { category: "Phones".to_string(), brand: "Apple".to_string(), _model: "iPhone 15".to_string(), price: 999.0, in_stock: true },
            Product { category: "Phones".to_string(), brand: "Apple".to_string(), _model: "iPhone 14".to_string(), price: 799.0, in_stock: true },
            Product { category: "Phones".to_string(), brand: "Samsung".to_string(), _model: "Galaxy S24".to_string(), price: 899.0, in_stock: true },
            Product { category: "Phones".to_string(), brand: "Samsung".to_string(), _model: "Galaxy S23".to_string(), price: 699.0, in_stock: false },
            Product { category: "Phones".to_string(), brand: "Google".to_string(), _model: "Pixel 8".to_string(), price: 699.0, in_stock: true },
            
            // Laptops
            Product { category: "Laptops".to_string(), brand: "Apple".to_string(), _model: "MacBook Pro".to_string(), price: 2499.0, in_stock: true },
            Product { category: "Laptops".to_string(), brand: "Apple".to_string(), _model: "MacBook Air".to_string(), price: 1299.0, in_stock: true },
            Product { category: "Laptops".to_string(), brand: "Dell".to_string(), _model: "XPS 15".to_string(), price: 1899.0, in_stock: true },
            Product { category: "Laptops".to_string(), brand: "Lenovo".to_string(), _model: "ThinkPad X1".to_string(), price: 1699.0, in_stock: false },
            Product { category: "Laptops".to_string(), brand: "HP".to_string(), _model: "Spectre x360".to_string(), price: 1499.0, in_stock: true },
            
            // Tablets
            Product { category: "Tablets".to_string(), brand: "Apple".to_string(), _model: "iPad Pro".to_string(), price: 1099.0, in_stock: true },
            Product { category: "Tablets".to_string(), brand: "Apple".to_string(), _model: "iPad Air".to_string(), price: 599.0, in_stock: true },
            Product { category: "Tablets".to_string(), brand: "Samsung".to_string(), _model: "Galaxy Tab S9".to_string(), price: 799.0, in_stock: true },
            Product { category: "Tablets".to_string(), brand: "Microsoft".to_string(), _model: "Surface Pro".to_string(), price: 999.0, in_stock: false },
        ]
    }

    #[test]
    fn test_parallel_filter() {
        println!("== Parallel Filter ==");
        
        let products = create_realistic_products();
        let catalog = GroupData::new_root("Catalog".to_string(), products, "All");
        
        catalog.group_by(|p| p.category.clone(), "Categories").unwrap();
        
        let phones = catalog.get_subgroup(&"Phones".to_string()).unwrap();
        let laptops = catalog.get_subgroup(&"Laptops".to_string()).unwrap();
        let tablets = catalog.get_subgroup(&"Tablets".to_string()).unwrap();
        
        // Parallel filtering using macro
        // Note: Макрос работает с фильтрацией, которая меняет состояние data
        let result = group_filter_parallel!(
            phones => |p: &Product| p.price > 800.0,
            laptops => |p: &Product| p.price > 1500.0,
            tablets => |p: &Product| p.in_stock,
        );
        assert!(result.is_ok());
        println!("Phones (>$800): {} products", phones.data.len());
        println!("Laptops (>$1500): {} products", laptops.data.len());
        println!("Tablets (in stock): {} products", tablets.data.len());
        
        // Verify filters worked
        assert!(phones.data.len() > 0);
        assert!(laptops.data.len() > 0);
        assert!(tablets.data.len() > 0);
        
        println!("Complete successfully!");
    }

    #[test]
    fn test_deep_hierarchy() {
        println!("== Deep Hierarchy Test ==");
        
        let products = create_realistic_products();
        let catalog = GroupData::new_root("Catalog".to_string(), products, "All");
        
        // Level 1: Categories
        catalog.group_by(|p| p.category.clone(), "Categories").unwrap();
        
        // Level 2: Brands
        let phones = catalog.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands").unwrap();
        
        // Level 3: Price ranges
        let keys = phones.subgroups_keys();
        for key in &keys {
            let brand = phones.get_subgroup(key).unwrap();
            brand.group_by(|p| {
                if p.price < 700.0 { "Budget".to_string() }
                else if p.price < 900.0 { "Mid-Range".to_string() }
                else { "Premium".to_string() }
            }, "Price Range").unwrap();
        }
        
        // Verify depth
        let brand = phones.get_subgroup(&keys[0]).unwrap();
        let price_keys = brand.subgroups_keys();
        if !price_keys.is_empty() {
            let price_range = brand.get_subgroup(&price_keys[0]).unwrap();
            assert_eq!(price_range.depth(), 3);
            
            let path = price_range.get_path();
            println!("Path: {:?}", path);
            assert_eq!(path.len(), 4); // Catalog -> Phones -> Brand -> Price Range
        }
        
        println!("Complete successfully!");
    }

    #[test]
    fn test_navigation() {
        println!("== Navigation Test ==");
        
        let products = create_realistic_products();
        let catalog = Arc::new(GroupData::new_root("Catalog".to_string(), products, "All"));
        
        catalog.group_by(|p| p.category.clone(), "Categories").unwrap();
        
        // Navigate: Catalog -> Phones -> Brand -> Back to Phones
        let phones = catalog.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands").unwrap();
        
        let keys = phones.subgroups_keys();
        assert!(!keys.is_empty(), "Should have brand subgroups");
        
        let brand = phones.get_subgroup(&keys[0]).unwrap();
        
        // Back to phones
        let back_to_phones = brand.go_to_parent().unwrap();
        assert_eq!(back_to_phones.key, "Phones");
        
        // Should be clean after go_to_parent (subgroups cleared)
        assert_eq!(back_to_phones.subgroups_count(), 0);
        
        println!("Navigation successful!");
    }


    #[test]
    fn test_parent_current() {
        println!("== Parent current ==");
        let products = create_realistic_products();
        let catalog = Arc::new(GroupData::new_root("Catalog".to_string(), products, "All"));
        catalog.group_by(|p| p.category.clone(), "Categories").unwrap();
        let phones = catalog.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands").unwrap();
        let apple = phones.get_subgroup(&"Apple".to_string()).unwrap();
        apple.group_by(|p| {
            if p.price < 700.0 { "Budget".to_string() }
            else if p.price < 900.0 { "Mid-Range".to_string() }
            else { "Premium".to_string() }
        }, "Price Range").unwrap();
        let premium = apple.get_subgroup(&"Premium".to_string()).unwrap();
        let exsits_keys = vec!["Apple","Phones","Catalog"];
        let path_parents = premium.get_parents().iter().map(|p| p.key.clone()).collect::<Vec<_>>();
        for (n,parent_key) in path_parents.iter().enumerate(){
            assert_eq!(parent_key,exsits_keys[n]);    
        }
        let back_to_phones_result = premium.go_to_parent_current(&"Phones".to_string());
        assert!(back_to_phones_result.is_some());
        let phones_cat = back_to_phones_result.unwrap();    
        let exsits_keys = vec!["Catalog"];
        let phones_path_parent = phones_cat.get_parents().iter().map(|p| p.key.clone()).collect::<Vec<_>>();
        for (n,parent_key) in phones_path_parent.iter().enumerate(){
            assert_eq!(parent_key,exsits_keys[n]);    
        }
        println!("Parent current successful!");
    }

    #[test]
    fn test_filter_and_reset() {
        println!("== Filter and Reset Test ==");
        
        let products = create_realistic_products();
        let catalog = GroupData::new_root("Catalog".to_string(), products, "All");
        
        let original_count = catalog.data.len();
        println!("Original count: {}", original_count);
        
        // Filter multiple times
        let filtered1 = catalog.filter(|p| p.price > 1000.0).unwrap();
        println!("After first filter (>$1000): {}", filtered1.len());
        assert!(filtered1.len() < original_count);
        assert_eq!(catalog.data.len(), filtered1.len());
        
        let filtered2 = catalog.filter(|p| p.in_stock).unwrap();
        println!("After second filter (in_stock): {}", filtered2.len());
        assert!(filtered2.len() <= filtered1.len());
        assert_eq!(catalog.data.len(), filtered2.len());
        
        // Reset
        catalog.reset_filters();
        assert_eq!(catalog.data.len(), original_count);
        
        println!("Complete successfully!");
    }

    #[test]
    fn test_collect_all_groups() {
        println!("== Collect All Groups Test ==");
        let products = create_realistic_products();
        let catalog = GroupData::new_root("Catalog".to_string(), products, "All");
        catalog.group_by(|p| p.category.clone(), "Categories").unwrap();
        let phones = catalog.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands").unwrap();
        let all_groups = catalog.collect_all_groups();
        // Catalog + 3 categories + brands in phones (Apple, Samsung, Google)
        println!("Collected {} groups total", all_groups.len());
        assert!(all_groups.len() >= 4); // At least: Catalog + 3 categories
        // Verify each group is valid
        for group in &all_groups {
            assert!(group.is_valid(), "Group {:?} should be valid", group.key);
        }
        println!("Complete successfully!");
    }

    #[test]
    fn test_max_depth() {
        println!("== Max Depth Test ==");
        let products = create_realistic_products();
        let catalog = GroupData::new_root("Catalog".to_string(), products, "All");
        assert_eq!(catalog.max_depth(), 0);
        catalog.group_by(|p| p.category.clone(), "Categories").unwrap();
        assert_eq!(catalog.max_depth(), 1);
        let phones = catalog.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands").unwrap();
        assert_eq!(catalog.max_depth(), 2);
        println!("Max depth: {}", catalog.max_depth());
        println!("Complete successfully!");
    }

    #[test]
    fn test_total_groups_count() {
        println!("== Total Groups Count Test ==");
        let products = create_realistic_products();
        let catalog = GroupData::new_root("Catalog".to_string(), products, "All");
        assert_eq!(catalog.total_groups_count(), 1); // Just root
        catalog.group_by(|p| p.category.clone(), "Categories").unwrap();
        let count = catalog.total_groups_count();
        println!("After grouping: {} groups", count);
        assert_eq!(count, 4); // Root + 3 categories
        println!("Complete successfully!");
    }

    #[test]
    fn test_go_to_root() {
        println!("== Go To Root Test ==");
        let products = create_realistic_products();
        let catalog = Arc::new(GroupData::new_root("Catalog".to_string(), products, "All"));
        // Build 3-level hierarchy
        catalog.group_by(|p| p.category.clone(), "Categories").unwrap();
        let phones = catalog.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands").unwrap();
        let brand_keys = phones.subgroups_keys();
        if let Some(brand_key) = brand_keys.first() {
            let brand = phones.get_subgroup(brand_key).unwrap();
            // Navigate to root from deep node
            let root = brand.go_to_root();
            assert!(root.is_root());
            assert_eq!(root.key, "Catalog");
            assert_eq!(root.depth(), 0);
            // Root should be cleaned (subgroups cleared)
            assert_eq!(root.subgroups_count(), 0);
            println!("Successfully navigated to root!");
        }
        println!("Complete successfully!");
    }

    #[test]
    fn test_filter_subgroups() {
        println!("== Filter Subgroups Test ==");
        let products = create_realistic_products();
        let catalog = GroupData::new_root("Catalog".to_string(), products, "All");
        catalog.group_by(|p| p.category.clone(), "Categories").unwrap();
        // Filter all subgroups in parallel
        let results = catalog.filter_subgroups(|p| p.price > 1000.0).unwrap();
        println!("Filtered {} categories", results.len());
        // Check results
        for (key, items) in results.iter() {
            println!("  {}: {} items", key, items.len());
            // Verify all items meet criteria
            for item in items.iter() {
                assert!(item.price > 1000.0, "Item price {} should be > 1000", item.price);
            }
        }
        println!("Complete successfully!");
    }

    #[test]
    fn test_with_indexes() {
        println!("== With Indexes Test ==");
        let products = create_realistic_products();
        let catalog = GroupData::new_root("Catalog".to_string(), products, "All");
        // Create regular index in root
        catalog.create_field_index("brand", |p| p.brand.clone()).unwrap();
        assert!(catalog.data.has_index("brand"));
        // Group by category with indexes
        catalog.group_by_with_indexes(
            |p| p.category.clone(),
            "Categories",
            |fd| {
                fd.create_field_index("brand", |p: &Product| p.brand.clone())?;
                fd.create_field_index("in_stock", |p: &Product| p.in_stock)?;
                Ok(())
            }
        ).unwrap();
        // Verify indexes created in subgroups
        let phones = catalog.get_subgroup(&"Phones".to_string()).unwrap();
        assert!(phones.data.has_index("brand"));
        assert!(phones.data.has_index("in_stock"));
        // Use index for filtering
        let apple_phones = phones.filter_by_field_ops(
            "brand",
            &[(FieldOperation::eq("Apple".to_string()),Op::And)]
        ).unwrap();
        println!("Apple phones: {}", apple_phones.len());
        assert!(apple_phones.len() > 0);
        println!("Complete successfully!");
    }

    #[test]
    fn test_traverse() {
        println!("== Traverse Test ==");
        let products = create_realistic_products();
        let catalog = GroupData::new_root("Catalog".to_string(), products, "All");
        catalog.group_by(|p| p.category.clone(), "Categories").unwrap();
        let phones = catalog.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands").unwrap();
        // Count all nodes
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&count);
        catalog.traverse(&|_group| {
            count_clone.fetch_add(1, Ordering::SeqCst);
        });
        let total = count.load(Ordering::SeqCst);
        println!("Traversed {} nodes", total);
        assert!(total >= 4); // At least: Catalog + 3 categories
        println!("Complete successfully!");
    }

    #[test]
    fn test_traverse_parallel() {
        println!("== Traverse Parallel Test ==");
        let products = create_realistic_products();
        let catalog = GroupData::new_root("Catalog".to_string(), products, "All");
        catalog.group_by(|p| p.category.clone(), "Categories").unwrap();
        let phones = catalog.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands").unwrap();
        // Count all nodes in parallel
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&count);
        catalog.traverse_parallel(&|_group| {
            count_clone.fetch_add(1, Ordering::SeqCst);
        });
        let total = count.load(Ordering::SeqCst);
        println!("Traversed {} nodes in parallel", total);
        assert!(total >= 4);
        println!("Complete successfully!");
    }

    #[test]
    fn test_validate_tree() {
        println!("== Validate Tree Test ==");
        let products = create_realistic_products();
        let catalog = GroupData::new_root("Catalog".to_string(), products, "All");
        catalog.group_by(|p| p.category.clone(), "Categories").unwrap();
        let phones = catalog.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands").unwrap();
        // Validate entire tree
        assert!(catalog.validate_tree(), "Tree should be valid");
        // Validate individual groups
        assert!(catalog.is_valid());
        assert!(phones.is_valid());
        println!("Tree validation passed!");
        println!("Complete successfully!");
    }
}