#[cfg(test)]
mod unit_tests{
    use tree_man::group::GroupData;
    use std::sync::Arc;

    #[derive(Debug,Clone)]
    struct Product {
        category: String,
        brand: String,
        price: f64,
    }

    fn create_test_products(count: usize) -> Vec<Product> {
        (0..count).map(|i| Product {
            category: ["Phones", "Laptops", "Tablets"][i % 3].to_string(),
            brand: ["Apple", "Samsung", "Dell", "Lenovo"][i % 4].to_string(),
            price: 500.0 + (i as f64) * 10.0,
        }).collect()
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
        let root = Arc::new(GroupData::new_root("Root".to_string(), products, "All"));
        
        root.group_by(|p| p.category.clone(), "Categories");
        
        let phones = root.get_subgroup(&"Phones".to_string()).unwrap();
        let back = phones.go_to_parent();
        
        assert!(back.is_some());
        let back = back.unwrap();
        assert_eq!(back.key, "Root");
        
        println!("== Navigation Up == work");
    }

    #[test]
    fn test_sibling_navigation() {
        println!("== Relatives Navigation ==");
        
        let products = create_test_products(12);
        let root = GroupData::new_root("Root".to_string(), products, "All");
        
        root.group_by(|p| p.category.clone(), "Categories");
        
        let keys = root.subgroups_keys();
        let first = root.get_subgroup(&keys[0]).unwrap();
        
        assert!(!first.has_prev_relative());
        assert!(first.has_next_relative());
        
        let second = first.go_to_next_relative();
        assert!(second.is_some());
        
        let second = second.unwrap();
        assert!(second.has_next_relative());
        
        println!("== Relatives Navigation == works");
    }

    #[test]
    fn test_linkedlist_structure() {
        println!("== Linked List ==");
        
        let products = create_test_products(16);
        let root = GroupData::new_root("Root".to_string(), products, "All");
        
        root.group_by(|p| p.brand.clone(), "Brands");
        
        let keys = root.subgroups_keys();
        assert_eq!(keys.len(), 4); // Apple, Samsung, Dell, Lenovo
        
        // Test forward traversal
        let mut current = root.get_subgroup(&keys[0]).unwrap();
        let mut count = 1;
        
        while let Some(next) = current.go_to_next_relative() {
            count += 1;
            current = next;
        }
        
        assert_eq!(count, 4);
        
        // Test backward traversal
        let mut count = 1;
        while let Some(prev) = current.go_to_prev_relative() {
            count += 1;
            current = prev;
        }
        
        assert_eq!(count, 4);
        
        println!("== Linked List == correct");
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
        
        println!("== Filter Group == works crrect");
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
        
        println!("== Depth Calculation == works crrect");
    }
}