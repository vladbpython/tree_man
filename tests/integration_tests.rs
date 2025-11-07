#[cfg(test)]
mod tests {
    use tree_man::{
        group_filter_parallel,
        group::{GroupData}
    };
    use std::sync::Arc;

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
        
        catalog.group_by(|p| p.category.clone(), "Categories");
        
        let phones = catalog.get_subgroup(&"Phones".to_string()).unwrap();
        let laptops = catalog.get_subgroup(&"Laptops".to_string()).unwrap();
        let tablets = catalog.get_subgroup(&"Tablets".to_string()).unwrap();
        
        // Parallel filtering using macro
        group_filter_parallel!(
            phones => |p: &Product| p.price > 800.0,
            laptops => |p: &Product| p.price > 1500.0,
            tablets => |p: &Product| p.in_stock,
        );
        
        println!("Phones (>$800): {} products", phones.data.len());
        println!("Laptops (>$1500): {} products", laptops.data.len());
        println!("Tablets (in stock): {} products", tablets.data.len());
        
        println!("Complete successfully!");
    }

    #[test]
    fn test_deep_hierarchy() {
        println!("== Deep Hierarchy Test ==");
        
        let products = create_realistic_products();
        let catalog = GroupData::new_root("Catalog".to_string(), products, "All");
        
        // Level 1: Categories
        catalog.group_by(|p| p.category.clone(), "Categories");
        
        // Level 2: Brands
        let phones = catalog.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands");
        
        // Level 3: Price ranges
        let keys = phones.subgroups_keys();
        for key in &keys {
            let brand = phones.get_subgroup(key).unwrap();
            brand.group_by(|p| {
                if p.price < 700.0 { "Budget".to_string() }
                else if p.price < 900.0 { "Mid-Range".to_string() }
                else { "Premium".to_string() }
            }, "Price Range");
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
        
        catalog.group_by(|p| p.category.clone(), "Categories");
        
        // Navigate: Catalog -> Phones -> Brand -> Back to Phones -> Laptops
        let phones = catalog.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands");
        
        let keys = phones.subgroups_keys();
        let brand = phones.get_subgroup(&keys[0]).unwrap();
        
        // Back to phones
        let back_to_phones = brand.go_to_parent().unwrap();
        assert_eq!(back_to_phones.key, "Phones");
        println!("navigation successful!");
    }

    #[test]
    fn test_filter_and_reset() {
        println!("== Filter and Reset Test ==");
        
        let products = create_realistic_products();
        let catalog = GroupData::new_root("Catalog".to_string(), products, "All");
        
        let original_count = catalog.data.len();
        
        // Filter multiple times
        catalog.filter(|p| p.price > 1000.0);
        let after_first_filter = catalog.data.len();
        assert!(after_first_filter < original_count);
        
        catalog.filter(|p| p.in_stock);
        let after_second_filter = catalog.data.len();
        assert!(after_second_filter <= after_first_filter);
        
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
        
        catalog.group_by(|p| p.category.clone(), "Categories");
        
        let phones = catalog.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands");
        
        let all_groups = catalog.collect_all_groups();
        
        // Catalog + 3 categories + brands in phones
        assert!(all_groups.len() >= 4);
        
        println!("Collected {} groups total", all_groups.len());
        
        println!("Complete successfully!");
    }

    #[test]
    fn test_max_depth() {
        println!("== Max Depth Test ==");
        
        let products = create_realistic_products();
        let catalog = GroupData::new_root("Catalog".to_string(), products, "All");
        
        assert_eq!(catalog.max_depth(), 0);
        
        catalog.group_by(|p| p.category.clone(), "Categories");
        assert_eq!(catalog.max_depth(), 1);
        
        let phones = catalog.get_subgroup(&"Phones".to_string()).unwrap();
        phones.group_by(|p| p.brand.clone(), "Brands");
        assert_eq!(catalog.max_depth(), 2);
        
        println!("Complete successfully!");
    }

    #[test]
    fn test_total_groups_count() {
        println!("== Total Groups Count Test ==");
        
        let products = create_realistic_products();
        let catalog = GroupData::new_root("Catalog".to_string(), products, "All");
        
        assert_eq!(catalog.total_groups_count(), 1); // Just root
        
        catalog.group_by(|p| p.category.clone(), "Categories");
        assert_eq!(catalog.total_groups_count(), 4); // Root + 3 categories
        
        println!("Complete successfully!");
    }
}