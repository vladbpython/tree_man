use tree_man::*; // или ваши импорты
use std::{thread, time};

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();
    
    println!("== Index Types Comparison ==\n");
    
    // Test 1: regular_index
    println!("Test 1: regular_index");
    test_field_index();
    println!("\nWaiting 1s...\n");
    thread::sleep(time::Duration::from_secs(1));
    println!("\n✓ All tests complete");
    println!("Check dhat-heap.json for comparison");
}

fn test_field_index() {
    for i in 0..50 {
        println!("create new data");
        let items: Vec<usize> = (0..1_000_000).collect();
        println!("new_data_created");
        println!("make FilterData");
        let data = filter::FilterData::from_vec(items);
        println!("FilterData maded");
        println!("create Field_index");
        data.create_field_index("len", |v| *v).unwrap();
        println!("Field_index created");
        let start = time::Instant::now();
        data.filter_by_field_ops("len", &[(FieldOperation::in_values(vec![1000,2000]),Op::And)]).unwrap();
        println!("execution for: {:?}",start.elapsed());
        data.clear_all_indexes();
        println!("  Iter {}: Created index", i);
        thread::sleep(std::time::Duration::from_millis(500));

        // data дропается здесь
        drop(data);
        println!("  Iter {}: Dropped data", i);
        thread::sleep(std::time::Duration::from_millis(200));
    }
}