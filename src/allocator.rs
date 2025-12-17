// Platform-specific memory allocator configuration

// Linux & macOS: Use jemalloc
#[cfg(all(
    feature = "jemalloc",
    not(target_env = "msvc"),
    not(target_os = "windows")
))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Windows (MSVC): Use mimalloc
#[cfg(all(
    feature = "mimalloc-allocator",
    target_env = "msvc"
))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// Get allocator name
pub fn allocator_info() -> &'static str {
    #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
    return "jemalloc";
    
    #[cfg(all(feature = "mimalloc-allocator", target_env = "msvc"))]
    return "mimalloc";
    
    #[cfg(not(any(
        all(feature = "jemalloc", not(target_env = "msvc")),
        all(feature = "mimalloc-allocator", target_env = "msvc")
    )))]
    return "system";
}

// Get allocator statistics
pub fn allocator_stats() -> AllocatorStats {
    #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
    {
        use tikv_jemalloc_ctl::{epoch, stats};
        
        // Обновляем статистику
        if let Ok(e) = epoch::mib() {
            let _ = e.advance();
        }
        
        let allocated = stats::allocated::mib()
            .and_then(|mib| mib.read())
            .unwrap_or(0);
        
        let resident = stats::resident::mib()
            .and_then(|mib| mib.read())
            .unwrap_or(0);
        
        let metadata = stats::metadata::mib()
            .and_then(|mib| mib.read())
            .unwrap_or(0);
        
        return AllocatorStats {
            allocator: "jemalloc",
            allocated,
            resident,
            metadata,
        };
    }
    
    #[cfg(all(feature = "mimalloc-allocator", target_env = "msvc"))]
    {
        return AllocatorStats {
            allocator: "mimalloc",
            allocated: 0,
            resident: 0,
            metadata: 0,
        };
    }
    
    #[cfg(not(any(
        all(feature = "jemalloc", not(target_env = "msvc")),
        all(feature = "mimalloc-allocator", target_env = "msvc")
    )))]
    {
        AllocatorStats {
            allocator: "system",
            allocated: 0,
            resident: 0,
            metadata: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AllocatorStats {
    pub allocator: &'static str,
    pub allocated: usize,
    pub resident: usize,
    pub metadata: usize,
}