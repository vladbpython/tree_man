use std::sync::{Arc, Weak};
use arc_swap::ArcSwap;


// DataStorage - Architecture


pub enum DataStorage<T>
where
    T: Send + Sync,
{
    Owned {
        // Текущее состояние
        source: Arc<Vec<Arc<T>>>,
        current_indices: ArcSwap<Vec<usize>>,
        current_cache: ArcSwap<Option<Arc<Vec<Arc<T>>>>>,
        full_indices: Arc<Vec<usize>>,
        // История для навигации
        levels: ArcSwap<Vec<Arc<Vec<Arc<T>>>>>, // кеш
        level_indices: ArcSwap<Vec<Arc<Vec<usize>>>>, // Индексы для навигации
    },
    Indexed {
        // Текущее состояние
        parent_data: Weak<Vec<Arc<T>>>,
        source_indices: Arc<Vec<usize>>,
        current_indices: ArcSwap<Vec<usize>>,
        // История для навигации
        index_levels: ArcSwap<Vec<Arc<Vec<usize>>>>, // Индексы для навигации
    },
}