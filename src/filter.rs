use super::{
    index::{
        decimal::{
            BucketedDecimalIndexWrapper,
        },
        float::{
            Float,
            FloatRangeBounds,
            BucketedFloatIndexWrapper
        },
        storage::{DataStorage,Index},
    },
    model::MemoryStats,
    bit_index::{BitIndex, BitOp, BitOpResult,BitIndexView},
};
use arc_swap::ArcSwap;
use parking_lot::RwLock;
use rayon::prelude::*;
use roaring::RoaringBitmap;
use rust_decimal::Decimal;
use std::{
    any::Any,
    collections::{HashMap,BTreeMap},
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering}
    },
};


const MAX_HISTORY: usize = 50;
const MAX_RETRIES: usize = 3;

// FilterData

pub struct FilterData<T>
where
    T: Send + Sync,
{
    storage: DataStorage<T>,
    
    levels: Option<ArcSwap<Vec<Arc<Vec<Arc<T>>>>>>,
    level_info: Option<ArcSwap<Vec<Arc<str>>>>,
    current_level: Option<Arc<AtomicUsize>>,
    
    write_lock: RwLock<()>,
    
    indexes: ArcSwap<BTreeMap<String, Arc<dyn Any + Send + Sync>>>,
    index_builders: Arc<RwLock<BTreeMap<String, Arc<dyn Fn(&[Arc<T>]) + Send + Sync>>>>,
}

impl<T> FilterData<T>
where
    T: Send + Sync + 'static,
{
    // Constructors
    
    pub fn from_vec(items: Vec<T>) -> Self {
        let len = items.len();
        
        let arc_items = match len {
            0..=499 => {
                Arc::new(items.into_iter().map(Arc::new).collect())
            }
            500..=50_000 => {
                let mut arcs = Vec::with_capacity(len);
                arcs.par_extend(items.into_par_iter().map(Arc::new));
                Arc::new(arcs)
            }
            _ => {
                Arc::new(
                    items
                        .into_par_iter()
                        .with_min_len(10_000)
                        .map(Arc::new)
                        .collect()
                )
            }
        };
        
        Self {
            storage: DataStorage::Owned {
                source: Arc::clone(&arc_items),
                current: ArcSwap::new(Arc::clone(&arc_items)),
            },
            levels: Some(ArcSwap::from_pointee(vec![Arc::clone(&arc_items)])),
            level_info: Some(ArcSwap::from_pointee(vec![Arc::from("Source")])),
            current_level: Some(Arc::new(AtomicUsize::new(0))),
            write_lock: RwLock::new(()),
            indexes: ArcSwap::from_pointee(BTreeMap::new()),
            index_builders: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn from_vec_arc_value(items: Vec<Arc<T>>) -> Self {
        let arc_items = Arc::new(items);
        
        Self {
            storage: DataStorage::Owned {
                source: Arc::clone(&arc_items),
                current: ArcSwap::new(Arc::clone(&arc_items)),
            },
            levels: Some(ArcSwap::from_pointee(vec![Arc::clone(&arc_items)])),
            level_info: Some(ArcSwap::from_pointee(vec![Arc::from("Source")])),
            current_level: Some(Arc::new(AtomicUsize::new(0))),
            write_lock: RwLock::new(()),
            indexes: ArcSwap::from_pointee(BTreeMap::new()),
            index_builders: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
    
    pub fn from_indices(parent_data: &Arc<Vec<Arc<T>>>, indices: Vec<usize>) -> Self {
        Self {
            storage: DataStorage::Indexed {
                parent_data: Arc::downgrade(parent_data),
                source_indices: Arc::new(indices.clone()),
                current_indices: ArcSwap::new(Arc::new(indices)),
            },
            levels: None,
            level_info: None,
            current_level: None,
            write_lock: RwLock::new(()),
            indexes: ArcSwap::from_pointee(BTreeMap::new()),
            index_builders: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
    
    // Core Access Methods
    
    pub fn items(&self) -> Arc<Vec<Arc<T>>> {
        match &self.storage {
            DataStorage::Owned { current, .. } => {
                current.load_full()
            }
            DataStorage::Indexed { parent_data, current_indices, .. } => {
                if let Some(parent) = parent_data.upgrade() {
                    let indices_guard = current_indices.load();
                    let items: Vec<Arc<T>> = if indices_guard.len() > 100_000 {
                        indices_guard
                            .par_iter()  // ← Параллельно!
                            .filter_map(|&idx| parent.get(idx).cloned())
                            .collect()
                    } else {
                        indices_guard
                            .iter()
                            .filter_map(|&idx| parent.get(idx).cloned())
                            .collect()
                    };
                    Arc::new(items)
                } else {
                    Arc::new(Vec::new())
                }
            }
        }
    }
    
    pub fn parent_data(&self) -> Option<Arc<Vec<Arc<T>>>> {
        match &self.storage {
            DataStorage::Owned { source, .. } => Some(Arc::clone(source)),
            DataStorage::Indexed { parent_data, .. } => parent_data.upgrade(),
        }
    }
    
    pub fn is_valid(&self) -> bool {
        match &self.storage {
            DataStorage::Owned { .. } => true,
            DataStorage::Indexed { parent_data, .. } => parent_data.strong_count() > 0,
        }
    }
    
    pub fn source_indices(&self) -> Vec<usize> {
        match &self.storage {
            DataStorage::Owned { source, .. } => (0..source.len()).collect(),
            DataStorage::Indexed { source_indices, .. } => (**source_indices).clone(),
        }
    }
    

    pub fn current_indices(&self) -> Vec<usize> {
        match &self.storage {
            DataStorage::Owned { current, source } => {
                let current_guard = current.load();
                // Fast path
                if Arc::ptr_eq(&current_guard, source) {
                    return (0..current_guard.len()).collect();
                }
                // HashMap для O(n)
                let source_map: HashMap<*const T, usize> = source
                    .iter()
                    .enumerate()
                    .map(|(idx, item)| (Arc::as_ptr(item) as *const T, idx))
                    .collect();
                current_guard
                    .iter()
                    .filter_map(|item| {
                        let ptr = Arc::as_ptr(item) as *const T;
                        source_map.get(&ptr).copied()
                    })
                    .collect()
            }
            DataStorage::Indexed { current_indices, .. } => {
                (**current_indices.load()).clone()
            }
        }
    }

    // Index-based API - ОПТИМИЗИРОВАНО с RoaringBitmap 
    
    // Применить индексы к данным (финальная материализация)
    // 
    // # Пример
    // ```
    // let indices = data.get_indices_by_bucketed_float_range_f64("price", 2000.0..3000.0, 100.0);
    // let items = data.apply_indices(&indices);
    // ```
    pub fn apply_indices(&self, indices: &[usize]) -> Vec<Arc<T>> {
        let items = self.items();
        // Оптимизация: сортируем для лучшей cache locality
        /*let sorted_needed = !indices.windows(2).all(|w| w[0] <= w[1]);
        if sorted_needed {
            println!("I AM HERE TO SORT");
            let mut sorted_indices = indices.to_vec();
            sorted_indices.sort_unstable();
            if sorted_indices.len() > 100_000 {
                return sorted_indices
                    .par_iter()
                    .filter_map(|&idx| items.get(idx).cloned())
                    .collect();
            } else {
                return sorted_indices
                    .iter()
                    .filter_map(|&idx| items.get(idx).cloned())
                    .collect();
            }
        }*/
        // Уже отсортированы - используем как есть
        if indices.len() > 100_000 {
            indices
                .par_iter()
                .filter_map(|&idx| items.get(idx).cloned())
                .collect()
        } else {
            indices
                .iter()
                .filter_map(|&idx| items.get(idx).cloned())
                .collect()
        }
    }
    
    // Пересечение индексов (AND) через RoaringBitmap 
    // 
    // Возвращает индексы элементов, которые присутствуют во ВСЕХ переданных массивах.
    // Использует RoaringBitmap для эффективного битового AND.
    // 
    // # Пример
    // ```
    // let a = vec![1, 2, 3, 4, 5];
    // let b = vec![2, 4, 6, 8];
    // let result = FilterData::intersect_indices(&a, &b);
    // assert_eq!(result, vec![2, 4]);
    // ```
    pub fn intersect_indices(a: &[usize], b: &[usize]) -> Vec<usize> {
        if a.is_empty() || b.is_empty() {
            return Vec::new();
        }
        let bitmap_a: RoaringBitmap = a.iter().map(|&i| i as u32).collect();
        let bitmap_b: RoaringBitmap = b.iter().map(|&i| i as u32).collect();
        let result = bitmap_a & bitmap_b;
        result.iter().map(|i| i as usize).collect()
    }
    
    // Объединение индексов (OR) через RoaringBitmap 
    // 
    // Возвращает индексы элементов из любого из переданных массивов (без дубликатов).
    // 
    // # Пример
    // ```
    // let a = vec![1, 2, 3];
    // let b = vec![3, 4, 5];
    // let result = FilterData::union_indices(&a, &b);
    // assert_eq!(result, vec![1, 2, 3, 4, 5]);
    // ```
    pub fn union_indices(a: &[usize], b: &[usize]) -> Vec<usize> {
        if a.is_empty() {
            return b.to_vec();
        }
        if b.is_empty() {
            return a.to_vec();
        }
        let bitmap_a: RoaringBitmap = a.iter().map(|&i| i as u32).collect();
        let bitmap_b: RoaringBitmap = b.iter().map(|&i| i as u32).collect();
        let result = bitmap_a | bitmap_b;
        result.iter().map(|i| i as usize).collect()
    }
    
    // Разность индексов (A - B) через RoaringBitmap 
    // 
    // Возвращает индексы из `a`, которые НЕ присутствуют в `b`.
    // 
    // # Пример
    // ```
    // let a = vec![1, 2, 3, 4, 5];
    // let b = vec![2, 4];
    // let result = FilterData::difference_indices(&a, &b);
    // assert_eq!(result, vec![1, 3, 5]);
    // ```
    pub fn difference_indices(a: &[usize], b: &[usize]) -> Vec<usize> {
        if a.is_empty() {
            return Vec::new();
        }
        if b.is_empty() {
            return a.to_vec();
        }
        let bitmap_a: RoaringBitmap = a.iter().map(|&i| i as u32).collect();
        let bitmap_b: RoaringBitmap = b.iter().map(|&i| i as u32).collect();
        let result = bitmap_a - bitmap_b;
        result.iter().map(|i| i as usize).collect()
    }
    
    // Множественное пересечение индексов (многопоточное) 
    // 
    // # Пример
    // ```
    // let indices = vec![indices1, indices2, indices3];
    // let result = FilterData::intersect_multiple_indices(&indices);
    // ```
    pub fn intersect_multiple_indices(indices_list: &[Vec<usize>]) -> Vec<usize> {
        if indices_list.is_empty() {
            return Vec::new();
        }
        if indices_list.len() == 1 {
            return indices_list[0].clone();
        }
        let bitmaps: Vec<RoaringBitmap> = indices_list
            .par_iter()
            .map(|indices| indices.iter().map(|&i| i as u32).collect())
            .collect();
        let mut result = bitmaps[0].clone();
        for bitmap in &bitmaps[1..] {
            result &= bitmap;
            
            if result.is_empty() {
                return Vec::new();
            }
        }
        result.iter().map(|i| i as usize).collect()
    }
    
    // Множественное объединение индексов (многопоточное) 
    // 
    // # Пример
    // ```
    // let indices = vec![indices1, indices2, indices3];
    // let result = FilterData::union_multiple_indices(&indices);
    // ```
    pub fn union_multiple_indices(indices_list: &[Vec<usize>]) -> Vec<usize> {
        if indices_list.is_empty() {
            return Vec::new();
        }
        if indices_list.len() == 1 {
            return indices_list[0].clone();
        }
        let bitmaps: Vec<RoaringBitmap> = indices_list
            .par_iter()
            .map(|indices| indices.iter().map(|&i| i as u32).collect())
            .collect();
        let mut result = bitmaps[0].clone();
        for bitmap in &bitmaps[1..] {
            result |= bitmap;
        }
        result.iter().map(|i| i as usize).collect()
    }
    
    // Симметричная разность (XOR) 
    pub fn symmetric_difference_indices(a: &[usize], b: &[usize]) -> Vec<usize> {
        if a.is_empty() {
            return b.to_vec();
        }
        if b.is_empty() {
            return a.to_vec();
        }
        let bitmap_a: RoaringBitmap = a.iter().map(|&i| i as u32).collect();
        let bitmap_b: RoaringBitmap = b.iter().map(|&i| i as u32).collect();
        let result = bitmap_a ^ bitmap_b;
        result.iter().map(|i| i as usize).collect()
    }
    
    // Проверка пересечения 
    pub fn has_intersection(a: &[usize], b: &[usize]) -> bool {
        if a.is_empty() || b.is_empty() {
            return false;
        }
        let bitmap_a: RoaringBitmap = a.iter().map(|&i| i as u32).collect();
        let bitmap_b: RoaringBitmap = b.iter().map(|&i| i as u32).collect();
        bitmap_a.intersection_len(&bitmap_b) > 0
    }
    
    // Подсчет пересечения 
    pub fn count_intersection(a: &[usize], b: &[usize]) -> usize {
        if a.is_empty() || b.is_empty() {
            return 0;
        }
        let bitmap_a: RoaringBitmap = a.iter().map(|&i| i as u32).collect();
        let bitmap_b: RoaringBitmap = b.iter().map(|&i| i as u32).collect();
        bitmap_a.intersection_len(&bitmap_b) as usize
    }
    
    // Проверка подмножества 
    pub fn is_subset(a: &[usize], b: &[usize]) -> bool {
        if a.is_empty() {
            return true;
        }
        if b.is_empty() {
            return false;
        }
        
        let bitmap_a: RoaringBitmap = a.iter().map(|&i| i as u32).collect();
        let bitmap_b: RoaringBitmap = b.iter().map(|&i| i as u32).collect();
        bitmap_a.is_subset(&bitmap_b)
    }
    
    // Конвертировать индексы в RoaringBitmap 
    pub fn indices_to_bitmap(indices: &[usize]) -> RoaringBitmap {
        indices.iter().map(|&i| i as u32).collect()
    }
    
    // Конвертировать RoaringBitmap в индексы 
    pub fn bitmap_to_indices(bitmap: &RoaringBitmap) -> Vec<usize> {
        bitmap.iter().map(|i| i as usize).collect()
    }

    // Standard Index Methods - возвращают ИНДЕКСЫ 

    pub fn create_index<K, F>(&self, name: &str, extractor: F) -> &Self
    where
        K: Ord + Clone + Send + Sync + 'static,
        F: Fn(&T) -> K + Send + Sync + 'static + Clone,
    {
        // Сначала удаляем старый индекс до взятия guard
        if self.has_index(name) {
            // drop_index берет свой собственный guard
            self.drop_index(name);
        }
        // Теперь безопасно создаем новый
        let _guard = self.write_lock.write();
        let parent_weak = match &self.storage {
            DataStorage::Owned { source, .. } => Arc::downgrade(source),
            DataStorage::Indexed { parent_data, .. } => parent_data.clone(),
        };
        let index = Arc::new(Index::new(&parent_weak));
        let items = self.items();
        if !index.build(&items, extractor.clone()) {
            #[cfg(debug_assertions)]
            eprintln!("WARNING: Failed to build index '{}'", name);
            return self;
        }
        let mut indexes = self.indexes.load().as_ref().clone();
        indexes.insert(
            name.to_string(), 
            index.clone() as Arc<dyn Any + Send + Sync>
        );
        self.indexes.store(Arc::new(indexes));
        let index_clone = index.clone();
        let builder = Arc::new(move |items: &[Arc<T>]| {
            index_clone.build(items, extractor.clone());
        }) as Arc<dyn Fn(&[Arc<T>]) + Send + Sync>;
        self.index_builders.write().insert(name.to_string(), builder);
        self
    }
    
    // Получить ИНДЕКСЫ по ключу 
    pub fn get_indices_by_index<K>(&self, index_name: &str, key: &K) -> Vec<usize>
    where
        K: Ord + Clone + Send + Sync + 'static,
    {
        let indexes = self.indexes.load();
        if let Some(index_any) = indexes.get(index_name) {
            if let Some(index) = index_any.downcast_ref::<Index<K, T>>() {
                if let Some(indices) = index.get_indices(key) {
                    return indices;
                }
                return Vec::new();
            }
        }
        Vec::new()
    }

    pub fn get_indices_by_index_keys<K>(&self, index_name: &str, keys: &[K]) -> Vec<usize>
    where
        K: Ord + Clone + Send + Sync + 'static,
    {
        if keys.is_empty() {
            return Vec::new();
        }
        if keys.len() == 1 {
            return self.get_indices_by_index(index_name, &keys[0]);
        }
        if keys.len() < 10 {
            let mut result = Vec::new();
            for key in keys {
                result.extend(self.get_indices_by_index(index_name, key));
            }
            return result;
        }
        let bitmaps: Vec<RoaringBitmap> = keys
            .par_iter()
            .map(|key| {
                let indices = self.get_indices_by_index(index_name, key);
                Self::indices_to_bitmap(&indices)
            })
            .collect();
        let mut result = RoaringBitmap::new();
        for bitmap in bitmaps {
            result |= bitmap;
        }
        Self::bitmap_to_indices(&result)
    }
    
    // Получить ИНДЕКСЫ по range 
    // 
    // Использует универсальный метод Index::range_indices_universal
    pub fn get_indices_by_index_range<K, R>(&self, index_name: &str, range: R) -> Vec<usize>
    where
        K: Ord + Clone + Send + Sync + 'static,
        R: std::ops::RangeBounds<K>,
    {
        let indexes = self.indexes.load();
        if let Some(index_any) = indexes.get(index_name) {
            if let Some(index) = index_any.downcast_ref::<Index<K, T>>() {
                return index.range_indices(range);  // ← Исправлено
            }
        }
        Vec::new()
    }
    
    // Convenience методы - возвращают данные
    
    pub fn filter_by_index<K>(&self, index_name: &str, key: &K) -> Vec<Arc<T>>
    where
        K: Ord + Clone + Send + Sync + 'static,
    {
        let indices = self.get_indices_by_index(index_name, key);
        self.apply_indices(&indices)
    }
    
    pub fn filter_by_index_keys<K>(&self, index_name: &str, keys: &[K]) -> Vec<Arc<T>>
    where
        K: Ord + Clone + Send + Sync + 'static,
    {
        let indices = self.get_indices_by_index_keys(index_name, keys);
        self.apply_indices(&indices)
    }
    
    // Получить данные по range 
    // 
    // Использует универсальный метод Index::range_items
    pub fn filter_by_index_range<K, R>(&self, index_name: &str, range: R) -> Vec<Arc<T>>
    where
        K: Ord + Clone + Send + Sync + 'static,
        R: std::ops::RangeBounds<K>,
    {
        let indexes = self.indexes.load();
        if let Some(index_any) = indexes.get(index_name) {
            if let Some(index) = index_any.downcast_ref::<Index<K, T>>() {
                return index.range_items(range);
            }
        }
        Vec::new()
    }
    
    pub fn apply_index_range<K, R>(&self, index_name: &str, range: R) -> &Self
    where
        K: Ord + Clone + Send + Sync + 'static,
        R: std::ops::RangeBounds<K>,
    {
        let filtered = self.filter_by_index_range(index_name, range);
        if filtered.is_empty() && self.len() > 0 {
            return self;
        }
        self.apply_filtered_items(filtered, format!("Range indexed by {}", index_name))
    }
    
    pub fn get_sorted_by_index<K>(&self, index_name: &str) -> Vec<Arc<T>>
    where
        K: Ord + Clone + Send + Sync + 'static,
    {
        let indexes = self.indexes.load();
        if let Some(index_any) = indexes.get(index_name) {
            if let Some(index) = index_any.downcast_ref::<Index<K, T>>() {
                let all_keys = index.keys();
                
                return all_keys
                    .into_par_iter()
                    .flat_map(|key| {
                        index.get(&key).unwrap_or_default()
                    })
                    .collect();
            }
        }
        Vec::new()
    }
    
    pub fn get_top_n_by_index<K>(&self, index_name: &str, n: usize) -> Vec<Arc<T>>
    where
        K: Ord + Clone + Send + Sync + 'static,
    {
        let indexes = self.indexes.load();
        if let Some(index_any) = indexes.get(index_name) {
            if let Some(index) = index_any.downcast_ref::<Index<K, T>>() {
                let keys = index.keys();
                let top_keys: Vec<_> = keys.into_iter().rev().take(n).collect();
                return top_keys
                    .into_par_iter()
                    .flat_map(|key| {
                        index.get(&key).unwrap_or_default()
                    })
                    .collect();
            }
        }
        Vec::new()
    }
    
    pub fn get_bottom_n_by_index<K>(&self, index_name: &str, n: usize) -> Vec<Arc<T>>
    where
        K: Ord + Clone + Send + Sync + 'static,
    {
        let indexes = self.indexes.load();
        if let Some(index_any) = indexes.get(index_name) {
            if let Some(index) = index_any.downcast_ref::<Index<K, T>>() {
                let keys = index.keys();
                let bottom_keys: Vec<_> = keys.into_iter().take(n).collect();
                return bottom_keys
                    .into_par_iter()
                    .flat_map(|key| {
                        index.get(&key).unwrap_or_default()
                    })
                    .collect();
            }
        }
        Vec::new()
    }
    
    pub fn get_index_min<K>(&self, index_name: &str) -> Option<K>
    where
        K: Ord + Clone + Send + Sync + 'static,
    {
        let indexes = self.indexes.load();
        if let Some(index_any) = indexes.get(index_name) {
            if let Some(index) = index_any.downcast_ref::<Index<K, T>>() {
                return index.first_key();
            }
        }
        None
    }
    
    pub fn get_index_max<K>(&self, index_name: &str) -> Option<K>
    where
        K: Ord + Clone + Send + Sync + 'static,
    {
        let indexes = self.indexes.load();
        if let Some(index_any) = indexes.get(index_name) {
            if let Some(index) = index_any.downcast_ref::<Index<K, T>>() {
                return index.last_key();
            }
        }
        None
    }
    
    pub fn get_index_keys<K>(&self, index_name: &str) -> Vec<K>
    where
        K: Ord + Clone + Send + Sync + 'static,
    {
        let indexes = self.indexes.load();
        if let Some(index_any) = indexes.get(index_name) {
            if let Some(index) = index_any.downcast_ref::<Index<K, T>>() {
                return index.keys();
            }
        }
        Vec::new()
    }
    
    pub fn apply_index_filter<K>(&self, index_name: &str, key: &K) -> &Self
    where
        K: Ord + Clone + Send + Sync + 'static,
    {
        let filtered = self.filter_by_index(index_name, key);
        if filtered.is_empty() && self.len() > 0 {
            return self;
        }
        self.apply_filtered_items(filtered, format!("Indexed by {}", index_name))
    }
    
    pub fn apply_index_filter_keys<K>(&self, index_name: &str, keys: &[K]) -> &Self
    where
        K: Ord + Clone + Send + Sync + 'static,
    {
        let filtered = self.filter_by_index_keys(index_name, keys);
        if filtered.is_empty() && self.len() > 0 {
            return self;
        }
        self.apply_filtered_items(
            filtered, 
            format!("Indexed by {} ({} keys)", index_name, keys.len())
        )
    }
    
    fn apply_filtered_items(&self, items: Vec<Arc<T>>, info: String) -> &Self {
        if let DataStorage::Owned { current, .. } = &self.storage {
            if let (Some(levels), Some(level_info), Some(current_level)) = 
                (&self.levels, &self.level_info, &self.current_level) 
            {
                let _guard = self.write_lock.write();
                let filtered_arc = Arc::new(items);
                let levels_guard = levels.load();
                let mut new_levels = if levels_guard.len() < MAX_HISTORY {
                    levels_guard.to_vec()
                } else {
                    let start = levels_guard.len().saturating_sub(MAX_HISTORY - 1);
                    levels_guard[start..].to_vec()
                };
                new_levels.push(Arc::clone(&filtered_arc));
                let adjusted_level = new_levels.len() - 1;
                levels.store(Arc::new(new_levels));
                let info_guard = level_info.load();
                let mut new_info = if info_guard.len() < MAX_HISTORY {
                    info_guard.to_vec()
                } else {
                    let start = info_guard.len().saturating_sub(MAX_HISTORY - 1);
                    info_guard[start..].to_vec()
                };
                new_info.push(Arc::from(info));
                level_info.store(Arc::new(new_info));
                current_level.store(adjusted_level, Ordering::Release);
                current.store(filtered_arc);
                drop(_guard);
                self.rebuild_indexes();
            }
        }
        
        self
    }
    
    pub fn has_index(&self, name: &str) -> bool {
        self.indexes.load().contains_key(name)
    }
    
    pub fn drop_index(&self, name: &str) -> &Self {
        let _guard = self.write_lock.write();
        let mut indexes = self.indexes.load().as_ref().clone();
        indexes.remove(name);
        self.indexes.store(Arc::new(indexes));
        self.index_builders.write().remove(name);
        self
    }

    // Очистить только битовые индексы
    pub fn clear_bit_indexes(&self) {
        let _guard = self.write_lock.write();
        let mut indexes = self.indexes.load().as_ref().clone();
        let mut builders = self.index_builders.write();
        // Удаляем все битовые индексы за один проход
        indexes.retain(|key, _| !key.starts_with("bit:"));
        builders.retain(|key, _| !key.starts_with("bit:"));
        self.indexes.store(Arc::new(indexes));
    }

    // Очистить только обычные индексы (не битовые)
    pub fn clear_regular_indexes(&self) {
        let _guard = self.write_lock.write();
        let mut indexes = self.indexes.load().as_ref().clone();
        let mut builders = self.index_builders.write();
        // Удаляем все НЕ битовые индексы за один проход
        indexes.retain(|key, _| key.starts_with("bit:"));
        builders.retain(|key, _| key.starts_with("bit:"));
        self.indexes.store(Arc::new(indexes));
    }

    // Очистить все индексы
    pub fn clear_all_indexes(&self) {
        let _guard = self.write_lock.write();
        // Просто создаем новые пустые коллекции
        self.indexes.store(Arc::new(BTreeMap::new()));
        *self.index_builders.write() = BTreeMap::new();
    }
    
    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.load().keys().cloned().collect()
    }
    
    fn rebuild_indexes(&self) {
        let items = self.items();
        let builders = self.index_builders.read();
        for builder in builders.values() {
            builder(&items);
        }
    }
    
    pub fn validate_indexes(&self) -> bool {
        let indexes = self.indexes.load();
        for _index_any in indexes.values() {
            match &self.storage {
                DataStorage::Owned { .. } => continue,
                DataStorage::Indexed { parent_data, .. } => {
                    if parent_data.strong_count() == 0 {
                        return false;
                    }
                }
            }
        }
        true
    }

    // Decimal Index Methods

    // Создать индекс для Decimal (обычный)
    pub fn create_decimal_index<F>(&self, name: &str, extractor: F) -> &Self
    where
        F: Fn(&T) -> Decimal + Send + Sync + 'static + Clone,
    {
        // Decimal уже Ord - wrapper НЕ нужен!
        self.create_index(name, extractor)
    }

    // Создать bucketed индекс для Decimal
    pub fn create_bucketed_decimal_index<F>(
        &self,
        name: &str,
        extractor: F,
        bucket_size: Decimal,
    ) -> &Self
    where
        F: Fn(&T) -> Decimal + Send + Sync + 'static + Clone,
    {
        if self.has_index(name) {
            self.drop_index(name);
        }

        let _guard = self.write_lock.write();
        let wrapper = Arc::new(BucketedDecimalIndexWrapper::new(bucket_size));
        let items = self.items();
        wrapper.build(&items, extractor.clone());
        let mut indexes = self.indexes.load().as_ref().clone();
        indexes.insert(
            name.to_string(),
            wrapper.clone() as Arc<dyn Any + Send + Sync>
        );
        self.indexes.store(Arc::new(indexes));
        let wrapper_clone = wrapper.clone();
        let builder = Arc::new(move |items: &[Arc<T>]| {
            wrapper_clone.build(items, extractor.clone());
        }) as Arc<dyn Fn(&[Arc<T>]) + Send + Sync>;
        self.index_builders.write().insert(name.to_string(), builder);
        self
    }

    // Получить ИНДЕКСЫ через bucketed Decimal range 
    // 
    // Возвращает отсортированные индексы для cache-friendly доступа.
    // 
    // 
    // # Пример
    // ```
    // use rust_decimal_macros::dec;
    // 
    // let indices = data.get_indices_by_bucketed_decimal_range(
    //     "price_bucketed",
    //     dec!(1000)..dec!(2000),
    //     dec!(100)
    // );
    // 
    // // Материализуем только нужные элементы
    // let items = data.apply_indices(&indices);
    // ```
    pub fn get_indices_by_bucketed_decimal_range<R>(
        &self,
        index_name: &str,
        range: R,
        _bucket_size: Decimal,  // Для API совместимости
    ) -> Vec<usize>
    where
        R: std::ops::RangeBounds<Decimal>,
    {
        let indexes = self.indexes.load();
        let index_any = match indexes.get(index_name) {
            Some(idx) => idx,
            None => return Vec::new(),
        };
        let wrapper = match index_any.downcast_ref::<BucketedDecimalIndexWrapper<T>>() {
            Some(w) => w,
            None => return Vec::new(),
        };
        wrapper.range_query_indices(range)
    }
    
    // Фильтрация по Decimal индексу
    pub fn filter_by_decimal(&self, index_name: &str, value: Decimal) -> Vec<Arc<T>> {
        self.filter_by_index(index_name, &value)
    }
    
    // Range query для Decimal
    pub fn filter_by_decimal_range<R>(&self, index_name: &str, range: R) -> Vec<Arc<T>>
    where
        R: std::ops::RangeBounds<Decimal>,
    {
        self.filter_by_index_range(index_name, range)
    }

    // Float Index Methods
    
    pub fn create_float_index_f32<F>(&self, name: &str, extractor: F) -> &Self
    where
        F: Fn(&T) -> f32 + Send + Sync + 'static + Clone,
    {
        self.create_index(name, move |item| Float(extractor(item)))
    }
    
    pub fn create_float_index_f64<F>(&self, name: &str, extractor: F) -> &Self
    where
        F: Fn(&T) -> f64 + Send + Sync + 'static + Clone,
    {
        self.create_index(name, move |item| Float(extractor(item)))
    }
    
    pub fn filter_by_float_f32(&self, index_name: &str, value: f32) -> Vec<Arc<T>> {
        self.filter_by_index(index_name, &Float(value))
    }
    
    pub fn filter_by_float_f64(&self, index_name: &str, value: f64) -> Vec<Arc<T>> {
        self.filter_by_index(index_name, &Float(value))
    }
    
    pub fn filter_by_float_range_f32<R>(&self, index_name: &str, range: R) -> Vec<Arc<T>>
    where
        R: FloatRangeBounds<f32>,
    {
        self.filter_by_index_range(index_name, range.to_ordered_range())
    }
    
    pub fn filter_by_float_range_f64<R>(&self, index_name: &str, range: R) -> Vec<Arc<T>>
    where
        R: FloatRangeBounds<f64>,
    {
        self.filter_by_index_range(index_name, range.to_ordered_range())
    }
    
    pub fn get_float_min_f32(&self, index_name: &str) -> Option<f32> {
        self.get_index_min::<Float<f32>>(index_name).map(|of| of.0)
    }
    
    pub fn get_float_max_f32(&self, index_name: &str) -> Option<f32> {
        self.get_index_max::<Float<f32>>(index_name).map(|of| of.0)
    }
    
    pub fn get_float_min_f64(&self, index_name: &str) -> Option<f64> {
        self.get_index_min::<Float<f64>>(index_name).map(|of| of.0)
    }
    
    pub fn get_float_max_f64(&self, index_name: &str) -> Option<f64> {
        self.get_index_max::<Float<f64>>(index_name).map(|of| of.0)
    }

    // Bucketed Float Index Methods - возвращают ИНДЕКСЫ 
    
    pub fn create_bucketed_float_index_f64<F>(
        &self, 
        name: &str, 
        extractor: F,
        bucket_size: f64
    ) -> &Self
    where
        F: Fn(&T) -> f64 + Send + Sync + 'static + Clone,
    {
        if self.has_index(name) {
            self.drop_index(name);
        }
        let _guard = self.write_lock.write();
        let wrapper = Arc::new(BucketedFloatIndexWrapper::new(bucket_size));
        let items = self.items();
        wrapper.build(&items, extractor.clone());
        let mut indexes = self.indexes.load().as_ref().clone();
        indexes.insert(
            name.to_string(),
            wrapper.clone() as Arc<dyn Any + Send + Sync>
        );
        self.indexes.store(Arc::new(indexes));
        let wrapper_clone = wrapper.clone();
        let builder = Arc::new(move |items: &[Arc<T>]| {
            wrapper_clone.build(items, extractor.clone());
        }) as Arc<dyn Fn(&[Arc<T>]) + Send + Sync>;
        self.index_builders.write().insert(name.to_string(), builder);
        self
    }
    
    // Получить ИНДЕКСЫ через bucketed float range 
    pub fn get_indices_by_bucketed_float_range_f64<R>(
        &self,
        index_name: &str,
        range: R,
        _bucket_size: f64
    ) -> Vec<usize>
    where
        R: std::ops::RangeBounds<f64>,
    {
        let indexes = self.indexes.load();
        let index_any = match indexes.get(index_name) {
            Some(idx) => idx,
            None => return Vec::new(),
        };
        let wrapper = match index_any.downcast_ref::<BucketedFloatIndexWrapper<T>>() {
            Some(w) => w,
            None => return Vec::new(),
        };
        wrapper.range_query_indices(range)
    }
    
    pub fn filter_by_bucketed_float_range_f64<R>(
        &self,
        index_name: &str,
        range: R,
        bucket_size: f64
    ) -> Vec<Arc<T>>
    where
        R: std::ops::RangeBounds<f64>,
    {
        let indices = self.get_indices_by_bucketed_float_range_f64(index_name, range, bucket_size);
        self.apply_indices(&indices)
    }


    // Bit Index Methods

    pub fn create_bit_index<F>(&self, name: &str, predicate: F) -> &Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static + Clone,
    {
        let bit_index_key = format!("bit:{}", name);
        if self.has_index(&bit_index_key) {
            self.drop_index(&bit_index_key);
        }
        let _guard = self.write_lock.write();
        let bit_index = Arc::new(BitIndex::new());
        let items = self.items();
        bit_index.build(&items, predicate.clone());
        let mut indexes = self.indexes.load().as_ref().clone();
        indexes.insert(
            bit_index_key.clone(),
            bit_index.clone() as Arc<dyn Any + Send + Sync>
        );
        self.indexes.store(Arc::new(indexes));
        let bit_index_clone = bit_index.clone();
        let builder = Arc::new(move |items: &[Arc<T>]| {
            bit_index_clone.build(items, predicate.clone());
        }) as Arc<dyn Fn(&[Arc<T>]) + Send + Sync>;
        self.index_builders.write().insert(
            bit_index_key,
            builder
        );
        self
    }

    // Получить ИНДЕКСЫ через битовый индекс 
    pub fn get_indices_by_bit_index(&self, name: &str) -> Vec<usize> {
        let indexes = self.indexes.load();
        let key = format!("bit:{}", name);
        if let Some(index_any) = indexes.get(&key) {
            if let Some(bit_index) = index_any.downcast_ref::<BitIndex>() {
                return bit_index.to_indices();
            }
        }
        Vec::new()
    }
    
    // Битовая операция возвращающая ИНДЕКСЫ 
    pub fn bit_operation_indices(&self, operations: &[(&str, BitOp)]) -> Vec<usize> {
        let result = self.bit_operation(operations);
        result.to_indices()
    }
    
    // Пересечение битовых индексов - возвращает ИНДЕКСЫ 
    // 
    // Возвращает индексы элементов, которые удовлетворяют ВСЕМ указанным битовым индексам.
    // Использует эффективные битовые операции RoaringBitmap.
    // 
    // # Пример
    // ```
    // data.create_bit_index("bullish", |c| c.close > c.open);
    // data.create_bit_index("high_volume", |c| c.volume > 5000.0);
    // 
    // let indices = data.intersect_bit_indices_get_indices(&["bullish", "high_volume"]);
    // let items = data.apply_indices(&indices);
    // ```
    pub fn intersect_bit_indices_get_indices(&self, names: &[&str]) -> Vec<usize> {
        if names.is_empty() {
            return Vec::new();
        }
        let indexes = self.indexes.load();
        let key = format!("bit:{}", names[0]);
        let first_index = match indexes.get(&key) {
            Some(idx) => match idx.downcast_ref::<BitIndex>() {
                Some(bi) => bi,
                None => return Vec::new(),
            },
            None => return Vec::new(),
        };
        if names.len() == 1 {
            return first_index.to_indices();
        }
        let mut other_indices = Vec::new();
        for &name in &names[1..] {
            let key = format!("bit:{}", name);
            if let Some(idx) = indexes.get(&key) {
                if let Some(bi) = idx.downcast_ref::<BitIndex>() {
                    other_indices.push(bi);
                }
            }
        }
        if other_indices.is_empty() {
            return first_index.to_indices();
        }
        let operations: Vec<(&BitIndex, BitOp)> = other_indices
            .iter()
            .map(|&bi| (bi, BitOp::And))
            .collect();
        
        let result = first_index.multi_operation(&operations);
        result.to_indices()
    }
    
    // Объединение битовых индексов - возвращает ИНДЕКСЫ 
    // 
    // Возвращает индексы элементов, которые удовлетворяют ХОТЯ БЫ ОДНОМУ из указанных битовых индексов.
    // 
    // # Пример
    // ```
    // data.create_bit_index("doji", |c| (c.close - c.open).abs() < 0.1);
    // data.create_bit_index("hammer", |c| c.low < c.open * 0.98);
    // 
    // let indices = data.union_bit_indices_get_indices(&["doji", "hammer"]);
    // let patterns = data.apply_indices(&indices);
    // ```
    pub fn union_bit_indices_get_indices(&self, names: &[&str]) -> Vec<usize> {
        if names.is_empty() {
            return Vec::new();
        }
        let indexes = self.indexes.load();
        let key = format!("bit:{}", names[0]);
        let first_index = match indexes.get(&key) {
            Some(idx) => match idx.downcast_ref::<BitIndex>() {
                Some(bi) => bi,
                None => return Vec::new(),
            },
            None => return Vec::new(),
        };
        if names.len() == 1 {
            return first_index.to_indices();
        }
        let mut other_indices = Vec::new();
        for &name in &names[1..] {
            let key = format!("bit:{}", name);
            if let Some(idx) = indexes.get(&key) {
                if let Some(bi) = idx.downcast_ref::<BitIndex>() {
                    other_indices.push(bi);
                }
            }
        }
        if other_indices.is_empty() {
            return first_index.to_indices();
        }
        let operations: Vec<(&BitIndex, BitOp)> = other_indices
            .iter()
            .map(|&bi| (bi, BitOp::Or))
            .collect();
        
        let result = first_index.multi_operation(&operations);
        result.to_indices()
    }
    
    // Разность битовых индексов - возвращает ИНДЕКСЫ 
    // 
    // Возвращает индексы элементов из индекса `a`, которые НЕ присутствуют в индексе `b`.
    // 
    // # Пример
    // ```
    // data.create_bit_index("bullish", |c| c.close > c.open);
    // data.create_bit_index("high_volume", |c| c.volume > 5000.0);
    // 
    // // Бычьи свечи с обычным объемом (не высоким)
    // let indices = data.difference_bit_indices_get_indices("bullish", "high_volume");
    // let result = data.apply_indices(&indices);
    // ```
    pub fn difference_bit_indices_get_indices(&self, a: &str, b: &str) -> Vec<usize> {
        let indexes = self.indexes.load();
        let key_a = format!("bit:{}", a);
        let key_b = format!("bit:{}", b);
        let index_a = match indexes.get(&key_a) {
            Some(idx) => match idx.downcast_ref::<BitIndex>() {
                Some(bi) => bi,
                None => return Vec::new(),
            },
            None => return Vec::new(),
        };
        let index_b = match indexes.get(&key_b) {
            Some(idx) => match idx.downcast_ref::<BitIndex>() {
                Some(bi) => bi,
                None => return Vec::new(),
            },
            None => return Vec::new(),
        };
        let result = index_a.difference(index_b);
        result.to_indices()
    }
    
    // Convenience методы - возвращают данные

    pub fn get_bit_index_result(&self, name: &str) -> Option<BitOpResult> {
        let indexes = self.indexes.load();
        let key = format!("bit:{}", name);
        if let Some(index_any) = indexes.get(&key) {
            if let Some(bit_index) = index_any.downcast_ref::<BitIndex>() {
                return Some(bit_index.get_result());
            }
        }
        
        None
    }

    // Фильтрация по битовому индексу (ОПТИМИЗИРОВАНО) 
    pub fn filter_by_bit_index(&self, name: &str) -> Vec<Arc<T>> {
        let indexes = self.indexes.load();
        let key = format!("bit:{}", name);
        if let Some(index_any) = indexes.get(&key) {
            if let Some(bit_index) = index_any.downcast_ref::<BitIndex>() {
                return bit_index.get_result().apply_to_fast(&self.items())
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    }

    pub fn filter_by_bit_index_parallel(&self, name: &str) -> Vec<Arc<T>> {
        let indices = self.get_indices_by_bit_index(name);
        if indices.len() > 10_000 {
            let items = self.items();
            indices
                .par_iter()
                .filter_map(|&idx| items.get(idx).cloned())
                .collect()
        } else {
            self.apply_indices(&indices)
        }
    }

    // Получить view на отфильтрованные данные
    pub fn view_by_bit_index(&self, name: &str) -> BitIndexView<T> {
        let indexes = self.indexes.load();
        let key = format!("bit:{}", name);
        if let Some(index_any) = indexes.get(&key) {
            if let Some(bit_index) = index_any.downcast_ref::<BitIndex>() {
                return BitIndexView::new(
                    self.items(),
                    bit_index.bitmap_arc(),
                )
            }
        }
        BitIndexView::new(
            Arc::new(Vec::new()),
            Arc::new(RoaringBitmap::new())
        )
    }

    #[inline]
    pub fn count_by_bit_index(&self, name: &str) -> usize {
        let indexes = self.indexes.load();
        let key = format!("bit:{}", name);
        if let Some(index_any) = indexes.get(&key) {
            if let Some(bit_index) = index_any.downcast_ref::<BitIndex>() {
                return bit_index.len();
            }
        }
        
        0
    }

    pub fn check_bit_index(&self, name: &str, position: usize) -> bool {
        let indexes = self.indexes.load();
        let key = format!("bit:{}", name);
        if let Some(index_any) = indexes.get(&key) {
            if let Some(bit_index) = index_any.downcast_ref::<BitIndex>() {
                return bit_index.get(position);
            }
        }
        
        false
    }

    pub fn bit_operation(&self, operations: &[(&str, BitOp)]) -> BitOpResult {
        if operations.is_empty() {
            return BitOpResult::empty();
        }
        let indexes = self.indexes.load();
        let key = format!("bit:{}", operations[0].0);
        let first_index = match indexes.get(&key) {
            Some(idx) => match idx.downcast_ref::<BitIndex>() {
                Some(bi) => bi,
                None => return BitOpResult::empty(),
            },
            None => return BitOpResult::empty(),
        };
        let other_operations: Vec<(&BitIndex, BitOp)> = operations[1..]
            .iter()
            .filter_map(|(name, op)| {
                let key = format!("bit:{}", name);
                indexes.get(&key)
                    .and_then(|idx| idx.downcast_ref::<BitIndex>())
                    .map(|bi| (bi, *op))
            })
            .collect();
        
        first_index.multi_operation(&other_operations)
    }

    pub fn apply_bit_operation(&self, operations: &[(&str, BitOp)]) -> &Self {
        let result = self.bit_operation(operations);
        let items = self.items();
        let filtered = result.apply_to_fast(&items);
        if filtered.is_empty() && self.len() > 0 {
            return self;
        }
        let op_desc = operations
            .iter()
            .map(|(name, op)| format!("{:?}({})", op, name))
            .collect::<Vec<_>>()
            .join(" ");
        self.apply_filtered_items(filtered, format!("Bit operation: {}", op_desc))
    }
    
    pub fn bit_index_stats(&self, name: &str) -> Option<(usize, usize)> {
        let indexes = self.indexes.load();
        let key = format!("bit:{}", name);
        if let Some(index_any) = indexes.get(&key) {
            if let Some(bit_index) = index_any.downcast_ref::<BitIndex>() {
                let ones = bit_index.count_ones();
                let zeros = bit_index.count_zeros();
                return Some((ones, zeros));
            }
        }
        None
    }
    
    // Пересечение битовых индексов - возвращает данные 
    // 
    // Возвращает элементы, которые удовлетворяют ВСЕМ указанным битовым индексам.
    // Это convenience метод, который вызывает `intersect_bit_indices_get_indices` 
    // и материализует результат.
    // 
    // # Пример
    // ```
    // data.create_bit_index("bullish", |c| c.close > c.open);
    // data.create_bit_index("high_volume", |c| c.volume > 5000.0);
    // 
    // let result = data.intersect_bit_indices(&["bullish", "high_volume"]);
    // println!("Found {} candles", result.len());
    // ```
    pub fn intersect_bit_indices(&self, names: &[&str]) -> Vec<Arc<T>> {
        let indices = self.intersect_bit_indices_get_indices(names);
        self.apply_indices(&indices)
    }
    
    // Объединение битовых индексов - возвращает данные 
    // 
    // Возвращает элементы, которые удовлетворяют ХОТЯ БЫ ОДНОМУ из указанных битовых индексов.
    // 
    // # Пример
    // ```
    // data.create_bit_index("doji", |c| (c.close - c.open).abs() < 0.1);
    // data.create_bit_index("hammer", |c| c.low < c.open * 0.98);
    // 
    // let patterns = data.union_bit_indices(&["doji", "hammer"]);
    // println!("Found {} pattern candles", patterns.len());
    // ```
    pub fn union_bit_indices(&self, names: &[&str]) -> Vec<Arc<T>> {
        let indices = self.union_bit_indices_get_indices(names);
        self.apply_indices(&indices)
    }
    
    // Разность битовых индексов - возвращает данные 
    // 
    // Возвращает элементы из индекса `a`, которые НЕ присутствуют в индексе `b`.
    // 
    // # Пример
    // ```
    // data.create_bit_index("bullish", |c| c.close > c.open);
    // data.create_bit_index("high_volume", |c| c.volume > 5000.0);
    // 
    // // Свечи с обычным объемом
    // let result = data.difference_bit_indices("bullish", "high_volume");
    // println!("Bullish with normal volume: {}", result.len());
    // ```
    pub fn difference_bit_indices(&self, a: &str, b: &str) -> Vec<Arc<T>> {
        let indices = self.difference_bit_indices_get_indices(a, b);
        self.apply_indices(&indices)
    }


    // Filter Methods

    fn filter_impl<F>(&self, predicate: F, retry_count: usize) -> &Self
    where
        F: Fn(&T) -> bool + Sync + Send,
    {
        match &self.storage {
            DataStorage::Owned { current, .. } => {
                if retry_count >= MAX_RETRIES {
                    return self;
                }
                let (current_level, levels, level_info) = match (&self.current_level, &self.levels, &self.level_info) {
                    (Some(cl), Some(l), Some(li)) => (cl, l, li),
                    _ => return self,
                };
                let (current_lvl, current_data) = {
                    let current_lvl = current_level.load(Ordering::Acquire);
                    let levels_guard = levels.load();
                    let current_data = match levels_guard.get(current_lvl).or_else(|| levels_guard.first()) {
                        Some(data) => Arc::clone(data),
                        None => return self,
                    };
                    (current_lvl, current_data)
                };
                let filtered: Vec<Arc<T>> = current_data
                    .par_iter()
                    .filter(|item| predicate(item))
                    .cloned()
                    .collect();
                
                let filtered_arc = Arc::new(filtered);
                {
                    let _guard = self.write_lock.write();
                    let actual_current = current_level.load(Ordering::Acquire);
                    if actual_current != current_lvl {
                        drop(_guard);
                        return self.filter_impl(predicate, retry_count + 1);
                    }
                    let levels_guard = levels.load();
                    let mut new_levels = if levels_guard.len() < MAX_HISTORY {
                        levels_guard.to_vec()
                    } else {
                        let start = levels_guard.len().saturating_sub(MAX_HISTORY - 1);
                        levels_guard[start..].to_vec()
                    };
                    new_levels.push(Arc::clone(&filtered_arc));
                    let adjusted_level = new_levels.len() - 1;
                    levels.store(Arc::new(new_levels));
                    let info_guard = level_info.load();
                    let mut new_info = if info_guard.len() < MAX_HISTORY {
                        info_guard.to_vec()
                    } else {
                        let start = info_guard.len().saturating_sub(MAX_HISTORY - 1);
                        info_guard[start..].to_vec()
                    };
                    new_info.push(Arc::from("Filtered"));
                    level_info.store(Arc::new(new_info));
                    current_level.store(adjusted_level, Ordering::Release);
                    current.store(filtered_arc);
                    drop(_guard);
                    self.rebuild_indexes();
                }
                self
            }
            
            DataStorage::Indexed { parent_data, current_indices, .. } => {
                if let Some(parent) = parent_data.upgrade() {
                    let _guard = self.write_lock.write();
                    let indices_guard = current_indices.load();
                    let filtered_indices: Vec<usize> = indices_guard
                        .par_iter()
                        .filter(|&&idx| {
                            if let Some(item) = parent.get(idx) {
                                predicate(item)
                            } else {
                                false
                            }
                        })
                        .copied()
                        .collect();
                    current_indices.store(Arc::new(filtered_indices));
                    drop(_guard);
                    self.rebuild_indexes();
                }
                self
            }
        }
    }

    pub fn filter<F>(&self, predicate: F) -> &Self
    where
        F: Fn(&T) -> bool + Sync + Send,
    {
        self.filter_impl(predicate, 0)
    }
    
    pub fn filter_adaptive<F>(&self, predicate: F) -> &Self
    where
        F: Fn(&T) -> bool + Sync + Send,
    {
        self.filter(predicate)
    }


    // Navigation Methods

    // Сброс к исходным данным с ПОЛНОЙ очисткой
    // 
    // Очищает:
    // - Все уровни фильтрации (кроме source)
    // - Все индексы (обычные + битовые)
    // - Историю операций
    pub fn reset_to_source(&self) -> &Self {
        match &self.storage {
            DataStorage::Owned { source, current, .. } => {
                if let (Some(levels), Some(level_info), Some(current_level)) = 
                    (&self.levels, &self.level_info, &self.current_level)
                {
                    let _guard = self.write_lock.write();
                    
                    let levels_guard = levels.load();
                    if let Some(level_0) = levels_guard.first() {
                        levels.store(Arc::new(vec![Arc::clone(level_0)]));
                    }
                    
                    level_info.store(Arc::new(vec![Arc::from("Source")]));
                    current_level.store(0, Ordering::Release);
                    current.store(Arc::clone(source));
                    drop(_guard);
                    // ОЧИЩАЕМ ВСЕ ИНДЕКСЫ перед пересборкой
                    self.clear_all_indexes();
                    // Теперь индексов нет, rebuild_indexes() ничего не сделает
                    self.rebuild_indexes();
                }
            }
            
            DataStorage::Indexed { source_indices, current_indices, .. } => {
                let _guard = self.write_lock.write();
                current_indices.store(Arc::clone(source_indices));
                drop(_guard);
                self.clear_all_indexes();
                self.rebuild_indexes();
               
            }
        }
        
        self
    }
    
    fn go_to_level(&self, target_level: usize) -> &Self {
        if let DataStorage::Owned { current, .. } = &self.storage {
            if let (Some(levels), Some(level_info), Some(current_level)) = 
                (&self.levels, &self.level_info, &self.current_level)
            {
                let _guard = self.write_lock.write();
                let levels_guard = levels.load();   
                if target_level >= levels_guard.len() {
                    return self;
                } 
                let new_levels = levels_guard[..=target_level].to_vec();
                levels.store(Arc::new(new_levels.clone()));     
                let info_guard = level_info.load();
                let new_info = info_guard[..=target_level].to_vec();
                level_info.store(Arc::new(new_info));         
                current_level.store(target_level, Ordering::Release);
                if let Some(target_data) = new_levels.get(target_level) {
                    current.store(Arc::clone(target_data));
                }
                
                drop(_guard);
                self.clear_all_indexes();
                self.rebuild_indexes();
            }
        }
        
        self
    }

    pub fn up(&self) -> &Self {
        if let Some(current_level) = &self.current_level {
            let current = current_level.load(Ordering::Acquire);
            if current > 0 {
                self.go_to_level(current - 1)
            } else {
                self
            }
        } else {
            self
        }
    }


    // Query Methods

    pub fn len(&self) -> usize {
        match &self.storage {
            DataStorage::Owned { current, .. } => {
                current.load().len()
            }
            DataStorage::Indexed { current_indices, .. } => {
                current_indices.load().len()
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn current_level(&self) -> usize {
        self.current_level.as_ref().map(|cl| cl.load(Ordering::Acquire)).unwrap_or(0)
    }

    pub fn stored_levels_count(&self) -> usize {
        self.levels.as_ref().map(|l| l.load().len()).unwrap_or(1)
    }

    pub fn total_stored_items(&self) -> usize {
        if let Some(levels) = &self.levels {
            levels.load()
                .iter()
                .map(|level| level.len())
                .sum()
        } else {
            self.len()
        }
    }

    pub fn memory_stats(&self) -> MemoryStats {
        match &self.storage {
            DataStorage::Owned { .. } => {
                if let (Some(levels), Some(current_level)) = (&self.levels, &self.current_level) {
                    let current_lvl = current_level.load(Ordering::Acquire);
                    let levels_guard = levels.load();
                    let mut stats = MemoryStats {
                        current_level: current_lvl,
                        stored_levels: levels_guard.len(),
                        current_level_items: 0,
                        total_stored_items: 0,
                        useful_items: 0,
                        wasted_items: 0,
                    };

                    for (idx, level_data) in levels_guard.iter().enumerate() {
                        let count = level_data.len();
                        stats.total_stored_items += count;
                        if idx == current_lvl {
                            stats.current_level_items = count;
                        }
                        
                        if idx <= current_lvl {
                            stats.useful_items += count;
                        } else {
                            stats.wasted_items += count;
                        }
                    }
                    stats
                } else {
                    MemoryStats {
                        current_level: 0,
                        stored_levels: 1,
                        current_level_items: self.len(),
                        total_stored_items: self.len(),
                        useful_items: self.len(),
                        wasted_items: 0,
                    }
                }
            }
            DataStorage::Indexed { .. } => {
                MemoryStats {
                    current_level: 0,
                    stored_levels: 1,
                    current_level_items: self.len(),
                    total_stored_items: self.len(),
                    useful_items: self.len(),
                    wasted_items: 0,
                }
            }
        }
    }
    
    pub fn level_name(&self, level: usize) -> Option<Arc<str>> {
        self.level_info.as_ref().and_then(|li| {
            li.load().get(level).map(Arc::clone)
        })
    }

    pub fn builder() -> FilterDataBuilder<T> {
        FilterDataBuilder::new()
    }
    
    pub fn new(data: Vec<T>) -> Self {
        Self::from_vec(data)
    }
}


// Builder

pub struct FilterDataBuilder<T>
where
    T: Send + Sync + 'static,
{
    data: Option<Vec<T>>,
    indexes: Vec<IndexDefinition<T>>,
    _phantom: PhantomData<T>,
}

struct IndexDefinition<T>
where
    T: Send + Sync + 'static,
{
    applier: Box<dyn FnOnce(&FilterData<T>) + Send>,
}

impl<T> FilterDataBuilder<T>
where
    T: Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            data: None,
            indexes: Vec::new(),
            _phantom: PhantomData,
        }
    }
    
    pub fn with_data(mut self, data: Vec<T>) -> Self {
        self.data = Some(data);
        self
    }
    
    pub fn with_index<K, F>(mut self, name: &str, extractor: F) -> Self
    where
        K: Ord + Clone + Send + Sync + 'static,
        F: Fn(&T) -> K + Send + Sync + 'static + Clone,
    {
        let name_owned = name.to_string();
        let extractor_clone = extractor.clone();
        let applier = Box::new(move |fd: &FilterData<T>| {
            fd.create_index(&name_owned, extractor_clone);
        }) as Box<dyn FnOnce(&FilterData<T>) + Send>;
        self.indexes.push(IndexDefinition {
            applier,
        });
        self
    }

    pub fn with_bit_index<F>(mut self, name: &str, predicate: F) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static + Clone,
    {
        let name_owned = name.to_string();
        let predicate_clone = predicate.clone();
        let applier = Box::new(move |fd: &FilterData<T>| {
            fd.create_bit_index(&name_owned, predicate_clone);
        }) as Box<dyn FnOnce(&FilterData<T>) + Send>;
        self.indexes.push(IndexDefinition {
            applier,
        });
        self
    }

    pub fn with_decimal_index<F>(mut self, name: &str, extractor: F) ->Self
    where
        F: Fn(&T) -> Decimal + Send + Sync + 'static + Clone,
    {
        let name_owned = name.to_string();
        let extractor_clone = extractor.clone();
        let applier = Box::new(move |fd: &FilterData<T>| {
            fd.create_decimal_index(&name_owned, extractor_clone);
        }) as Box<dyn FnOnce(&FilterData<T>) + Send>;
        self.indexes.push(IndexDefinition {
            applier,
        });
        
        self
    }

    pub fn with_bucketed_decimal_index<F>(
        mut self,
        name: &str,
        extractor: F,
        bucket_size: Decimal
    ) -> Self
    where
        F: Fn(&T) -> Decimal + Send + Sync + 'static + Clone,
    {
        let name_owned = name.to_string();
        let extractor_clone = extractor.clone();
        let applier = Box::new(move |fd: &FilterData<T>| {
            fd.create_bucketed_decimal_index(&name_owned, extractor_clone, bucket_size);
        }) as Box<dyn FnOnce(&FilterData<T>) + Send>;
        self.indexes.push(IndexDefinition {
            applier,
        });
        self
    }

    pub fn with_float_index_f32<F>(mut self, name: &str, extractor: F) -> Self
    where
        F: Fn(&T) -> f32 + Send + Sync + 'static + Clone,
    {
        let name_owned = name.to_string();
        let extractor_clone = extractor.clone();
        let applier = Box::new(move |fd: &FilterData<T>| {
            fd.create_float_index_f32(&name_owned, extractor_clone);
        }) as Box<dyn FnOnce(&FilterData<T>) + Send>;
        self.indexes.push(IndexDefinition {
            applier,
        });
        self
    }
    
    pub fn with_float_index_f64<F>(mut self, name: &str, extractor: F) -> Self
    where
        F: Fn(&T) -> f64 + Send + Sync + 'static + Clone,
    {
        let name_owned = name.to_string();
        let extractor_clone = extractor.clone();
        let applier = Box::new(move |fd: &FilterData<T>| {
            fd.create_float_index_f64(&name_owned, extractor_clone);
        }) as Box<dyn FnOnce(&FilterData<T>) + Send>;
        
        self.indexes.push(IndexDefinition {
            applier,
        });
        self
    }
    
    pub fn with_bucketed_float_index_f64<F>(
        mut self,
        name: &str,
        extractor: F,
        bucket_size: f64
    ) -> Self
    where
        F: Fn(&T) -> f64 + Send + Sync + 'static + Clone,
    {
        let name_owned = name.to_string();
        let extractor_clone = extractor.clone();
        let applier = Box::new(move |fd: &FilterData<T>| {
            fd.create_bucketed_float_index_f64(&name_owned, extractor_clone, bucket_size);
        }) as Box<dyn FnOnce(&FilterData<T>) + Send>;
        self.indexes.push(IndexDefinition {
            applier,
        });
        self
    }
    
    pub fn build(self) -> FilterData<T> {
        let data = self.data.expect("Data must be provided via with_data()");
        let fd = FilterData::from_vec(data);
        for index_def in self.indexes {
            (index_def.applier)(&fd);
        }
        
        fd
    }
}

impl<T> Default for FilterDataBuilder<T>
where
    T: Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

// Traits

pub trait IntoFilterData {
    type Item: Send + Sync;
    
    fn into_filtered(self) -> FilterData<Self::Item>;
}

impl<T: Send + Sync + 'static> IntoFilterData for Vec<T> {
    type Item = T;
    
    fn into_filtered(self) -> FilterData<T> {
        FilterData::from_vec(self)
    }
}


#[cfg(test)]
mod memory_leak_tests {
    use super::*;
    use std::sync::Arc;
    
    #[test]
    fn test_no_leak_repeated_index_creation() {
        let items: Vec<i32> = (0..10_000).collect();
        let data = FilterData::from_vec(items);
        // Создаем индекс многократно с тем же именем
        for i in 0..100 {
            data.create_index("test", move |&n| n % (i + 1));
        }
        // Должен быть только ОДИН индекс и ОДИН builder
        assert_eq!(data.list_indexes().len(), 1);
        assert_eq!(data.index_builders.read().len(), 1);
    }
    
    #[test]
    fn test_no_leak_many_filters() {
        use memory_stats::memory_stats;
        let start_mem = memory_stats().map(|m| m.physical_mem);
        for _ in 0..100 {
            let items: Vec<i32> = (0..10_000).collect();
            let data = FilterData::from_vec(items);
            // Много фильтраций
            for i in 0..50 {
                data.filter(|&n| n > i * 100);
            }
            // data дропается здесь
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        let end_mem = memory_stats().map(|m| m.physical_mem);
        if let (Some(start), Some(end)) = (start_mem, end_mem) {
            let diff = end.saturating_sub(start);
            println!("Memory growth: {} MB", diff / 1024 / 1024);
            
            assert!(diff < 50_000_000, 
                    "Possible memory leak! Growth: {} bytes", diff);
        }
    }
    
    #[test]
    fn test_levels_bounded() {
        let items: Vec<i32> = (0..1000).collect();
        let data = FilterData::from_vec(items);
        // Делаем 100 фильтраций (больше MAX_HISTORY)
        for i in 0..100 {
            data.filter(|&n| n > i * 10);
        }
        let stats = data.memory_stats();
        // Должно быть <= MAX_HISTORY
        assert!(stats.stored_levels <= MAX_HISTORY,
                "Too many levels stored: {}", stats.stored_levels);
    }
    
    #[test]
    fn test_index_builders_not_accumulating() {
        let items: Vec<i32> = (0..1000).collect();
        let data = FilterData::from_vec(items);
        // Создаем индекс многократно
        for i in 0..100 {
            data.create_index("price", move |&n| n % (i + 1));
        }
        // Должен быть только 1 builder
        let builders_count = data.index_builders.read().len();
        assert_eq!(builders_count, 1,
                   "Builders accumulating! Count: {}", builders_count);
    }
    
    #[test]
    fn test_reset_doesnt_leak() {
        let items: Vec<i32> = (0..10_000).collect();
        let data = FilterData::from_vec(items);
        for _ in 0..100 {
            // Фильтруем
            data.filter(|&n| n > 5000);
            // Создаем индексы
            data.create_index("test", |&n| n % 10);
            data.create_bit_index("even", |&n| n % 2 == 0);
            // Сбрасываем
            data.reset_to_source();
        }
        // Levels должны быть сброшены
        assert_eq!(data.stored_levels_count(), 1);
        // Индексов не должно быть (были очищены)
        assert_eq!(data.list_indexes().len(), 0);
    }
    
    #[test]
    fn test_concurrent_no_leak() {
        use std::thread;
        let items: Vec<i32> = (0..10_000).collect();
        let data = Arc::new(FilterData::from_vec(items));
        let mut handles = vec![];
        for i in 0..10 {
            let data_clone = Arc::clone(&data);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    data_clone.create_index(&format!("idx_{}", i), |&n| n % 10);
                    let _ = data_clone.filter_by_index(&format!("idx_{}", i), &5);
                    data_clone.drop_index(&format!("idx_{}", i));
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
        // После всех операций не должно быть лишних индексов
        assert!(data.list_indexes().len() <= 10);
    }
    
    #[test]
    fn test_indexed_storage_weak_valid() {
        let items: Vec<Arc<i32>> = (0..1000).map(Arc::new).collect();
        let parent = Arc::new(items);
        let indices = vec![0, 100, 200, 300];
        let data = FilterData::from_indices(&parent, indices);
        assert!(data.is_valid());
        // Дропаем parent
        drop(parent);
        // Data должен стать невалидным
        assert!(!data.is_valid());
        // Операции должны вернуть пусто
        assert!(data.items().is_empty());
    }

    #[test]
    fn test_no_deadlock_replace_index() {
        let items: Vec<i32> = (0..1000).collect();
        let data = Arc::new(FilterData::from_vec(items));
        // Создаем индекс
        data.create_index("test", |&n| n % 10);
        // Пересоздаем многократно в одном потоке
        for i in 0..100 {
            data.create_index("test", move |&n| n % (i + 1));
        }
        println!("No deadlock in single thread");
    }
    
    #[test]
    fn test_no_deadlock_concurrent_replace() {
        use std::{
            thread,
            time::Duration,
        };
        let items: Vec<i32> = (0..10_000).collect();
        let data = Arc::new(FilterData::from_vec(items));
        let mut handles = vec![];
        for i in 0..10 {
            let data_clone = Arc::clone(&data);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    // Многократно пересоздаем индекс
                    data_clone.create_index("shared", move |&n| n % (i * 10 + j + 1));
                    thread::sleep(Duration::from_micros(10));
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
        println!(" No deadlock in concurrent threads");
    }

    #[test]
    fn test_bit_index_key_consistency() {
        let items: Vec<i32> = (0..1000).collect();
        let data = FilterData::from_vec(items);
        // Создаем битовый индекс
        data.create_bit_index("test", |&n| n % 2 == 0);
        // Проверяем что ключ правильный
        let indexes = data.list_indexes();
        assert!(indexes.contains(&"bit:test".to_string()),
                "Should have 'bit:test', got: {:?}", indexes);
        assert!(!indexes.contains(&"bit::test".to_string()),
                "Should NOT have 'bit::test'");
        // Пересоздаем - не должно быть ошибок
        data.create_bit_index("test", |&n| n % 3 == 0);
        let indexes = data.list_indexes();
        assert_eq!(indexes.len(), 1, "Should have only 1 index after replace");
    }
    
    #[test]
    fn test_clear_indexes_efficient() {
        let items: Vec<i32> = (0..1000).collect();
        let data = FilterData::from_vec(items);
        // Создаем много индексов
        for i in 0..100 {
            data.create_index(&format!("idx_{}", i), move |&n| n % (i + 1));
            data.create_bit_index(&format!("bit_{}", i), move |&n| n % (i + 1) == 0);
        }
        assert_eq!(data.list_indexes().len(), 200);
        // Очищаем битовые
        use std::time::Instant;
        let start = Instant::now();
        data.clear_bit_indexes();
        let elapsed = start.elapsed();
        println!("Clear 100 bit indexes: {:?}", elapsed);
        assert!(elapsed.as_millis() < 100, "Should be fast");
        assert_eq!(data.list_indexes().len(), 100);
        // Очищаем все
        data.clear_all_indexes();
        assert_eq!(data.list_indexes().len(), 0);
    }
    
    #[test]
    fn test_reset_to_source_order() {
        let items: Vec<i32> = (0..1000).collect();
        let data = FilterData::from_vec(items);
        // Создаем индексы
        data.create_index("test", |&n| n % 10);
        data.create_bit_index("even", |&n| n % 2 == 0);
        // Фильтруем
        data.filter(|&n| n > 500);
        assert_eq!(data.len(), 499);
        assert_eq!(data.list_indexes().len(), 2);
        // Сбрасываем
        data.reset_to_source();
        assert_eq!(data.len(), 1000);
        assert_eq!(data.list_indexes().len(), 0, "All indexes should be cleared");
    }
}