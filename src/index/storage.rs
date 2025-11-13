use std::sync::{Arc, Weak};
use arc_swap::ArcSwap;
use rayon::prelude::*;
use std::collections::BTreeMap;


// DataStorage - Zero-Copy Architecture


pub enum DataStorage<T>
where
    T: Send + Sync,
{
    Owned {
        source: Arc<Vec<Arc<T>>>,
        current: ArcSwap<Vec<Arc<T>>>,
    },
    
    Indexed {
        parent_data: Weak<Vec<Arc<T>>>,
        source_indices: Arc<Vec<usize>>,
        current_indices: ArcSwap<Vec<usize>>,
    },
}


// Index Storage - Smart Indexing

// Индекс хранит только позиции, не данные!
// Это обеспечивает:
// - Минимальное использование памяти
// - Отсутствие дублирования данных
// - Быстрые операции
pub struct IndexStorage<K, T>
where
    K: Ord + Clone + Send + Sync,
    T: Send + Sync,
{
    // BTreeMap: ключ -> позиции в parent_data
    positions: ArcSwap<BTreeMap<K, Arc<Vec<usize>>>>,
    // Слабая ссылка на родительские данные
    parent_data: Weak<Vec<Arc<T>>>,
}

pub struct Index<K, T>
where
    K: Ord + Clone + Send + Sync,
    T: Send + Sync,
{
    storage: IndexStorage<K, T>,
}

impl<K, T> Index<K, T>
where
    K: Ord + Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
{
    // Создать новый индекс
    pub fn new(parent_data: &Weak<Vec<Arc<T>>>) -> Self {
        Self {
            storage: IndexStorage {
                positions: ArcSwap::from_pointee(BTreeMap::new()),
                parent_data: parent_data.clone(),
            },
        }
    }
    
    // Получить данные по ключу
    pub fn get(&self, key: &K) -> Option<Vec<Arc<T>>> {
        let parent = self.storage.parent_data.upgrade()?;
        let guard_pos = self.storage.positions.load();
        let pos = guard_pos.get(key)?;
        Some(
            pos.iter()
                .filter_map(|&idx| parent.get(idx).cloned())
                .collect()
        )
    }
    
    // Получить индексы по ключу 
    #[inline]
    pub fn get_indices(&self, key: &K) -> Option<Vec<usize>> {
        self.storage.parent_data.upgrade()?;
        self.storage.positions.load().get(key).map(|pos| (**pos).clone())
    }
    
    // Получить все ключи
    pub fn keys(&self) -> Vec<K> {
        self.storage.positions.load().keys().cloned().collect()
    }
    
    // Первый ключ
    pub fn first_key(&self) -> Option<K> {
        self.storage.positions.load().keys().next().cloned()
    }
    
    // Последний ключ
    pub fn last_key(&self) -> Option<K> {
        self.storage.positions.load().keys().next_back().cloned()
    }
    
    // Range query возвращающий индексы 
    pub fn range_indices<R>(&self, range: R) -> Vec<usize>
    where
        R: std::ops::RangeBounds<K>,
    {
        if self.storage.parent_data.upgrade().is_none() {
            return Vec::new();
        }
        let pos_guard = self.storage.positions.load();
        // Собираем все индексы из range
        let mut result: Vec<usize> = pos_guard
            .range(range)
            .flat_map(|(_, pos)| pos.iter().copied())
            .collect();
        // Сортируем для cache locality!
        result.sort_unstable();
        result
    }

    
    // Range query возвращающий данные 
    pub fn range_items<R>(&self, range: R) -> Vec<Arc<T>>
    where
        R: std::ops::RangeBounds<K>,
    {
        if let Some(parent) = self.storage.parent_data.upgrade() {
            let pos_guard = self.storage.positions.load();
            let mut result = Vec::new();
            
            for (_, pos) in pos_guard.range(range) {
                for &idx in pos.iter() {
                    if let Some(item) = parent.get(idx) {
                        result.push(Arc::clone(item));
                    }
                }
            }
            
            result
        } else {
            Vec::new()
        }
    }
    
    // Построить индекс
    pub fn build<F>(&self, items: &[Arc<T>], extractor: F) -> bool
    where
        F: Fn(&T) -> K + Send + Sync,
    {
        if self.storage.parent_data.upgrade().is_none() {
            eprintln!("WARNING: Cannot build index - parent_data was dropped");
            return false
        }
        // Параллельное извлечение ключей
        let pairs: Vec<(K, usize)> = items
            .par_iter()
            .enumerate()
            .map(|(idx, item)| (extractor(item), idx))
            .collect();
        // Группировка по ключам
        let mut index: BTreeMap<K, Vec<usize>> = BTreeMap::new();
        for (key, idx) in pairs {
            index.entry(key).or_default().push(idx);
        }
        // СОРТИРОВКА индексов для cache locality!
        let positional_index: BTreeMap<K, Arc<Vec<usize>>> = index
            .into_par_iter()  // Параллельная обработка ключей
            .map(|(k, mut indices)| {
                // Сортируем индексы для sequential access
                indices.sort_unstable();
                (k, Arc::new(indices))
            })
            .collect();
        
        self.storage.positions.store(Arc::new(positional_index));
        true
    }

    // Проверить что индекс валиден (parent_data еще жив)
    #[allow(dead_code)]
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.storage.parent_data.upgrade().is_some()
    }
    
    // Количество уникальных ключей в индексе
     #[allow(dead_code)]
    #[inline]
    pub fn len(&self) -> usize {
        self.storage.positions.load().len()
    }
    
    // Проверка на пустоту
     #[allow(dead_code)]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.storage.positions.load().is_empty()
    }
    
    // Общее количество индексов (сумма по всем ключам)
     #[allow(dead_code)]
    pub fn total_indices_count(&self) -> usize {
        self.storage.positions.load()
            .values()
            .map(|indices| indices.len())
            .sum()
    }
    
    // Проверить содержит ли индекс ключ
    #[allow(dead_code)]
    #[inline]
    pub fn contains_key(&self, key: &K) -> bool {
        self.storage.positions.load().contains_key(key)
    }
    
    // Получить количество индексов для конкретного ключа
    #[allow(dead_code)]
    pub fn key_count(&self, key: &K) -> usize {
        self.storage.positions.load()
            .get(key)
            .map(|indices| indices.len())
            .unwrap_or(0)
    }
    
    // Очистить индекс
    #[allow(dead_code)]
    pub fn clear(&self) {
        self.storage.positions.store(Arc::new(BTreeMap::new()));
    }


    // Debug
    
    // Получить статистику индекса
    #[allow(dead_code)]
    pub fn stats(&self) -> IndexStats {
        let positions = self.storage.positions.load();
        let unique_keys = positions.len();
        let total_indices: usize = positions.values()
            .map(|v| v.len())
            .sum();
        let min_indices = positions.values()
            .map(|v| v.len())
            .min()
            .unwrap_or(0);
        let max_indices = positions.values()
            .map(|v| v.len())
            .max()
            .unwrap_or(0);
        let avg_indices = if unique_keys > 0 {
            total_indices as f64 / unique_keys as f64
        } else {
            0.0
        };
        IndexStats {
            is_valid: self.is_valid(),
            unique_keys,
            total_indices,
            min_indices_per_key: min_indices,
            max_indices_per_key: max_indices,
            avg_indices_per_key: avg_indices,
        }
    }

    
}

// Статистика индекса
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub is_valid: bool,
    pub unique_keys: usize,
    pub total_indices: usize,
    pub min_indices_per_key: usize,
    pub max_indices_per_key: usize,
    pub avg_indices_per_key: f64,
}

impl std::fmt::Display for IndexStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IndexStats {{ valid: {}, keys: {}, total: {}, min/avg/max: {}/{:.1}/{} }}",
            self.is_valid,
            self.unique_keys,
            self.total_indices,
            self.min_indices_per_key,
            self.avg_indices_per_key,
            self.max_indices_per_key
        )
    }
}

#[cfg(test)]
mod memory_leak_tests {
    use super::*;
    
    #[test]
    fn test_index_no_leak_after_parent_drop() {
        for _ in 0..1000 {
            let items: Vec<Arc<i32>> = (0..1000).map(Arc::new).collect();
            let parent = Arc::new(items.clone());
            let weak = Arc::downgrade(&parent);
            let index = Index::<i32, i32>::new(&weak);
            assert!(index.build(&parent, |&n| n % 10));
            // Используем индекс
            for i in 0..10 {
                let _ = index.get(&i);
            }
            // Дропаем parent
            drop(parent);
            // Индекс должен стать невалидным
            assert!(!index.is_valid());
            assert!(index.get(&0).is_none());
            // Weak должен быть мертвым
            assert!(weak.upgrade().is_none());
        }
        // Если здесь не краш - утечек нет!
    }
    
    #[test]
    fn test_index_build_updates_correctly() {
        let items: Vec<Arc<String>> = (0..100)
            .map(|i| Arc::new(format!("item_{}", i)))
            .collect();
        let parent = Arc::new(items);
        let weak = Arc::downgrade(&parent);
        let index = Index::<usize, String>::new(&weak);
        // Первая сборка
        assert!(index.build(&parent, |s| s.len()));
        let stats1 = index.stats();
        println!("After first build: {}", stats1);
        // Вторая сборка (с другим extractor)
        assert!(index.build(&parent, |s| s.chars().count()));
        let stats2 = index.stats();
        println!("After second build: {}", stats2);
        // Индексы должны обновиться
        assert_eq!(stats1.unique_keys, stats2.unique_keys);
        assert!(index.is_valid());
    }
    
    #[test]
    fn test_index_concurrent_access() {
        use std::thread;
        let items: Vec<Arc<i32>> = (0..10000).map(Arc::new).collect();
        let parent = Arc::new(items);
        let weak = Arc::downgrade(&parent);
        let index = Arc::new(Index::<i32, i32>::new(&weak));
        index.build(&parent, |&n| n % 100);
        // Множество потоков читают индекс одновременно
        let mut handles = vec![];
        for i in 0..10 {
            let index_clone = Arc::clone(&index);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let key = i % 100;
                    let _ = index_clone.get(&key);
                    let _ = index_clone.get_indices(&key);
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert!(index.is_valid());
    }
    
    #[test]
    fn test_index_stats() {
        let items: Vec<Arc<i32>> = (0..1000).map(Arc::new).collect();
        let parent = Arc::new(items);
        let weak = Arc::downgrade(&parent);
        let index = Index::<i32, i32>::new(&weak);
        assert!(index.build(&parent, |&n| n % 10));
        let stats = index.stats();
        println!("{}", stats);
        assert!(stats.is_valid);
        assert_eq!(stats.unique_keys, 10); // 0..9
        assert_eq!(stats.total_indices, 1000);
        assert_eq!(stats.min_indices_per_key, 100);
        assert_eq!(stats.max_indices_per_key, 100);
        assert_eq!(stats.avg_indices_per_key, 100.0);
    }
    
    #[test]
    fn test_index_range_sorted() {
        let items: Vec<Arc<i32>> = (0..1000).map(Arc::new).collect();
        let parent = Arc::new(items);
        let weak = Arc::downgrade(&parent);
        let index = Index::<i32, i32>::new(&weak);
        assert!(index.build(&parent, |&n| n / 100)); // Keys: 0-9
        // Проверяем что range_indices возвращает отсортированные индексы
        let indices = index.range_indices(3..7);
        // Должны быть отсортированы
        assert!(indices.windows(2).all(|w| w[0] < w[1]),
                "Range indices not sorted!");
        // Должны быть в правильном диапазоне
        for &idx in &indices {
            let value = *parent[idx];
            assert!((300..700).contains(&value),
                    "Index {} has value {} outside range", idx, value);
        }
    }
    
    #[test]
    fn test_clear_index() {
        let items: Vec<Arc<i32>> = (0..100).map(Arc::new).collect();
        let parent = Arc::new(items);
        let weak = Arc::downgrade(&parent);
        let index = Index::<i32, i32>::new(&weak);
        assert!(index.build(&parent, |&n| n % 10));
        assert!(!index.is_empty());
        assert_eq!(index.len(), 10);
        index.clear();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert_eq!(index.total_indices_count(), 0);
    }
}