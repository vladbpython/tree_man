use super::model::MemoryStats;
use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
};
use arc_swap::ArcSwap;
use parking_lot::RwLock;
use rayon::prelude::*;

pub struct FilterData<T>
where
    T: Send + Sync,
{
    levels: ArcSwap<BTreeMap<usize, Arc<Vec<Arc<T>>>>>,
    level_info: ArcSwap<BTreeMap<usize, String>>,
    current_level: Arc<AtomicUsize>,
    // RwLock только для write операций (минимум contention)
    write_lock: RwLock<()>,
}

impl<T> FilterData<T>
where
    T: Send + Sync,
{
    pub fn from_vec(items: Vec<T>) -> Self {
        let len = items.len();
        
        let arc_items = match len {
            0..=499 => {
                // Малые - последовательно
                Arc::new(items.into_iter().map(Arc::new).collect())
            }
            500..=50_000 => {
                // Средние - простой параллелизм
                let mut arcs = Vec::with_capacity(len);
                arcs.par_extend(items.into_par_iter().map(Arc::new));
                Arc::new(arcs)
            }
            _ => {
                // Большие - с чанками для лучшей локальности кэша
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
            levels: ArcSwap::from_pointee(BTreeMap::from([(0, arc_items)])),
            level_info: ArcSwap::from_pointee(BTreeMap::from([(0, "Source".to_string())])),
            current_level: Arc::new(AtomicUsize::new(0)),
            write_lock: RwLock::new(()),
        }
    }

    pub fn from_vec_arc_value(items: Vec<Arc<T>>) -> Self{
        let mut levels = BTreeMap::new();
        levels.insert(0, Arc::new(items));
        
        let mut level_info = BTreeMap::new();
        level_info.insert(0, "Source".to_string());
        
        Self {
            levels: ArcSwap::from_pointee(levels),
            level_info: ArcSwap::from_pointee(level_info),
            current_level: Arc::new(AtomicUsize::new(0)),
            write_lock: RwLock::new(()),
        }
    }

    pub fn filter<F>(&self, predicate: F) -> &Self
    where
        F: Fn(&T) -> bool + Sync + Send,
    {
        // Шаг 1: Читаем данные БЕЗ блокировки 
        let (current_lvl, current_data) = {
            let current_lvl = self.current_level.load(Ordering::Acquire);
            let levels_guard = self.levels.load(); // ← Дешёвый load, не load_full
            let current_data = match levels_guard.get(&current_lvl) {
                Some(data) => Arc::clone(data),
                None => Arc::clone(levels_guard.get(&0).expect("Oops level 0 must always exist")),
            };
            
            (current_lvl, current_data)
        }; // Guard автоматически освобождается
        
        // Шаг 2: Фильтрация БЕЗ блокировок (параллельная обработка)
        let filtered: Vec<Arc<T>> = current_data
            .par_iter()
            .filter(|item| predicate(item))
            .cloned()
            .collect();
        
        // Фаза 3: Обновление (короткая критическая секция)
        {
            let _guard = self.write_lock.write();
            
            // Ещё раз проверяем current_level под блокировкой
            let actual_current = self.current_level.load(Ordering::Acquire);
            if actual_current != current_lvl {
                // Уровень изменился - нужно retry
                drop(_guard);
                return self.filter(predicate);
            }
            
            let new_level = current_lvl + 1;
            
            // Эффективное создание нового BTreeMap через итератор
            let levels_guard = self.levels.load();
            let new_levels: BTreeMap<_, _> = levels_guard
                .iter()
                .filter(|&(&k, _)| k <= current_lvl)
                .map(|(&k, v)| (k, Arc::clone(v)))
                .chain(std::iter::once((new_level, Arc::new(filtered))))
                .collect();
            
            self.levels.store(Arc::new(new_levels));
            
            // Обновляем level_info
            let info_guard = self.level_info.load();
            let new_info: BTreeMap<_, _> = info_guard
                .iter()
                .filter(|&(&k, _)| k <= current_lvl)
                .map(|(&k, v)| (k, v.clone()))
                .chain(std::iter::once((new_level, "Filtered".to_string())))
                .collect();
            self.level_info.store(Arc::new(new_info));
            self.current_level.store(new_level, Ordering::Release);
            
        }
        
        self
    }

    // Cбросить все фильтры и вернуться к исходнику
    pub fn reset_to_source(&self) -> &Self {
        let _guard = self.write_lock.write();
        
        let levels_guard = self.levels.load();
        
        // Создаём новый BTreeMap с только level 0
        let mut new_levels = BTreeMap::new();
        if let Some(level_0) = levels_guard.get(&0) {
            new_levels.insert(0, Arc::clone(level_0));
        }
        
        self.levels.store(Arc::new(new_levels));
        self.current_level.store(0, Ordering::Release);
        
        // Обновляем level_info
        let mut new_info = BTreeMap::new();
        new_info.insert(0, "Source".to_string());
        self.level_info.store(Arc::new(new_info));
        
        self
    }

    pub fn go_to_level(&self, target_level: usize) -> &Self {
        let _guard = self.write_lock.write();
        
        let levels_guard = self.levels.load();
        
        if !levels_guard.contains_key(&target_level) {
            println!("no key");
            return self;
        }
        
        // Эффективное создание через итератор
        let new_levels: BTreeMap<_, _> = levels_guard
            .iter()
            .filter(|&(&k, _)| k <= target_level)
            .map(|(&k, v)| (k, Arc::clone(v)))
            .collect();
        
        self.levels.store(Arc::new(new_levels));
        self.current_level.store(target_level, Ordering::Release);
        
        // Обновляем level_info
        let info_guard = self.level_info.load();
        println!("keys: {:?}",info_guard.keys());
        let new_info: BTreeMap<_, _> = info_guard
            .iter()
            .filter(|&(&k, _)| k <= target_level)
            .map(|(&k, v)| (k, v.clone()))
            .collect();
        
        self.level_info.store(Arc::new(new_info));
        
        self
    }

    pub fn up(&self) -> &Self {
        let current = self.current_level.load(Ordering::Acquire);
        if current > 0 {
            self.go_to_level(current - 1)
        } else {
            self
        }
    }

    pub fn items(&self) -> Arc<Vec<Arc<T>>> {
        let current_lvl = self.current_level.load(Ordering::Acquire);
        let levels_guard = self.levels.load();
        
        match levels_guard.get(&current_lvl) {
            Some(data) => Arc::clone(data),
            None => Arc::clone(levels_guard.get(&0).expect("Oops level 0 must always exist")),
        }
    }

    pub fn len(&self) -> usize {
        self.items().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn current_level(&self) -> usize {
        self.current_level.load(Ordering::Acquire)
    }

    pub fn stored_levels_count(&self) -> usize {
        self.levels.load().len()
    }

    pub fn total_stored_items(&self) -> usize {
        self.levels.load()
            .values()
            .map(|level| level.len())
            .sum()
    }

    pub fn memory_stats(&self) -> MemoryStats {
        let current_lvl = self.current_level.load(Ordering::Acquire);
        let levels_guard = self.levels.load();
        
        let mut stats = MemoryStats {
            current_level: current_lvl,
            stored_levels: levels_guard.len(),
            current_level_items: 0,
            total_stored_items: 0,
            useful_items: 0,
            wasted_items: 0,
        };

        for (lvl, level_data) in levels_guard.iter() {
            let count = level_data.len();
            stats.total_stored_items += count;
            
            if *lvl == current_lvl {
                stats.current_level_items = count;
            }
            
            if *lvl <= current_lvl {
                stats.useful_items += count;
            } else {
                stats.wasted_items += count;
            }
        }
        stats
    }
}



pub trait IntoFilterData {
    type Item: Send + Sync;
    
    fn into_filtered(self) -> FilterData<Self::Item>;
}

impl<T: Send + Sync> IntoFilterData for Vec<T> {
    type Item = T;
    
    fn into_filtered(self) -> FilterData<T> {
        FilterData::from_vec(self)
    }
}