use super::model::MemoryStats;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use arc_swap::ArcSwap;
use parking_lot::RwLock;
use rayon::prelude::*;

const MAX_HISTORY: usize = 50;
const MAX_RETRIES: usize = 3;

pub struct FilterData<T>
where
    T: Send + Sync,
{
    levels: ArcSwap<Vec<Arc<Vec<Arc<T>>>>>,
    level_info: ArcSwap<Vec<Arc<str>>>,
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
            levels: ArcSwap::from_pointee(vec![arc_items]),
            level_info: ArcSwap::from_pointee(vec![Arc::from("Source")]),
            current_level: Arc::new(AtomicUsize::new(0)),
            write_lock: RwLock::new(()),
        }
    }

    pub fn from_vec_arc_value(items: Vec<Arc<T>>) -> Self {
        Self {
            levels: ArcSwap::from_pointee(vec![Arc::new(items)]),
            level_info: ArcSwap::from_pointee(vec![Arc::from("Source")]),
            current_level: Arc::new(AtomicUsize::new(0)),
            write_lock: RwLock::new(()),
        }
    }

    fn filter_impl<F>(&self, predicate: F, retry_count: usize) -> &Self
    where
        F: Fn(&T) -> bool + Sync + Send,
    {
        if retry_count >= MAX_RETRIES {
            #[cfg(debug_assertions)]
            eprintln!("FilterData: max retries reached");
            return self;
        }

        // Шаг 1: Читаем данные БЕЗ блокировки 
        let (current_lvl, current_data) = {
            let current_lvl = self.current_level.load(Ordering::Acquire);
            let levels_guard = self.levels.load();
            
            let current_data = levels_guard
                .get(current_lvl)
                .or_else(|| levels_guard.first())
                .expect("Oops Level 0 must exist");
            
            (current_lvl, Arc::clone(current_data))
        };
        
        // Шаг 2: Параллельная фильтрация
        let filtered: Vec<Arc<T>> = current_data
            .par_iter()
            .filter(|item| predicate(item))
            .cloned()
            .collect();
        
        // Шаг 3: Обновление
        {
            let _guard = self.write_lock.write();
            
            let actual_current = self.current_level.load(Ordering::Acquire);
            if actual_current != current_lvl {
                drop(_guard);
                return self.filter_impl(predicate, retry_count + 1);
            }
            
            let levels_guard = self.levels.load();
            
            // просто копируем Vec до нужного уровня
            let mut new_levels = if levels_guard.len() < MAX_HISTORY {
                // Копируем все + добавляем новый
                levels_guard.to_vec()
            } else {
                // Cleanup: берём последние MAX_HISTORY-1 уровней
                let start = levels_guard.len().saturating_sub(MAX_HISTORY - 1);
                levels_guard[start..].to_vec()
            };
            
            new_levels.push(Arc::new(filtered));
            
            // Вычисляем длину ДО перемещения в Arc
            let adjusted_level = new_levels.len() - 1;
            
            // Теперь перемещаем
            self.levels.store(Arc::new(new_levels));
            
            // level_info аналогично
            let info_guard = self.level_info.load();
            let mut new_info = if info_guard.len() < MAX_HISTORY {
                info_guard.to_vec()
            } else {
                let start = info_guard.len().saturating_sub(MAX_HISTORY - 1);
                info_guard[start..].to_vec()
            };
            new_info.push(Arc::from("Filtered"));
            self.level_info.store(Arc::new(new_info));
            
            // Устанавливаем current_level
            self.current_level.store(adjusted_level, Ordering::Release);
        }
        self
    }

    pub fn filter<F>(&self, predicate: F) -> &Self
    where
        F: Fn(&T) -> bool + Sync + Send,
    {
        self.filter_impl(predicate, 0)
    }

    pub fn reset_to_source(&self) -> &Self {
        let _guard = self.write_lock.write();
        
        let levels_guard = self.levels.load();
        if let Some(level_0) = levels_guard.first() {
            self.levels.store(Arc::new(vec![Arc::clone(level_0)]));
        }
        
        self.level_info.store(Arc::new(vec![Arc::from("Source")]));
        self.current_level.store(0, Ordering::Release);
        
        self
    }

    pub fn go_to_level(&self, target_level: usize) -> &Self {
        let _guard = self.write_lock.write();
        
        let levels_guard = self.levels.load();
        
        if target_level >= levels_guard.len() {
            return self; // Недопустимый уровень
        }
        
        // Просто обрезаем Vec
        let new_levels = levels_guard[..=target_level].to_vec();
        self.levels.store(Arc::new(new_levels));
        
        let info_guard = self.level_info.load();
        let new_info = info_guard[..=target_level].to_vec();
        self.level_info.store(Arc::new(new_info));
        
        self.current_level.store(target_level, Ordering::Release);
        
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
        
        // O(1) доступ!
        Arc::clone(
            levels_guard
                .get(current_lvl)
                .unwrap_or_else(|| levels_guard.first().expect("Oops Level 0 must exist"))
        )
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
            .iter()
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
    }
    
    pub fn level_name(&self, level: usize) -> Option<Arc<str>> {
        let info_guard = self.level_info.load();
        info_guard.get(level).map(Arc::clone)
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