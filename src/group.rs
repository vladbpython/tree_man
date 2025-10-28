use super::filter::FilterData;
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use rayon::prelude::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
};


pub struct GroupData<K, V>
where
    K: Ord + Clone + Send + Sync,  // Send + Sync для параллельной обработки
    V: Send + Sync,
{
    pub key: K,
    pub data: Arc<FilterData<V>>,
    
    // Дерево
    // Weak ссылка на родителя (нет циклов)
    parent: Option<Weak<GroupData<K, V>>>,
    subgroups: ArcSwap<BTreeMap<K, Arc<GroupData<K, V>>>>,
    prev_relative: ArcSwap<Option<Weak<GroupData<K, V>>>>,
    next_relative: ArcSwap<Option<Weak<GroupData<K, V>>>>,
    
    pub description: Option<String>,
    depth: usize,
    // Mutex только для group_by 
    write_lock: Mutex<()>,
}

impl<K, V> GroupData<K, V>
where
    K: Ord + Clone + std::fmt::Debug + Send + Sync,
    V: Send + Sync + Clone,
{
    /// Создать корневую группу
    pub fn new_root(key: K, data: Vec<V>, description: &str) -> Arc<Self> {
        Arc::new(Self {
            key,
            data: Arc::new(FilterData::from_vec(data)),
            parent: None,
            subgroups: ArcSwap::from_pointee(BTreeMap::new()),
            prev_relative: ArcSwap::from_pointee(None),
            next_relative: ArcSwap::from_pointee(None),
            description: Some(description.to_string()),
            depth: 0,
            write_lock: Mutex::new(()),
        })
    }

    fn new_child(
        key: K,
        data: Arc<FilterData<V>>,
        parent: &Arc<Self>,
        description: &str,
        depth: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            key,
            data,
            parent: Some(Arc::downgrade(parent)),
            subgroups: ArcSwap::from_pointee(BTreeMap::new()),
            prev_relative: ArcSwap::from_pointee(None),
            next_relative: ArcSwap::from_pointee(None),
            description: Some(description.to_string()),
            depth,
            write_lock: Mutex::new(()),
        })
    }

    // Группировка с созданием сязанного между детьми
    pub fn group_by<F>(self: &Arc<Self>, extractor: F, description: &str)
    where
        F: Fn(&V) -> K + Sync + Send,
    {
        let items = self.data.items();
        
        // Параллельная группировка
        let grouped: BTreeMap<K, Vec<Arc<V>>> = items
            .par_iter()
            .fold(
                || BTreeMap::new(),
                |mut acc: BTreeMap<K, Vec<Arc<V>>>, item| {
                    let key = extractor(item);
                    acc.entry(key).or_insert_with(Vec::new).push(Arc::clone(item));
                    acc
                }
            )
            .reduce(
                || BTreeMap::new(),
                |mut acc, map| {
                    for (key, mut items) in map {
                        acc.entry(key).or_insert_with(Vec::new).append(&mut items);
                    }
                    acc
                }
            );
        
        let new_depth = self.depth + 1;
        
        // Создаем подгруппы
        let new_subgroups: Vec<(K, Arc<GroupData<K, V>>)> = grouped
            .into_iter()
            .map(|(key, items)| {
                let subgroup = Self::new_child(
                    key.clone(),
                    Arc::new(FilterData::from_vec_arc_value(items)),
                    self,
                    description,
                    new_depth,
                );
                (key, subgroup)
            })
            .collect();
        
        // строим родсвтенные связи детей (горизонтально)  
        for i in 0..new_subgroups.len() {
            // Предыдущий родственник
            if i > 0 {
                let prev = &new_subgroups[i - 1].1;
                new_subgroups[i].1.prev_relative.store(Arc::new(Some(Arc::downgrade(prev))));
            }
            
            // Следующий родственник
            if i + 1 < new_subgroups.len() {
                let next = &new_subgroups[i + 1].1;
                new_subgroups[i].1.next_relative.store(Arc::new(Some(Arc::downgrade(next))));
            }
        }
        
        let new_subgroups: BTreeMap<K, Arc<GroupData<K, V>>> = 
            new_subgroups.into_iter().collect();
        
        let _guard = self.write_lock.lock();
        self.subgroups.store(Arc::new(new_subgroups));
    }

    // Переходим к следующему родсвтеннику
    pub fn go_to_next_relative(self: &Arc<Self>) -> Option<Arc<Self>> {
        let next_weak_opt = self.next_relative.load();
        
        if let Some(weak) = next_weak_opt.as_ref() {
            if let Some(next) = weak.upgrade() {
                return Some(next);
            }
        }
        None
    }

    // Переходим предыдущему родственнику
    pub fn go_to_prev_relative(self: &Arc<Self>) -> Option<Arc<Self>> {
        let prev_weak_opt = self.prev_relative.load();
        
        if let Some(weak) = prev_weak_opt.as_ref() {
            if let Some(prev) = weak.upgrade() {
                return Some(prev);
            }
        }
        None
    }

    // Переходим к самому первому родственнику
    pub fn go_to_first_relative(self: &Arc<Self>) -> Arc<Self> {
        let mut current = Arc::clone(self);
        while let Some(prev) = current.go_to_prev_relative() {
            current = prev;
        }
        current
    }

    // Переходим к самому последнему родсвтеннику
    pub fn go_to_last_relative(self: &Arc<Self>) -> Arc<Self> {
        let mut current = Arc::clone(self);
        while let Some(next) = current.go_to_next_relative() {
            current = next;
        }
        current
    }

    // Провекрка на сущетсвование предыдущего родственника
    pub fn has_prev_relative(&self) -> bool {
        self.prev_relative.load().is_some()
    }

    // Провекра на существование следуюзего родсвтенника
    pub fn has_next_relative(&self) -> bool {
        self.next_relative.load().is_some()
    }

    // Получаем всех родсвтенников (включая себя)
    pub fn get_all_relatives(&self) -> Vec<Arc<Self>> {
        let first = Arc::new(Self {
            key: self.key.clone(),
            data: Arc::clone(&self.data),
            parent: self.parent.clone(),
            subgroups: ArcSwap::new(self.subgroups.load_full()),
            prev_relative: ArcSwap::new(self.prev_relative.load_full()),
            next_relative: ArcSwap::new(self.next_relative.load_full()),
            description: self.description.clone(),
            depth: self.depth,
            write_lock: Mutex::new(()),
        });
        
        let first = first.go_to_first_relative();
        
        let mut relatives = vec![Arc::clone(&first)];
        let mut current = first;
        
        while let Some(next) = current.go_to_next_relative() {
            relatives.push(Arc::clone(&next));
            current = next;
        }
        
        relatives
    }

    // Переходим к родителю (с автоочисткой)
    pub fn go_to_parent(self: &Arc<Self>) -> Option<Arc<Self>> {
        if let Some(parent_weak) = &self.parent {
            if let Some(parent) = parent_weak.upgrade() {
                parent.clear_subgroups();
                return Some(parent);
            }
        }
        None
    }

    // Спускаемся к указанному ребенку
    pub fn go_to_subgroup(self: &Arc<Self>, key: &K) -> Option<Arc<Self>> {
        if let Some(subgroup) = self.get_subgroup(key) {
            Some(subgroup)
        } else {
            None
        }
    }

    // Возвращаемся в начало и чисти все данные
    pub fn go_to_root(self: &Arc<Self>) -> Arc<Self> {
        let mut current = Arc::clone(self);
        while let Some(parent) = current.go_to_parent() {
            current = parent;
        }
        current
    }

    // Проверка что наш уровень - начало
    pub fn is_root(&self) -> bool {
        self.parent.is_none()
    }

    // Получаем абсолютный путь где мы находимся сейчас (Хлебные крошки)
    pub fn get_path(&self) -> Vec<K> {
        let mut path = Vec::new();
        let mut current_weak = self.parent.clone();
        path.push(self.key.clone());
        
        while let Some(parent_weak) = current_weak {
            if let Some(parent) = parent_weak.upgrade() {
                path.push(parent.key.clone());
                current_weak = parent.parent.clone();
            } else {
                break;
            }
        }
        
        path.reverse();
        path
    }

    // Получаем конкретного ребенка
    pub fn get_subgroup(&self, key: &K) -> Option<Arc<GroupData<K, V>>> {
        self.subgroups.load().get(key).map(Arc::clone)
    }

    // Получучаем ключи от всех наших детей
    pub fn subgroups_keys(&self) -> Vec<K> {
        self.subgroups.load().keys().cloned().collect()
    }

    // Количество детей
    pub fn subgroups_count(&self) -> usize {
        self.subgroups.load().len()
    }

    // Получаем всех наши детей
    pub fn get_all_subgroups(&self) -> Vec<Arc<GroupData<K, V>>> {
        self.subgroups.load().values().cloned().collect()
    }

    // Очищаем всех наших детей (рекурсивно)
    // ВНИАНИЕ: Очищаем горизонтальные связи между детьми!
    pub fn clear_subgroups(&self) {
        let current_subgroups = self.subgroups.load();
        
        for (_, subgroup) in current_subgroups.iter() {
            // Очищаем связи между relatives
            subgroup.prev_relative.store(Arc::new(None));
            subgroup.next_relative.store(Arc::new(None));
            
            // Рекурсивно очищаем детей
            subgroup.clear_subgroups();
        }
        
        let _guard = self.write_lock.lock();
        self.subgroups.store(Arc::new(BTreeMap::new()));
    }

    // Обойти всё дерево
    pub fn traverse<F>(&self, callback: &F)
    where
        F: Fn(&Arc<GroupData<K, V>>) + Sync,
    {
        let self_arc = Arc::new(Self {
            key: self.key.clone(),
            data: Arc::clone(&self.data),
            parent: self.parent.clone(),
            subgroups: ArcSwap::new(self.subgroups.load_full()),
            prev_relative: ArcSwap::new(self.prev_relative.load_full()),
            next_relative: ArcSwap::new(self.next_relative.load_full()),
            description: self.description.clone(),
            depth: self.depth,
            write_lock: Mutex::new(()),
        });
        
        callback(&self_arc);
        
        let subgroups = self.subgroups.load();
        for (_, subgroup) in subgroups.iter() {
            subgroup.traverse(callback);
        }
    }

    // Параллельный обход дерева
    pub fn traverse_parallel<F>(&self, callback: &F)
    where
        F: Fn(&Arc<GroupData<K, V>>) + Sync + Send,
    {
        let self_arc = Arc::new(Self {
            key: self.key.clone(),
            data: Arc::clone(&self.data),
            parent: self.parent.clone(),
            subgroups: ArcSwap::new(self.subgroups.load_full()),
            prev_relative: ArcSwap::new(self.prev_relative.load_full()),
            next_relative: ArcSwap::new(self.next_relative.load_full()),
            description: self.description.clone(),
            depth: self.depth,
            write_lock: Mutex::new(()),
        });
        
        callback(&self_arc);
        
        let subgroups_vec: Vec<_> = self.subgroups.load().values().cloned().collect();
        
        subgroups_vec.par_iter().for_each(|subgroup| {
            subgroup.traverse_parallel(callback);
        });
    }

    // Собрать всех детей (рекурсивно)
    pub fn collect_all_groups(&self) -> Vec<Arc<GroupData<K, V>>> {
        let mut result = Vec::new();
        self.collect_recursive(&mut result);
        result
    }

    // Рекурсивный сбор детей
    fn collect_recursive(&self, result: &mut Vec<Arc<GroupData<K, V>>>) {
        // Создаем Arc текущей родителя
        let self_arc = Arc::new(Self {
            key: self.key.clone(),
            data: Arc::clone(&self.data),
            parent: self.parent.clone(),
            subgroups: ArcSwap::new(self.subgroups.load_full()),
            prev_relative: ArcSwap::new(self.prev_relative.load_full()),
            next_relative: ArcSwap::new(self.next_relative.load_full()),
            description: self.description.clone(),
            depth: self.depth,
            write_lock: parking_lot::Mutex::new(()),
        });
        
        result.push(self_arc);
        
        // Рекурсивно собираем детей
        for subgroup in self.get_all_subgroups() {
            subgroup.collect_recursive(result);
        }
    }

    // Текущая максимальная глубина
    pub fn max_depth(&self) -> usize {
        let subgroups = self.subgroups.load();
        if subgroups.is_empty() {
            self.depth
        } else {
            subgroups.values()
                .map(|sg| sg.max_depth())
                .max()
                .unwrap_or(self.depth)
        }
    }

    // Общее количество детей
    pub fn total_groups_count(&self) -> usize {
        let subgroups = self.subgroups.load();
        1 + subgroups.values()
            .map(|sg| sg.total_groups_count())
            .sum::<usize>()
    }

    pub fn filter<F>(&self, predicate: F)
    where
        F: Fn(&V) -> bool + Sync + Send,
    {
        self.data.filter(predicate);
    }

    pub fn reset_filters(&self) {
        self.data.reset_to_source();
    }

    pub fn filter_subgroups<F>(&self, predicate: F)
    where
        F: Fn(&V) -> bool + Sync + Send + Clone,
    {
        let subgroups_vec = self.get_all_subgroups();    
        subgroups_vec.par_iter().for_each(|subgroup| {
            subgroup.filter(predicate.clone());
        });
    }

    // Дебажим наше дереров    
    pub fn print_tree(&self, indent: usize) {
        let prefix = "  ".repeat(indent);
        
        let relative_info = format!(
            " [prev: {}, next: {}]",
            if self.has_prev_relative() { "yes" } else { "no" },
            if self.has_next_relative() { "yes" } else { "no" }
        );
        
        println!("{}📁 {:?} ({} items, depth: {}){}", 
                 prefix, self.key, self.data.len(), self.depth, relative_info);
        
        let subgroups = self.subgroups.load();
        for (_, subgroup) in subgroups.iter() {
            subgroup.print_tree(indent + 1);
        }
    }

    // Дебажим где мы находимся
    pub fn print_info(&self) {
        println!("\n📊 Group: {:?}", self.key);
        println!("  Path: {:?}", self.get_path());
        println!("  Items: {}", self.data.len());
        println!("  Depth: {}", self.depth);
        println!("  Is root: {}", self.is_root());
        println!("  Has prev relative: {}", self.has_next_relative());
        println!("  Has next relative: {}", self.has_next_relative());
        println!("  Subgroups: {}", self.subgroups_count());
        println!("  Max depth: {}", self.max_depth());
        println!("  Total groups: {}", self.total_groups_count());
    }

    // текущая глубина
    pub fn depth(&self) -> usize {
        self.depth
    }
}


pub struct FilterGroup;

impl FilterGroup {
    pub fn filter_parallel<K, V, F>(groups_and_filters: Vec<(Arc<GroupData<K, V>>, F)>)
    where
        K: Ord + Clone + std::fmt::Debug + Send + Sync,
        V: Send + Sync + Clone,
        F: Fn(&V) -> bool + Send + Sync,
    {   
        groups_and_filters.into_par_iter().for_each(|(group, filter)| {
            group.filter(filter);
        });
    }

    pub fn filter_subgroups_parallel<K, V, F>(groups_and_filters: Vec<(Arc<GroupData<K, V>>, F)>)
    where
        K: Ord + Clone + std::fmt::Debug + Send + Sync,
        V: Send + Sync + Clone,
        F: Fn(&V) -> bool + Send + Sync + Clone,
    {
        
        groups_and_filters.into_par_iter().for_each(|(group, filter)| {
            group.filter_subgroups(filter);
        });
    }
}

#[macro_export]
macro_rules! group_filter_parallel {
    ( $( $group:expr => $filter:expr ),+ $(,)? ) => {
        {
            rayon::scope(|s| {
                $(
                    s.spawn(|_| {
                        $group.filter($filter);
                    });
                )+
            });
        }
    };
}

#[macro_export]
macro_rules! group_filter_subgroups_parallel {
    ( $( $group:expr => $filter:expr ),+ $(,)? ) => {
        {
            rayon::scope(|s| {
                $(
                    s.spawn(|_| {
                        $group.filter_subgroups($filter);
                    });
                )+
            });
        }
    };
}