use super::filter::FilterData;
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use rayon::prelude::*;
use std::{
    fmt::Debug,
    collections::HashMap,
    hash::Hash, 
    sync::{
        Arc, 
        Weak
    },
};


pub struct GroupData<K, V>
where
    K: Ord + Eq + Hash + Clone + Send + Sync,
    V: Send + Sync,
{
    pub key: K,
    pub data: Arc<FilterData<V>>,
    
    // Дерево - Weak ссылка на родителя (циклическая ссылка)
    parent: Option<Weak<GroupData<K, V>>>,
    subgroups: ArcSwap<HashMap<K, Arc<GroupData<K, V>>>>,
    
    pub description: Option<Arc<str>>,
    depth: usize,
    
    // Mutex только для group_by 
    write_lock: Mutex<()>,
}

impl<K, V> GroupData<K, V>
where
    K: Ord + Eq + Hash + Clone + Debug + Send + Sync,
    V: Send + Sync + Clone,
{
    // Создать корневую группу
    pub fn new_root(key: K, data: Vec<V>, description: &str) -> Arc<Self> {
        Arc::new(Self {
            key,
            data: Arc::new(FilterData::from_vec(data)),
            parent: None,
            subgroups: ArcSwap::from_pointee(HashMap::new()),
            description: Some(Arc::from(description)),
            depth: 0,
            write_lock: Mutex::new(()),
        })
    }

    fn new_child(
        key: K,
        data: Arc<FilterData<V>>,
        parent: &Arc<Self>,
        description: Arc<str>,
        depth: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            key,
            data,
            parent: Some(Arc::downgrade(parent)),
            subgroups: ArcSwap::from_pointee(HashMap::new()),
            description: Some(description),
            depth,
            write_lock: Mutex::new(()),
        })
    }

    pub fn group_by<F>(self: &Arc<Self>, extractor: F, description: &str)
    where
        F: Fn(&V) -> K + Sync + Send,
    {
        let items = self.data.items();
        let description_arc: Arc<str> = Arc::from(description);
        
        // 🚀 FxHashMap - самая быстрая группировка
        let grouped: HashMap<K, Vec<Arc<V>>> = items
            .par_iter()
            .fold(
                || HashMap::new(),
                |mut acc, item| {
                    acc.entry(extractor(item))
                        .or_insert_with(|| Vec::with_capacity(64))
                        .push(Arc::clone(item));
                    acc
                },
            )
            .reduce(
                || HashMap::new(),
                |mut acc, map| {
                    for (key, mut items) in map {
                        match acc.entry(key) {
                            std::collections::hash_map::Entry::Vacant(e) => {
                                e.insert(items);
                            }
                            std::collections::hash_map::Entry::Occupied(mut e) => {
                                e.get_mut().append(&mut items);
                            }
                        }
                    }
                    acc
                },
            );
        
        let new_depth = self.depth + 1;
        let mut new_subgroups = HashMap::with_capacity_and_hasher(
            grouped.len(),
            Default::default(),
        );
        
        for (key, items) in grouped {
            new_subgroups.insert(
                key.clone(),
                Self::new_child(
                    key,
                    Arc::new(FilterData::from_vec_arc_value(items)),
                    self,
                    Arc::clone(&description_arc),
                    new_depth,
                ),
            );
        }
        
        let _guard = self.write_lock.lock();
        self.subgroups.store(Arc::new(new_subgroups));
    }

    // Переход к родителю (с автоочисткой подгрупп)
    pub fn go_to_parent(self: &Arc<Self>) -> Option<Arc<Self>> {
        if let Some(parent_weak) = &self.parent {
            if let Some(parent) = parent_weak.upgrade() {
                parent.clear_subgroups();
                return Some(parent);
            }
        }
        None
    }

    // Спуск к указанному ребенку
    pub fn go_to_subgroup(self: &Arc<Self>, key: &K) -> Option<Arc<Self>> {
        self.get_subgroup(key)
    }

    // Возврат в корень с очисткой всех промежуточных данных
    pub fn go_to_root(self: &Arc<Self>) -> Arc<Self> {
        let mut current = Arc::clone(self);
        while let Some(parent) = current.go_to_parent() {
            current = parent;
        }
        current
    }

    // Проверка что текущий узел - корень
    pub fn is_root(&self) -> bool {
        self.parent.is_none()
    }

    // Получить абсолютный путь от корня до текущего узла (breadcrumbs)
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

    // Получить конкретную подгруппу по ключу
    pub fn get_subgroup(&self, key: &K) -> Option<Arc<GroupData<K, V>>> {
        self.subgroups.load().get(key).map(Arc::clone)
    }

    // Получить ключи всех подгрупп
    pub fn subgroups_keys(&self) -> Vec<K> {
        self.subgroups.load().keys().cloned().collect()
    }

    // Количество подгрупп
    pub fn subgroups_count(&self) -> usize {
        self.subgroups.load().len()
    }

    // Получить все подгруппы
    pub fn get_all_subgroups(&self) -> Vec<Arc<GroupData<K, V>>> {
        self.subgroups.load().values().cloned().collect()
    }

    // Очистить все подгруппы рекурсивно
    pub fn clear_subgroups(&self) {
        let current_subgroups = self.subgroups.load();
        
        // Рекурсивно очищаем детей
        for (_, subgroup) in current_subgroups.iter() {
            subgroup.clear_subgroups();
        }
        
        let _guard = self.write_lock.lock();
        self.subgroups.store(Arc::new(HashMap::new()));
    }

    // Обойти всё дерево последовательно
    pub fn traverse(self: &Arc<Self>, callback: &impl Fn(&Arc<GroupData<K, V>>))
    {
        callback(self);
        
        let subgroups = self.subgroups.load();
        for (_, subgroup) in subgroups.iter() {
            subgroup.traverse(callback);
        }
    }

    // Обойти всё дерево параллельно
    pub fn traverse_parallel<F>(self: &Arc<Self>, callback: &F)
    where
        F: Fn(&Arc<GroupData<K, V>>) + Sync + Send,
    {
        callback(self);
        
        let subgroups_vec: Vec<_> = self.subgroups.load().values().cloned().collect();
        
        subgroups_vec.par_iter().for_each(|subgroup| {
            subgroup.traverse_parallel(callback);
        });
    }

    // Собрать все группы рекурсивно
    pub fn collect_all_groups(self: &Arc<Self>) -> Vec<Arc<GroupData<K, V>>> {
        let mut result = Vec::new();
        self.collect_recursive(&mut result);
        result
    }

    fn collect_recursive(self: &Arc<Self>, result: &mut Vec<Arc<GroupData<K, V>>>) {
        result.push(Arc::clone(self));
        
        for subgroup in self.get_all_subgroups() {
            subgroup.collect_recursive(result);
        }
    }

    // Максимальная глубина дерева
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

    // Общее количество групп в дереве
    pub fn total_groups_count(&self) -> usize {
        let subgroups = self.subgroups.load();
        1 + subgroups.values()
            .map(|sg| sg.total_groups_count())
            .sum::<usize>()
    }

    // Фильтрация данных в текущей группе
    pub fn filter<F>(&self, predicate: F)
    where
        F: Fn(&V) -> bool + Sync + Send,
    {
        self.data.filter(predicate);
    }

    // Сброс фильтров к исходным данным
    pub fn reset_filters(&self) {
        self.data.reset_to_source();
    }

    // Фильтрация всех подгрупп
    pub fn filter_subgroups<F>(&self, predicate: F)
    where
        F: Fn(&V) -> bool + Sync + Send + Clone,
    {
        let subgroups_vec = self.get_all_subgroups();    
        subgroups_vec.par_iter().for_each(|subgroup| {
            subgroup.filter(predicate.clone());
        });
    }

    // Вывод дерева в консоль для отладки
    pub fn print_tree(&self, indent: usize) {
        let prefix = "  ".repeat(indent);
        
        println!("{}📁 {:?} ({} items, depth: {})", 
                 prefix, self.key, self.data.len(), self.depth);
        
        let subgroups = self.subgroups.load();
        for (_, subgroup) in subgroups.iter() {
            subgroup.print_tree(indent + 1);
        }
    }

    // Вывод информации о текущей группе
    pub fn print_info(&self) {
        println!("\n📊 Group: {:?}", self.key);
        println!("  Path: {:?}", self.get_path());
        println!("  Items: {}", self.data.len());
        println!("  Depth: {}", self.depth);
        println!("  Is root: {}", self.is_root());
        println!("  Subgroups: {}", self.subgroups_count());
        println!("  Max depth: {}", self.max_depth());
        println!("  Total groups: {}", self.total_groups_count());
    }

    // Текущая глубина узла
    pub fn depth(&self) -> usize {
        self.depth
    }
}


pub struct FilterGroup;

impl FilterGroup {
    pub fn filter_parallel<K, V, F>(groups_and_filters: Vec<(Arc<GroupData<K, V>>, F)>)
    where
        K: Ord + Hash + Clone + Debug + Send + Sync,
        V: Send + Sync + Clone,
        F: Fn(&V) -> bool + Send + Sync,
    {   
        groups_and_filters.into_par_iter().for_each(|(group, filter)| {
            group.filter(filter);
        });
    }

    pub fn filter_subgroups_parallel<K, V, F>(groups_and_filters: Vec<(Arc<GroupData<K, V>>, F)>)
    where
        K: Ord + Hash + Clone + Debug + Send + Sync,
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