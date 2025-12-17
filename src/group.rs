use super::{
    errors::{
        GLobalError,
        FilterDataError,
    },
    index::{
        bit::Op,
        field::{
            IndexField,
            IntoIndexFieldEnum,
            FieldOperation,
            FieldValue,
        }
    },
    filter::FilterData,
    result::GlobalResult,
};
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use rayon::prelude::*;
use std::{
    collections::{BTreeMap,btree_map}, 
    fmt::{Debug, Display}, 
    hash::Hash, 
    ops::RangeBounds,
    sync::{
        Arc, 
        Weak
    }
};

pub struct GroupData<K, V>
where
    K: Ord + Clone + Send + Sync + Display + Hash,
    V: Send + Sync + 'static,
{
    pub key: K,
    pub data: Arc<FilterData<V>>,
    // Дерево - Weak ссылка на родителя (циклическая ссылка)
    parent: Option<Weak<GroupData<K, V>>>,
    subgroups: ArcSwap<BTreeMap<K, Arc<GroupData<K, V>>>>,
    pub description: Option<Arc<str>>,
    depth: usize,
    // Mutex только для group_by 
    write_lock: Mutex<()>,
}

impl<K, V> GroupData<K, V>
where
    K: Ord + Clone + Debug + Send + Sync + Display + Hash + 'static,
    V: Send + Sync + Clone + 'static,
{
    // ========================================================================
    // Constructors
    // ========================================================================
    
    // Создать корневую группу
    pub fn new_root(key: K, data: Vec<V>, description: &str) -> Arc<Self> {
        Arc::new(Self {
            key,
            data: Arc::new(FilterData::from_vec(data)),
            parent: None,
            subgroups: ArcSwap::from_pointee(BTreeMap::new()),
            description: Some(Arc::from(description)),
            depth: 0,
            write_lock: Mutex::new(()),
        })
    }
    
    // Создать корневую группу с индексами
    pub fn new_root_with_indexes<F>(
        key: K, 
        data: Vec<V>, 
        description: &str,
        index_builder: F,
    ) -> GlobalResult<Arc<Self>>
    where
        F: FnOnce(FilterData<V>) -> GlobalResult<FilterData<V>>,
    {
        let filter_data = FilterData::from_vec(data);
        let filter_data = index_builder(filter_data)?;
        
        Ok(Arc::new(Self {
            key,
            data: Arc::new(filter_data),
            parent: None,
            subgroups: ArcSwap::from_pointee(BTreeMap::new()),
            description: Some(Arc::from(description)),
            depth: 0,
            write_lock: Mutex::new(()),
        }))
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
            subgroups: ArcSwap::from_pointee(BTreeMap::new()),
            description: Some(description),
            depth,
            write_lock: Mutex::new(()),
        })
    }

    // Grouping Methods 

    // group_by с автоматической сортировкой индексов
    #[inline]
    pub fn group_by<F>(self: &Arc<Self>, extractor: F, description: &str) -> GlobalResult<()>
    where
        F: Fn(&V) -> K + Sync + Send,
    {
        self.group_by_with_indexes(extractor, description, |_| Ok(()))
    }
    
    
    // Индексы автоматически сортируются для cache-friendly доступа
    // ВНИМАНИЕ: Индексы в подгруппах будут хранить Arc<V>, что увеличит ref count!
    // 
    // Используйте когда:
    // - Нужны индексы сразу после группировки
    // - Подгруппы будут активно фильтроваться
    // - Требуется быстрый доступ по ключам
    #[inline]
    pub fn group_by_with_indexes<F, IF>(
        self: &Arc<Self>, 
        extractor: F, 
        description: &str,
        index_creator: IF,
    ) -> GlobalResult<()>
    where
        F: Fn(&V) -> K + Sync + Send,
        IF: Fn(&FilterData<V>) -> GlobalResult<()> + Sync + Send,
    {
        let description_arc: Arc<str> = Arc::from(description);
        let parent_data = match self.data.parent_data() {
            Some(data) => data,
            None => {
                return Err(GLobalError::ParentDataIsEmpty)
            }
        };
        let current_indices = self.data.current_indices();
        // Группируем индексы
        let grouped: BTreeMap<K, Vec<usize>> = current_indices
            .par_iter()
            .fold(
                || BTreeMap::new(),
                |mut acc, &idx| {
                    let item = &parent_data[idx];
                    let key = extractor(item);
                    acc.entry(key)
                        .or_insert_with(|| Vec::with_capacity(64))
                        .push(idx);
                    acc
                },
            )
            .reduce(
                || BTreeMap::new(),
                |mut acc, map| {
                    for (key, mut indices) in map {
                        match acc.entry(key) {
                            btree_map::Entry::Vacant(e) => {
                                e.insert(indices);
                            }
                            btree_map::Entry::Occupied(mut e) => {
                                e.get_mut().append(&mut indices);
                            }
                        }
                    }
                    acc
                },
            );
        let new_depth = self.depth + 1;
        // Параллельное создание подгрупп с индексами
        let result_new_subgroups: GlobalResult<BTreeMap<K, Arc<GroupData<K, V>>>> = grouped
            .into_par_iter()
            .map(|(key, mut indices)| {
                //  СОРТИРУЕМ индексы для cache-friendly доступа!
                indices.sort_unstable();
                let filter_data = FilterData::from_indices(
                    &parent_data,
                    indices,  // Отсортированные индексы
                );
                // Создаём индексы
                // ВАЖНО: Индексы будут хранить Arc<V>, увеличивая ref count!
                index_creator(&filter_data)?;
                let child = Self::new_child(
                    key.clone(),
                    Arc::new(filter_data),
                    self,
                    Arc::clone(&description_arc),
                    new_depth,
                );
                Ok((key, child))
            })
            .collect();

        let new_subgroups = result_new_subgroups?;
        let _guard = self.write_lock.lock();
        self.subgroups.store(Arc::new(new_subgroups));
        Ok(())
    }

    // Index Methods
    
    // Создать индекс в текущей группе
    pub fn create_field_index<IK, F>(&self, name: &str, extractor: F) -> GlobalResult<&Self> 
    where
        IK: Ord + Hash + Clone + Send + Sync + Display + 'static,
        IK: Into<FieldValue>,
        F: Fn(&V) -> IK + Send + Sync + 'static + Clone,
        IndexField<IK>: IntoIndexFieldEnum,
    {
        self.data.create_field_index(name, extractor)?;
        Ok(self)
    }
    
    // Создать индекс во всех подгруппах
    pub fn create_field_index_in_subgroups<IK, F>(&self, name: &str, extractor: F) -> GlobalResult<()>
    where
        IK: Ord + Hash + Clone + Send + Sync + Display + 'static,
        IK: Into<FieldValue>,
        F: Fn(&V) -> IK + Send + Sync + 'static + Clone,
        IndexField<IK>: IntoIndexFieldEnum,
    {
        self.with_all_subgroups(|subgroups| {
            subgroups.par_iter().try_for_each(|subgroup| {
                subgroup.data.create_field_index(name, extractor.clone())
                .map(|_| ())
                .map_err(|err| err)
            })
        })
    }

    // Создать индекс рекурсивно во всём дереве
    pub fn create_field_index_recursive<IK, F>(self: &Arc<Self>, name: &str, extractor: F) -> GlobalResult<()>
    where
        IK: Ord + Hash + Clone + Send + Sync + Display + 'static,
        IK: Into<FieldValue>,
        F: Fn(&V) -> IK + Send + Sync + 'static + Clone,
        IndexField<IK>: IntoIndexFieldEnum,
    {
        self.data.create_field_index(name, extractor.clone())?;
        let subgroups_vec = self.get_all_subgroups();
        subgroups_vec.par_iter().try_for_each(|subgroup: &Arc<GroupData<K, V>>| {
            subgroup.create_field_index_recursive(name, extractor.clone())
            .map(|_|())
            .map_err(|err| err)
        })
    }
    
    // Фильтрация через индекс (read-only)
    pub fn filter_by_field_ops(&self, name: &str, operations: &[(FieldOperation, Op)]) -> GlobalResult<Arc<Vec<Arc<V>>>>
    {
        Ok(self.data.filter_by_field_ops(name, operations)?.items())
    }

    pub fn filter_by_fields_ops(&self, fields: &[(&str, &[(FieldOperation, Op)])]) -> GlobalResult<Arc<Vec<Arc<V>>>>
    {
        Ok(self.data.filter_by_fields_ops(fields)?.items())
    }

    pub fn create_text_index<F>(
        &self,
        name: &str,
        extractor: F
    ) -> GlobalResult<()>
    where F: Fn(&V) -> String + Send + Sync + 'static + Clone,
    {
        let _ = self.data.create_text_index(name, extractor)?;
        Ok(())
    }

    pub fn search_with_text(&self,name:&str, query: &str) -> GlobalResult<Arc<Vec<Arc<V>>>>{
        Ok(self.data.search_with_text(name, query)?.items())
    }

    pub fn search_complex_words_text(
        &self,
        name: &str,
        or_words: &[&str],
        and_words: &[&str],
        not_words: &[&str],
    ) -> GlobalResult<Arc<Vec<Arc<V>>>>{
        Ok(self.data.search_complex_words_text(name, or_words, and_words, not_words)?.items())
    }

    // Validation Methods
    
    // Проверить валидность всех данных в дереве
    // 
    // Возвращает false если где-то parent_data был dropped
    pub fn validate_tree(&self) -> bool {
        if !self.data.is_valid() {
            return false;
        }
        for subgroup in self.get_all_subgroups() {
            if !subgroup.validate_tree() {
                return false;
            }
        }
        true
    }
    
    // Проверить валидность данных текущей группы
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.data.is_valid()
    }

    


    // Navigation Methods

    // Очищаем пути детей
    fn clean_path_to_target(path: &[Arc<Self>]) {
        // Очищаем все узлы кроме последнего
        if path.len() > 1 {
            for node in &path[..path.len() - 1] {
                node.clear_subgroups();
                node.reset_filters();
                node.clear_all_indexes();
            }
        }
        // Очищаем целевой узел
        if let Some(target) = path.last() {
            target.clear_subgroups();
            target.reset_filters();
            target.clear_all_indexes();
        }
    }

    // Получаем список всех родителей к корню
    pub fn get_parents(&self) -> Vec<Arc<Self>> {
        let mut parents = Vec::new();
        let mut current_weak = self.parent.clone();
        while let Some(parent_weak) = current_weak {
            if let Some(parent) = parent_weak.upgrade() {
                parents.push(Arc::clone(&parent));
                current_weak = parent.parent.clone();
            } else {
                break;
            }
        }
        parents
    }

    // Переход к родителю (с полной очисткой состояния) 
    // Очищает:
    // - Все подгруппы (рекурсивно)
    // - Все фильтры (сброс к source)
    // - Все индексы
    pub fn go_to_parent(self: &Arc<Self>) -> Option<Arc<Self>> {
        if let Some(parent_weak) = &self.parent {
            if let Some(parent) = parent_weak.upgrade() {
                // Очищаем подгруппы родителя
                parent.clear_subgroups();
                // Сбрасываем фильтры родителя
                parent.reset_filters();
                // Очищаем все индексы родителя
                parent.clear_all_indexes();
                return Some(parent);
            }
        }
        None
    }

    // Переход к указаному родителю (с полной очисткой состояния) 
    // Очищает:
    // - Все подгруппы (рекурсивно)
    // - Все фильтры (сброс к source)
    // - Все индексы
    pub fn go_to_parent_current(&self, key: &K) -> Option<Arc<Self>> {
        let mut path = Vec::new();
        let mut current_weak = self.parent.clone();
        while let Some(parent_weak) = current_weak {
            if let Some(parent) = parent_weak.upgrade(){
                path.push(Arc::clone(&parent));
                if &parent.key == key {
                    Self::clean_path_to_target(&path);
                    return path.last().cloned()
                }
                current_weak = parent.parent.clone();
            } else {
                break;
            }
        }
        None
    }

    /// Найти родителя по ключу (без очистки, read-only)
    pub fn find_parent(&self, key: &K) -> Option<Arc<Self>> {
        let mut current_weak = self.parent.clone();
        while let Some(parent_weak) = current_weak {
            if let Some(parent) = parent_weak.upgrade() {
                if &parent.key == key {
                    return Some(parent);
                }
                current_weak = parent.parent.clone();
            } else {
                break;
            }
        }
        None
    }

    // Спуск к указанному ребенку
    #[inline]
    pub fn go_to_subgroup(self: &Arc<Self>, key: &K) -> Option<Arc<Self>> {
        self.get_subgroup(key)
    }

    // Возврат в корень с полной очисткой всех промежуточных данных
    // 
    // Очищает ВСЕ узлы на пути к корню
    pub fn go_to_root(self: &Arc<Self>) -> Arc<Self> {
        let mut current = Arc::clone(self);
        // Собираем путь к корню
        let mut path = Vec::new();
        path.push(Arc::clone(&current));
        while let Some(parent_weak) = &current.parent {
            if let Some(parent) = parent_weak.upgrade() {
                path.push(Arc::clone(&parent));
                current = parent;
            } else {
                break;
            }
        }
        // Очищаем все узлы на пути (кроме корня)
        for node in &path[..path.len().saturating_sub(1)] {
            node.clear_subgroups();
            node.reset_filters();
            node.clear_all_indexes();
        }
        // Очищаем только подгруппы корня (фильтры и индексы оставляем)
        let root = path.last().unwrap();
        root.clear_subgroups();
        root.reset_filters();
        root.clear_all_indexes();
        
        Arc::clone(root)
    }

    // Проверка что текущий узел - корень
    #[inline]
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


    // Subgroup Acces

    // Получить конкретную подгруппу по ключу (оптимизировано)
    #[inline]
    pub fn get_subgroup(&self, key: &K) -> Option<Arc<GroupData<K, V>>> {
        self.subgroups.load().get(key).map(Arc::clone)
    }
    
    // Проверка существования подгруппы (быстрее чем get, без Arc clone)
    #[inline]
    pub fn has_subgroup(&self, key: &K) -> bool {
        self.subgroups.load().contains_key(key)
    }

    // Количество подгрупп (оптимизировано - только чтение счетчика)
    #[inline]
    pub fn subgroups_count(&self) -> usize {
        self.subgroups.load().len()
    }

    // Получить ключи всех подгрупп (отсортированные!)
    pub fn subgroups_keys(&self) -> Vec<K> {
        self.subgroups.load().keys().cloned().collect()
    }
    
    // Получить ключи с переиспользованием аллокации (эффективнее для циклов)
    pub fn subgroups_keys_into(&self, keys: &mut Vec<K>) {
        keys.clear();
        let subgroups = self.subgroups.load();
        keys.reserve(subgroups.len());
        keys.extend(subgroups.keys().cloned());
    }
    
    // Быстрый доступ к подгруппам без клонирования (callback pattern)
    pub fn with_subgroups<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&BTreeMap<K, Arc<GroupData<K, V>>>) -> R,
    {
        let subgroups = self.subgroups.load();
        f(&subgroups)
    }
    
    // Получить несколько подгрупп за один load() (batch operation)
    pub fn get_subgroups_batch(&self, keys: &[K]) -> Vec<Option<Arc<GroupData<K, V>>>> {
        let subgroups = self.subgroups.load();
        keys.iter()
            .map(|key| subgroups.get(key).map(Arc::clone))
            .collect()
    }
    
    // Проверить существование нескольких подгрупп (batch operation)
    pub fn has_subgroups_batch(&self, keys: &[K]) -> Vec<bool> {
        let subgroups = self.subgroups.load();
        keys.iter()
            .map(|key| subgroups.contains_key(key))
            .collect()
    }
    
    // Получить первый ключ подгруппы (минимальный)
    #[inline]
    pub fn first_subgroup_key(&self) -> Option<K> {
        self.subgroups.load().keys().next().cloned()
    }
    
    // Получить последний ключ подгруппы (максимальный)
    #[inline]
    pub fn last_subgroup_key(&self) -> Option<K> {
        self.subgroups.load().keys().next_back().cloned()
    }
    
    // Получить подгруппы в диапазоне ключей
    pub fn get_subgroups_range<R>(&self, range: R) -> Vec<Arc<GroupData<K, V>>>
    where
        R: RangeBounds<K>,
    {
        self.subgroups.load()
            .range(range)
            .map(|(_, v)| Arc::clone(v))
            .collect()
    }
    
    // Получить топ N подгрупп (по наибольшим ключам)
    pub fn get_top_n_subgroups(&self, n: usize) -> Vec<Arc<GroupData<K, V>>> {
        self.subgroups.load()
            .iter()
            .rev()
            .take(n)
            .map(|(_, v)| Arc::clone(v))
            .collect()
    }
    
    // Получить нижние N подгрупп (по наименьшим ключам)
    pub fn get_bottom_n_subgroups(&self, n: usize) -> Vec<Arc<GroupData<K, V>>> {
        self.subgroups.load()
            .iter()
            .take(n)
            .map(|(_, v)| Arc::clone(v))
            .collect()
    }

    // ``````
    // let subgroups = group.get_subgroups();
    // for key in keys {
    //     subgroups.get(&key);
    // }
    // ```
    pub fn get_subgroups(&self) -> Arc<BTreeMap<K, Arc<GroupData<K, V>>>> {
        self.subgroups.load_full()
    }

    // Получить все подгруппы (в отсортированном порядке!)
    pub fn get_all_subgroups(&self) -> Vec<Arc<GroupData<K, V>>> {
        self.subgroups.load().values().cloned().collect()
    }

    // Версия БЕЗ клонирования Arc (callback pattern)
    pub fn with_all_subgroups<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[Arc<GroupData<K, V>>]) -> R,
    {
        let subgroups = self.subgroups.load();
        let vec: Vec<_> = subgroups.values().cloned().collect();
        f(&vec)
    }

    // Очистить все подгруппы рекурсивно
    pub fn clear_subgroups(&self) {
        let current_subgroups = self.subgroups.load();
        // Рекурсивно очищаем детей
        for (_, subgroup) in current_subgroups.iter() {
            subgroup.clear_subgroups();
        }
        let _guard = self.write_lock.lock();
        self.subgroups.store(Arc::new(BTreeMap::new()));
    }

    // Очистить все индексы в текущей группе
    pub fn clear_all_indexes(&self) {
        self.data.clear_all_indexes();
    }

    // Очистить только field индексы
    pub fn clear_field_indexes(&self) {
        self.data.clear_filed_index();
    }

    // Очистить только text индексы
    pub fn clear_text_indexes(&self) {
        self.data.clear_text_indexes();
    }


    // Tree Traversal

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
        let all_nodes = self.collect_all_groups();
        all_nodes.par_iter().for_each(|node| {
            callback(node);
        }); 
    }

    // Собрать все группы рекурсивно
    pub fn collect_all_groups(self: &Arc<Self>) -> Vec<Arc<GroupData<K, V>>> {
        let mut result = Vec::new();
        let mut stack = vec![Arc::clone(self)];
        while let Some(node) = stack.pop() {
            result.push(Arc::clone(&node));
            let subgroups = node.subgroups.load();
            stack.extend(subgroups.values().cloned());
        }
        result
    }

    // Statistics

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
    
    // Текущая глубина узла
    #[inline]
    pub fn depth(&self) -> usize {
        self.depth
    }

    // Filtering

    // Фильтрация данных в текущей группе
    pub fn filter<F>(&self, predicate: F) -> GlobalResult<Arc<Vec<Arc<V>>>>
    where
        F: Fn(&V) -> bool + Sync + Send,
    {
        Ok(self.data.filter(predicate)?.items())
    }

    // Сброс фильтров к исходным данным
    pub fn reset_filters(&self) {
        self.data.reset_to_source();
    }

    // Фильтрация всех подгрупп
    pub fn filter_subgroups<F>(&self, predicate: F) -> GlobalResult<BTreeMap<K,Arc<Vec<Arc<V>>>>>
    where
        F: Fn(&V) -> bool + Sync + Send + Clone,
    {   
        let subgroups = self.subgroups.load();
        if subgroups.len() < 8 {
            // Последовательно
            let mut results = BTreeMap::new();
            for (key, subgroup) in subgroups.iter() {
                let items = match subgroup.filter(predicate.clone()) {
                    Ok(items) => items,
                    Err(GLobalError::FilterData(FilterDataError::DataNotFound)) => {
                        Arc::new(Vec::new())
                    }
                    Err(err) => return Err(err),
                };
                results.insert(key.clone(), items);
            }
            return Ok(results);
        }
        
        // Параллельно
        let results: Result<BTreeMap<K, Arc<Vec<Arc<V>>>>, GLobalError> = subgroups
            .par_iter()
            .map(|(key, subgroup)| {
                let items = match subgroup.filter(predicate.clone()) {
                    Ok(items) => items,
                    Err(GLobalError::FilterData(FilterDataError::DataNotFound)) => {
                        Arc::new(Vec::new())
                    }
                    Err(err) => return Err(err),
                };
                Ok((key.clone(), items))
            })
            .collect();
        results
    }

    // Display/Debug

    // Вывод дерева в консоль для отладки
    pub fn print_tree(&self, indent: usize) {
        let prefix = "  ".repeat(indent);
        let valid_marker = if self.is_valid() { "✓" } else { "✗" };
        println!("{}📁 {:?} ({} items, depth: {}) {}", 
                 prefix, self.key, self.data.len(), self.depth, valid_marker);
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
        println!("  Valid: {}", self.is_valid());
        println!("  Depth: {}", self.depth);
        println!("  Is root: {}", self.is_root());
        println!("  Subgroups: {}", self.subgroups_count());
        println!("  Max depth: {}", self.max_depth());
        println!("  Total groups: {}", self.total_groups_count());
        
        if self.subgroups_count() > 0 {
            let keys = self.subgroups_keys();
            println!("  Subgroup keys (sorted): {:?}", &keys[..keys.len().min(10)]);
        }
        
        let indexes = self.data.list_indexes();
        if !indexes.is_empty() {
            println!("  Indexes: {:?}", indexes);
        }
    }
}


// Parallel Operations Helper

pub struct FilterGroup;

impl FilterGroup {
    pub fn filter_parallel<K, V, F>(groups_and_filters: Vec<(Arc<GroupData<K, V>>, F)>) -> GlobalResult<()>
    where
        K: Ord + Clone + Debug + Send + Sync + Display + Hash + 'static,
        V: Send + Sync + Clone + 'static,
        F: Fn(&V) -> bool + Send + Sync,
    {   
        groups_and_filters.into_par_iter().try_for_each(|(group, filter)| {
            group.filter(filter)?;
            Ok(())
        })
    }

    pub fn filter_subgroups_parallel<K, V, F>(groups_and_filters: Vec<(Arc<GroupData<K, V>>, F)>) -> GlobalResult<()>
    where
        K: Ord + Clone + Debug + Send + Sync + Display + Hash + 'static,
        V: Send + Sync + Clone + 'static,
        F: Fn(&V) -> bool + Send + Sync + Clone,
    {
        groups_and_filters.into_par_iter().try_for_each(|(group, filter)| {
            group.filter_subgroups(filter)?;
            Ok(())
        })
    }
    
    // Создать индексы во всех группах параллельно
    pub fn create_field_indexes_parallel<K, V, IK, F>(
        groups: Vec<Arc<GroupData<K, V>>>,
        index_name: &str,
        extractor: F,
    ) -> GlobalResult<()>
    where
        K: Ord + Clone + Debug + Send + Sync + Display + Hash + 'static,
        V: Send + Sync + Clone + 'static,
        IK: Ord + Hash + Clone + Send + Sync + Display + 'static,
        IK: Into<FieldValue>,
        F: Fn(&V) -> IK + Send + Sync + Clone + 'static,
        IndexField<IK>: IntoIndexFieldEnum,
    {
        let name = index_name.to_string();
        groups.into_par_iter().try_for_each(|group| {
            group.data.create_field_index(&name, extractor.clone())
            .map(|_| ())
            .map_err(|err| err)
        })
    }
}

#[macro_export]
macro_rules! group_filter_parallel {
    ( $( $group:expr => $filter:expr ),+ $(,)? ) => {
        {   
            use parking_lot::Mutex;
            use std::sync::Arc;

            let results = Arc::new(Mutex::new(Vec::new()));
            rayon::scope(|s| {
                $(
                    {
                        let results = Arc::clone(&results);
                        let group = Arc::clone(&$group);
                        s.spawn(move |_| {
                            let result = group.filter($filter);
                            results.lock().push(result);
                        });
                    }
                )+
            });
            let results = Arc::try_unwrap(results)
                .unwrap()
                .into_inner();
            
            results.into_iter()
                .find_map(|r| r.err())
                .map_or(Ok(()), Err)
        }
    };
}

#[macro_export]
macro_rules! group_filter_subgroups_parallel {
    ( $( $group:expr => $filter:expr ),+ $(,)? ) => {
        {
            use parking_lot::Mutex;

            let results = Arc::new(Mutex::new(Vec::new()));
            rayon::scope(|s| {
                $(
                    {
                        let results = Arc::clone(&results);
                        let group = Arc::clone(&$group);
                        s.spawn(move |_| {
                            let result = group.filter_subgroups($filter);
                            results.lock().push(result);
                        });
                    }
                )+
            });
            let results = Arc::try_unwrap(results)
                .unwrap()
                .into_inner();
            
            results.into_iter()
                .find_map(|r| r.err())
                .map_or(Ok(()), Err)
        }
    };
}