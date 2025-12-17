use crate::index::field::IndexFieldEnum;

use super::{
    errors::{
        GLobalError,
        IndexError,
        FilterDataError,
    },
    index::{
        INDEX_FIELD,
        INDEX_TEXT,
        CompatibilityAction as IndexCompatibilityAction,
        ExtractorFieldValue,
        IndexType,
        bit::Op,
        field::{
            FieldValue,
            IntoIndexFieldEnum,
            IndexField,
            FieldOperation,
        },
        storage::DataStorage,
        text::{TextIndex,TextIndexStats},
    },
    model::MemoryStats,
    result::{
        IndexResult,
        GlobalResult
    },
};
use arc_swap::ArcSwap;
use dashmap::DashMap;
use parking_lot::RwLock;
use rayon::prelude::*;
use roaring::RoaringBitmap;
use std::{
    cmp::{Ord,PartialOrd},
    fmt::Display,
    hash::Hash,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering}
    },
};


const MAX_HISTORY: usize = 50;
const MATERIALIZATION_THRESHOLD: usize = 50_000;
const SMALL_DATASET_THRESHOLD: usize = 1000;
const SELECTIVITY_THRESHOLD: f64 = 0.1;

// FilterData

pub struct FilterData<T>
where
    T: Send + Sync + 'static,
{
    storage: DataStorage<T>,
    level_info: ArcSwap<Vec<Arc<str>>>,
    current_level: Arc<AtomicUsize>,
    indexes: DashMap<String, Arc<IndexType<T>>>,
    source_indices_mask: ArcSwap<Option<Arc<RoaringBitmap>>>,
    write_lock: RwLock<()>,
}

struct FilterResult {
    bitmap: RoaringBitmap,
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
        let initial_indices: Vec<usize> = (0..arc_items.len()).collect();
        let initial_indices_arc = Arc::new(initial_indices);
        
        Self {
            storage: DataStorage::Owned {
                source: Arc::clone(&arc_items),
                current_indices: ArcSwap::new(initial_indices_arc.clone()),
                current_cache: ArcSwap::new(Arc::new(None)),
                full_indices: initial_indices_arc,
                levels: ArcSwap::from_pointee(vec![Arc::clone(&arc_items)]),
                level_indices:  ArcSwap::from_pointee(vec![Arc::new((0..arc_items.len()).collect())]),
            },
            level_info: ArcSwap::from_pointee(vec![Arc::from("Source")]),
            current_level: Arc::new(AtomicUsize::new(0)),
            indexes: DashMap::new(),
            source_indices_mask: ArcSwap::from_pointee(None),
            write_lock: RwLock::new(()),
        }
    }

    pub fn from_vec_arc_value(items: Vec<Arc<T>>) -> Self {
        let arc_items = Arc::new(items);
        let initial_indices: Vec<usize> = (0..arc_items.len()).collect();
        let initial_indices_arc = Arc::new(initial_indices);
        Self {
            storage: DataStorage::Owned {
                source: Arc::clone(&arc_items),
                current_indices: ArcSwap::new(initial_indices_arc.clone()),
                current_cache: ArcSwap::new(Arc::new(None)),
                full_indices: initial_indices_arc,
                levels: ArcSwap::from_pointee(vec![Arc::clone(&arc_items)]),
                level_indices: ArcSwap::from_pointee(vec![Arc::new((0..arc_items.len()).collect())]),
            },
            level_info: ArcSwap::from_pointee(vec![Arc::from("Source")]),
            current_level: Arc::new(AtomicUsize::new(0)),
            indexes: DashMap::new(),
            source_indices_mask: ArcSwap::from_pointee(None),
            write_lock: RwLock::new(()),
        }
    }
    
    pub fn from_indices(parent_data: &Arc<Vec<Arc<T>>>, indices: Vec<usize>) -> Self {
        let source_indices = Arc::new(indices);
        Self {
            storage: DataStorage::Indexed {
                parent_data: Arc::downgrade(parent_data),
                source_indices: Arc::clone(&source_indices),
                current_indices: ArcSwap::new(Arc::clone(&source_indices)),
                index_levels: ArcSwap::from_pointee(vec![source_indices]),
            },
            level_info: ArcSwap::from_pointee(vec![Arc::from("Source")]),
            current_level: Arc::new(AtomicUsize::new(0)),
            indexes: DashMap::new(),
            source_indices_mask: ArcSwap::from_pointee(None),
            write_lock: RwLock::new(()),
        }
    }
    
    // Core Access Methods


    pub fn current_indices(&self) -> Arc<Vec<usize>> {
        match &self.storage {
            DataStorage::Owned { current_indices, .. } |
            DataStorage::Indexed { current_indices, .. } => {
                (*current_indices.load()).clone()
            }
        }
    }
    
    pub fn items(&self) -> Arc<Vec<Arc<T>>> {
        match &self.storage {
            DataStorage::Owned {
                current_indices,
                current_cache,
                source,
                ..
            } => {
                // Проверяем кеш
                let cache_guard = current_cache.load();
                if let Some(cached) = cache_guard.as_ref() {
                    return Arc::clone(cached);
                }
                
                // Материализуем из индексов
                let indices = current_indices.load();  // Arc<Vec<usize>>
                let items: Vec<Arc<T>> = indices
                    .iter()
                    .filter_map(|&idx| source.get(idx).cloned())
                    .collect();
                
                let items_arc = Arc::new(items);
                if items_arc.len() < MATERIALIZATION_THRESHOLD {
                    current_cache.store(Arc::new(Some(Arc::clone(&items_arc))));
                }
                
                items_arc
            }
            
            DataStorage::Indexed {
                parent_data,
                current_indices,
                ..
            } => {
                if let Some(parent) = parent_data.upgrade() {
                    let indices = current_indices.load();  // Arc<Vec<usize>>
                    
                    // Параллельная материализация для больших наборов
                    let items: Vec<Arc<T>> = if indices.len() > 100_000 {
                        indices
                            .par_iter()
                            .filter_map(|&idx| parent.get(idx).cloned())
                            .collect()
                    } else {
                        indices
                            .iter()
                            .filter_map(|&idx| parent.get(idx).cloned())
                            .collect()
                    };
                    
                    Arc::new(items)
                } else {
                    // Родительские данные удалены
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

    pub fn indexes(&self) -> &DashMap<String, Arc<IndexType<T>>>{
        &self.indexes
    }
    
    // Пересечение индексов (AND) через RoaringBitmap 
    // 
    // Возвращает индексы элементов, которые присутствуют во ВСЕХ переданных массивах.
    // Использует RoaringBitmap для эффективного битового AND.
    // 
    // # Пример
    // 
    // let a = vec![1, 2, 3, 4, 5];
    // let b = vec![2, 4, 6, 8];
    // let result = FilterData::intersect_indices(&a, &b);
    // assert_eq!(result, vec![2, 4]);
    // 
    fn intersect_indices(a: &[usize], b: &[usize]) -> Vec<usize> {
        if a.is_empty() || b.is_empty() {
            return Vec::new();
        }
        let bitmap_a: RoaringBitmap = a.iter().map(|&i| i as u32).collect();
        let bitmap_b: RoaringBitmap = b.iter().map(|&i| i as u32).collect();
        let result = bitmap_a & bitmap_b;
        result.iter().map(|i| i as usize).collect()
    }

    // Standard Index Methods - возвращают ИНДЕКСЫ 

    fn check_index_type_compability(
        &self,
        name: &str, 
        expected_type: &str, 
        action: IndexCompatibilityAction
    ) -> IndexResult<()> {
        match self.indexes.get(name) {
            Some(exist_index_ref) => {
                let exist_index_type = exist_index_ref.value().index_type();
                if expected_type != exist_index_type {
                    let err = match action {
                        IndexCompatibilityAction::Check => IndexError::Compatibility {
                            name: name.to_string(), 
                            type_exist: exist_index_type.to_string(), 
                            type_expect: expected_type.to_string(),
                        },
                        IndexCompatibilityAction::Replace => IndexError::Replace { 
                            name: name.to_string(), 
                            type_exist: exist_index_type.to_string(), 
                            type_expect: expected_type.to_string(),
                        }    
                    };
                    Err(err)
                } else {
                    Ok(())
                }
            }
            None => Err(IndexError::NotFound { name: name.to_string() })
        }
    }

    fn create_field_value_extractor<F,V>(extractor: F) -> Arc<dyn Fn(&T) -> FieldValue + Send + Sync>
    where 
        F: Fn(&T) -> V + Send + Sync + 'static,
        V: Into<FieldValue> + 'static,

    {

        Arc::new(move |item: &T| -> FieldValue{
            let value: V = extractor(item);
            value.into()
        })
    }

    pub fn create_field_index<V,F>(
        &self,
        name: &str,
        extractor: F,
    ) -> GlobalResult<&Self>
    where 
        V: Eq + Hash + Clone + Send + Sync + Ord + PartialOrd + Display + 'static,
        F: Fn(&T) -> V + Send + Sync + Clone + 'static,
        IndexField<V>: IntoIndexFieldEnum,
        V: Into<FieldValue> + 'static, 

    {
        // Проверяем существует ли Index с таким наименованием
        if self.has_index(name) {
            if let Err(err) = self.check_index_type_compability(
                name, 
                INDEX_FIELD, 
                IndexCompatibilityAction::Replace
            ) {
                return Err(GLobalError::Index(err));
            }
            // drop_index теперь просто удаляет из DashMap
            self.drop_index(name);
        }
        let extractor_clone = extractor.clone();
        let items = self.items();
        let index = IndexField::build(&items, extractor);
        self.indexes.insert(
            name.to_string(),
            Arc::new(
                IndexType::Field(
                    (
                        index.into_enum(),
                        Self::create_field_value_extractor(extractor_clone),
                    )
                )
            ),
        );
        Ok(self)
    }

    pub fn get_index(&self, name: &str) -> GlobalResult<Arc<IndexType<T>>> {
        self.indexes.get(name)
            .ok_or(GLobalError::Index(IndexError::NotFound {
                name: name.to_string(),
            }))
            .map(|guard| guard.clone())
    }

    fn apply_field_operations(
        &self,
        field_index: &IndexFieldEnum,
        operations: &[(FieldOperation, Op)],
    ) -> GlobalResult<RoaringBitmap> {
        let bitmap = if operations.len() == 1 {
            field_index.filter_operation(&operations[0].0)
        } else {
            field_index.filter_operations(operations)
        };
        let result = bitmap.map_err(|err|GLobalError::Index(IndexError::Field(err)))?;
        Ok(result)
    }

    fn apply_field_bitmap(
        &self,
        bitmap: RoaringBitmap,
        description: String,
    ) -> GlobalResult<&Self> {
        if bitmap.is_empty() {
            return Err(GLobalError::FilterData(FilterDataError::DataNotFoundByIndex {
                name: description.clone(),
            }));
        }
        
        let current_mask_opt = self.source_indices_mask.load();
        let final_bitmap = if let Some(current_mask) = current_mask_opt.as_ref() {
            // Маска есть - используем напрямую
            &**current_mask & &bitmap
        } else {
            // маски нет - создаем из current_indices
            match &self.storage {
                DataStorage::Owned { current_indices, full_indices, .. } => {
                    let current = current_indices.load();
                    let full = full_indices;
                    if current.len() < full.len() {
                        // Есть фильтрация - создаем маску из current_indices
                        let current_bitmap: RoaringBitmap = current.iter()
                            .map(|&i| i as u32)
                            .collect();
                        // Сохраняем маску для следующих индексных операций
                        self.source_indices_mask.store(Arc::new(Some(Arc::new(current_bitmap.clone()))));
                        // Пересечение с индексом
                        current_bitmap & bitmap
                    } else {
                        // Нет фильтрации - используем индекс напрямую
                        bitmap
                    }
                }
                DataStorage::Indexed { .. } => {
                    // Для Indexed storage используем bitmap как есть
                    bitmap
                }
            }
        };
        if final_bitmap.is_empty() {
            return Err(GLobalError::FilterData(FilterDataError::DataNotFoundByIndexCurrent {
                name: description.clone(),
            }));
        }
        self.apply_filtered_items_with_bitmap(final_bitmap, description)
    }

    #[inline(always)]
    fn evaluate_field_operations(
        value: &FieldValue,
        operations: &[(FieldOperation, Op)],
    ) -> bool {
        let mut result = true;
        
        for (operation, op_type) in operations {
            let matches = operation.evaluate(value);
            
            match op_type {
                Op::And => {
                    result = result && matches;
                    if !result {
                        return false;
                    }
                }
                Op::Or => {
                    result = result || matches;
                }
                Op::AndNot => {
                    result = result && !matches;
                    if !result {
                        return false;
                    }
                }
                Op::Xor => {
                    result = result ^ matches;
                }
                Op::Invert => {
                    result = !result;
                }
            }
        }
        result
    }

    fn build_field_predicate(
        &self,
        fields: &[(&ExtractorFieldValue<T>, &[(FieldOperation, Op)])],
    ) -> GlobalResult<impl Fn(&T) -> bool + Send + Sync + '_> {
        let field_predicates = fields.iter()
        .map(|(extractor,operations)| {
            ((*extractor).clone(),operations.to_vec())
            })
        .collect::<Vec<(ExtractorFieldValue<T>, Vec<(FieldOperation, Op)>)>>();
        Ok(move |item: &T| -> bool {
            for (extractor, operations) in &field_predicates {
                let field_value = extractor(item);
                if !Self::evaluate_field_operations(&field_value, operations) {
                    return false;
                }
            }
            true
        })
    }

    fn estimate_selectivity_from_indexes(
        &self, 
        container: &[(&str,&IndexFieldEnum, &[(FieldOperation, Op)])]
    ) -> f64 {
        if container.is_empty() {
            return 1.0;
        }
        let mut combined_selectivity = 1.0;
        for (_,index, operations) in container {       
            let selectivity = index.estimate_operations_selectivity(operations);
            combined_selectivity *= selectivity;
            if combined_selectivity < 0.001 {
                return combined_selectivity;
            }
        }
        combined_selectivity
    }

    fn need_to_use_index(&self, fields: &[(&str,&IndexFieldEnum, &[(FieldOperation, Op)])]) -> GlobalResult<bool> {
        if self.len() < SMALL_DATASET_THRESHOLD {
            return Ok(false)
        }

        if fields.iter().any(|(_, index, operations)| {
            operations.iter().any(|(op, _)| !index.is_efficient_for(op))
        }) {
            return Ok(false);
        }
        
        let estimate_selectivity = self.estimate_selectivity_from_indexes(fields);
        if estimate_selectivity > SELECTIVITY_THRESHOLD{
            return Ok(false)
        }
        
        Ok(true)
    }

    pub fn filter_by_field_ops(
        &self,
        name: &str,
        operations: &[(FieldOperation, Op)],
    ) -> GlobalResult<&Self> {
        if operations.is_empty() {
            return Err(GLobalError::FilterData(FilterDataError::EmptyOperations));
        }
        let index = self.get_index(name)?;
        let (field_index,extractor) = index.as_field().ok_or(GLobalError::Index(IndexError::Compatibility 
            {
                name: name.to_string(),
                type_exist: index.index_type().to_string(),
                type_expect: INDEX_FIELD.to_string(),
            }
        ))?;
        let mut temp_container = Vec::<(&str,&IndexFieldEnum,&[(FieldOperation, Op)])>::with_capacity(1);
        let mut extractor_fields = Vec::<(&ExtractorFieldValue<T>,&[(FieldOperation, Op)])>::with_capacity(1);
        temp_container.push((name,field_index,operations));
        extractor_fields.push((extractor,operations));
        let can_use_field_indexes = self.need_to_use_index(&temp_container)?;
        if can_use_field_indexes{
            self.do_filter_by_fields_ops(&temp_container)?;
        } else {
            let predicate = self.build_field_predicate(&extractor_fields)?;
            self.filter(predicate)?;
        }
        Ok(self)
    }

    fn do_filter_by_fields_ops(
        &self,
        fields: &[(&str,&IndexFieldEnum, &[(FieldOperation, Op)])],
    ) -> GlobalResult<&Self> {
        if fields.is_empty() {
            return Err(GLobalError::FilterData(FilterDataError::EmptyOperations));
        }
        // Получаем bitmap от каждого индекса
        let mut combined_bitmap: Option<RoaringBitmap> = None;
        let mut descriptions = Vec::<String>::with_capacity(fields.len());
        for (field_name,field_index, operations) in fields {
            if operations.is_empty() {
                continue;
            }
            // Получаем bitmap для текущего поля
            let field_bitmap = self.apply_field_operations(field_index, operations)?;
            // Формируем описание операции
            let op_desc = operations.iter()
                .map(|(op, _)| format!("{}", op))
                .collect::<Vec<_>>()
                .join(", ");
            descriptions.push(format!("{}: {}", field_name, op_desc));
            // Объединяем bitmapы через AND
            combined_bitmap = Some(match combined_bitmap {
                None => field_bitmap,
                Some(existing) => existing & field_bitmap,
            });
        }

        let final_bitmap = combined_bitmap
            .ok_or(GLobalError::FilterData(FilterDataError::EmptyOperations))?;
        // Формируем итоговое описание
        let description = descriptions.join(" AND ");
        // Применяем результат ОДИН раз
        self.apply_field_bitmap(final_bitmap, description)
    }

    pub fn filter_by_fields_ops(
        &self,
        fields: &[(&str, &[(FieldOperation, Op)])],
    ) -> GlobalResult<&Self> {
        if fields.is_empty() {
            return Err(GLobalError::FilterData(FilterDataError::EmptyOperations));
        }
        let indexes: Vec<Arc<IndexType<T>>> = fields
        .iter()
        .map(|(name, _)| self.get_index(name))
        .collect::<Result<_, _>>()?;

        let mut temp_container = Vec::<
            (
                &str,
                &IndexFieldEnum,
                &[(FieldOperation, Op)],
            )
        >::with_capacity(fields.len());
        let mut temp_extractors = Vec::<(&ExtractorFieldValue<T>,&[(FieldOperation, Op)])>::with_capacity(fields.len());
        for (n,(name,operations)) in fields.iter().enumerate(){
            //let field_index = self.get_field_index(*name)?;
            let index_ref = &indexes[n];
            let (field_index,extractor) = index_ref.as_field()
                .ok_or(GLobalError::Index(IndexError::Compatibility {
                    name: name.to_string(),
                    type_exist: index_ref.index_type().to_string(),
                    type_expect: INDEX_FIELD.to_string(),
                }
            ))?;
            temp_container.push((*name,field_index,*operations));
            temp_extractors.push((extractor,*operations));
        }

        let can_use_field_indexes = self.need_to_use_index(&temp_container)?;   
        if can_use_field_indexes{
            self.do_filter_by_fields_ops(&temp_container)?;
        } else {
            let predicate = self.build_field_predicate(&temp_extractors)?;
            self.filter(predicate)?;
        }
        Ok(self)
    }

    #[inline]
    fn update_level_metadata(&self, current_level: usize, info: String) -> GlobalResult<()> {
        let mut new_level_info = Vec::with_capacity(current_level + 2);
        new_level_info.extend_from_slice(&self.level_info.load());
        new_level_info.push(Arc::from(info));
        self.level_info.store(Arc::new(new_level_info));
        self.current_level.store(current_level + 1, Ordering::Release);
        Ok(())
    }

    fn apply_owned_data(&self, result: FilterResult, info: String) -> GlobalResult<()> {
        match &self.storage {
            DataStorage::Owned { 
                source, 
                current_indices, 
                current_cache,
                full_indices: _,
                levels, 
                level_indices 
            } => {
                let total_level = self.current_level.load(Ordering::Relaxed);
                let bitmap_arc = Arc::new(result.bitmap);
                // сохраняем bitmap для drill-down с индексами
                self.source_indices_mask.store(Arc::new(Some(Arc::clone(&bitmap_arc))));
                // конвертируем bitmap → Vec<usize>
                let indices: Vec<usize> = bitmap_arc.iter().map(|i| i as usize).collect();
                let indices_arc = Arc::new(indices);
                current_indices.store(Arc::clone(&indices_arc));
                // обновляем level_indices
                let mut new_level_indices = Vec::with_capacity(total_level + 2);
                new_level_indices.extend_from_slice(&level_indices.load());
                new_level_indices.push(indices_arc.clone());
                level_indices.store(Arc::new(new_level_indices));
                // синхронизируем levels с level_indices
                if indices_arc.len() < MATERIALIZATION_THRESHOLD {
                    // Материализуем для маленьких
                    let items: Vec<Arc<T>> = indices_arc
                        .iter()
                        .filter_map(|&idx| source.get(idx).cloned())
                        .collect();
                    let items_arc = Arc::new(items);
                    let mut new_levels = Vec::with_capacity(total_level + 2);
                    new_levels.extend_from_slice(&levels.load());
                    new_levels.push(items_arc.clone());
                    levels.store(Arc::new(new_levels));
                    current_cache.store(Arc::new(Some(items_arc)));
                } else {
                    // для больших результатов
                    let mut new_levels = Vec::with_capacity(total_level + 2);
                    new_levels.extend_from_slice(&levels.load());
                    new_levels.push(Arc::new(Vec::new()));  // Пустой placeholder
                    levels.store(Arc::new(new_levels));
                    current_cache.store(Arc::new(None));
                }
                
                // Метаданные
                self.update_level_metadata(total_level, info)?;
                Ok(())
            },
            _ => Err(GLobalError::FilterData(FilterDataError::WrongSaveDataIndexed)),
        }
    }

    fn apply_indexed_data(
        &self,
        indices: Vec<usize>,
        info: String,
    ) -> GlobalResult<()> {
        match &self.storage {
            DataStorage::Indexed {
                parent_data,
                current_indices,
                index_levels,
                ..
            } => {
                let _parent = parent_data.upgrade()
                    .ok_or(GLobalError::FilterData(FilterDataError::ParentDataIsEmpty))?;
                
                let levels_guard = index_levels.load();
                let total_level = levels_guard.len();
                if indices.is_empty() {
                    return Err(GLobalError::FilterData(
                        FilterDataError::DataNotFoundByIndexCurrent { name: info }
                    ));
                }

                current_indices.store(Arc::new(indices.clone()));
                let indices_arc = Arc::new(indices);
                let mut new_levels = Vec::with_capacity(total_level + 1);
                new_levels.extend_from_slice(&levels_guard);
                new_levels.push(indices_arc);
                index_levels.store(Arc::new(new_levels));
                // Метаданные
                let info_guard = self.level_info.load();
                let mut new_info = Vec::with_capacity(info_guard.len() + 1);
                new_info.extend_from_slice(&info_guard);
                new_info.push(Arc::from(info));
                self.level_info.store(Arc::new(new_info));
                self.current_level.store(total_level, Ordering::Release);
                Ok(())
            },
            _ => Err(GLobalError::FilterData(FilterDataError::WrongSaveDataOwned)),
        }
    }

    fn apply_filtered_items_with_bitmap(
        &self,
        final_bitmap: RoaringBitmap,
        info: String
    ) -> GlobalResult<&Self> {
        let _guard = self.write_lock.write();
        
        match &self.storage {
            DataStorage::Owned { levels, .. } => {
                let levels_guard = levels.load();
                if levels_guard.len() > MAX_HISTORY {
                    return Err(GLobalError::FilterData(FilterDataError::MaxHistoryExceeded {
                        current: levels_guard.len(),
                        max: MAX_HISTORY,
                    }));
                }

                if final_bitmap.is_empty() {
                    return Err(GLobalError::FilterData(
                        FilterDataError::DataNotFoundByIndexCurrent { name: info }
                    ));
                }

                let result = FilterResult {
                    bitmap: final_bitmap,
                };
                self.apply_owned_data(result, info)?;
            },
            DataStorage::Indexed { index_levels, .. } => {
                let levels_guard = index_levels.load();
                if levels_guard.len() > MAX_HISTORY {
                    return Err(GLobalError::FilterData(FilterDataError::MaxHistoryExceeded {
                        current: levels_guard.len(),
                        max: MAX_HISTORY,
                    }));
                }

                let indices: Vec<usize> = final_bitmap.iter()
                    .map(|i| i as usize)
                    .collect();
                self.apply_indexed_data(indices, info)?;
            }
        }
        
        Ok(self)
    }

    fn apply_filtered_indices(
        &self,
        indices: Vec<usize>,
        info: String,
    ) -> GlobalResult<()> {
        if indices.is_empty() {
            return Err(GLobalError::FilterData(FilterDataError::DataNotFound));
        }
        
        match &self.storage {
            DataStorage::Owned {
                source,
                current_indices,
                current_cache,
                levels,
                level_indices,
                ..
            } => {
                let total_level = self.current_level.load(Ordering::Relaxed);
                let levels_guard = levels.load();
                if levels_guard.len() > MAX_HISTORY {
                    return Err(GLobalError::FilterData(FilterDataError::MaxHistoryExceeded {
                        current: levels_guard.len(),
                        max: MAX_HISTORY,
                    }));
                }
                
                let indices_arc = Arc::new(indices);
                current_indices.store(indices_arc.clone());
                self.source_indices_mask.store(Arc::new(None));
                let mut new_level_indices = Vec::with_capacity(total_level + 2);
                new_level_indices.extend_from_slice(&level_indices.load());
                new_level_indices.push(indices_arc.clone());
                level_indices.store(Arc::new(new_level_indices));
                if indices_arc.len() < MATERIALIZATION_THRESHOLD {
                    let items: Vec<Arc<T>> = indices_arc
                        .iter()
                        .filter_map(|&idx| source.get(idx).cloned())
                        .collect();
                    let items_arc = Arc::new(items);
                    
                    let mut new_levels = Vec::with_capacity(total_level + 2);
                    new_levels.extend_from_slice(&levels_guard);
                    new_levels.push(items_arc.clone());
                    levels.store(Arc::new(new_levels));
                    current_cache.store(Arc::new(Some(items_arc)));
                } else {
                    let mut new_levels = Vec::with_capacity(total_level + 2);
                    new_levels.extend_from_slice(&levels_guard);
                    new_levels.push(Arc::new(Vec::new()));
                    levels.store(Arc::new(new_levels));
                    current_cache.store(Arc::new(None));
                }
                self.update_level_metadata(total_level, info)?;
                Ok(())
            },
            DataStorage::Indexed {
                parent_data,
                current_indices,
                index_levels,
                ..
            } => {
                let _parent = parent_data.upgrade()
                    .ok_or(GLobalError::FilterData(FilterDataError::ParentDataIsEmpty))?;
                
                let total_level = self.current_level.load(Ordering::Relaxed);
                let levels_guard = index_levels.load();
                if levels_guard.len() > MAX_HISTORY {
                    return Err(GLobalError::FilterData(FilterDataError::MaxHistoryExceeded {
                        current: levels_guard.len(),
                        max: MAX_HISTORY,
                    }));
                }
                
                let indices_arc = Arc::new(indices);
                current_indices.store(indices_arc.clone());
                let mut new_levels = Vec::with_capacity(total_level + 2);
                new_levels.extend_from_slice(&levels_guard);
                new_levels.push(indices_arc);
                index_levels.store(Arc::new(new_levels));
                self.update_level_metadata(total_level, info)?;
                
                Ok(())
            }
        }
    }

    fn apply_filtered_items_with_indices(
        &self,
        indices: Vec<usize>,
        info: String
    ) -> GlobalResult<&Self> {
        let _guard = self.write_lock.write();
        self.apply_filtered_indices(indices, info)?;
        Ok(self)
    }
    
    fn apply_filtered_items<F>(&self, predicate: F, info: String) -> GlobalResult<&Self>
    where
        F: Fn(&T) -> bool + Send + Sync,
    {
        let _guard = self.write_lock.write();
        match &self.storage {
            DataStorage::Owned {
                source,
                current_indices,
                levels,
                ..
            } => {
                let levels_guard = levels.load();
                if levels_guard.len() > MAX_HISTORY {
                    return Err(GLobalError::FilterData(FilterDataError::MaxHistoryExceeded {
                        current: levels_guard.len(),
                        max: MAX_HISTORY,
                    }));
                }
                
                let current = current_indices.load();
                let filtered_indices: Vec<usize> = if current.len() < 10_000 {
                    current.iter()
                        .filter_map(|&idx| {
                            source.get(idx)
                                .filter(|item| predicate(item))
                                .map(|_| idx)
                        })
                        .collect()
                } else {
                    current.par_iter()
                        .filter_map(|&idx| {
                            source.get(idx)
                                .filter(|item| predicate(item))
                                .map(|_| idx)
                        })
                        .collect()
                };
                if filtered_indices.is_empty() {
                    return Err(GLobalError::FilterData(FilterDataError::DataNotFound));
                }
                // Сразу применяем через apply_filtered_items_with_indices
                self.apply_filtered_indices(filtered_indices, info)?;
            },
            DataStorage::Indexed {
                parent_data,
                current_indices,
                index_levels,
                ..
            } => {
                let parent = parent_data.upgrade()
                    .ok_or(GLobalError::FilterData(FilterDataError::ParentDataIsEmpty))?;
                
                let levels_guard = index_levels.load();
                if levels_guard.len() > MAX_HISTORY {
                    return Err(GLobalError::FilterData(FilterDataError::MaxHistoryExceeded {
                        current: levels_guard.len(),
                        max: MAX_HISTORY,
                    }));
                }
                
                let current = current_indices.load();
                let filtered_indices: Vec<usize> = if current.len() < 10_000 {
                    current.iter()
                        .filter_map(|&idx| {
                            parent.get(idx)
                                .filter(|item| predicate(item))
                                .map(|_| idx)
                        })
                        .collect()
                } else {
                    current.par_iter()
                        .filter_map(|&idx| {
                            parent.get(idx)
                                .filter(|item| predicate(item))
                                .map(|_| idx)
                        })
                        .collect()
                };
                if filtered_indices.is_empty() {
                    return Err(GLobalError::FilterData(FilterDataError::DataNotFound));
                }
                self.apply_filtered_indices(filtered_indices, info)?;
            }
        }
        Ok(self)
    }
    
    pub fn has_index(&self, name: &str) -> bool {
        self.indexes.contains_key(name)
    }
    
    pub fn drop_index(&self, name: &str) -> &Self {
        self.indexes.remove(name);
        self
    }

    pub fn clear_filed_index(&self) {
        self.indexes.retain(|_, index| {
            if index.is_field() {
                false
            } else {
                true
            }
        });
    }

    pub fn clear_text_indexes(&self) {
        self.indexes.retain(|_k, v| !v.is_text());
    }

    // Очистить все индексы
    pub fn clear_all_indexes(&self) {
        self.indexes.clear();
    }
    
    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.iter().map(|entry| entry.key().clone()).collect()
    }
    
    pub fn validate_indexes(&self) -> bool {
        if let DataStorage::Indexed { parent_data, .. } = &self.storage {
                if parent_data.strong_count() == 0 {
                    return false;
                }
            }
        // Проверяем каждый индекс
        for entry in self.indexes.iter() {
            if !entry.value().is_valid() {
                return false;
            }
        }
        true
    }

    /// Создать Text индекс для быстрого substring search
    /// 
    /// Text индекс разбивает тексты на n-граммы и строит инвертированный индекс
    /// используя BitIndex. Это дает 5-10x speedup для substring поиска.
    /// 
    /// # Arguments
    /// * `name` - имя индекса
    /// * `extractor` - функция извлечения текста
    /// * `n` - размер n-граммы (обычно 3 для trigrams)
    /// 
    /// # Example
    /// 
    /// // Создаем tri-gram индекс
    /// data.create_text_index("search", |log| log.message.clone(), 3);
    /// 
    /// // Теперь substring search будет в 5-10x быстрее!
    /// let results = data.search_with_text("search", "user_id: 12345");
    /// 
    pub fn create_text_index<F>(
        &self,
        name: &str,
        extractor: F,
    ) -> GlobalResult< &Self>
    where
        F: Fn(&T) -> String + Send + Sync + 'static + Clone,
    {
        if self.has_index(name) {
            if let Err(err) = self.check_index_type_compability(
            name, 
            INDEX_TEXT,
            IndexCompatibilityAction::Replace
            ){
                return Err(GLobalError::Index(err))
            }
            self.drop_index(name);
        }
        let mut text_index = TextIndex::new_tri_gram();
        let items = self.items();
        text_index.build(&items, extractor);
        self.indexes.insert(
            name.to_string(),
            Arc::new(IndexType::Text(text_index))
        );
        Ok(self)
    }

    /// Быстрый substring search через Text индекс
    /// 
    /// # Пример
    /// 
    /// let results = data.search_with_text("search", "user_id: 12345");
    /// // 5-10x быстрее чем naive substring search!
    /// 
    pub fn search_with_text(&self, name: &str, query: &str) -> GlobalResult<&Self> {
        self.apply_text_search(name, query)
    }

    /// Получить индексы через text search
    /// 
    /// # Пример
    /// 
    /// let indices = data.get_indices_with_text("search", "payment failed");
    /// let items = data.apply_indices(&indices);
    /// 
    pub fn get_indices_with_text(&self, name: &str, query: &str) -> GlobalResult<Vec<usize>> {
        let index_ref = self.indexes.get(name)
        .ok_or(GLobalError::Index(IndexError::NotFound { name: name.to_string() }))?;
        let ngram_index = index_ref.as_text()
        .ok_or(GLobalError::Index(IndexError::Compatibility 
            { 
                name: name.to_string(), 
                type_exist: index_ref.index_type().to_string(), 
                type_expect: INDEX_TEXT.to_string(),
            }
        ))?;
        Ok(ngram_index.search(query))
    }

    /// Применить n-gram фильтр (drill-down)
    /// 
    /// # Example
    /// 
    /// data.apply_text_search("search", "user_id: 12345")
    ///     .apply_index_filter("level", &"ERROR");
    /// 
    fn apply_text_search(&self, name: &str, query: &str) -> GlobalResult<&Self> {
        let text_indices = self.get_indices_with_text(name, query)?;
        if text_indices.is_empty() {
            return Err(GLobalError::FilterData(FilterDataError::DataNotFoundByIndex { 
                name: name.to_string() 
            }));
        }
        let current_indices = self.current_indices();
        let intersected_indices = if current_indices.len() == self.parent_data().map(|d| d.len()).unwrap_or(0) {
            // Если текущие индексы = все данные, используем результат напрямую
            text_indices
        } else {
            // Иначе делаем drill-down
            Self::intersect_indices(&current_indices, &text_indices)
        };
        if intersected_indices.is_empty() {
            return Err(GLobalError::FilterData(FilterDataError::DataNotFoundByIndexCurrent { 
                name: name.to_string() 
            }));
        }
        if self.parent_data().is_none(){
            return Err(GLobalError::FilterData(FilterDataError::ParentDataIsEmpty)) 
        }
        let desc = format!("Text search: '{}'", query);
        self.apply_filtered_items_with_indices(intersected_indices, desc)
    }

    /// Комплексный поиск по словам через текстовый индекс
    pub fn search_complex_words_text(
        &self,
        name: &str,
        or_words: &[&str],
        and_words: &[&str],
        not_words: &[&str],
    ) -> GlobalResult<&Self>{
        self.apply_complex_words_text_search(name, or_words, and_words, not_words)
    }
    
    /// Получить индексы через комплексный поиск по словам
    fn get_indices_complex_words(
        &self,
        name: &str,
        or_words: &[&str],
        and_words: &[&str],
        not_words: &[&str],
    ) -> GlobalResult<Vec<usize>> {
        let index_ref = self.indexes.get(name)
        .ok_or(GLobalError::Index(IndexError::NotFound { name: name.to_string() }))?;
        let index = index_ref.as_text()
        .ok_or(GLobalError::Index(IndexError::Compatibility 
            { 
                name: name.to_string(), 
                type_exist: index_ref.index_type().to_string(), 
                type_expect: INDEX_TEXT.to_string() 
            }
        ))?;
        Ok(index.search_complex_words(or_words,and_words,not_words))
    }


    /// Применить комплексный поиск по словам к текущей выборке (drill-down)
    /// 
    /// Работает по аналогии с apply_text_search:
    /// 1. Получает индексы через комплексный поиск (OR/AND/NOT)
    /// 2. Пересекает с текущими индексами (drill-down)
    /// 3. Материализует результат из SOURCE
    /// 
    /// # Arguments
    /// * `name` - Имя текстового индекса
    /// * `or_words` - Слова для OR (любое должно присутствовать)
    /// * `and_words` - Слова для AND (все должны присутствовать)
    /// * `not_words` - Слова для NOT (не должны присутствовать)
    /// 
    /// # Example
    /// 
    /// // Drill-down: (payment OR transaction) AND failed AND NOT timeout
    /// data.apply_complex_words_text_search(
    ///     "messages",
    ///     &["payment", "transaction"],  // OR
    ///     &["failed"],                  // AND
    ///     &["timeout"]                  // NOT
    /// );
    /// 
    fn apply_complex_words_text_search(
        &self,
        name: &str,
        or_words: &[&str],
        and_words: &[&str],
        not_words: &[&str],
    ) -> GlobalResult<&Self> {
        let complex_indices = self.get_indices_complex_words(
            name,
            or_words,
            and_words,
            not_words
        )?;
        if complex_indices.is_empty() {
            return Err(GLobalError::FilterData(FilterDataError::DataNotFoundByIndex { 
                name: name.to_string() 
            }))
        }
        let current_indices = self.current_indices();
        let intersected_indices = if current_indices.len() == self.parent_data().map(|d| d.len()).unwrap_or(0) {
            // Если текущие индексы = все данные, используем результат напрямую
            complex_indices
        } else {
            // Иначе делаем drill-down
            Self::intersect_indices(&current_indices, &complex_indices)
        };
        if intersected_indices.is_empty() {
            return Err(GLobalError::FilterData(FilterDataError::DataNotFoundByIndexCurrent { 
                name: name.to_string() 
            }))
        }
        if self.parent_data().is_none(){
            return Err(GLobalError::FilterData(FilterDataError::ParentDataIsEmpty)) 
        }
        let desc = Self::format_complex_query_desc(or_words, and_words, not_words);
        self.apply_filtered_items_with_indices(
            intersected_indices,
            format!("Complex search: {}", desc)
        )
    }

    /// Форматирует описание комплексного запроса для логов
    /// 
    /// # Example
    /// 
    /// format_complex_query_desc(
    ///     &["payment", "transaction"],
    ///     &["failed"],
    ///     &["timeout"]
    /// )
    /// // => "(payment OR transaction) AND failed NOT timeout"
    /// 
    pub fn format_complex_query_desc(
        or_words: &[&str],
        and_words: &[&str],
        not_words: &[&str],
    ) -> String {
        let mut parts = Vec::new();
        // OR часть
        if !or_words.is_empty() {
            if or_words.len() == 1 {
                parts.push(or_words[0].to_string());
            } else {
                parts.push(format!("({})", or_words.join(" OR ")));
            }
        }
        // AND часть
        for word in and_words {
            parts.push(word.to_string());
        }
        // NOT часть
        if !not_words.is_empty() {
            let not_part = format!("NOT {}", not_words.join(" NOT "));
            parts.push(not_part);
        }
        if parts.is_empty() {
            "all".to_string()
        } else {
            parts.join(" AND ")
        }
    }

    // Статистика n-gram индекса
    /// 
    /// # Пример
    /// 
    /// if let Some(stats) = data.text_index_stats("search") {
    ///     println!("{}", stats);
    /// }
    /// 
    pub fn text_index_stats(&self, name: &str) -> GlobalResult<TextIndexStats> {
        let index_ref = self.indexes.get(name)
        .ok_or(GLobalError::Index(IndexError::NotFound { name: name.to_string() }))?;
        let index = index_ref.as_text()
        .ok_or(GLobalError::Index(IndexError::Compatibility 
            { 
                name: name.to_string(), 
                type_exist: index_ref.index_type().to_string(), 
                type_expect: INDEX_TEXT.to_string() 
            }
        ))?;
        Ok(index.stats())
    }

    /// Получить топ N самых частых n-грамм
    /// 
    /// # Пример
    /// 
    /// let top = data.top_text("search", 10);
    /// for (ngram, count) in top {
    ///     println!("'{}' -> {} times", ngram, count);
    /// }
    /// 
    pub fn top_text(&self, name: &str, n: usize) -> GlobalResult<Vec<(String, usize)>> {
        let index_ref = self.indexes.get(name)
        .ok_or(GLobalError::Index(IndexError::NotFound { name: name.to_string() }))?;
        let index = index_ref.as_text()
        .ok_or(GLobalError::Index(IndexError::Compatibility 
            { 
                name: name.to_string(), 
                type_exist: index_ref.index_type().to_string(), 
                type_expect: INDEX_TEXT.to_string() 
            }
        ))?;
        Ok(index.top_ngrams(n))
    }

    /// Список всех n-грамм в индексе
    pub fn list_text_ngrams(&self, name: &str) -> GlobalResult<Vec<String>> {
        let index_ref = self.indexes.get(name)
        .ok_or(GLobalError::Index(IndexError::NotFound { name: name.to_string() }))?;
        let index = index_ref.as_text()
        .ok_or(GLobalError::Index(IndexError::Compatibility 
            { 
                name: name.to_string(), 
                type_exist: index_ref.index_type().to_string(), 
                type_expect: INDEX_TEXT.to_string() 
            }
        ))?;
        Ok(index.list_ngrams())
    }

    /// Получить статистику по конкретной n-грамме
    /// 
    /// # Example
    /// 
    /// if let Some(stats) = data.text_stats("search", "pay") {
    ///     println!("N-gram 'pay': {}", stats);
    /// }
    /// 
    pub fn text_stats(&self, name: &str, ngram: &str) -> GlobalResult<Option<String>> {
        let index_ref = self.indexes.get(name)
        .ok_or(GLobalError::Index(IndexError::NotFound { name: name.to_string() }))?;
        let index = index_ref.as_text()
        .ok_or(GLobalError::Index(IndexError::Compatibility 
            { 
                name: name.to_string(), 
                type_exist: index_ref.index_type().to_string(), 
                type_expect: INDEX_TEXT.to_string() 
            }
        ))?;
        Ok(index.ngram_stats(ngram))
    }

    // Filter Methods

   fn filter_impl<F>(&self, predicate: F) -> GlobalResult<&Self>
    where
        F: Fn(&T) -> bool + Sync + Send,
    {
        self.apply_filtered_items(predicate, "Filtered".to_string())
    }

    pub fn filter<F>(&self, predicate: F) -> GlobalResult<&Self>
    where
        F: Fn(&T) -> bool + Sync + Send,
    {
        self.filter_impl(predicate)
    }


    // Navigation Methods

    // Сброс к исходным данным с ПОЛНОЙ очисткой
    // 
    // Очищает:
    // - Все уровни фильтрации (кроме source)
    // - Историю операций
    pub fn reset_to_source(&self) -> &Self {
        let _guard = self.write_lock.write();
        match &self.storage {
            DataStorage::Owned {
                source,
                current_indices,
                current_cache,
                full_indices,  // Используем кеш!
                levels,
                level_indices,
            } => {
                current_indices.store(Arc::clone(full_indices));
                current_cache.store(Arc::new(Some(Arc::clone(source))));
                levels.store(Arc::new(vec![Arc::clone(source)]));
                level_indices.store(Arc::new(vec![Arc::clone(full_indices)]));
            },
            DataStorage::Indexed {
                source_indices,
                current_indices,
                index_levels,
                ..
            } => {
                current_indices.store(Arc::clone(source_indices));
                index_levels.store(Arc::new(vec![Arc::clone(source_indices)]));
            }
        }
        self.level_info.store(Arc::new(vec![Arc::from("Source")]));
        self.current_level.store(0, Ordering::Release);
        self.source_indices_mask.store(Arc::new(None));
        self
    }
    
    pub fn go_to_level(&self, target_level: usize) -> &Self {
        let _guard = self.write_lock.write();
        let total_levels = self.level_info.load().len();
        if target_level >= total_levels {
            return self;
        }
        
        match &self.storage {
            DataStorage::Owned {
                current_indices,
                current_cache,
                levels,
                level_indices,
                ..
            } => {
                if let Some(indices) = level_indices.load().get(target_level) {
                    current_indices.store(Arc::clone(indices));
                }
                // Восстанавливаем кеш
                if let Some(cached_level) = levels.load().get(target_level) {
                    current_cache.store(Arc::new(Some(Arc::clone(cached_level))));
                } else {
                    current_cache.store(Arc::new(None));
                }
                // Обрезаем историю
                if target_level < total_levels - 1 {
                    let trimmed_levels: Vec<Arc<Vec<Arc<T>>>> = levels.load()
                        .iter()
                        .take(target_level + 1)
                        .cloned()
                        .collect();
                    levels.store(Arc::new(trimmed_levels));
                    
                    let trimmed_indices: Vec<Arc<Vec<usize>>> = level_indices.load()
                        .iter()
                        .take(target_level + 1)
                        .cloned()
                        .collect();
                    level_indices.store(Arc::new(trimmed_indices));
                }
            },
            DataStorage::Indexed {
                current_indices,
                index_levels,
                ..
            } => {
                if let Some(indices) = index_levels.load().get(target_level) {
                    current_indices.store(Arc::clone(indices));
                }
                
                if target_level < total_levels - 1 {
                    let trimmed: Vec<Arc<Vec<usize>>> = index_levels.load()
                        .iter()
                        .take(target_level + 1)
                        .cloned()
                        .collect();
                    index_levels.store(Arc::new(trimmed));
                }
            }
        }
        // Обновляем метаданные...
        if target_level < total_levels - 1 {
            let trimmed_info: Vec<Arc<str>> = self.level_info.load()
                .iter()
                .take(target_level + 1)
                .cloned()
                .collect();
            self.level_info.store(Arc::new(trimmed_info));
        }
        self.current_level.store(target_level, Ordering::Relaxed);
        let source_len = self.parent_data().map(|d| d.len()).unwrap_or(0);
        if source_len > 0 {
            let current = match &self.storage {
                DataStorage::Owned { current_indices, .. } => current_indices.load(),
                DataStorage::Indexed { current_indices, .. } => current_indices.load(),
            };
            if current.len() < source_len {
                // Пересоздаем маску из текущих индексов
                let bitmap: RoaringBitmap = current.iter().map(|&i| i as u32).collect();
                self.source_indices_mask.store(Arc::new(Some(Arc::new(bitmap))));
            } else {
                // Все элементы - маска не нужна
                self.source_indices_mask.store(Arc::new(None));
            }
        }
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


    // Query Methods

    pub fn len(&self) -> usize {
        match &self.storage {
            DataStorage::Owned { current_indices, .. } => {
                current_indices.load().len()
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
        self.current_level.load(Ordering::Relaxed)
    }

    pub fn stored_levels_count(&self) -> usize {
        match &self.storage {
            DataStorage::Owned { levels, .. } => {
                levels.load().len()
            }
            DataStorage::Indexed { index_levels, .. } => {
                index_levels.load().len()
            }
        }
    }

    pub fn total_stored_items(&self) -> usize {
        match &self.storage {
            DataStorage::Owned { levels, .. } => {
                levels.load()
                    .iter()
                    .map(|level| level.len())
                    .sum()
            }
            DataStorage::Indexed { index_levels, .. } => {
                index_levels.load()
                    .iter()
                    .map(|level| level.len())
                    .sum()
            }
        }
    }

    pub fn memory_stats(&self) -> MemoryStats {
        match &self.storage {
            DataStorage::Owned { levels, .. } => {
                let current_lvl = self.current_level.load(Ordering::Acquire);
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
            },
            DataStorage::Indexed { index_levels, .. } => {
                let current_lvl = self.current_level.load(Ordering::Acquire);
                let levels_guard = index_levels.load();
                let mut stats = MemoryStats {
                    current_level: current_lvl,
                    stored_levels: levels_guard.len(),
                    current_level_items: 0,
                    total_stored_items: 0,
                    useful_items: 0,
                    wasted_items: 0,
                };
                for (idx, level_indices) in levels_guard.iter().enumerate() {
                    let count = level_indices.len();
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
        }
    }
    
    pub fn level_name(&self, level: usize) -> Option<Arc<str>> {
        self.level_info.load().get(level).map(Arc::clone)
    }

    pub fn builder() -> FilterDataBuilder<T> {
        FilterDataBuilder::new()
    }
    
    pub fn new(data: Vec<T>) -> Self {
        Self::from_vec(data)
    }

    pub fn filter_state_info(&self) -> FilterStateInfo {
        let source_len = self.parent_data().map(|d| d.len()).unwrap_or(0);
        let filtered_len = self.len();
        let mask_opt = self.source_indices_mask.load();
        let has_mask = mask_opt.is_some();
        let (mask_bits_set, mask_memory_bytes) = if let Some(mask) = mask_opt.as_ref() {
            let cardinality = mask.len() as usize;
            let memory = mask.serialized_size();
            (cardinality, memory)
        } else {
            (0, 0)
        };
        FilterStateInfo {
            source_len,
            filtered_len,
            has_mask,
            mask_bits_set,
            mask_memory_bytes,
        }
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
    applier: Box<dyn FnOnce(&FilterData<T>) -> GlobalResult<()> + Send>,
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

    pub fn with_field_index<V, F>(mut self, name: &str, extractor: F) -> Self
    where
        V: Eq + Hash + Clone + Send + Sync + Ord + PartialOrd + Display + 'static,
        V: Into<FieldValue>,
        F: Fn(&T) -> V + Send + Sync + 'static + Clone,
        IndexField<V>: IntoIndexFieldEnum,
    {
        let name_owned = name.to_string();
        let extractor_clone = extractor.clone();
        
        let applier = Box::new(move |fd: &FilterData<T>| -> GlobalResult<()> {
            fd.create_field_index(&name_owned, extractor_clone)?;
            Ok(())
        }) as Box<dyn FnOnce(&FilterData<T>) -> GlobalResult<()> + Send>;
        
        self.indexes.push(IndexDefinition {
            applier,
        });
        self
    }


/// Добавить n-gram индекс
    /// 
    /// # Example
    /// 
    /// let data = FilterData::builder()
    ///     .with_data(logs)
    ///     .with_text_index("search", |log| log.message.clone(), 3)
    ///     .build();
    /// 
    pub fn with_text_index<F>(mut self, name: &str, extractor: F) -> Self
    where
        F: Fn(&T) -> String + Send + Sync + 'static + Clone,
    {
        let name_owned = name.to_string();
        let extractor_clone = extractor.clone();
        
        let applier = Box::new(move |fd: &FilterData<T>| -> GlobalResult<()>  {
            fd.create_text_index(&name_owned, extractor_clone.clone())?;
            Ok(())
        }) as Box<dyn FnOnce(&FilterData<T>) -> GlobalResult<()>  + Send>;
        
        self.indexes.push(IndexDefinition {
            applier,
        });
        self
    }
    
    pub fn build(self) -> GlobalResult<FilterData<T>> {
        let data = self.data.expect("Data must be provided via with_data()");
        let fd = FilterData::from_vec(data);
        for index_def in self.indexes {
            (index_def.applier)(&fd)?;
        }
        
        Ok(fd)
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

#[derive(Debug, Clone)]
pub struct FilterStateInfo {
    pub source_len: usize,
    pub filtered_len: usize,
    pub has_mask: bool,
    pub mask_bits_set: usize,
    pub mask_memory_bytes: usize,
}


#[cfg(test)]
mod unit_tests {
    use super::*;
    use std::{
        sync::Arc,
        thread,
        time,
    };
    
    #[test]
    fn test_no_leak_repeated_index_creation() {
        let items: Vec<i32> = (0..10_000).collect();
        let data = FilterData::from_vec(items);
        
        for i in 0..100 {
            assert!(data.create_field_index("test", move |&n| n % (i + 1)).is_ok());
        }
    }
    
    #[test]
    fn test_levels_bounded() {
        let items: Vec<i32> = (0..1000).collect();
        let data = FilterData::from_vec(items);
        for i in 0..100 {
            if i < MAX_HISTORY {
                assert!(data.filter(|&n| n > i as i32 * 10).is_ok());
            } else {
                assert!(data.filter(|&n| n > i as i32 * 10).is_err());
            }
        }
        let stats = data.memory_stats();
        println!("stats levels: {}", stats.stored_levels);
        assert!(stats.current_level <= MAX_HISTORY,
                "Too many levels stored: {}", stats.current_level);
    }
    
    #[test]
    fn test_index_builders_not_accumulating() {
        let items: Vec<i32> = (0..1000).collect();
        let data = FilterData::from_vec(items);
        for i in 0..100 {
            assert!(data.create_field_index("price", move |&n| n % (i + 1)).is_ok())
        }
    }
    
    #[test]
    fn test_reset_doesnt_leak() {
        let items: Vec<i32> = (0..10_000).collect();
        let data = FilterData::from_vec(items);
        assert_eq!(data.stored_levels_count(), 1);
        for _ in 0..100 {
            data.filter(|&n| n > 5000).unwrap();
            data.create_field_index("test", |&n| n % 10).unwrap();    
            data.create_field_index("even", |&n| n % 2).unwrap();
            data.reset_to_source();
            data.clear_all_indexes();
        }
        assert_eq!(data.stored_levels_count(), 1);
        assert_eq!(data.list_indexes().len(), 0);
    }
    
    #[test]
    fn test_concurrent_no_leak() {
        let items: Vec<i32> = (0..10_000).collect();
        let data = Arc::new(FilterData::from_vec(items));
        let mut handles = vec![];
        for i in 0..10 {
            let data_clone = Arc::clone(&data);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    assert!(data_clone.create_field_index(
                        &format!("idx_{}", i), 
                        |&n| n % 10
                    ).is_ok());
                    
                    let _ = data_clone.filter_by_field_ops(
                        &format!("idx_{}", i), 
                        &[(FieldOperation::eq(5), Op::And)]
                    );
                    
                    data_clone.drop_index(&format!("idx_{}", i));
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert!(data.list_indexes().len() <= 10);
    }
    
    #[test]
    fn test_indexed_storage_weak_valid() {
        let items: Vec<Arc<i32>> = (0..1000).map(Arc::new).collect();
        let parent = Arc::new(items);
        let indices = vec![0, 100, 200, 300];
        let data = FilterData::from_indices(&parent, indices);
        assert!(data.is_valid());
        drop(parent);
        assert!(!data.is_valid());
        assert!(data.items().is_empty());
    }

    #[test]
    fn test_no_deadlock_replace_index() {
        let items: Vec<i32> = (0..1000).collect();
        let data = Arc::new(FilterData::from_vec(items));
        assert!(data.create_field_index("test", |&n| n % 10).is_ok());
        for i in 0..100 {
            assert!(data.create_field_index("test", move |&n| n % (i + 1)).is_ok());
        }
        println!("No deadlock in single thread");
    }
    
    #[test]
    fn test_no_deadlock_concurrent_create_replace() {
        let items: Vec<i32> = (0..10_000).collect();
        let data = Arc::new(FilterData::from_vec(items));
        let mut handles = vec![];
        for i in 0..10 {
            let data_clone = Arc::clone(&data);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    let index_name = format!("index_{}", i);
                    let result = data_clone.create_field_index(
                        &index_name, 
                        move |&n| n % (i * 10 + j + 1)
                    );
                    if let Err(e) = &result {
                        println!("Thread {} iteration {}: Error creating index: {:?}", i, j, e);
                    }
                    assert!(result.is_ok(), 
                        "Thread {} failed to create index: {:?}", i, result.err());
                    thread::sleep(time::Duration::from_micros(10));
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
        println!("✓ No deadlock in concurrent threads");
        let indexes = data.list_indexes();
        println!("Created {} indexes", indexes.len());
        assert_eq!(indexes.len(), 10, "Should have 10 indexes (one per thread)");
    }

    #[test]
    fn test_concurrent_same_index_replace() {
        println!("== Concurrent Same Index Replace ==");
        let items: Vec<i32> = (0..10_000).collect();
        let data = Arc::new(FilterData::from_vec(items));
        let mut handles = vec![];
        for i in 0..10 {
            let data_clone = Arc::clone(&data);
            let handle = thread::spawn(move || {
                for j in 0..50 {
                    let mut attempts = 0;
                    loop {
                        let result = data_clone.create_field_index(
                            "shared", 
                            move |&n| n % (i * 10 + j + 1)
                        );
                        match result {
                            Ok(_) => break,
                            Err(e) => {
                                attempts += 1;
                                if attempts > 5 {
                                    panic!("Thread {} failed after 5 attempts: {:?}", i, e);
                                }
                                thread::sleep(time::Duration::from_micros(50));
                            }
                        }
                    }
                    thread::sleep(time::Duration::from_micros(10));
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
        println!("✓ No deadlock with concurrent replace of same index");
    }

    #[test]
    fn test_field_index_key_consistency() {
        let items: Vec<i32> = (0..1000).collect();
        let data = FilterData::from_vec(items);
        assert!(data.create_field_index("test", |&n| n % 2).is_ok());
        let indexes = data.list_indexes();
        assert!(indexes.contains(&"test".to_string()),
                "Should have 'test', got: {:?}", indexes);
        assert!(data.create_field_index("test", |&n| n % 3).is_ok());
        let indexes = data.list_indexes();
        assert_eq!(indexes.len(), 1, "Should have only 1 index after replace");
    }
    
    #[test]
    fn test_field_index_boolean_values() {
        let items: Vec<i32> = (0..1000).collect();
        let data = FilterData::from_vec(items);
        assert!(data.create_field_index("even", |&n| n % 2 == 0).is_ok());
        data.filter_by_field_ops("even", &[
            (FieldOperation::eq(true), Op::And)
        ]).unwrap();
        assert_eq!(data.len(), 500);
        let result = data.items();
        assert!(result.iter().all(|n| **n % 2 == 0));
    }
    
    #[test]
    fn test_clear_indexes_efficient() {
        let items: Vec<i32> = (0..1000).collect();
        let data = FilterData::from_vec(items);
        
        for i in 0..100 {
            let result_num = data.create_field_index(
                &format!("idx_{}", i), 
                move |&n| n % (i + 1)
            );
            
            let result_bool = data.create_field_index(
                &format!("bool_{}", i), 
                move |&n| n % (i + 1) == 0
            );
            assert!(result_num.is_ok());
            assert!(result_bool.is_ok());
        }
        assert_eq!(data.list_indexes().len(), 200);
        let start = time::Instant::now();
        data.clear_all_indexes();
        let elapsed = start.elapsed();
        println!("Clear 200 field indexes: {:?}", elapsed);
        assert!(elapsed.as_millis() < 100, "Should be fast");
        assert_eq!(data.list_indexes().len(), 0);
    }
    
    #[test]
    fn test_reset_to_source_and_clear_indexes() {
        let items: Vec<i32> = (0..1000).collect();
        let data = FilterData::from_vec(items);
        data.create_field_index("test", |&n| n % 10).unwrap();
        data.create_field_index("even", |&n| n % 2 == 0).unwrap();
        data.filter(|&n| n > 500).unwrap();
        assert_eq!(data.len(), 499);
        assert_eq!(data.list_indexes().len(), 2);
        data.reset_to_source();
        data.clear_all_indexes();
        assert_eq!(data.len(), 1000);
        assert_eq!(data.list_indexes().len(), 0, "All indexes should be cleared");
    }

    #[test]
    fn test_field_index_type_conversion() {
        let items: Vec<i32> = (0..1000).collect();
        let data = FilterData::from_vec(items);
        data.create_field_index("value", |&n| n as u64).unwrap();
        data.filter_by_field_ops("value", &[
            (FieldOperation::gte(500), Op::And),
        ]).unwrap();
        assert_eq!(data.len(), 500);
    }

    #[test]
    fn test_field_index_multiple_operations() {
        let items: Vec<i32> = (0..1000).collect();
        let data = FilterData::from_vec(items);
        data.create_field_index("value", |&n| n as u64).unwrap();
        data.filter_by_field_ops("value", &[
            (FieldOperation::gte(100), Op::And),
            (FieldOperation::lte(200), Op::And),
            (FieldOperation::not_eq(150), Op::And),
        ]).unwrap();
        let result = data.items();
        assert!(result.iter().all(|n| {
            let val = **n;
            val >= 100 && val <= 200 && val != 150
        }));
    }

    #[test]
    fn test_field_index_in_operation() {
        let items: Vec<i32> = (0..100).collect();
        let data = FilterData::from_vec(items);
        data.create_field_index("value", |&n| n as u64).unwrap();
        data.filter_by_field_ops("value", &[
            (FieldOperation::in_values(vec![10, 20, 30, 40, 50]), Op::And),
        ]).unwrap();
        assert_eq!(data.len(), 5);
        let result = data.items();
        let ids: Vec<i32> = result.iter().map(|n| **n).collect();
        assert_eq!(ids, vec![10, 20, 30, 40, 50]);
    }

    #[test]
    fn test_field_index_range_operation() {
        let items: Vec<i32> = (0..1000).collect();
        let data = FilterData::from_vec(items);
        data.create_field_index("value", |&n| n as u64).unwrap();
        data.filter_by_field_ops("value", &[
            (FieldOperation::range(100, 200), Op::And),
        ]).unwrap();
        assert_eq!(data.len(), 101);
        let result = data.items();
        assert!(result.iter().all(|n| {
            let val = **n;
            val >= 100 && val <= 200
        }));
    }

    #[test]
    fn test_field_index_concurrent_filtering() {
        let items: Vec<i32> = (0..10_000).collect();
        let data = Arc::new(FilterData::from_vec(items));
        data.create_field_index("value", |&n| n as u64).unwrap();
        let mut handles = vec![];
        for i in 0..10 {
            let data_clone = Arc::clone(&data);
            let handle = thread::spawn(move || {
                for j in 0..50 {
                    data_clone.reset_to_source();
                    let threshold = (i * 1000 + j * 10) as i32;
                    data_clone.filter_by_field_ops("value", &[
                        (FieldOperation::gte(threshold), Op::And),
                    ]).unwrap();
                    let count = data_clone.len();
                    assert!(count <= 10_000);
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
        println!("✓ Concurrent field index filtering successful");
    }

    #[test]
    fn test_field_index_not_in_operation() {
        let items: Vec<i32> = (0..100).collect();
        let data = FilterData::from_vec(items);
        data.create_field_index("value", |&n| n as u64).unwrap();
        data.filter_by_field_ops("value", &[
            (FieldOperation::not_in_values(vec![10, 20, 30, 40, 50]), Op::And),
        ]).unwrap();
        assert_eq!(data.len(), 95);
        let result = data.items();
        let excluded = [10, 20, 30, 40, 50];
        assert!(result.iter().all(|n| !excluded.contains(&**n)));
    }
}