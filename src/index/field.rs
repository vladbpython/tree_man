use super::bit::{
    Index,
    Op,
};
use super::super::{
    errors::IndexFieldError,
    result::IndexFieldResult,
};
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;
use rayon::prelude::*;
use std::{
    collections::{BTreeMap, btree_map},
    cmp::{self,Ord},
    hash::Hash,
    fmt::{Debug,Display},
    ops::Bound,
    sync::Arc,
};
use rust_decimal::{
    Decimal,
    prelude::*,
};

const CARDINALITY_RATIO_LOW_THRESHOLD: f64 = 0.05;
const CARDINALITY_RATIO_HIGH_THRESHOLD: f64 = 0.50;
const SELECTIVITY_RATIO_EXCELLENT: f64 = 0.001;
const SELECTIVITY_RATIO_GOOD: f64 = 0.01;
const SELECTIVITY_RATIO_BAD: f64 = 0.30;
const SELECTIVITY_RATIO_BAD_SKEWED: f64 = 0.50;
const SELECTIVITY_RANGE_RATIO_EXCELLENT: f64 = 0.01;
const SELECTIVITY_RANGE_RATIO_GOOD: f64 = 0.05;
const SELECTIVITY_RANGE_RATIO_BAD: f64 = 0.20;
const SELECTIVITY_RANGE_RATIO_BAD_SKEWED: f64 = 0.40;
const SKEWED_RATIO: f64 = 0.30;
const VALUE_OFTEN_RATIO: f64 = 0.5;

pub type F64 = OrderedFloat<f64>;
pub type F32 = OrderedFloat<f32>;

#[derive(Debug,Clone,Copy,PartialEq)]
pub enum TypeFamily {
    Integer,
    Float,
    Decimal,
    String,
    Bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FieldValue {
    U128(u128),
    I128(i128),
    U64(u64),
    I64(i64),
    U32(u32),
    I32(i32),
    U16(u16),
    I16(i16),
    U8(u8),
    I8(i8),
    Usize(usize),
    Isize(isize),
    F64(F64),
    F32(F32),
    Decimal(Decimal),
    String(String),
    Bool(bool),
}

impl FieldValue {

    pub fn type_family(&self) -> TypeFamily {
        match self {
            // Целые числа (singend и unsigned)
            FieldValue::U128(_) | FieldValue::I128(_) |
            FieldValue::U64(_) | FieldValue::I64(_) |
            FieldValue::U32(_) | FieldValue::I32(_) |
            FieldValue::U16(_) | FieldValue::I16(_) |
            FieldValue::U8(_) | FieldValue::I8(_) |
            FieldValue::Usize(_) | FieldValue::Isize(_) => TypeFamily::Integer,
            // Дробные числа
            FieldValue::F64(_) | FieldValue::F32(_) => TypeFamily::Float,
            // Точные десятичные числа (Decimal)
            FieldValue::Decimal(_) => TypeFamily::Decimal,
            // Строки
            FieldValue::String(_) => TypeFamily::String,
            // Утверждения (Boolean)
            FieldValue::Bool(_) => TypeFamily::Bool,
        }
    }

    #[inline(always)]
    pub fn eq(&self, other: &Self) -> bool {
        if self == other {
            return true;
        }

        match (self, other) {
            // U64 vs I32/I64
            (FieldValue::U64(a), FieldValue::I32(b)) => {
                return *b >= 0 && *a == (*b as u64);
            },
            (FieldValue::I32(a), FieldValue::U64(b)) => {
                return *a >= 0 && (*a as u64) == *b;
            },
            (FieldValue::U64(a), FieldValue::I64(b)) => {
                return *b >= 0 && *a == (*b as u64);
            },
            (FieldValue::I64(a), FieldValue::U64(b)) => {
                return *a >= 0 && (*a as u64) == *b;
            },
            (FieldValue::U64(a), FieldValue::U64(b)) => return *a == *b,
            (FieldValue::I32(a), FieldValue::I32(b)) => return *a == *b,
            (FieldValue::I64(a), FieldValue::I64(b)) => return *a == *b,
            (FieldValue::U32(a), FieldValue::U32(b)) => return *a == *b,
            _ => {}
        }
        
        let self_family = self.type_family();
        let other_family = other.type_family();

        match (self_family, other_family) {
            (TypeFamily::String, TypeFamily::String) => return false,
            (TypeFamily::Bool, TypeFamily::Bool) => return false,
            (TypeFamily::String, _) | (_, TypeFamily::String) => return false,
            (TypeFamily::Bool, _) | (_, TypeFamily::Bool) => return false,
            _ => {}
        }

        // Если оба integer - upcast к самому широкому в семействе
        if matches!(self_family, TypeFamily::Integer) && matches!(other_family, TypeFamily::Integer) {
            // Попытка 1: unsigned path (u128)
            if let (Some(a), Some(b)) = (self.try_to_u128(), other.try_to_u128()) {
                return a == b;
            }
    
            // Попытка 2: signed path (i128)
            if let (Some(a), Some(b)) = (self.try_to_i128(), other.try_to_i128()) {
                return a == b;
            }
        }
        
        // Decimal path (для Integer + Float + Decimal)
        if let (Some(a), Some(b)) = (self.try_to_decimal(), other.try_to_decimal()) {
            return a == b;
        }
        
        // Float path (для всех numeric)
        if let (Some(a), Some(b)) = (self.try_to_f64(), other.try_to_f64()) {
            return a == b;
        }
        
        false
    }

    #[inline(always)]
    pub fn gt(&self, other: &Self) -> bool {
        if self == other {
            return false;
        }

        match (self, other) {
            // U64 vs I32
            (FieldValue::U64(a), FieldValue::I32(b)) => {
                if *b < 0 {
                    return true;  // U64 > negative
                }
                // *a > (*b as u64) правильно обрабатывает равенство!
                return *a > (*b as u64);
            },
            (FieldValue::I32(a), FieldValue::U64(b)) => {
                if *a < 0 {
                    return false;  // negative < U64
                }
                return (*a as u64) > *b;  // Включает проверку равенства
            },
            (FieldValue::U64(a), FieldValue::I64(b)) => {
                if *b < 0 {
                    return true;
                }
                return *a > (*b as u64);
            },
            (FieldValue::I64(a), FieldValue::U64(b)) => {
                if *a < 0 {
                    return false;
                }
                return (*a as u64) > *b;
            },
            (FieldValue::U64(a), FieldValue::U64(b)) => return *a > *b,
            (FieldValue::I32(a), FieldValue::I32(b)) => return *a > *b,
            (FieldValue::I64(a), FieldValue::I64(b)) => return *a > *b,
            (FieldValue::U32(a), FieldValue::U32(b)) => return *a > *b,
            (FieldValue::U16(a), FieldValue::U16(b)) => return *a > *b,
            (FieldValue::I16(a), FieldValue::I16(b)) => return *a > *b,
            (FieldValue::U8(a), FieldValue::U8(b)) => return *a > *b,
            (FieldValue::I8(a), FieldValue::I8(b)) => return *a > *b,
            _ => {}
        }
        
        let self_family = self.type_family();
        let other_family = other.type_family();
        
        // String через PartialOrd
        if matches!(self_family, TypeFamily::String) || matches!(other_family, TypeFamily::String) {
            return matches!(self.partial_cmp(other), Some(cmp::Ordering::Greater));
        }
        
        // Bool
        if matches!(self_family, TypeFamily::Bool) || matches!(other_family, TypeFamily::Bool) {
            return false;
        }
        
        // Для например: U8 vs I64, U16 vs F32, etc.
        if self.eq(other) {
            return false;
        }
        
        if matches!(self_family, TypeFamily::Integer) && matches!(other_family, TypeFamily::Integer) {
            // Unsigned path
            if let (Some(a), Some(b)) = (self.try_to_u128(), other.try_to_u128()) {
                return a > b;
            }
            
            // Signed path
            if let (Some(a), Some(b)) = (self.try_to_i128(), other.try_to_i128()) {
                return a > b;
            }
        }

        // Decimal path
        if let (Some(a), Some(b)) = (self.try_to_decimal(), other.try_to_decimal()) {
            return a > b;
        }

        // Float path
        if let (Some(a), Some(b)) = (self.try_to_f64(), other.try_to_f64()) {
            return a > b;
        }

        matches!(self.partial_cmp(other), Some(cmp::Ordering::Greater))
    }

    #[inline(always)]
    pub fn gte(&self, other: &Self) -> bool {
        if self == other {
            return true;
        }
        
        match (self, other) {
            // U64 vs I32 - самая частая комбинация
            (FieldValue::U64(a), FieldValue::I32(b)) => {
                if *b < 0 {
                    return true;  // U64 > negative
                }
                return *a >= (*b as u64);  // Включает проверку равенства
            },
            (FieldValue::I32(a), FieldValue::U64(b)) => {
                if *a < 0 {
                    return false;  // negative < U64
                }
                return (*a as u64) >= *b;
            },
            (FieldValue::U64(a), FieldValue::I64(b)) => {
                if *b < 0 {
                    return true;
                }
                return *a >= (*b as u64);
            },
            (FieldValue::I64(a), FieldValue::U64(b)) => {
                if *a < 0 {
                    return false;
                }
                return (*a as u64) >= *b;
            },
            (FieldValue::U64(a), FieldValue::U64(b)) => return *a >= *b,
            (FieldValue::I32(a), FieldValue::I32(b)) => return *a >= *b,
            (FieldValue::I64(a), FieldValue::I64(b)) => return *a >= *b,
            (FieldValue::U32(a), FieldValue::U32(b)) => return *a >= *b,
            _ => {}
        }

        if self.eq(other) {
            return true;
        }
        // Иначе используем gt()
        self.gt(other)
    }

    #[inline(always)]
    pub fn lt(&self, other: &Self) -> bool {
        !self.gte(other)
    }

    #[inline(always)]
    pub fn lte(&self, other: &Self) -> bool {
        !self.gt(other)
    }
    
}

impl From<u128> for FieldValue {
    fn from(v: u128) -> Self { 
        FieldValue::U128(v) 
    }
}

impl From<i128> for FieldValue {
    fn from(v: i128) -> Self { 
        FieldValue::I128(v) 
    }
}

impl From<u64> for FieldValue {
    fn from(v: u64) -> Self { FieldValue::U64(v) }
}

impl From<i64> for FieldValue {
    fn from(v: i64) -> Self { FieldValue::I64(v) }
}

impl From<u32> for FieldValue {
    fn from(v: u32) -> Self { FieldValue::U32(v) }
}

impl From<i32> for FieldValue {
    fn from(v: i32) -> Self { FieldValue::I32(v) }
}

impl From<u16> for FieldValue {
    fn from(v: u16) -> Self { FieldValue::U16(v) }
}

impl From<i16> for FieldValue {
    fn from(v: i16) -> Self { FieldValue::I16(v) }
}

impl From<u8> for FieldValue {
    fn from(v: u8) -> Self { FieldValue::U8(v) }
}

impl From<i8> for FieldValue {
    fn from(v: i8) -> Self { FieldValue::I8(v) }
}

impl From<usize> for FieldValue {
    fn from(v: usize) -> Self {
        FieldValue::Usize(v)
    }
}

impl From<isize> for FieldValue {
    fn from(v: isize) -> Self {
        FieldValue::Isize(v)
    }
}

impl From<f64> for FieldValue {
    fn from(v: f64) -> Self { 
        FieldValue::F64(OrderedFloat(v)) 
    }
}

impl From<f32> for FieldValue {
    fn from(v: f32) -> Self { 
        FieldValue::F32(OrderedFloat(v)) 
    }
}

impl From<OrderedFloat<f64>> for FieldValue {
    fn from(v: OrderedFloat<f64>) -> Self { 
        FieldValue::F64(v) 
    }
}

impl From<OrderedFloat<f32>> for FieldValue {
    fn from(v: OrderedFloat<f32>) -> Self { 
        FieldValue::F32(v) 
    }
}

impl From<Decimal> for FieldValue {
    fn from(v: Decimal) -> Self { 
        FieldValue::Decimal(v) 
    }
}

impl From<String> for FieldValue {
    fn from(v: String) -> Self { FieldValue::String(v) }
}

impl From<&str> for FieldValue {
    fn from(v: &str) -> Self { FieldValue::String(v.to_string()) }
}

impl From<bool> for FieldValue {
    fn from(v: bool) -> Self { FieldValue::Bool(v) }
}


// FieldOperation - API операции

#[derive(Clone, Debug,PartialEq)]
pub enum FieldOperation {
    // Равенство: field == value
    Eq(FieldValue),
    
    // Не равно: field != value
    NotEq(FieldValue),
    
    // Больше: field > value
    Gt(FieldValue),
    
    // Больше или равно: field >= value
    Gte(FieldValue),
    
    // Меньше: field < value
    Lt(FieldValue),
    
    // Меньше или равно: field <= value
    Lte(FieldValue),
    
    // IN: field IN (values...)
    In(Vec<FieldValue>),
    
    // NOT IN: field NOT IN (values...)
    NotIn(Vec<FieldValue>),
    
    // Диапазон: start <= field <= end
    Range(FieldValue, FieldValue),
}


// Конструкторы для FieldOperation

impl FieldOperation {
    pub fn eq(value: impl Into<FieldValue>) -> Self {
        FieldOperation::Eq(value.into())
    }
    
    pub fn not_eq(value: impl Into<FieldValue>) -> Self {
        FieldOperation::NotEq(value.into())
    }
    
    pub fn gt(value: impl Into<FieldValue>) -> Self {
        FieldOperation::Gt(value.into())
    }
    
    pub fn gte(value: impl Into<FieldValue>) -> Self {
        FieldOperation::Gte(value.into())
    }
    
    pub fn lt(value: impl Into<FieldValue>) -> Self {
        FieldOperation::Lt(value.into())
    }
    
    pub fn lte(value: impl Into<FieldValue>) -> Self {
        FieldOperation::Lte(value.into())
    }
    
    pub fn in_values<V>(values: Vec<V>) -> Self 
    where
        V: Into<FieldValue>,
    {
        FieldOperation::In(values.into_iter().map(|v| v.into()).collect())
    }
    
    pub fn not_in_values<V>(values: Vec<V>) -> Self 
    where
        V: Into<FieldValue>,
    {
        FieldOperation::NotIn(values.into_iter().map(|v| v.into()).collect())
    }
    
    pub fn range(start: impl Into<FieldValue>, end: impl Into<FieldValue>) -> Self {
        FieldOperation::Range(start.into(), end.into())
    }

    #[inline(always)]
    pub fn evaluate(&self, value: &FieldValue) -> bool {
        match self {
            // Используем типовое сравнение
            FieldOperation::Eq(target) => value.eq(target),
            FieldOperation::NotEq(target) => !value.eq(target),
            FieldOperation::Gt(target) => value.gt(target),
            FieldOperation::Gte(target) => value.gte(target),
            FieldOperation::Lt(target) => value.lt(target),
            FieldOperation::Lte(target) => value.lte(target),
            // In - проверяем каждое значение
            FieldOperation::In(targets) => {
                targets.iter().any(|t| value.eq(t))
            },
            // NotIn - обратная операция
            FieldOperation::NotIn(targets) => {
                !targets.iter().any(|t| value.eq(t))
            },
            // Range - оба сравнения
            FieldOperation::Range(start, end) => {
                value.gte(start) && value.lte(end)
            },
        }
    }

    // Является ли операция точечным запросом (equality)
    pub fn is_equality_query(&self) -> bool {
        matches!(self, 
            FieldOperation::Eq(_) |
            FieldOperation::In(_)
        )
    }

    // Является ли операция обратным запросом (inverse)
    pub fn is_inverse_query(&self) -> bool {
        matches!(self,
            FieldOperation::NotEq(_) |
            FieldOperation::NotIn(_)
        )
    }

    // Является ли операция range запросом
    pub fn is_range_query(&self) -> bool {
        matches!(self,
            FieldOperation::Gt(_) |
            FieldOperation::Gte(_) |
            FieldOperation::Lt(_) |
            FieldOperation::Lte(_) |
            FieldOperation::Range(_, _)
        )
    }

}

impl Display for FieldOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldOperation::Eq(v) => write!(f, "== {:?}", v),
            FieldOperation::NotEq(v) => write!(f, "!= {:?}", v),
            FieldOperation::Gt(v) => write!(f, "> {:?}", v),
            FieldOperation::Gte(v) => write!(f, ">= {:?}", v),
            FieldOperation::Lt(v) => write!(f, "< {:?}", v),
            FieldOperation::Lte(v) => write!(f, "<= {:?}", v),
            FieldOperation::In(values) => write!(f, "IN ({:?})", values),
            FieldOperation::NotIn(values) => write!(f, "NOT IN ({:?})", values),
            FieldOperation::Range(start, end) => write!(f, "BETWEEN {:?} AND {:?}", start, end),
        }
    }
}

// Анализитор выборки через Index
#[derive(Debug, Clone)]
pub enum IndexAnalizer {
    Excellent,
    Good,
    Bad,
}

impl Display for IndexAnalizer {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Excellent => {
                write!(f, "EXCELLENT")
            }
            Self::Good => {
                write!(f, "GOOD")
            }
            Self::Bad  => {
                write!(f, "BAD")
            }
        }
    }
    
}


// IndexField<V> - типизированный индекс

pub struct IndexField<V>
where 
    V: Eq + Hash + Clone + Send + Sync + PartialOrd + Ord
{
    values: BTreeMap<V, Index>,
    size: usize,
    sorted_values: Option<Vec<(V, usize)>>,
    cardinality_ratio: f64,
    unique_count: usize, // Количество уникальных значений
    index_quality: f64,
    index_skewed: bool,
    index_analyzer: IndexAnalizer,
}

impl<V> IndexField<V>  
where 
    V: Eq + Hash + Clone + Send + Sync + PartialOrd + Ord + 'static
{
    // Конструкторы
    
    pub fn new(
        values: BTreeMap<V, Index>,
        size: usize,
        sorted_values: Option<Vec<(V, usize)>>,
        cardinality_ratio: f64,
        unique_count: usize,
        index_quality: f64,
        index_skewed: bool,
        index_analyzer: IndexAnalizer,
    ) -> Self {
        Self { 
            values, 
            size,
            sorted_values,
            cardinality_ratio,
            unique_count,
            index_quality,
            index_skewed,
            index_analyzer,
        }
    }

    // Построить индекс из данных
    pub fn build<T, F>(items: &[Arc<T>], extractor: F) -> Self
    where
        T: Send + Sync,
        F: Fn(&T) -> V + Send + Sync,
    {
        let size = items.len();
        if size == 0 {
            return Default::default()
        }

        // Извлечение значений (параллельно для больших наборов)
        let values: Vec<(usize, V)> = if items.len() > 10_000 {
            items
                .par_iter()
                .enumerate()
                .map(|(id, item)| (id, extractor(item)))
                .collect()    
        } else {
            items 
                .iter()
                .enumerate()
                .map(|(id, item)| (id, extractor(item)))
                .collect()
        };

        let mut sorted_values: Vec<(V, usize)> = values.iter()
            .map(|(idx, val)| (val.clone(), *idx))
            .collect();
        sorted_values.sort_by(|a, b| a.0.cmp(&b.0));

        // Группировка индексов по значениям
        let mut values_indices = BTreeMap::<V, Vec<usize>>::new();
        for (id, value) in values {
            values_indices.entry(value).or_default().push(id);
        }

        // вычесляем cardinality ratio
        let unique_count = values_indices.len();
        let cardinality_ratio = if size > 0 {
            unique_count as f64 / size as f64
        } else {
            0.0
        };

        // Вычисляем min/max
        let (_min_count, max_count) = values_indices.values()
            .map(|v| v.len())
            .fold((usize::MAX, 0), |(min, max), count| {
                (min.min(count), max.max(count))
            }
        );

        // Создание BitIndex для каждого значения
        let indexes: BTreeMap<V, Index> = if values_indices.len() > 100 {
            values_indices
                .into_par_iter()
                .map(|(value, indices)| {
                    let bitmap: RoaringBitmap = indices.iter().map(|&i| i as u32).collect();
                    let bit_index = Index::with_bitmap(bitmap, size);
                    (value, bit_index)    
                })
                .collect()
        } else {
            values_indices
                .into_iter()
                .map(|(value, indices)| {
                    let bitmap: RoaringBitmap = indices.iter().map(|&i| i as u32).collect();
                    let bit_index = Index::with_bitmap(bitmap, size);
                    (value, bit_index) 
                })
                .collect()
        };
        let index_quality = Self::build_index_quantity(size, unique_count, max_count);
        let index_skewed = Self::build_index_skewed(size, max_count);
        let index_analyzer = Self::build_index_analyzier(index_quality, cardinality_ratio);


        Self { 
            values: indexes, 
            size,
            sorted_values: Some(sorted_values),
            cardinality_ratio,
            unique_count,
            index_quality,
            index_skewed,
            index_analyzer,
        }
    }

    fn build_index_quantity(
       size: usize,
       unique_count: usize,
       max_count: usize, 
    ) -> f64{
        if unique_count == 0 || size == 0{
            return 0.0
        }

        let greate_count = size as f64 / unique_count as f64;
        let deviation = (max_count as f64 - greate_count).abs();
        1.0 - (deviation / size as f64)
    }

    fn build_index_skewed(
        size: usize,
        max_count: usize,
    ) -> bool {
        if size > 0 {
            return (max_count as f64 / size as f64) > VALUE_OFTEN_RATIO
        }
        false
    }

    fn build_index_analyzier(
        index_quality: f64,
        cardinality_ratio: f64,
    ) -> IndexAnalizer{
        let cardinality = cardinality_ratio;
        // Высокая кардинальности (> 50% уникальных)
        if cardinality > CARDINALITY_RATIO_HIGH_THRESHOLD {
            return IndexAnalizer::Excellent
        }
        //Очень низкая кардинальность + перекос
        if cardinality < CARDINALITY_RATIO_LOW_THRESHOLD{
            return IndexAnalizer::Bad;
        } 
        // Набдюается сильный перекос независимо от кардинальности
        if index_quality < SKEWED_RATIO {
            return IndexAnalizer::Bad
        }
        // Остальные случаи могут быть приемлимыми
        IndexAnalizer::Good
    }

    // Информация об индексе
    
    pub fn len(&self) -> usize {
        self.size
    }
    
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn unique_values_count(&self) -> usize {
        self.values.len()
    }

    pub fn values(&self) -> Vec<V> {
        self.values.keys().cloned().collect()
    }

    pub fn get_bitmap(&self, value: &V) -> Option<&RoaringBitmap> {
        self.values.get(value).map(|idx| idx.bitmap())
    }

    pub fn contains_value(&self, value: &V) -> bool {
        self.values.contains_key(value)
    }

    pub fn value_count(&self, value: &V) -> usize {
        self.values
            .get(value)
            .map(|idx| idx.len())
            .unwrap_or(0)
    }

    pub fn cardinality_ratio(&self) -> f64{
        self.cardinality_ratio
    }

    pub fn is_high_cardinality(&self) -> bool {
        self.cardinality_ratio > CARDINALITY_RATIO_HIGH_THRESHOLD
    }

    pub fn is_low_cardinality(&self) -> bool {
        self.cardinality_ratio() < CARDINALITY_RATIO_LOW_THRESHOLD
    }

    // Анадизатор индексов

    // Расчет рапспределения индексации
    pub fn quality_distribution(&self) -> f64{
        self.index_quality
    }

    // Является ли распредиление перекошенным?
    // Вернет true в случае если значение встречается очень часто (> 50% записей)
    pub fn is_skewed(&self) -> bool {
        self.index_skewed
    }

    pub fn index_analize(&self) -> IndexAnalizer{
        self.index_analyzer.clone()
    }

    pub fn is_efficient_for_equality(&self) -> bool {
        true
    }

    // Эффективен только при низкой кардинальности + перекос
    // Когда мы исключаем часто встречающееся значение (например, 80% данных)
    pub fn is_efficient_for_inverse(&self) -> bool {
        self.index_skewed && self.cardinality_ratio < CARDINALITY_RATIO_LOW_THRESHOLD
    }

    pub fn is_efficient_for_range(&self) -> bool {
        self.cardinality_ratio >= CARDINALITY_RATIO_LOW_THRESHOLD && !self.index_skewed
    }

    pub fn is_efficient_for(&self, operation: &FieldOperation) -> bool {
        if operation.is_equality_query() {
            self.is_efficient_for_equality()
        } else if operation.is_inverse_query() {
            self.is_efficient_for_inverse()
        } else if operation.is_range_query() {
            self.is_efficient_for_range()
        } else {
            true
        }
    }

    // Оценка селектиновсти индексов
    
    // Селективность операции
    fn estimate_comparison_selectivity(&self) -> f64 {
        match &self.index_analyzer {
            IndexAnalizer::Excellent { .. } => {
                // Высокая кардинальность
                SELECTIVITY_RATIO_EXCELLENT
            }
            IndexAnalizer::Good { .. } => {
                // Средняя/умеренная кардинальность
                SELECTIVITY_RATIO_GOOD
            }
            IndexAnalizer::Bad { .. } => {
                // Низкая кардинальность
                if self.is_skewed() {
                    SELECTIVITY_RATIO_BAD_SKEWED // Сильный перекос
                } else {
                    SELECTIVITY_RATIO_BAD // Просто низкая кардинальность
                }
            }
        }
    }

    // селективность операции (range)
    fn estimate_range_selectivity(&self) -> f64 {
        match &self.index_analyzer {
            IndexAnalizer::Excellent { .. } => {
                // Высокая кардинальность
                SELECTIVITY_RANGE_RATIO_EXCELLENT
            }
            IndexAnalizer::Good { .. } => {
                // Средняя/умеренная кардинальность
                SELECTIVITY_RANGE_RATIO_GOOD
            }
            IndexAnalizer::Bad { .. } => {
                // Низкая кардинальность
                if self.is_skewed() {
                    SELECTIVITY_RANGE_RATIO_BAD_SKEWED
                } else {
                    SELECTIVITY_RANGE_RATIO_BAD
                }
            }
        }
    }

    // селективность операции операции по индексу
    pub fn estimate_operation_selectivity(&self, operation: &FieldOperation) -> f64 {
        if self.size == 0 {
            return 0.0;
        }
        
        match operation {
            // Точные операции (используем статистику индекса)
            
            // Eq: среднее количество записей на одно значение
            FieldOperation::Eq(_) => {
                if self.unique_count > 0 {
                    1.0 / self.unique_count as f64
                } else {
                    0.0
                }
            },
            // Eq: среднее количество записей на одно значение
            FieldOperation::NotEq(_) => {
                if self.unique_count > 0 {
                    (self.unique_count - 1) as f64 / self.unique_count as f64
                } else {
                    1.0
                }
            },
            // In: сумма селективностей всех значений
            FieldOperation::In(values) => {
               
                if self.unique_count > 0 {
                    (values.len().min(self.unique_count) as f64) / self.unique_count as f64
                } else {
                    0.0
                }
            },
            // NotIn: обратная операция от In
            FieldOperation::NotIn(values) => {
                
                if self.unique_count > 0 {
                    1.0 - ((values.len().min(self.unique_count) as f64) / self.unique_count as f64)
                } else {
                    1.0
                }
            },
            // Range операции
            FieldOperation::Gt(_) | FieldOperation::Gte(_) | 
            FieldOperation::Lt(_) | FieldOperation::Lte(_) => {
                self.estimate_comparison_selectivity()
            },
            FieldOperation::Range(_, _) => {
                self.estimate_range_selectivity()
            }
        }
    }


    // селективность множестенных операций с условиями
    pub fn estimate_operations_selectivity(&self, operations: &[(FieldOperation, Op)]) -> f64 {
        if operations.is_empty() {
            return 1.0;
        }
        
        let mut result_selectivity = self.estimate_operation_selectivity(&operations[0].0);
        for (operation, op_type) in &operations[1..] {
            let op_selectivity = self.estimate_operation_selectivity(operation);
            
            result_selectivity = match op_type {
                Op::And => {
                    // P(A AND B) = P(A) * P(B) (если независимы)
                    result_selectivity * op_selectivity
                }
                Op::Or => {
                    // P(A) + P(B) если независимы
                    (result_selectivity + op_selectivity).min(1.0)
                }
                Op::AndNot => {
                    // P(A AND NOT B) = P(A) * (1 - P(B))
                    result_selectivity * (1.0 - op_selectivity)
                }
                Op::Xor => {
                    // P(A XOR B) = P(A) + P(B) - 2*P(A AND B)
                    // Упрощение для независимых
                    ((result_selectivity + op_selectivity) / 2.0).min(1.0)
                }
                Op::Invert => {
                    // NOT A
                    1.0 - result_selectivity
                }
            };
        }
        result_selectivity.max(0.0).min(1.0)
    }

    // Итераторы
    
    pub fn iter_indexes(&self) -> btree_map::Iter<'_, V, Index> {
        self.values.iter()
    }

    pub fn iter_values(&self) -> btree_map::Keys<'_, V, Index> {
        self.values.keys()
    }
    
    pub fn iter_bit_indexes(&self) -> btree_map::Values<'_, V, Index> {
        self.values.values()
    }


    // ФИЛЬТРАЦИИ

    // Равенство: field == value
    pub fn value_eq(&self, value: &V) -> Option<RoaringBitmap> {
        self.get_bitmap(value).map(|b| (*b).clone())
    }

    // Не равно: field != value
    pub fn value_not_eq(&self, value: &V) -> Option<RoaringBitmap> {
        let mut result = RoaringBitmap::from_iter(0..(self.size as u32));
        if let Some(bitmap) = self.get_bitmap(value) {
            result -= bitmap;
        }
        Some(result)
    }

    // IN: field IN (values...)
    pub fn value_in(&self, values: &[V]) -> Option<RoaringBitmap> {
        let mut result = RoaringBitmap::new();
        for value in values {
            if let Some(bitmap) = self.get_bitmap(value) {
                result |= bitmap;
            }
        }
        Some(result)
    }

    // NOT IN: field NOT IN (values...)
    pub fn value_not_in(&self, values: &[V]) -> Option<RoaringBitmap> {
        let mut result = RoaringBitmap::from_iter(0..(self.size as u32));
        for value in values {
            if let Some(bitmap) = self.get_bitmap(value) {
                result -= bitmap;
            }
        }
        Some(result)
    }

    // Больше: field > value
    pub fn value_gt(&self, threshold: &V) -> Option<RoaringBitmap> {
        self.value_range(
            Bound::Excluded(threshold),
            Bound::Unbounded,
        )
    }

    // Больше или равно: field >= value
    pub fn value_gte(&self, threshold: &V) -> Option<RoaringBitmap> {
        self.value_range(
            Bound::Included(threshold),
            Bound::Unbounded,
        )
    }

    // Меньше: field < value
    pub fn value_lt(&self, threshold: &V) -> Option<RoaringBitmap> {
        self.value_range(
            Bound::Unbounded,
            Bound::Excluded(threshold),
        )
    }

    // Меньше или равно: field <= value
    pub fn value_lte(&self, threshold: &V) -> Option<RoaringBitmap> {
        self.value_range(
            Bound::Unbounded,
            Bound::Included(threshold),
        )
    }

    // Диапазон включительно: start <= field <= end
    pub fn value_range_inclusive(
        &self,
        start: &V,
        end: &V,
    ) -> Option<RoaringBitmap> {
        self.value_range(
            Bound::Included(start),
            Bound::Included(end),
        )
    }

    // Диапазон: start <= field < end
    pub fn value_range_exclusive(
        &self,
        start: &V,
        end: &V,
    ) -> Option<RoaringBitmap> {
        self.value_range(
            Bound::Included(start),
            Bound::Excluded(end),
        )
    }

    fn value_range(
        &self,
        start: Bound<&V>,
        end: Bound<&V>,
    ) -> Option<RoaringBitmap> {
        if let Some(bitmap) = self.value_range_fast(start, end) {
            return Some(bitmap);
        }
        let mut result = RoaringBitmap::new();
        for (_, index) in self.values.range((start, end)) {
            result |= index.bitmap();
        }
        Some(result)
    }

    fn value_range_fast(
        &self,
        start: Bound<&V>,
        end: Bound<&V>,
    ) -> Option<RoaringBitmap> {
        let sorted = self.sorted_values.as_ref()?;
        // Binary search для start
        let start_idx = match start {
            Bound::Included(val) => {
                sorted.binary_search_by(|(v, _)| v.cmp(val))
                    .unwrap_or_else(|idx| idx)
            }
            Bound::Excluded(val) => {
                match sorted.binary_search_by(|(v, _)| v.cmp(val)) {
                    Ok(idx) => idx + 1,
                    Err(idx) => idx,
                }
            }
            Bound::Unbounded => 0,
        };
        // Binary search для end
        let end_idx = match end {
            Bound::Included(val) => {
                match sorted.binary_search_by(|(v, _)| v.cmp(val)) {
                    Ok(idx) => {
                        // Найти последний элемент с этим значением
                        let mut last = idx;
                        while last + 1 < sorted.len() && sorted[last + 1].0 == *val {
                            last += 1;
                        }
                        last + 1
                    }
                    Err(idx) => idx,
                }
            }
            Bound::Excluded(val) => {
                sorted.binary_search_by(|(v, _)| v.cmp(val))
                    .unwrap_or_else(|idx| idx)
            }
            Bound::Unbounded => sorted.len(),
        };
        if start_idx >= end_idx {
            return Some(RoaringBitmap::new());
        }
        // Собираем bitmap из индексов
        let bitmap: RoaringBitmap = sorted[start_idx..end_idx]
            .iter()
            .map(|(_, idx)| *idx as u32)
            .collect();

        Some(bitmap)
    }

    // Комбинация значений с произвольной операцией
    pub fn filter_operation_values(&self, operations: &[(&V, Op)]) -> Option<RoaringBitmap> {
        if operations.is_empty() {
            return None;
        }

        let first_index = self.values.get(operations[0].0)?;
        let mut result = (*first_index.bitmap()).clone();

        for (value, op) in &operations[1..] {
            if op.is_unary() {
                match op {
                    Op::Invert => {
                        let full = RoaringBitmap::from_iter(0..(self.size as u32));
                        result = full - &result;
                    }
                    _ => unreachable!("Only Invert is unary"),
                }
            } else {
                if let Some(index) = self.values.get(value) {
                    match op {
                        Op::And => result &= index.bitmap(),
                        Op::Or => result |= index.bitmap(),
                        Op::AndNot => result -= index.bitmap(),
                        Op::Xor => result ^= index.bitmap(),
                        Op::Invert => unreachable!(),
                    }
                }
            }
        }

        Some(result)
    }
}

impl<V> Default for IndexField<V>
where 
    V: Eq + Hash + Clone + Send + Sync + PartialOrd + Ord + 'static
{
    fn default() -> Self {
        Self::new(
            BTreeMap::new(),
            0,
            None,
            0f64,
            0,
            0.0,
            false,
            IndexAnalizer::Good
        )
    }
}

pub trait IntoIndexFieldEnum {
    fn into_enum(self) -> IndexFieldEnum;
}

trait TypeConvert {
    fn try_to_u128(&self) -> Option<u128>;
    fn try_to_i128(&self) -> Option<i128>;
    fn try_to_u64(&self) -> Option<u64>;
    fn try_to_i64(&self) -> Option<i64>;
    fn try_to_u32(&self) -> Option<u32>;
    fn try_to_i32(&self) -> Option<i32>;
    fn try_to_u16(&self) -> Option<u16>;
    fn try_to_i16(&self) -> Option<i16>;
    fn try_to_u8(&self) -> Option<u8>;
    fn try_to_i8(&self) -> Option<i8>;
    fn try_to_usize(&self) -> Option<usize>;
    fn try_to_isize(&self) -> Option<isize>;  
    fn try_to_f64(&self) -> Option<F64>;
    fn try_to_f32(&self) -> Option<F32>;
    fn try_to_decimal(&self) -> Option<Decimal>;
    fn try_to_string(&self) -> Option<String>;
    fn try_to_bool(&self) -> Option<bool>;
}

impl TypeConvert for FieldValue {
    // u128
    fn try_to_u128(&self) -> Option<u128> {
        match self {
            FieldValue::U128(v) => Some(*v),
            FieldValue::U64(v) => Some(*v as u128),
            FieldValue::U32(v) => Some(*v as u128),
            FieldValue::U16(v) => Some(*v as u128),
            FieldValue::U8(v) => Some(*v as u128),
            FieldValue::Usize(v) => Some(*v as u128),
            FieldValue::I128(v) if *v >= 0 => Some(*v as u128),
            FieldValue::I64(v) if *v >= 0 => Some(*v as u128),
            FieldValue::I32(v) if *v >= 0 => Some(*v as u128),
            FieldValue::I16(v) if *v >= 0 => Some(*v as u128),
            FieldValue::I8(v) if *v >= 0 => Some(*v as u128),
            FieldValue::Isize(v) if *v >= 0 => Some(*v as u128),
            _ => None,
        }
    }

    //i128
    fn try_to_i128(&self) -> Option<i128> {
        match self {
            FieldValue::I128(v) => Some(*v),
            FieldValue::I64(v) => Some(*v as i128),
            FieldValue::I32(v) => Some(*v as i128),
            FieldValue::I16(v) => Some(*v as i128),
            FieldValue::I8(v) => Some(*v as i128),
            FieldValue::Isize(v) => Some(*v as i128),
            FieldValue::U128(v) if *v <= i128::MAX as u128 => Some(*v as i128),
            FieldValue::U64(v) => Some(*v as i128),
            FieldValue::U32(v) => Some(*v as i128),
            FieldValue::U16(v) => Some(*v as i128),
            FieldValue::U8(v) => Some(*v as i128),
            FieldValue::Usize(v) => Some(*v as i128),
            _ => None,
        }
    }

    // u64
    fn try_to_u64(&self) -> Option<u64> {
        match self {
            FieldValue::U128(v) if *v <= u64::MAX as u128 => Some(*v as u64),
            FieldValue::I128(v) if *v >= 0 && *v <= u64::MAX as i128 => Some(*v as u64),
            FieldValue::U64(v) => Some(*v),
            FieldValue::U32(v) => Some(*v as u64),
            FieldValue::U16(v) => Some(*v as u64),
            FieldValue::U8(v) => Some(*v as u64),
            FieldValue::Usize(v) => (*v).try_into().ok(),
            FieldValue::I64(v) if *v >= 0 => Some(*v as u64),
            FieldValue::I32(v) if *v >= 0 => Some(*v as u64),
            FieldValue::I16(v) if *v >= 0 => Some(*v as u64),
            FieldValue::I8(v) if *v >= 0 => Some(*v as u64),
            FieldValue::Isize(v) if *v >= 0 => (*v).try_into().ok(),
            _ => None,
        }
    }

    // i64
    fn try_to_i64(&self) -> Option<i64> {
        match self {
            FieldValue::I128(v) if *v >= i64::MIN as i128 && *v <= i64::MAX as i128 => Some(*v as i64),
            FieldValue::I64(v) => Some(*v),
            FieldValue::I32(v) => Some(*v as i64),
            FieldValue::I16(v) => Some(*v as i64),
            FieldValue::I8(v) => Some(*v as i64),
            FieldValue::Isize(v) => (*v).try_into().ok(),
            FieldValue::U128(v) if *v <= i64::MAX as u128 => Some(*v as i64),
            FieldValue::U64(v) if *v <= i64::MAX as u64 => Some(*v as i64),
            FieldValue::U32(v) => Some(*v as i64),
            FieldValue::U16(v) => Some(*v as i64),
            FieldValue::U8(v) => Some(*v as i64),
            FieldValue::Usize(v) => (*v).try_into().ok(),
            _ => None,
        }
    }

    // u32
    fn try_to_u32(&self) -> Option<u32> {
        match self {
            FieldValue::U32(v) => Some(*v),
            FieldValue::U16(v) => Some(*v as u32),
            FieldValue::U8(v) => Some(*v as u32),
            FieldValue::U64(v) if *v <= u32::MAX as u64 => Some(*v as u32),
            FieldValue::U128(v) if *v <= u32::MAX as u128 => Some(*v as u32),
            FieldValue::Usize(v) => (*v).try_into().ok(),
            FieldValue::I32(v) if *v >= 0 => Some(*v as u32),
            FieldValue::I16(v) if *v >= 0 => Some(*v as u32),
            FieldValue::I8(v) if *v >= 0 => Some(*v as u32),
            FieldValue::I64(v) if *v >= 0 && *v <= u32::MAX as i64 => Some(*v as u32),
            FieldValue::I128(v) if *v >= 0 && *v <= u32::MAX as i128 => Some(*v as u32),
            FieldValue::Isize(v) if *v >= 0 => (*v).try_into().ok(),
            _ => None,
        }
    }

    // i32
    fn try_to_i32(&self) -> Option<i32> {
        match self {
            FieldValue::I32(v) => Some(*v),
            FieldValue::I16(v) => Some(*v as i32),
            FieldValue::I8(v) => Some(*v as i32),
            FieldValue::I64(v) if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 => Some(*v as i32),
            FieldValue::I128(v) if *v >= i32::MIN as i128 && *v <= i32::MAX as i128 => Some(*v as i32),
            FieldValue::Isize(v) => (*v).try_into().ok(),
            FieldValue::U32(v) if *v <= i32::MAX as u32 => Some(*v as i32),
            FieldValue::U16(v) => Some(*v as i32),
            FieldValue::U8(v) => Some(*v as i32),
            FieldValue::U64(v) if *v <= i32::MAX as u64 => Some(*v as i32),
            FieldValue::U128(v) if *v <= i32::MAX as u128 => Some(*v as i32),
            FieldValue::Usize(v) => (*v).try_into().ok(),
            _ => None,
        }
    }

    // u16
    fn try_to_u16(&self) -> Option<u16> {
        match self {
            FieldValue::U16(v) => Some(*v),
            FieldValue::U8(v) => Some(*v as u16),
            FieldValue::U32(v) if *v <= u16::MAX as u32 => Some(*v as u16),
            FieldValue::U64(v) if *v <= u16::MAX as u64 => Some(*v as u16),
            FieldValue::U128(v) if *v <= u16::MAX as u128 => Some(*v as u16),
            FieldValue::Usize(v) if *v <= u16::MAX as usize => Some(*v as u16),
            FieldValue::I16(v) if *v >= 0 => Some(*v as u16),
            FieldValue::I8(v) if *v >= 0 => Some(*v as u16),
            FieldValue::I32(v) if *v >= 0 && *v <= u16::MAX as i32 => Some(*v as u16),
            FieldValue::I64(v) if *v >= 0 && *v <= u16::MAX as i64 => Some(*v as u16),
            FieldValue::I128(v) if *v >= 0 && *v <= u16::MAX as i128 => Some(*v as u16),
            FieldValue::Isize(v) if *v >= 0 && *v <= u16::MAX as isize => Some(*v as u16),
            _ => None,
        }
    }

    // i16
    fn try_to_i16(&self) -> Option<i16> {
        match self {
            FieldValue::I16(v) => Some(*v),
            FieldValue::I8(v) => Some(*v as i16),
            FieldValue::I32(v) if *v >= i16::MIN as i32 && *v <= i16::MAX as i32 => Some(*v as i16),
            FieldValue::I64(v) if *v >= i16::MIN as i64 && *v <= i16::MAX as i64 => Some(*v as i16),
            FieldValue::I128(v) if *v >= i16::MIN as i128 && *v <= i16::MAX as i128 => Some(*v as i16),
            FieldValue::Isize(v) if *v >= i16::MIN as isize && *v <= i16::MAX as isize => Some(*v as i16),
            FieldValue::U16(v) if *v <= i16::MAX as u16 => Some(*v as i16),
            FieldValue::U8(v) => Some(*v as i16),
            FieldValue::U32(v) if *v <= i16::MAX as u32 => Some(*v as i16),
            FieldValue::U64(v) if *v <= i16::MAX as u64 => Some(*v as i16),
            FieldValue::U128(v) if *v <= i16::MAX as u128 => Some(*v as i16),
            FieldValue::Usize(v) if *v <= i16::MAX as usize => Some(*v as i16),
            _ => None,
        }
    }

    // u8
    fn try_to_u8(&self) -> Option<u8> {
        match self {
            FieldValue::U8(v) => Some(*v),
            FieldValue::U16(v) if *v <= u8::MAX as u16 => Some(*v as u8),
            FieldValue::U32(v) if *v <= u8::MAX as u32 => Some(*v as u8),
            FieldValue::U64(v) if *v <= u8::MAX as u64 => Some(*v as u8),
            FieldValue::U128(v) if *v <= u8::MAX as u128 => Some(*v as u8),
            FieldValue::Usize(v) if *v <= u8::MAX as usize => Some(*v as u8),
            FieldValue::I8(v) if *v >= 0 => Some(*v as u8),
            FieldValue::I16(v) if *v >= 0 && *v <= u8::MAX as i16 => Some(*v as u8),
            FieldValue::I32(v) if *v >= 0 && *v <= u8::MAX as i32 => Some(*v as u8),
            FieldValue::I64(v) if *v >= 0 && *v <= u8::MAX as i64 => Some(*v as u8),
            FieldValue::I128(v) if *v >= 0 && *v <= u8::MAX as i128 => Some(*v as u8),
            FieldValue::Isize(v) if *v >= 0 && *v <= u8::MAX as isize => Some(*v as u8),
            _ => None,
        }
    }

    // i8
    fn try_to_i8(&self) -> Option<i8> {
        match self {
            FieldValue::I8(v) => Some(*v),
            FieldValue::I16(v) if *v >= i8::MIN as i16 && *v <= i8::MAX as i16 => Some(*v as i8),
            FieldValue::I32(v) if *v >= i8::MIN as i32 && *v <= i8::MAX as i32 => Some(*v as i8),
            FieldValue::I64(v) if *v >= i8::MIN as i64 && *v <= i8::MAX as i64 => Some(*v as i8),
            FieldValue::I128(v) if *v >= i8::MIN as i128 && *v <= i8::MAX as i128 => Some(*v as i8),
            FieldValue::Isize(v) if *v >= i8::MIN as isize && *v <= i8::MAX as isize => Some(*v as i8),
            FieldValue::U8(v) if *v <= i8::MAX as u8 => Some(*v as i8),
            FieldValue::U16(v) if *v <= i8::MAX as u16 => Some(*v as i8),
            FieldValue::U32(v) if *v <= i8::MAX as u32 => Some(*v as i8),
            FieldValue::U64(v) if *v <= i8::MAX as u64 => Some(*v as i8),
            FieldValue::U128(v) if *v <= i8::MAX as u128 => Some(*v as i8),
            FieldValue::Usize(v) if *v <= i8::MAX as usize => Some(*v as i8),
            _ => None,
        }
    }

    fn try_to_usize(&self) -> Option<usize> {
        match self {
            FieldValue::Usize(v) => Some(*v),
            FieldValue::U8(v) => Some(*v as usize),
            FieldValue::U16(v) => Some(*v as usize),
            FieldValue::U32(v) => (*v).try_into().ok(),
            FieldValue::U64(v) => (*v).try_into().ok(),
            FieldValue::U128(v) => (*v).try_into().ok(),
            FieldValue::I8(v) if *v >= 0 => Some(*v as usize),
            FieldValue::I16(v) if *v >= 0 => Some(*v as usize),
            FieldValue::I32(v) if *v >= 0 => (*v).try_into().ok(),
            FieldValue::I64(v) if *v >= 0 => (*v).try_into().ok(),
            FieldValue::I128(v) if *v >= 0 => (*v).try_into().ok(),
            FieldValue::Isize(v) if *v >= 0 => (*v).try_into().ok(),
            _ => None,
        }
    }

    fn try_to_isize(&self) -> Option<isize> {
        match self {
            FieldValue::Isize(v) => Some(*v),
            FieldValue::I8(v) => Some(*v as isize),
            FieldValue::I16(v) => Some(*v as isize),
            FieldValue::I32(v) => (*v).try_into().ok(),
            FieldValue::I64(v) => (*v).try_into().ok(),
            FieldValue::I128(v) => (*v).try_into().ok(),
            FieldValue::U8(v) => Some(*v as isize),
            FieldValue::U16(v) => Some(*v as isize),
            FieldValue::U32(v) => (*v).try_into().ok(),
            FieldValue::U64(v) => (*v).try_into().ok(),
            FieldValue::U128(v) => (*v).try_into().ok(),
            FieldValue::Usize(v) => (*v).try_into().ok(),
            _ => None,
        }
    }

    // f64 (OrderedFloat)
    fn try_to_f64(&self) -> Option<F64> {
        match self {
            FieldValue::F64(v) => Some(*v),
            FieldValue::F32(v) => Some(OrderedFloat(v.0 as f64)),
            FieldValue::U64(v) => Some(OrderedFloat(*v as f64)),
            FieldValue::I64(v) => Some(OrderedFloat(*v as f64)),
            FieldValue::U32(v) => Some(OrderedFloat(*v as f64)),
            FieldValue::I32(v) => Some(OrderedFloat(*v as f64)),
            FieldValue::U16(v) => Some(OrderedFloat(*v as f64)),
            FieldValue::I16(v) => Some(OrderedFloat(*v as f64)),
            FieldValue::U8(v) => Some(OrderedFloat(*v as f64)),
            FieldValue::I8(v) => Some(OrderedFloat(*v as f64)),
            FieldValue::U128(v) => Some(OrderedFloat(*v as f64)),
            FieldValue::I128(v) => Some(OrderedFloat(*v as f64)),
            FieldValue::Usize(v) => Some(OrderedFloat(*v as f64)),
            FieldValue::Isize(v) => Some(OrderedFloat(*v as f64)),
            FieldValue::Decimal(v) => v.to_f64().map(OrderedFloat),
            _ => None,
        }
    }

    // f32 (OrderedFloat)
    fn try_to_f32(&self) -> Option<F32> {
        match self {
            FieldValue::F32(v) => Some(*v),
            FieldValue::F64(v) => Some(OrderedFloat(v.0 as f32)),
            FieldValue::U32(v) => Some(OrderedFloat(*v as f32)),
            FieldValue::I32(v) => Some(OrderedFloat(*v as f32)),
            FieldValue::U16(v) => Some(OrderedFloat(*v as f32)),
            FieldValue::I16(v) => Some(OrderedFloat(*v as f32)),
            FieldValue::U8(v) => Some(OrderedFloat(*v as f32)),
            FieldValue::I8(v) => Some(OrderedFloat(*v as f32)),
            FieldValue::U128(v) => Some(OrderedFloat(*v as f32)),
            FieldValue::I128(v) => Some(OrderedFloat(*v as f32)),
            FieldValue::Usize(v) => Some(OrderedFloat(*v as f32)),
            FieldValue::Isize(v) => Some(OrderedFloat(*v as f32)), 
            FieldValue::Decimal(v) => v.to_f32().map(OrderedFloat),
            _ => None,
        }
    }

    // Decimal
    fn try_to_decimal(&self) -> Option<Decimal> {
        match self {
            FieldValue::Decimal(v) => Some(*v),
            FieldValue::U64(v) => Some(Decimal::from(*v)),
            FieldValue::I64(v) => Some(Decimal::from(*v)),
            FieldValue::U32(v) => Some(Decimal::from(*v)),
            FieldValue::I32(v) => Some(Decimal::from(*v)),
            FieldValue::U16(v) => Some(Decimal::from(*v)),
            FieldValue::I16(v) => Some(Decimal::from(*v)),
            FieldValue::U8(v) => Some(Decimal::from(*v)),
            FieldValue::I8(v) => Some(Decimal::from(*v)),
            FieldValue::U128(v) => Decimal::from_u128(*v),
            FieldValue::I128(v) => Decimal::from_i128(*v),
            FieldValue::Usize(v) => Decimal::from_usize(*v),
            FieldValue::Isize(v) => Decimal::from_isize(*v), 
            FieldValue::F64(v) => Decimal::from_f64_retain(v.0),
            FieldValue::F32(v) => Decimal::from_f32_retain(v.0),
            _ => None,
        }
    }

    // String - только точное соответствие
    fn try_to_string(&self) -> Option<String> {
        match self {
            FieldValue::String(v) => Some(v.clone()),
            _ => None,
        }
    }

    // Bool - только точное соответствие
    fn try_to_bool(&self) -> Option<bool> {
        match self {
            FieldValue::Bool(v) => Some(*v),
            _ => None,
        }
    }
}

#[macro_export]
macro_rules! define_index_field_enum {
    (
        $(
            $variant:ident => $type:ty => $field_value:ident => $convert_method:ident
        ),* $(,)?
    ) => {
        // Enum-обертка для IndexField с разными типами
        pub enum IndexFieldEnum {
            $(
                $variant(IndexField<$type>),
            )*
        }

        impl IndexFieldEnum {
            
            pub fn type_name(&self) -> &'static str {
                match self {
                    $(
                        IndexFieldEnum::$variant(_) => stringify!($type),
                    )*
                }
            }

            pub fn len(&self) -> usize {
                match self {
                    $(
                        IndexFieldEnum::$variant(idx) => idx.len(),
                    )*
                }
            }

            pub fn is_empty(&self) -> bool {
                self.len() == 0
            }

            pub fn unique_values_count(&self) -> usize {
                match self {
                    $(
                        IndexFieldEnum::$variant(idx) => idx.unique_values_count(),
                    )*
                }
            }

            pub fn values_as_strings(&self) -> Vec<String> {
                match self {
                    $(
                        IndexFieldEnum::$variant(idx) => {
                            idx.values().into_iter()
                                .map(|v| format!("{:?}", v))
                                .collect()
                        }
                    )*
                }
            }

            // Применить FieldOperation (напрямую вызывает методы IndexField)
            #[allow(unreachable_patterns)]
            pub fn filter_operation(
                &self, 
                operation: &FieldOperation
            ) -> IndexFieldResult<RoaringBitmap> {
                match (self, operation) {
                    $(
                        // Eq
                        (
                            IndexFieldEnum::$variant(idx),
                            FieldOperation::Eq(value)
                        ) => {
                            match value.$convert_method(){
                                Some(converted) => idx.value_eq(&converted)
                                    .ok_or_else(|| IndexFieldError::OperationEq{field_type: stringify!($type).to_string()}),
                                None => Err(IndexFieldError::ConvertType{
                                    field_type: stringify!($type).to_string(),
                                    operation: "eq".to_string()
                                })
                            }
                        },
                        // NotEq
                        (
                            IndexFieldEnum::$variant(idx),
                            FieldOperation::NotEq(value)
                        ) => {
                            match value.$convert_method(){
                                Some(converted) => idx.value_not_eq(&converted)
                                    .ok_or_else(|| IndexFieldError::OperationNotEq{field_type: stringify!($type).to_string()}),
                                None => Err(IndexFieldError::ConvertType{
                                    field_type: stringify!($type).to_string(),
                                    operation: "not_eq".to_string()
                                })
                            }
                        },
                        // Gt
                        (
                            IndexFieldEnum::$variant(idx),
                            FieldOperation::Gt(value)
                        ) => {
                            match value.$convert_method(){
                                Some(converted) => idx.value_gt(&converted)
                                .ok_or_else(|| IndexFieldError::OperationGt{field_type: stringify!($type).to_string()}),
                                None => Err(IndexFieldError::ConvertType{
                                    field_type: stringify!($type).to_string(),
                                    operation: "gt".to_string()
                                })
                            }
                        },
                        // Gte
                        (
                            IndexFieldEnum::$variant(idx),
                            FieldOperation::Gte(value)
                        ) => {
                            match value.$convert_method(){
                                Some(converted) => idx.value_gte(&converted)
                                .ok_or_else(|| IndexFieldError::OperationGte{field_type: stringify!($type).to_string()}),
                                None => Err(IndexFieldError::ConvertType{
                                    field_type: stringify!($type).to_string(),
                                    operation: "gte".to_string()
                                })
                            }
                        },
                        // Lt
                        (
                            IndexFieldEnum::$variant(idx),
                            FieldOperation::Lt(value)
                        ) => {
                            match value.$convert_method(){
                                Some(converted) => idx.value_lt(&converted)
                                .ok_or_else(|| IndexFieldError::OperationLt{field_type: stringify!($type).to_string()}),
                                None => Err(IndexFieldError::ConvertType{
                                    field_type: stringify!($type).to_string(),
                                    operation: "lt".to_string()
                                })
                            }
                        },
                        // Lte
                        (
                            IndexFieldEnum::$variant(idx),
                            FieldOperation::Lte(value)
                        ) => {
                            match value.$convert_method(){
                                Some(converted) => idx.value_lte(&converted)
                                .ok_or_else(|| IndexFieldError::OperationLte{field_type: stringify!($type).to_string()}),
                                None => Err(IndexFieldError::ConvertType{
                                    field_type: stringify!($type).to_string(),
                                    operation: "lte".to_string()
                                })
                            }
                        },
                        // In
                        (
                            IndexFieldEnum::$variant(idx),
                            FieldOperation::In(values)
                        ) => {
                            let typed_values: Vec<$type> = values.iter()
                                .filter_map(|v|v.$convert_method())
                                .collect();
                            
                            if typed_values.is_empty() {
                                return Err(
                                    IndexFieldError::OperationIn{field_type: stringify!($type).to_string()}
                                );
                            }
                            
                            idx.value_in(&typed_values)
                                .ok_or_else(|| IndexFieldError::OperationIn{field_type: stringify!($type).to_string()})
                        },
                        // NotIn
                        (
                            IndexFieldEnum::$variant(idx),
                            FieldOperation::NotIn(values)
                        ) => {
                            let typed_values: Vec<$type> = values.iter()
                                .filter_map(|v| v.$convert_method())
                                .collect();
                            
                            if typed_values.is_empty() {
                                return Err(IndexFieldError::OperationIn{field_type: stringify!($type).to_string()});
                            }
                            
                            idx.value_not_in(&typed_values)
                                .ok_or_else(|| IndexFieldError::OperationIn{field_type: stringify!($type).to_string()})
                        },
                        // Range
                        (
                            IndexFieldEnum::$variant(idx),
                            FieldOperation::Range(start, end)
                        ) => {
                            match (start.$convert_method(), end.$convert_method()){
                                (Some(s),Some(e)) => idx.value_range_inclusive(&s, &e)
                                .ok_or_else(|| IndexFieldError::OperationRange{field_type: stringify!($type).to_string()}),
                                _ => Err(IndexFieldError::ConvertType{
                                    field_type: stringify!($type).to_string(),
                                    operation: "lte".to_string()
                                })
                            }
                            
                        }
                    )*
                    // Несовпадение типов
                    _ => Err(IndexFieldError::OperationUndefinedType{field_type: self.type_name().to_string()}),
                }
            }

            // Множественные операции с Op
            
            pub fn filter_operations(
                &self, 
                operations: &[(FieldOperation, Op)]
            ) -> IndexFieldResult<RoaringBitmap> {
                if operations.is_empty() {
                    return Err(IndexFieldError::OperationListEmpty)
                }

                let mut result = self.filter_operation(&operations[0].0)?;
                for (operation, op) in &operations[1..] {
                    let bitmap = self.filter_operation(operation)?;
                    result = if op == &Op::Invert{
                        let size = self.len();
                        let full = RoaringBitmap::from_iter(0..(size as u32));
                        full - &result
                    } else {
                        match op {
                            Op::And => result & bitmap,
                            Op::Or => result | bitmap,
                            Op::Xor => result ^ bitmap,
                            Op::AndNot => result - bitmap,
                            Op::Invert => unreachable!("Invert is not binary operation")
                        }
                    }
                }

                Ok(result)
            }

            pub fn index_analize(&self) -> IndexAnalizer {
                match self {
                    IndexFieldEnum::U128(idx) => idx.index_analize(),
                    IndexFieldEnum::I128(idx) => idx.index_analize(),
                    IndexFieldEnum::U64(idx) => idx.index_analize(),
                    IndexFieldEnum::I64(idx) => idx.index_analize(),
                    IndexFieldEnum::U32(idx) => idx.index_analize(),
                    IndexFieldEnum::I32(idx) => idx.index_analize(),
                    IndexFieldEnum::U16(idx) => idx.index_analize(),
                    IndexFieldEnum::I16(idx) => idx.index_analize(),
                    IndexFieldEnum::U8(idx) => idx.index_analize(),
                    IndexFieldEnum::I8(idx) => idx.index_analize(),
                    IndexFieldEnum::Usize(idx) => idx.index_analize(),
                    IndexFieldEnum::Isize(idx) => idx.index_analize(),
                    IndexFieldEnum::F64(idx) => idx.index_analize(),
                    IndexFieldEnum::F32(idx) => idx.index_analize(),
                    IndexFieldEnum::Decimal(idx) => idx.index_analize(),
                    IndexFieldEnum::String(idx) => idx.index_analize(),
                    IndexFieldEnum::Bool(idx) => idx.index_analize(),
                }
            }

            pub fn is_efficient_for(&self, operation: &FieldOperation) -> bool {
                match self {
                    IndexFieldEnum::U128(idx) => idx.is_efficient_for(operation),
                    IndexFieldEnum::I128(idx) => idx.is_efficient_for(operation),
                    IndexFieldEnum::U64(idx) => idx.is_efficient_for(operation),
                    IndexFieldEnum::I64(idx) => idx.is_efficient_for(operation),
                    IndexFieldEnum::U32(idx) => idx.is_efficient_for(operation),
                    IndexFieldEnum::I32(idx) => idx.is_efficient_for(operation),
                    IndexFieldEnum::U16(idx) => idx.is_efficient_for(operation),
                    IndexFieldEnum::I16(idx) => idx.is_efficient_for(operation),
                    IndexFieldEnum::U8(idx) => idx.is_efficient_for(operation),
                    IndexFieldEnum::I8(idx) => idx.is_efficient_for(operation),
                    IndexFieldEnum::Usize(idx) => idx.is_efficient_for(operation),
                    IndexFieldEnum::Isize(idx) => idx.is_efficient_for(operation),
                    IndexFieldEnum::F64(idx) => idx.is_efficient_for(operation),
                    IndexFieldEnum::F32(idx) => idx.is_efficient_for(operation),
                    IndexFieldEnum::Decimal(idx) => idx.is_efficient_for(operation),
                    IndexFieldEnum::String(idx) => idx.is_efficient_for(operation),
                    IndexFieldEnum::Bool(idx) => idx.is_efficient_for(operation),
                }
            }

            pub fn is_high_cardinality(&self) -> bool {
                match self {
                    IndexFieldEnum::U128(idx) => idx.is_high_cardinality(),
                    IndexFieldEnum::I128(idx) => idx.is_high_cardinality(),
                    IndexFieldEnum::U64(idx) => idx.is_high_cardinality(),
                    IndexFieldEnum::I64(idx) => idx.is_high_cardinality(),
                    IndexFieldEnum::U32(idx) => idx.is_high_cardinality(),
                    IndexFieldEnum::I32(idx) => idx.is_high_cardinality(),
                    IndexFieldEnum::U16(idx) => idx.is_high_cardinality(),
                    IndexFieldEnum::I16(idx) => idx.is_high_cardinality(),
                    IndexFieldEnum::U8(idx) => idx.is_high_cardinality(),
                    IndexFieldEnum::I8(idx) => idx.is_high_cardinality(),
                    IndexFieldEnum::Usize(idx) => idx.is_high_cardinality(),
                    IndexFieldEnum::Isize(idx) => idx.is_high_cardinality(),
                    IndexFieldEnum::F64(idx) => idx.is_high_cardinality(),
                    IndexFieldEnum::F32(idx) => idx.is_high_cardinality(),
                    IndexFieldEnum::Decimal(idx) => idx.is_high_cardinality(),
                    IndexFieldEnum::String(idx) => idx.is_high_cardinality(),
                    IndexFieldEnum::Bool(idx) => idx.is_high_cardinality(),
                }
            }

            // Оценить селективность операции
            pub fn estimate_operation_selectivity(&self, operation: &FieldOperation) -> f64 {
                match self {
                    IndexFieldEnum::U128(idx) => idx.estimate_operation_selectivity(operation),
                    IndexFieldEnum::I128(idx) => idx.estimate_operation_selectivity(operation),
                    IndexFieldEnum::U64(idx) => idx.estimate_operation_selectivity(operation),
                    IndexFieldEnum::I64(idx) => idx.estimate_operation_selectivity(operation),
                    IndexFieldEnum::U32(idx) => idx.estimate_operation_selectivity(operation),
                    IndexFieldEnum::I32(idx) => idx.estimate_operation_selectivity(operation),
                    IndexFieldEnum::U16(idx) => idx.estimate_operation_selectivity(operation),
                    IndexFieldEnum::I16(idx) => idx.estimate_operation_selectivity(operation),
                    IndexFieldEnum::U8(idx) => idx.estimate_operation_selectivity(operation),
                    IndexFieldEnum::I8(idx) => idx.estimate_operation_selectivity(operation),
                    IndexFieldEnum::Usize(idx) => idx.estimate_operation_selectivity(operation),
                    IndexFieldEnum::Isize(idx) => idx.estimate_operation_selectivity(operation),
                    IndexFieldEnum::F64(idx) => idx.estimate_operation_selectivity(operation),
                    IndexFieldEnum::F32(idx) => idx.estimate_operation_selectivity(operation),
                    IndexFieldEnum::Decimal(idx) => idx.estimate_operation_selectivity(operation),
                    IndexFieldEnum::String(idx) => idx.estimate_operation_selectivity(operation),
                    IndexFieldEnum::Bool(idx) => idx.estimate_operation_selectivity(operation),
                }
            }

            // Оценить комбинацию операций
            pub fn estimate_operations_selectivity(&self, operations: &[(FieldOperation, Op)]) -> f64 {
                match self {
                    IndexFieldEnum::U128(idx) => idx.estimate_operations_selectivity(operations),
                    IndexFieldEnum::I128(idx) => idx.estimate_operations_selectivity(operations),
                    IndexFieldEnum::U64(idx) => idx.estimate_operations_selectivity(operations),
                    IndexFieldEnum::I64(idx) => idx.estimate_operations_selectivity(operations),
                    IndexFieldEnum::U32(idx) => idx.estimate_operations_selectivity(operations),
                    IndexFieldEnum::I32(idx) => idx.estimate_operations_selectivity(operations),
                    IndexFieldEnum::U16(idx) => idx.estimate_operations_selectivity(operations),
                    IndexFieldEnum::I16(idx) => idx.estimate_operations_selectivity(operations),
                    IndexFieldEnum::U8(idx) => idx.estimate_operations_selectivity(operations),
                    IndexFieldEnum::I8(idx) => idx.estimate_operations_selectivity(operations),
                    IndexFieldEnum::Usize(idx) => idx.estimate_operations_selectivity(operations),
                    IndexFieldEnum::Isize(idx) => idx.estimate_operations_selectivity(operations),
                    IndexFieldEnum::F64(idx) => idx.estimate_operations_selectivity(operations),
                    IndexFieldEnum::F32(idx) => idx.estimate_operations_selectivity(operations),
                    IndexFieldEnum::Decimal(idx) => idx.estimate_operations_selectivity(operations),
                    IndexFieldEnum::String(idx) => idx.estimate_operations_selectivity(operations),
                    IndexFieldEnum::Bool(idx) => idx.estimate_operations_selectivity(operations),
                }
            }
        }

        impl Debug for IndexFieldEnum {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "IndexFieldEnum::{} {{ size: {}, unique: {} }}",
                    self.type_name(),
                    self.len(),
                    self.unique_values_count()
                )
            }
        }

        // IntoIndexFieldEnum trait для удобного преобразования
        $(
            impl IntoIndexFieldEnum for IndexField<$type> {
                fn into_enum(self) -> IndexFieldEnum {
                    IndexFieldEnum::$variant(self)
                }
            }
        )*
    };
}


// ОПРЕДЕЛЕНИЕ ВСЕХ ТИПОВ (единая точка изменения!)
define_index_field_enum! {
    U128 => u128 => U128 => try_to_u128,
    I128 => i128 => I128 => try_to_i128,
    U64 => u64 => U64 => try_to_u64,
    I64 => i64 => I64 => try_to_i64,
    U32 => u32 => U32 => try_to_u32,
    I32 => i32 => I32 => try_to_i32,
    U16 => u16 => U16 => try_to_u16,
    I16 => i16 => I16 => try_to_i16,
    U8 => u8 => U8 => try_to_u8,
    I8 => i8 => I8 => try_to_i8,
    Usize => usize => Usize => try_to_usize,
    Isize => isize => Isize => try_to_isize,
    F64 => F64 => F64 => try_to_f64,
    F32 => F32 => F32 => try_to_f32,
    Decimal => Decimal => Decimal => try_to_decimal,
    String => String => String => try_to_string,
    Bool => bool => Bool => try_to_bool,
}


#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug)]
    #[allow(dead_code)]
    struct Product {
        id: u64,
        price: u64,
        name: String,
        in_stock: bool,
        rating: f64,
    }

    #[test]
    fn test_index_field_direct_api() {
        let products = vec![
            Arc::new(Product { id: 1, price: 100, name: "A".into(), in_stock: true, rating: 4.5 }),
            Arc::new(Product { id: 2, price: 200, name: "B".into(), in_stock: false, rating: 3.8 }),
            Arc::new(Product { id: 3, price: 300, name: "C".into(), in_stock: true, rating: 4.9 }),
        ];
        let price_index = IndexField::build(&products, |p| p.price);
        // Тест Eq
        let result = price_index.value_eq(&200).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains(1));
        // Тест Gte
        let result = price_index.value_gte(&200).unwrap();
        assert_eq!(result.len(), 2);
        // Тест Range
        let result = price_index.value_range_inclusive(&100, &250).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_index_field_enum_u64() {
        let products = vec![
            Arc::new(Product { id: 1, price: 100, name: "A".into(), in_stock: true, rating: 4.5 }),
            Arc::new(Product { id: 2, price: 200, name: "B".into(), in_stock: false, rating: 3.8 }),
            Arc::new(Product { id: 3, price: 300, name: "C".into(), in_stock: true, rating: 4.9 }),
        ];
        let price_index = IndexField::build(&products, |p| p.price);
        let price_enum = IndexFieldEnum::U64(price_index);
        // Тест через FieldOperation
        let result = price_enum.filter_operation(&FieldOperation::eq(200)).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains(1));

        let result = price_enum.filter_operation(&FieldOperation::gte(200)).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_index_field_enum_string() {
        let products = vec![
            Arc::new(Product { id: 1, price: 100, name: "Apple".into(), in_stock: true, rating: 4.5 }),
            Arc::new(Product { id: 2, price: 200, name: "Banana".into(), in_stock: false, rating: 3.8 }),
            Arc::new(Product { id: 3, price: 300, name: "Cherry".into(), in_stock: true, rating: 4.9 }),
        ];

        let name_index = IndexField::build(&products, |p| p.name.clone());
        let name_enum = IndexFieldEnum::String(name_index);

        let result = name_enum.filter_operation(&FieldOperation::eq("Banana")).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains(1));

        let result = name_enum.filter_operation(
            &FieldOperation::in_values(vec!["Apple", "Cherry"])
        ).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_apply_operations_complex() {
        let products = vec![
            Arc::new(Product { id: 1, price: 100, name: "A".into(), in_stock: true, rating: 4.5 }),
            Arc::new(Product { id: 2, price: 200, name: "B".into(), in_stock: false, rating: 3.8 }),
            Arc::new(Product { id: 3, price: 300, name: "C".into(), in_stock: true, rating: 4.9 }),
            Arc::new(Product { id: 4, price: 400, name: "D".into(), in_stock: true, rating: 3.2 }),
        ];

        let price_index = IndexField::build(&products, |p| p.price);
        let price_enum = IndexFieldEnum::U64(price_index);

        // (price >= 200) AND (price <= 400) AND (price != 300)
        let result = price_enum.filter_operations(&[
            (FieldOperation::gte(200u64), Op::And),
            (FieldOperation::lte(400u64), Op::And),
            (FieldOperation::not_eq(300u64), Op::And),
        ]).unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains(1)); // 200
        assert!(result.contains(3)); // 400
        assert!(!result.contains(2)); // 300 excluded
    }

    #[test]
    fn test_type_mismatch_error() {
        let products = vec![
            Arc::new(Product { id: 1, price: 100, name: "A".into(), in_stock: true, rating: 4.5 }),
        ];

        let price_index = IndexField::build(&products, |p| p.price);
        let price_enum = IndexFieldEnum::U64(price_index);

        let result = price_enum.filter_operation(&FieldOperation::eq("string"));
        
        assert!(result.is_err());
        //assert!(result.unwrap_err().contains("Type mismatch"));
    }

    #[test]
    fn test_into_enum_trait() {
        let products = vec![
            Arc::new(Product { id: 1, price: 100, name: "A".into(), in_stock: true, rating: 4.5 }),
        ];

        let price_index = IndexField::build(&products, |p| p.price);
        let price_enum: IndexFieldEnum = price_index.into_enum();

        assert_eq!(price_enum.type_name(), "u64");
        assert_eq!(price_enum.len(), 1);
    }

    #[test]
    fn test_not_in_operation() {
        let products = vec![
            Arc::new(Product { id: 1, price: 100, name: "A".into(), in_stock: true, rating: 4.5 }),
            Arc::new(Product { id: 2, price: 200, name: "B".into(), in_stock: false, rating: 3.8 }),
            Arc::new(Product { id: 3, price: 300, name: "C".into(), in_stock: true, rating: 4.9 }),
            Arc::new(Product { id: 4, price: 400, name: "D".into(), in_stock: true, rating: 3.2 }),
        ];

        let price_index = IndexField::build(&products, |p| p.price);
        let price_enum = IndexFieldEnum::U64(price_index);

        // NOT IN (200, 300)
        let result = price_enum.filter_operation(
            &FieldOperation::not_in_values(vec![200, 300])
        ).unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains(0)); // 100
        assert!(result.contains(3)); // 400
    }

    #[test]
    fn test_or_operations() {
        let products = vec![
            Arc::new(Product { id: 1, price: 100, name: "A".into(), in_stock: true, rating: 4.5 }),
            Arc::new(Product { id: 2, price: 200, name: "B".into(), in_stock: false, rating: 3.8 }),
            Arc::new(Product { id: 3, price: 300, name: "C".into(), in_stock: true, rating: 4.9 }),
            Arc::new(Product { id: 4, price: 400, name: "D".into(), in_stock: true, rating: 3.2 }),
        ];

        let price_index = IndexField::build(&products, |p| p.price);
        let price_enum = IndexFieldEnum::U64(price_index);

        // (price < 150) OR (price > 350)
        let result = price_enum.filter_operations(&[
            (FieldOperation::lt(150), Op::Or),
            (FieldOperation::gt(350), Op::Or),
        ]).unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains(0)); // 100
        assert!(result.contains(3)); // 400
    }

    #[test]
    fn test_metadata() {
        let products = vec![
            Arc::new(Product { id: 1, price: 100, name: "A".into(), in_stock: true, rating: 4.5 }),
            Arc::new(Product { id: 2, price: 200, name: "B".into(), in_stock: false, rating: 3.8 }),
        ];

        let price_index = IndexField::build(&products, |p| p.price);
        
        assert_eq!(price_index.len(), 2);
        assert_eq!(price_index.unique_values_count(), 2);
        assert!(!price_index.is_empty());
        assert!(price_index.contains_value(&100));
        assert_eq!(price_index.value_count(&100), 1);
    }

    #[test]
    fn test_index_analize_excellent() {
        // High cardinality - unique IDs
        let items: Vec<Arc<u64>> = (0..100_000)
            .map(|i| Arc::new(i as u64))
            .collect();
        let index = IndexField::build(&items, |&n| n);
        let rec = index.index_analize();
        println!("{}", rec);
        match rec {
            IndexAnalizer::Excellent { .. } => {}
            _ => panic!("Expected Excellent"),
        }
        assert!(index.cardinality_ratio() > 0.9);
    }

    #[test]
    fn test_index_analize_good() {
        let items: Vec<Arc<String>> = (0..100_000)
            .map(|i| Arc::new(format!("category_{}", i % 10_000)))
            .collect();
        let index = IndexField::build(&items, |s| s.clone());
        let rec = index.index_analize();
        match rec {
            IndexAnalizer::Good => {}
            _ => panic!("Expected Good, got: {:?}", rec),
        }
        assert!(index.cardinality_ratio() >= CARDINALITY_RATIO_LOW_THRESHOLD);
        assert!(index.cardinality_ratio() < CARDINALITY_RATIO_HIGH_THRESHOLD);
        assert!(index.quality_distribution() > 0.9);
    }


    #[test]
    fn test_index_analize_bad_skewed() {
        // Low cardinality + skewed (95% one value)
        let items: Vec<Arc<String>> = (0..100_000)
            .map(|i| Arc::new(
                if i < 95_000 { "active" }
                else if i < 98_000 { "pending" }
                else { "deleted" }
                .to_string()
            ))
            .collect();
        let index = IndexField::build(&items, |s| s.clone());
        let rec = index.index_analize();
        println!("{}", rec);
        match rec {
            IndexAnalizer::Bad => {
            }
            _ => panic!("Expected Bad"),
        }
        assert!(index.is_skewed());
    }

    #[test]
    fn test_index_analize_bad_quality() {
        // Medium cardinality but very skewed
        let items: Vec<Arc<String>> = (0..100_000)
            .map(|i| Arc::new(
                if i < 80_000 { "USA".to_string() }       // 80%
                else if i < 90_000 { "UK".to_string() }   // 10%
                else { format!("Country_{}", i % 20) } // 10% distributed
            ))
            .collect();
        let index = IndexField::build(&items, |s| s.clone());
        let rec = index.index_analize();
        println!("{}", rec);
        match rec {
            IndexAnalizer::Bad => {
            }
            _ => panic!("Expected Bad, got: {:?}", rec),
        }
        assert!(index.quality_distribution() < 0.3);
    }


}