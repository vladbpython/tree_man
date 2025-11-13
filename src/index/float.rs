use arc_swap::ArcSwap;
use std::{
    collections::BTreeMap,
    marker::PhantomData,
    ops::{Bound,RangeBounds},
    sync::Arc,
};
use rayon::prelude::*;

// Float - Wrapper для Float типов с Ord

use std::cmp::Ordering as CmpOrdering;

#[derive(Debug, Clone, Copy)]
pub struct Float<F>(pub F);

// Реализации для f32
impl PartialEq for Float<f32> {
    fn eq(&self, other: &Self) -> bool {
        if self.0.is_nan() && other.0.is_nan() {
            true
        } else {
            self.0 == other.0
        }
    }
}

impl Eq for Float<f32> {}

impl PartialOrd for Float<f32> {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for Float<f32> {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        match self.0.partial_cmp(&other.0) {
            Some(ord) => ord,
            None => {
                let self_nan = self.0.is_nan();
                let other_nan = other.0.is_nan();
                if self_nan && other_nan {
                    CmpOrdering::Equal
                } else if self_nan {
                    CmpOrdering::Less
                } else {
                    CmpOrdering::Greater
                }
            }
        }
    }
}

impl Float<f32> {
    pub fn new(value: f32) -> Self {
        assert!(!value.is_nan(), "Float cannot contain NaN");
        Self(value)
    }
    
    pub fn try_new(value: f32) -> Option<Self> {
        if value.is_nan() { None } else { Some(Self(value)) }
    }
    
    #[inline]
    pub fn into_inner(self) -> f32 { self.0 }
}

impl From<f32> for Float<f32> {
    fn from(value: f32) -> Self { Self(value) }
}

impl From<Float<f32>> for f32 {
    fn from(of: Float<f32>) -> Self { of.0 }
}

// Реализации для f64
impl PartialEq for Float<f64> {
    fn eq(&self, other: &Self) -> bool {
        if self.0.is_nan() && other.0.is_nan() {
            true
        } else {
            self.0 == other.0
        }
    }
}

impl Eq for Float<f64> {}

impl PartialOrd for Float<f64> {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for Float<f64> {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        match self.0.partial_cmp(&other.0) {
            Some(ord) => ord,
            None => {
                let self_nan = self.0.is_nan();
                let other_nan = other.0.is_nan();
                
                if self_nan && other_nan {
                    CmpOrdering::Equal
                } else if self_nan {
                    CmpOrdering::Less
                } else {
                    CmpOrdering::Greater
                }
            }
        }
    }
}

impl Float<f64> {
    pub fn new(value: f64) -> Self {
        assert!(!value.is_nan(), "Float cannot contain NaN");
        Self(value)
    }
    
    pub fn try_new(value: f64) -> Option<Self> {
        if value.is_nan() { None } else { Some(Self(value)) }
    }
    
    #[inline]
    pub fn into_inner(self) -> f64 { self.0 }
}

impl From<f64> for Float<f64> {
    fn from(value: f64) -> Self { Self(value) }
}

impl From<Float<f64>> for f64 {
    fn from(of: Float<f64>) -> Self { of.0 }
}

impl std::fmt::Display for Float<f32> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for Float<f64> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}


// BucketedFloat - для bucketed индексов

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BucketedFloat {
    bucket: i64,
}

impl BucketedFloat {
    pub fn from_f64(value: f64, bucket_size: f64) -> Self {
        let bucket = (value / bucket_size).floor() as i64;
        Self { bucket }
    }
    
    #[allow(dead_code)]
    pub fn from_f32(value: f32, bucket_size: f32) -> Self {
        let bucket = (value / bucket_size).floor() as i64;
        Self { bucket }
    }
    
    #[allow(dead_code)]
    pub fn range_start(&self, bucket_size: f64) -> f64 {
        self.bucket as f64 * bucket_size
    }
    
    #[allow(dead_code)]
    pub fn range_end(&self, bucket_size: f64) -> f64 {
        (self.bucket + 1) as f64 * bucket_size
    }
}

impl std::fmt::Display for BucketedFloat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bucket({})", self.bucket)
    }
}



// BucketedFloatIndexWrapper - Индекс с бакетами

pub struct BucketedFloatIndexWrapper<T>
where
    T: Send + Sync,
{
    index: ArcSwap<BTreeMap<BucketedFloat, Vec<(usize, f64)>>>,
    bucket_size: f64,
    _phantom: PhantomData<T>,
}

impl<T> BucketedFloatIndexWrapper<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(bucket_size: f64) -> Self {
        Self {
            index: ArcSwap::from_pointee(BTreeMap::new()),
            bucket_size,
            _phantom: PhantomData,
        }
    }
    
    pub fn build<F>(&self, items: &[Arc<T>], extractor: F)
    where
        F: Fn(&T) -> f64 + Send + Sync,
    {
        let entries: Vec<(BucketedFloat, usize, f64)> = items
            .par_iter()
            .enumerate()
            .map(|(idx, item)| {
                let value = extractor(item);
                let bucket = BucketedFloat::from_f64(value, self.bucket_size);
                (bucket, idx, value)
            })
            .collect();
        let mut index_map: BTreeMap<BucketedFloat, Vec<(usize, f64)>> = BTreeMap::new();
        for (bucket, idx, value) in entries {
            index_map.entry(bucket).or_default().push((idx, value));
        }
        // Сортируем для cache locality
        for (_, entries) in index_map.iter_mut() {
            entries.sort_unstable_by_key(|(idx, _)| *idx);
        }
        // Atomic swap - НЕ блокирует readers!
        self.index.store(Arc::new(index_map));
    }
    
    // Range query
    pub fn range_query_indices<R>(&self, range: R) -> Vec<usize>
    where
        R: RangeBounds<f64>,
    {
        let start_bucket = match range.start_bound() {
            Bound::Included(&s) => BucketedFloat::from_f64(s, self.bucket_size),
            Bound::Excluded(&s) => {
                let b = BucketedFloat::from_f64(s, self.bucket_size);
                BucketedFloat { bucket: b.bucket + 1 }
            }
            Bound::Unbounded => BucketedFloat { bucket: i64::MIN },
        };
        let end_bucket = match range.end_bound() {
            Bound::Included(&e) => BucketedFloat::from_f64(e, self.bucket_size),
            Bound::Excluded(&e) => BucketedFloat::from_f64(e, self.bucket_size),
            Bound::Unbounded => BucketedFloat { bucket: i64::MAX },
        };
        let bucket_range = start_bucket..=end_bucket;
        // Load snapshot
        let index_snapshot = self.index.load();
        let mut result = Vec::new();
        for (_, entries) in index_snapshot.range(bucket_range) {
            for &(idx, value) in entries {
                let in_start = match range.start_bound() {
                    Bound::Included(&s) => value >= s,
                    Bound::Excluded(&s) => value > s,
                    Bound::Unbounded => true,
                };
                let in_end = match range.end_bound() {
                    Bound::Included(&e) => value <= e,
                    Bound::Excluded(&e) => value < e,
                    Bound::Unbounded => true,
                };
                if in_start && in_end {
                    result.push(idx);
                }
            }
        }
        // Сортируем для cache locality
        result.sort_unstable();
        result
    }
}

// FloatRangeBounds - Helper

pub trait FloatRangeBounds<F> {
    type OrderedRange: std::ops::RangeBounds<Float<F>>;
    fn to_ordered_range(self) -> Self::OrderedRange;
}

impl FloatRangeBounds<f32> for std::ops::Range<f32> {
    type OrderedRange = std::ops::Range<Float<f32>>;
    fn to_ordered_range(self) -> Self::OrderedRange {
        Float(self.start)..Float(self.end)
    }
}

impl FloatRangeBounds<f32> for std::ops::RangeInclusive<f32> {
    type OrderedRange = std::ops::RangeInclusive<Float<f32>>;
    fn to_ordered_range(self) -> Self::OrderedRange {
        Float(*self.start())..=Float(*self.end())
    }
}

impl FloatRangeBounds<f32> for std::ops::RangeFrom<f32> {
    type OrderedRange = std::ops::RangeFrom<Float<f32>>;
    fn to_ordered_range(self) -> Self::OrderedRange {
        Float(self.start)..
    }
}

impl FloatRangeBounds<f32> for std::ops::RangeTo<f32> {
    type OrderedRange = std::ops::RangeTo<Float<f32>>;
    fn to_ordered_range(self) -> Self::OrderedRange {
        ..Float(self.end)
    }
}

impl FloatRangeBounds<f32> for std::ops::RangeToInclusive<f32> {
    type OrderedRange = std::ops::RangeToInclusive<Float<f32>>;
    fn to_ordered_range(self) -> Self::OrderedRange {
        ..=Float(self.end)
    }
}

impl FloatRangeBounds<f64> for std::ops::Range<f64> {
    type OrderedRange = std::ops::Range<Float<f64>>;
    fn to_ordered_range(self) -> Self::OrderedRange {
        Float(self.start)..Float(self.end)
    }
}

impl FloatRangeBounds<f64> for std::ops::RangeInclusive<f64> {
    type OrderedRange = std::ops::RangeInclusive<Float<f64>>;
    fn to_ordered_range(self) -> Self::OrderedRange {
        Float(*self.start())..=Float(*self.end())
    }
}

impl FloatRangeBounds<f64> for std::ops::RangeFrom<f64> {
    type OrderedRange = std::ops::RangeFrom<Float<f64>>;
    fn to_ordered_range(self) -> Self::OrderedRange {
        Float(self.start)..
    }
}

impl FloatRangeBounds<f64> for std::ops::RangeTo<f64> {
    type OrderedRange = std::ops::RangeTo<Float<f64>>;
    fn to_ordered_range(self) -> Self::OrderedRange {
        ..Float(self.end)
    }
}

impl FloatRangeBounds<f64> for std::ops::RangeToInclusive<f64> {
    type OrderedRange = std::ops::RangeToInclusive<Float<f64>>;
    fn to_ordered_range(self) -> Self::OrderedRange {
        ..=Float(self.end)
    }
}