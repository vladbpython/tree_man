use arc_swap::ArcSwap;
use rayon::prelude::*;
use rust_decimal::{
    Decimal,
    prelude::*,
};
use std::{
    collections::BTreeMap,
    marker::PhantomData,
    ops::{Bound,RangeBounds},
    sync::Arc,
};

// BucketedDecimal - для bucketed индексов

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BucketedDecimal {
    bucket: i64,
}

impl BucketedDecimal {
    pub fn from_decimal(value: Decimal, bucket_size: Decimal) -> Self {
        // Decimal поддерживает точное деление
        let bucket = (value / bucket_size).floor().to_i64().unwrap_or(0);
        Self { bucket }
    }
}


pub struct BucketedDecimalIndexWrapper<T>
    where
        T: Send + Sync,
    {
        index:  ArcSwap<BTreeMap<BucketedDecimal, Vec<(usize, Decimal)>>>,
        bucket_size: Decimal,
        _phantom: PhantomData<T>,
    }
    
impl<T> BucketedDecimalIndexWrapper<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(bucket_size: Decimal) -> Self {
        Self {
            index: ArcSwap::from_pointee(BTreeMap::new()),
            bucket_size,
            _phantom: PhantomData,
        }
    }
    
    pub fn build<F>(&self, items: &[Arc<T>], extractor: F)
    where
        F: Fn(&T) -> Decimal + Send + Sync,
    {
        let entries: Vec<(BucketedDecimal, usize, Decimal)> = items
            .par_iter()
            .enumerate()
            .map(|(idx, item)| {
                let value = extractor(item);
                let bucket = BucketedDecimal::from_decimal(value, self.bucket_size);
                (bucket, idx, value)
            })
            .collect();
        let mut index_map: BTreeMap<BucketedDecimal, Vec<(usize, Decimal)>> = BTreeMap::new();
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
    
    pub fn range_query_indices<R>(&self, range: R) -> Vec<usize>
    where
        R: RangeBounds<Decimal>,
    {
        let start_bucket = match range.start_bound() {
            Bound::Included(&s) => BucketedDecimal::from_decimal(s, self.bucket_size),
            Bound::Excluded(&s) => {
                let b = BucketedDecimal::from_decimal(s, self.bucket_size);
                BucketedDecimal { bucket: b.bucket + 1 }
            }
            Bound::Unbounded => BucketedDecimal { bucket: i64::MIN },
        };
        let end_bucket = match range.end_bound() {
            Bound::Included(&e) => BucketedDecimal::from_decimal(e, self.bucket_size),
            Bound::Excluded(&e) => BucketedDecimal::from_decimal(e, self.bucket_size),
            Bound::Unbounded => BucketedDecimal { bucket: i64::MAX },
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
