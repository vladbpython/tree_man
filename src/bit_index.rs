use arc_swap::ArcSwap;
use std::sync::{
    Arc,atomic::{AtomicUsize,Ordering}};
use rayon::prelude::*;
use roaring::RoaringBitmap;

// BitOp - Битовые операции

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitOp {
    And,    // Пересечение (∩)
    Or,     // Объединение (∪)
    Xor,    // Симметрическая разность (△)
    AndNot, // Разность (A - B)
}

// BitOpResult - Результат битовой операции

pub struct BitOpResult {
    bitmap: Arc<RoaringBitmap>,  // ← Arc вместо owned
    count: usize,
}

impl BitOpResult {
    // Основной конструктор для Arc
    pub fn new(bitmap: Arc<RoaringBitmap>, count: usize) -> Self {
        Self { bitmap, count }
    }
    
    // Для обратной совместимости с owned bitmap
    pub fn from_owned(bitmap: RoaringBitmap, count: usize) -> Self {
        Self {
            bitmap: Arc::new(bitmap),
            count,
        }
    }
    
    // Пустой результат
    pub fn empty() -> Self {
        Self {
            bitmap: Arc::new(RoaringBitmap::new()),
            count: 0,
        }
    }
    
    #[inline]
    pub fn count(&self) -> usize {
        self.count
    }
    
    #[inline]
    pub fn bitmap(&self) -> &RoaringBitmap {
        &self.bitmap
    }
    
    pub fn to_indices(&self) -> Vec<usize> {
        self.bitmap.iter().map(|idx| idx as usize).collect()
    }
    
    // Применить битовую маску к данным
    #[inline]
    pub fn apply_to_fast<T>(&self, items: &[Arc<T>]) -> Vec<Arc<T>>
    where
        T: Send + Sync,
    {
        if self.count == 0 {
            return Vec::new();
        }
        let total_items = items.len();
        let result_count = self.count;
        // Вычисляем плотность (density)
        let density = if total_items > 0 {
            result_count as f64 / total_items as f64
        } else {
            0.0
        };
        if density > 0.3 && result_count > 100_000{
            let indices: Vec<u32> = self.bitmap.iter().collect();
            return indices
                    .par_chunks(CHUNK_SIZE)
                    .flat_map_iter(|chunk| {
                        let mut batch = Vec::with_capacity(chunk.len());
                        unsafe {
                            let items_ptr = items.as_ptr();
                            for &idx in chunk {
                                batch.push(Arc::clone(&*items_ptr.add(idx as usize)));
                            }
                        }
                        batch
                    })
                    .collect()
        }
        let mut result = Vec::with_capacity(result_count);
        unsafe {
            let items_ptr = items.as_ptr();
            for idx in self.bitmap.iter() {
                result.push(Arc::clone(&*items_ptr.add(idx as usize)));
            }
        }
        result
          
    }
    
    #[inline]
    pub fn contains(&self, index: usize) -> bool {
        self.bitmap.contains(index as u32)
    }
    
    pub fn stats(&self) -> BitIndexStats {
        BitIndexStats {
            ones: self.count,
            zeros: 0,
            total: self.count,
            density: 0.0,
            memory_bytes: self.bitmap.serialized_size(),
        }
    }
}

// BitIndexStats - Статистика битового индекса

const CHUNK_SIZE: usize = 4096;

#[derive(Debug, Clone)]
pub struct BitIndexStats {
    pub ones: usize,
    pub zeros: usize,
    pub total: usize,
    pub density: f64,
    pub memory_bytes: usize,
}

// BitIndex - Битовый индекс на основе RoaringBitmap

pub struct BitIndex {
    // RoaringBitmap для хранения индексов (lock-free чтение!)
    bitmap: ArcSwap<RoaringBitmap>,
    
    // Общее количество элементов (атомарный доступ)
    total_size: Arc<AtomicUsize>,
}

impl BitIndex {
    // Создать новый пустой битовый индекс
    pub fn new() -> Self {
        Self {
            bitmap: ArcSwap::from_pointee(RoaringBitmap::new()),
            total_size: Arc::new(AtomicUsize::new(0)),
        }
    }
    
    // Создать битовый индекс с заданной емкостью
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bitmap: ArcSwap::from_pointee(RoaringBitmap::new()),
            total_size: Arc::new(AtomicUsize::new(capacity)),
        }
    }

    // Получить Arc на RoaringBitmap (zero-copy) 
    #[inline]
    pub fn bitmap_arc(&self) -> Arc<RoaringBitmap> {
        self.bitmap.load_full()
    }

    // Получить количество элементов (быстро) 
    #[inline]
    pub fn len(&self) -> usize {
        self.bitmap.load().len() as usize
    }

    // Проверка на пустоту
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bitmap.load().is_empty()
    }
    
    // Построение индекса
    
    // Построить битовый индекс из данных
    // 
    // # Пример
    // ```
    // let bit_index = BitIndex::new();
    // bit_index.build(&items, |item| item.price > 1000.0);
    // ```
    pub fn build<T, F>(&self, items: &[Arc<T>], predicate: F)
    where
        T: Send + Sync,
        F: Fn(&T) -> bool + Send + Sync,
    {
        let len = items.len();
        let bitmap = if len < 100_000 {
            // Для маленьких наборов - последовательно
            let mut bitmap = RoaringBitmap::new();
            for (idx, item) in items.iter().enumerate() {
                if predicate(item) {
                    bitmap.insert(idx as u32);
                }
            }
            bitmap
        } else {
            //  Для больших наборов - параллельно
            let chunks: Vec<_> = items.chunks(CHUNK_SIZE).enumerate().collect();
            let bitmaps: Vec<RoaringBitmap> = chunks
                .into_par_iter()
                .map(|(chunk_idx, chunk)| {
                    let offset = chunk_idx * CHUNK_SIZE;
                    let mut local_bitmap = RoaringBitmap::new();
                    for (idx, item) in chunk.iter().enumerate() {
                        if predicate(item) {
                            local_bitmap.insert((offset + idx) as u32);
                        }
                    }
                    local_bitmap
                })
                .collect();
            // Объединяем все bitmap'ы
            bitmaps.into_iter().fold(RoaringBitmap::new(), |mut acc, b| {
                acc |= b;
                acc
            })
        };
        self.bitmap.store(Arc::new(bitmap));
        self.total_size.store(len, Ordering::Release);
    }
    
    // Построить индекс из готовых индексов
    pub fn from_indices(indices: &[usize], total_size: usize) -> Self {
        let bitmap = RoaringBitmap::from_iter(
            indices.iter().map(|&idx| idx as u32)
        );
        
        Self {
            bitmap: ArcSwap::from_pointee(bitmap),
            total_size: Arc::new(AtomicUsize::new(total_size)),
        }
    }
    
    // Построить индекс из RoaringBitmap
    pub fn from_bitmap(bitmap: RoaringBitmap, total_size: usize) -> Self {
        Self {
            bitmap: ArcSwap::from_pointee(bitmap),
            total_size: Arc::new(AtomicUsize::new(total_size)),
        }
    }
    
    // Базовые операции (Copy-On-Write для модификаций)
    
    // Установить бит на позиции (создает новую копию)
    pub fn set(&self, index: usize) {
        let current = self.bitmap.load();
        let mut bitmap = (**current).clone();  // ← Правильное клонирование из Arc
        bitmap.insert(index as u32);
        self.bitmap.store(Arc::new(bitmap));
    }

    // Очистить бит на позиции (создает новую копию)
    pub fn clear(&self, index: usize) {
        let current = self.bitmap.load();
        let mut bitmap = (**current).clone();
        bitmap.remove(index as u32);
        self.bitmap.store(Arc::new(bitmap));
    }

    // Получить значение бита на позиции  (lock-free чтение!)
    #[inline]
    pub fn get(&self, index: usize) -> bool {
        self.bitmap.load().contains(index as u32)
    }

    // Переключить бит на позиции (создает новую копию)
    pub fn toggle(&self, index: usize) {
        let current = self.bitmap.load();
        let mut bitmap = (**current).clone();
        let idx = index as u32;
        if bitmap.contains(idx) {
            bitmap.remove(idx);
        } else {
            bitmap.insert(idx);
        }
        self.bitmap.store(Arc::new(bitmap));
    }
    
    // Очистить все биты
    pub fn clear_all(&self) {
        self.bitmap.store(Arc::new(RoaringBitmap::new()));
    }
    
    // Получение данных
    
    // Получить копию RoaringBitmap
    pub fn get_bitmap(&self) -> RoaringBitmap {
        (**self.bitmap.load()).clone()
    }
    
    // Получить Arc на RoaringBitmap zero-copy
    pub fn get_bitmap_arc(&self) -> Arc<RoaringBitmap> {
        self.bitmap.load_full()
    }
    
    // Получить все индексы как Vec<usize> 
    pub fn to_indices(&self) -> Vec<usize> {
        let bitmap = self.bitmap.load();
        bitmap.iter().map(|idx| idx as usize).collect()
    }
    
    // Получить индексы параллельно для больших bitmap
    pub fn to_indices_parallel(&self) -> Vec<usize> {
        let bitmap = self.bitmap.load();
        if bitmap.len() > 10_000 {
            let indices: Vec<u32> = bitmap.iter().collect();
            indices
                .par_chunks(4096)
                .flat_map_iter(|chunk| {
                    chunk.iter().map(|&idx| idx as usize)
                })
                .collect()
        } else {
            bitmap.iter().map(|idx| idx as usize).collect()
        }
    }

    // Статистика (все операции lock-free!)
    
    // Количество установленных битов (ones)
    #[inline]
    pub fn count_ones(&self) -> usize {
        self.bitmap.load().len() as usize
    }
    
    // Количество неустановленных битов (zeros)
    #[inline]
    pub fn count_zeros(&self) -> usize {
        let total = self.total_size.load(Ordering::Acquire);
        let ones = self.count_ones();
        total.saturating_sub(ones)
    }
    
    // Общий размер
    #[inline]
    pub fn total_size(&self) -> usize {
        self.total_size.load(Ordering::Acquire)
    }
    
    // Плотность (процент установленных битов)
    pub fn density(&self) -> f64 {
        let total = self.total_size();
        if total == 0 {
            return 0.0;
        }
        
        let ones = self.count_ones();
        (ones as f64 / total as f64) * 100.0
    }
    
    // Размер в памяти (байты)
    pub fn memory_size(&self) -> usize {
        self.bitmap.load().serialized_size()
    }
    
    // Полная статистика
    pub fn stats(&self) -> BitIndexStats {
        let bitmap = self.bitmap.load();
        let ones = bitmap.len() as usize;
        let total = self.total_size();
        let zeros = total.saturating_sub(ones);
        let density = if total > 0 {
            (ones as f64 / total as f64) * 100.0
        } else {
            0.0
        };
        
        BitIndexStats {
            ones,
            zeros,
            total,
            density,
            memory_bytes: bitmap.serialized_size(),
        }
    }
    
    // Битовые операции
    pub fn get_result(&self) -> BitOpResult {
        let bitmap_arc = self.bitmap.load_full();  // ← Arc, не клонирование!
        let count = bitmap_arc.len() as usize;
        BitOpResult::new(bitmap_arc, count)
    }
    
    // Пересечение (AND) с другим битовым индексом
    pub fn and(&self, other: &BitIndex) -> BitOpResult {
        let bitmap_a = self.bitmap.load();
        let bitmap_b = other.bitmap.load();
        let result = bitmap_a.as_ref() & bitmap_b.as_ref();
        let count = result.len() as usize;
        BitOpResult::from_owned(result, count)  // ← from_owned для owned bitmap
    }
    
    // Объединение (OR) с другим битовым индексом
    pub fn or(&self, other: &BitIndex) -> BitOpResult {
        let bitmap_a = self.bitmap.load();
        let bitmap_b = other.bitmap.load();
        let result = bitmap_a.as_ref() | bitmap_b.as_ref();
        let count = result.len() as usize;
        BitOpResult::from_owned(result, count)
    }
    
    // Симметрическая разность (XOR)
    pub fn xor(&self, other: &BitIndex) -> BitOpResult {
        let bitmap_a = self.bitmap.load();
        let bitmap_b = other.bitmap.load();
        let result = bitmap_a.as_ref() ^ bitmap_b.as_ref();
        let count = result.len() as usize;
        BitOpResult::from_owned(result, count)
    }
    
    // Разность (A - B)
    pub fn difference(&self, other: &BitIndex) -> BitOpResult {
        let bitmap_a = self.bitmap.load();
        let bitmap_b = other.bitmap.load();
        let result = bitmap_a.as_ref() - bitmap_b.as_ref();
        let count = result.len() as usize;
        BitOpResult::from_owned(result, count)
    }
    
    // Отрицание (NOT)
    pub fn not(&self) -> BitOpResult {
        let bitmap = self.bitmap.load();
        let total = self.total_size();
        
        let full = RoaringBitmap::from_iter(0..(total as u32));
        let result = full - bitmap.as_ref();
        let count = result.len() as usize;
        
        BitOpResult::from_owned(result, count)
    }
    
    // Множественные операции (цепочка)
    // 
    // # Пример
    // ```
    // let result = bit_index_a.multi_operation(&[
    //     (&bit_index_b, BitOp::And),
    //     (&bit_index_c, BitOp::Or),
    //     (&bit_index_d, BitOp::AndNot),
    // ]);
    // ```
    pub fn multi_operation(&self, operations: &[(&BitIndex, BitOp)]) -> BitOpResult {
        if operations.is_empty() {
            return self.get_result();
        }
        let mut result = self.get_bitmap();
        for (other_index, op) in operations {
            let other = other_index.bitmap.load();
            result = match op {
                BitOp::And => &result & other.as_ref(),
                BitOp::Or => &result | other.as_ref(),
                BitOp::Xor => &result ^ other.as_ref(),
                BitOp::AndNot => &result - other.as_ref(),
            };
        }
        let count = result.len() as usize;
        BitOpResult::from_owned(result, count)
    }
    

    // Диапазонные операции
    
    // Получить минимальный индекс
    pub fn min(&self) -> Option<usize> {
        self.bitmap.load().min().map(|v| v as usize)
    }
    
    // Получить максимальный индекс
    pub fn max(&self) -> Option<usize> {
        self.bitmap.load().max().map(|v| v as usize)
    }
    
    // Получить индексы в диапазоне [start, end)
    pub fn range(&self, start: usize, end: usize) -> Vec<usize> {
        let bitmap = self.bitmap.load();
        bitmap
            .iter()
            .filter(|&idx| {
                let i = idx as usize;
                i >= start && i < end
            })
            .map(|idx| idx as usize)
            .collect()
    }
    
    // Количество установленных битов в диапазоне
    pub fn count_range(&self, start: usize, end: usize) -> usize {
        let bitmap = self.bitmap.load();
        bitmap
            .iter()
            .filter(|&idx| {
                let i = idx as usize;
                i >= start && i < end
            })
            .count()
    }
}

impl Default for BitIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for BitIndex {
    fn clone(&self) -> Self {
        Self {
            bitmap: ArcSwap::new(self.bitmap.load_full()),
            total_size: Arc::new(AtomicUsize::new(self.total_size())),
        }
    }
}

// Debug и Display

impl std::fmt::Debug for BitIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.stats();
        f.debug_struct("BitIndex")
            .field("ones", &stats.ones)
            .field("zeros", &stats.zeros)
            .field("total", &stats.total)
            .field("density", &format!("{:.2}%", stats.density))
            .field("memory_bytes", &stats.memory_bytes)
            .finish()
    }
}

impl std::fmt::Display for BitIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.stats();
        write!(
            f,
            "BitIndex[1s:{}, 0s:{}, density:{:.2}%, mem:{}B]",
            stats.ones, stats.zeros, stats.density, stats.memory_bytes
        )
    }
}  

// View на битовый индекс
pub struct BitIndexView<T> {
    items: Arc<Vec<Arc<T>>>,
    bitmap: Arc<RoaringBitmap>,
}

impl<T> BitIndexView<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(items: Arc<Vec<Arc<T>>>, bitmap: Arc<RoaringBitmap>) -> Self {
        Self { items, bitmap }
    }
    
    // Создать view со всеми элементами
    pub fn all_create(items: Arc<Vec<Arc<T>>>) -> Self {
        let bitmap = RoaringBitmap::from_iter(0..items.len() as u32);
        Self {
            items,
            bitmap: Arc::new(bitmap),
        }
    }
    
    // Итератор по ссылкам (БЕЗ клонирования Arc!) 
    pub fn iter(&self) -> impl Iterator<Item = &Arc<T>> + '_ {
        self.bitmap.iter().filter_map(move |idx| {
            self.items.get(idx as usize)
        })
    }
    
    // Итератор с клонированием Arc (когда нужно владение)
    pub fn iter_cloned(&self) -> impl Iterator<Item = Arc<T>> + '_ {
        self.bitmap.iter().filter_map(move |idx| {
            self.items.get(idx as usize).cloned()
        })
    }
    
    // Параллельный итератор 
    pub fn par_iter(&self) -> impl ParallelIterator<Item = &Arc<T>> + '_ {
        let indices: Vec<u32> = self.bitmap.iter().collect();
        let items = &self.items;
        indices.into_par_iter().filter_map(move |idx| {
            items.get(idx as usize)
        })
    }
    
    // Количество элементов 
    #[inline]
    pub fn len(&self) -> usize {
        self.bitmap.len() as usize
    }
    
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }
    
    // Применить функцию к каждому элементу (без материализации!) 
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&T),
    {
        for idx in self.bitmap.iter() {
            if let Some(item) = self.items.get(idx as usize) {
                f(item);
            }
        }
    }
    
    // Параллельный for_each 
    pub fn par_for_each<F>(&self, f: F)
    where
        F: Fn(&T) + Send + Sync,
    {
        let indices: Vec<u32> = self.bitmap.iter().collect();
        indices.into_par_iter().for_each(|idx| {
            if let Some(item) = self.items.get(idx as usize) {
                f(item);
            }
        });
    }
    
    // Композиция фильтров (БЕЗ материализации!) 
    
    // Дополнительная фильтрация (создает новый view с подмножеством индексов)
    pub fn filter<F>(&self, predicate: F) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync,
    {
        let indices: Vec<u32> = self.bitmap.iter().collect();
        let filtered: Vec<u32> = indices
            .into_par_iter()
            .filter(|&idx| {
                self.items.get(idx as usize)
                    .map(|item| predicate(item))
                    .unwrap_or(false)
            })
            .collect();
        
        Self {
            items: Arc::clone(&self.items),
            bitmap: Arc::new(RoaringBitmap::from_iter(filtered)),
        }
    }
    
    // Пересечение с другим view (битовая операция AND) 
    pub fn intersect(&self, other: &BitIndexView<T>) -> Self {
        let new_bitmap = self.bitmap.as_ref() & other.bitmap.as_ref();
        Self {
            items: Arc::clone(&self.items),
            bitmap: Arc::new(new_bitmap),
        }
    }
    
    // Объединение с другим view (битовая операция OR) 
    pub fn union(&self, other: &BitIndexView<T>) -> Self {
        let new_bitmap = self.bitmap.as_ref() | other.bitmap.as_ref();
        Self {
            items: Arc::clone(&self.items),
            bitmap: Arc::new(new_bitmap),
        }
    }
    
    // Разность (A - B) 
    pub fn difference(&self, other: &BitIndexView<T>) -> Self {
        let new_bitmap = self.bitmap.as_ref() - other.bitmap.as_ref();
        Self {
            items: Arc::clone(&self.items),
            bitmap: Arc::new(new_bitmap),
        }
    }
    
    // Симметрическая разность (XOR) 
    pub fn symmetric_difference(&self, other: &BitIndexView<T>) -> Self {
        let new_bitmap = self.bitmap.as_ref() ^ other.bitmap.as_ref();
        Self {
            items: Arc::clone(&self.items),
            bitmap: Arc::new(new_bitmap),
        }
    }
    
    // ========================================================================
    // Агрегация (БЕЗ материализации!) 
    // ========================================================================
    
    // Map-reduce без материализации
    pub fn map_reduce<M, R, A>(&self, map_fn: M, reduce_fn: R, init: A) -> A
    where
        M: Fn(&T) -> A + Send + Sync,
        R: Fn(A, A) -> A + Send + Sync,
        A: Send + Sync + Clone,  // ← Добавить Sync
    {
        let indices: Vec<u32> = self.bitmap.iter().collect();
        indices
            .into_par_iter()
            .filter_map(|idx| self.items.get(idx as usize))
            .map(|item| map_fn(item))
            .reduce(|| init.clone(), reduce_fn)
    }
    
    // Подсчет элементов по условию (БЕЗ материализации!)
    pub fn count_where<F>(&self, predicate: F) -> usize
    where
        F: Fn(&T) -> bool + Send + Sync,
    {
        let indices: Vec<u32> = self.bitmap.iter().collect();
        indices
            .into_par_iter()
            .filter(|&idx| {
                self.items.get(idx as usize)
                    .map(|item| predicate(item))
                    .unwrap_or(false)
            })
            .count()
    }
    
    
    // Проверка существования (БЕЗ материализации!)
    pub fn any<F>(&self, predicate: F) -> bool
    where
        F: Fn(&T) -> bool + Send + Sync,
    {
        let indices: Vec<u32> = self.bitmap.iter().collect();
        indices
            .into_par_iter()
            .any(|idx| {
                self.items.get(idx as usize)
                    .map(|item| predicate(item))
                    .unwrap_or(false)
            })
    }

    pub fn all<F>(&self, predicate: F) -> bool
    where
        F: Fn(&T) -> bool + Send + Sync,
    {
        let indices: Vec<u32> = self.bitmap.iter().collect();
        indices
            .into_par_iter()
            .all(|idx| {
                self.items.get(idx as usize)
                    .map(|item| predicate(item))
                    .unwrap_or(false)
            })
    }
    
    
    // Материализация (только когда ДЕЙСТВИТЕЛЬНО нужно!)
    
    // Материализовать в Vec (последовательно)
    pub fn collect(&self) -> Vec<Arc<T>> {
        self.iter_cloned().collect()
    }
    
    // Материализовать параллельно (для больших наборов)
    pub fn collect_par(&self) -> Vec<Arc<T>> {
        let indices: Vec<u32> = self.bitmap.iter().collect();
        indices
            .into_par_iter()
            .filter_map(|idx| self.items.get(idx as usize).cloned())
            .collect()
    }
    
    // Получить индексы как Vec<usize>
    pub fn indices(&self) -> Vec<usize> {
        self.bitmap.iter().map(|i| i as usize).collect()
    }
    
    // Получить bitmap напрямую
    #[inline]
    pub fn bitmap(&self) -> &RoaringBitmap {
        &self.bitmap
    }
}

// Implement Clone
impl<T> Clone for BitIndexView<T> {
    fn clone(&self) -> Self {
        Self {
            items: Arc::clone(&self.items),
            bitmap: Arc::clone(&self.bitmap),
        }
    }
}


// Tests

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_bit_index_basic() {
        let index = BitIndex::new();
        index.set(5);
        index.set(10);
        index.set(15);
        assert!(index.get(5));
        assert!(index.get(10));
        assert!(index.get(15));
        assert!(!index.get(0));
        assert_eq!(index.count_ones(), 3);
    }
    
    #[test]
    fn test_bit_index_build() {
        let data: Vec<Arc<i32>> = (0..100).map(|i| Arc::new(i)).collect();
        let index = BitIndex::new();
        index.build(&data, |&x| x % 2 == 0);
        assert_eq!(index.count_ones(), 50);
        assert_eq!(index.total_size(), 100);
    }
    
    #[test]
    fn test_bit_operations() {
        let data: Vec<Arc<i32>> = (0..100).map(|i| Arc::new(i)).collect();
        let index_a = BitIndex::new();
        index_a.build(&data, |&x| x < 50);
        let index_b = BitIndex::new();
        index_b.build(&data, |&x| x % 2 == 0);
        // AND
        let result_and = index_a.and(&index_b);
        assert_eq!(result_and.count(), 25); // четные < 50
        // OR
        let result_or = index_a.or(&index_b);
        assert_eq!(result_or.count(), 75); // < 50 OR четные
        // Difference
        let result_diff = index_a.difference(&index_b);
        assert_eq!(result_diff.count(), 25); // нечетные < 50
    }
    
    #[test]
    fn test_to_indices() {
        let data: Vec<Arc<i32>> = (0..10).map(|i| Arc::new(i)).collect();
        let index = BitIndex::new();
        index.build(&data, |&x| x % 2 == 0);
        let indices = index.to_indices();
        assert_eq!(indices, vec![0, 2, 4, 6, 8]);
    }
    
    #[test]
    fn test_multi_operation() {
        let data: Vec<Arc<i32>> = (0..100).map(|i| Arc::new(i)).collect();
        let idx_a = BitIndex::new();
        idx_a.build(&data, |&x| x < 80);
        let idx_b = BitIndex::new();
        idx_b.build(&data, |&x| x > 20);
        let idx_c = BitIndex::new();
        idx_c.build(&data, |&x| x % 2 == 0);
        // (< 80) AND (> 20) AND (четные)
        let result = idx_a.multi_operation(&[
            (&idx_b, BitOp::And),
            (&idx_c, BitOp::And),
        ]);
        // Должно быть: четные числа от 22 до 78
        assert_eq!(result.count(), 29); // 22, 24, ..., 78 = 29 чисел
    }
    
    #[test]
    fn test_density() {
        let data: Vec<Arc<i32>> = (0..100).map(|i| Arc::new(i)).collect();
        let index = BitIndex::new();
        index.build(&data, |&x| x < 25);
        assert_eq!(index.count_ones(), 25);
        assert_eq!(index.count_zeros(), 75);
        assert!((index.density() - 25.0).abs() < 0.01);
    }
}