use std::{
    fmt::{Debug,Display},
    sync::Arc,
};
use rayon::prelude::*;
use roaring::RoaringBitmap;

// Op - Битовые операции

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    And,    // Пересечение (∩)
    Or,     // Объединение (∪)
    Xor,    // Симметрическая разность (△)
    AndNot, // Разность (A - B)
    Invert, // Полное отрицание (¬A) - унарная операция!
}

impl Op {
    //Проверить является ли операция унарной
    #[inline]
    pub fn is_unary(&self) -> bool {
        matches!(self, Op::Invert)
    }
    
    //Проверить является ли операция бинарной
    #[inline]
    pub fn is_binary(&self) -> bool {
        !self.is_unary()
    }
}

// OpResult - Результат битовой операции

pub struct OpResult{
    bitmap: RoaringBitmap,
    count: usize,
}

impl OpResult {
    // Основной конструктор для Arc
    pub fn new(bitmap: RoaringBitmap, count: usize) -> Self {
        Self { 
            bitmap: bitmap, 
            count }
    }
    
    // Для обратной совместимости с owned bitmap
    pub fn from_owned(bitmap: RoaringBitmap, count: usize) -> Self {
        Self {
            bitmap: bitmap,
            count,
        }
    }
    
    #[inline]
    pub fn len(&self) -> usize {
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
    
    pub fn stats(&self) -> IndexStats {
        IndexStats {
            ones: self.count,
            zeros: 0,
            total: self.count,
            density: 0.0,
            memory_bytes: self.bitmap.serialized_size(),
        }
    }
}

// IndexStats - Статистика битового индекса

const CHUNK_SIZE: usize = 4096;

#[derive(Debug, Clone)]
pub struct IndexStats {
    pub ones: usize,
    pub zeros: usize,
    pub total: usize,
    pub density: f64,
    pub memory_bytes: usize,
}

// BitIndex - Битовый индекс на основе RoaringBitmap

pub struct Index {
    // RoaringBitmap для хранения индексов (lock-free чтение!)
    bitmap: RoaringBitmap,
    
    // Общее количество элементов (атомарный доступ)
    total_size: usize,
}

impl Index {
    // Создать новый пустой битовый индекс
    pub fn new() -> Self {
        Self {
            bitmap: RoaringBitmap::new(),
            total_size: 0,
        }
    }

    pub fn with_bitmap(
        bitmap: RoaringBitmap,
        total_size: usize
    ) -> Self{
        Self { 
            bitmap, 
            total_size 
        }
    }
    
    // Создать битовый индекс с заданной емкостью
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bitmap: RoaringBitmap::new(),
            total_size: capacity,
        }
    }

    // Построение индекса
    
    // Построить битовый индекс из данных
    // 
    // # Пример
    // ```
    // let bit_index = BitIndex::new();
    // bit_index.build(&items, |item| item.price > 1000.0);
    // ```
    pub fn build<T, F>(items: &[Arc<T>], predicate: F) -> Self
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
        Self{
            bitmap: bitmap,
            total_size: len
        }
    }

    // Построить индекс из готовых индексов
    pub fn from_indices(indices: &[usize], total_size: usize) -> Self {
        let bitmap = RoaringBitmap::from_iter(
            indices.iter().map(|&idx| idx as u32)
        );
        
        Self {
            bitmap: bitmap,
            total_size,
        }
    }
    
    // Построить индекс из RoaringBitmap
    pub fn from_bitmap(bitmap: RoaringBitmap, total_size: usize) -> Self {
        Self {
            bitmap,
            total_size,
        }
    }

    // Получить ссылку на bitmap
    #[inline]
    pub fn bitmap(&self) -> &RoaringBitmap {
        &self.bitmap
    }

    // Получить значение бита на позиции 
    #[inline]
    pub fn get(&self, index: usize) -> bool {
        self.bitmap.contains(index as u32)
    }

    // Получить количество элементов (быстро) 
    #[inline]
    pub fn len(&self) -> usize {
        self.bitmap.len() as usize
    }

    // Проверка на пустоту
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }

    #[inline]
    pub fn count_ones(&self) -> usize {
        self.bitmap.len() as usize
    }
    
    #[inline]
    pub fn count_zeros(&self) -> usize {
        self.total_size.saturating_sub(self.count_ones())
    }
    
    #[inline]
    pub fn total_size(&self) -> usize {
        self.total_size
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
        self.bitmap.serialized_size()
    }
    
    // Полная статистика
    pub fn stats(&self) -> IndexStats {
        let ones = self.count_ones();
        let zeros = self.count_zeros();
        let density = if self.total_size > 0 {
            (ones as f64 / self.total_size as f64) * 100.0
        } else {
            0.0
        };
        IndexStats {
            ones,
            zeros,
            total: self.total_size,
            density,
            memory_bytes: self.bitmap.serialized_size(),
        }
    }

    // Получить все индексы как Vec<usize> 
    pub fn to_indices(&self) -> Vec<usize> {
        self.bitmap.iter().map(|idx| idx as usize).collect()
    }
    
    // Получить индексы параллельно для больших bitmap
    pub fn to_indices_parallel(&self) -> Vec<usize> {
        let bitmap = self.bitmap.clone();
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
    
    // Битовые операции
    pub fn get_result(&self) -> OpResult {
        OpResult::new(self.bitmap.clone(), self.count_ones())
    }
    
    // Пересечение (AND) с другим битовым индексом
    pub fn and(&self, other: &Index) -> OpResult {
        let result = &self.bitmap & &other.bitmap;
        let count = result.len() as usize;
        OpResult::from_owned(result, count)
    }
    
    // Объединение (OR) с другим битовым индексом
    pub fn or(&self, other: &Index) -> OpResult {
        let result = &self.bitmap | &other.bitmap;
        let count = result.len() as usize;
        OpResult::from_owned(result, count)
    }
    
    // Симметрическая разность (XOR)
    pub fn xor(&self, other: &Index) -> OpResult {
        let result = &self.bitmap ^ &other.bitmap;
        let count = result.len() as usize;
        OpResult::from_owned(result, count)
    }
    
    // Разность (A - B)
    pub fn not(&self, other: &Index) -> OpResult {
        let result = &self.bitmap - &other.bitmap;
        let count = result.len() as usize;
        OpResult::from_owned(result, count)
    }
    
    // полное отриацние
    pub fn invert(&self) -> OpResult {
        let full = RoaringBitmap::from_iter(0..(self.total_size as u32));
        let result = &full - &self.bitmap;
        let count = result.len() as usize;
        OpResult::from_owned(result, count)
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
    pub fn multi_operation(&self, operations: &[(&Index, Op)]) -> OpResult {
        if operations.is_empty() {
            return self.get_result();
        }
        let mut result = self.bitmap.clone();
        for (other_index, op) in operations {
            result = match op {
                Op::And => &result & &other_index.bitmap,
                Op::Or => &result | &other_index.bitmap,
                Op::Xor => &result ^ &other_index.bitmap,
                Op::AndNot => &result - &other_index.bitmap,
                Op::Invert => {
                    // Полное отрицание (унарная операция - other_index игнорируется)
                    let full = RoaringBitmap::from_iter(0..(self.total_size as u32));
                    full - &result
                }
            };
        }
        let count = result.len() as usize;
        OpResult::from_owned(result, count)
    }
    

    // Диапазонные операции
    
    // Получить минимальный индекс
    pub fn min(&self) -> Option<usize> {
        self.bitmap.min().map(|v| v as usize)
    }
    
    // Получить максимальный индекс
    pub fn max(&self) -> Option<usize> {
        self.bitmap.max().map(|v| v as usize)
    }
    
    // Получить индексы в диапазоне [start, end)
    pub fn range(&self, start: usize, end: usize) -> Vec<usize> {
        self.bitmap
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
        self.bitmap
            .iter()
            .filter(|&idx| {
                let i = idx as usize;
                i >= start && i < end
            })
            .count()
    }

}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

// Debug и Display

impl Debug for Index {
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

impl Display for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.stats();
        write!(
            f,
            "BitIndex[1s:{}, 0s:{}, density:{:.2}%, mem:{}B]",
            stats.ones, stats.zeros, stats.density, stats.memory_bytes
        )
    }
}


// Tests

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_bit_index_build() {
        let data: Vec<Arc<i32>> = (0..100).map(|i| Arc::new(i)).collect();
        let index = Index::build(&data, |&x| x % 2 == 0);
        assert_eq!(index.count_ones(), 50);
        assert_eq!(index.total_size(), 100);
    }
    
    #[test]
    fn test_bit_operations() {
        let data: Vec<Arc<i32>> = (0..100).map(|i| Arc::new(i)).collect();
        let index_a = Index::build(&data, |&x| x < 50);
        let index_b = Index::build(&data, |&x| x % 2 == 0);
        // AND
        let result_and = index_a.and(&index_b);
        assert_eq!(result_and.len(), 25); // четные < 50
        // OR
        let result_or = index_a.or(&index_b);
        assert_eq!(result_or.len(), 75); // < 50 OR четные
        // Difference
        let result_diff = index_a.not(&index_b);
        assert_eq!(result_diff.len(), 25); // нечетные < 50
    }
    
    #[test]
    fn test_to_indices() {
        let data: Vec<Arc<i32>> = (0..10).map(|i| Arc::new(i)).collect();
        let index = Index::build(&data, |&x| x % 2 == 0);
        let indices = index.to_indices();
        assert_eq!(indices, vec![0, 2, 4, 6, 8]);
    }
    
    #[test]
    fn test_multi_operation() {
        let data: Vec<Arc<i32>> = (0..100).map(|i| Arc::new(i)).collect();
        let idx_a = Index::build(&data, |&x| x < 80);
        let idx_b = Index::build(&data, |&x| x > 20);
        let idx_c = Index::build(&data, |&x| x % 2 == 0);
        // (< 80) AND (> 20) AND (четные)
        let result = idx_a.multi_operation(&[
            (&idx_b, Op::And),
            (&idx_c, Op::And),
        ]);
        // Должно быть: четные числа от 22 до 78
        assert_eq!(result.len(), 29); // 22, 24, ..., 78 = 29 чисел
    }
    
    #[test]
    fn test_density() {
        let data: Vec<Arc<i32>> = (0..100).map(|i| Arc::new(i)).collect();
        let index = Index::build(&data, |&x| x < 25);
        assert_eq!(index.count_ones(), 25);
        assert_eq!(index.count_zeros(), 75);
        assert!((index.density() - 25.0).abs() < 0.01);
    }
}