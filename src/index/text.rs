use super::bit::{
    Index as BitIndex,
    Op as BitOp,
    OpResult as BitOpResult,
};
use ahash::{AHashMap, HashMap};
use memchr::memmem::Finder;
use rayon::prelude::*;
use roaring::RoaringBitmap;
use smallvec::SmallVec;
use std::{
    fmt::Display,
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize,Ordering},
        Arc,
    }
};

// N-gram индекс для быстрого substring search
pub struct TextIndex<T>
where
    T: Send + Sync,
{
    ngrams: Arc<AHashMap<String, BitIndex>>,
    // Store texts for full verification
    item_texts: Arc<Vec<String>>,
    // N-gram size (3 для trigrams)
    n: usize,
    total_items: usize,
    // Stats
    unique_ngrams: usize,
    total_ngrams: usize,
    _phantom: PhantomData<T>,
}

impl<T> TextIndex<T>
where
    T: Send + Sync + 'static,
{
    // Создать новый n-gram индекс
    pub fn new(n: usize) -> Self {
        Self {
            ngrams: Arc::new(AHashMap::new()),
            item_texts: Arc::new(Vec::new()),
            n,
            total_items: 0,
            unique_ngrams: 0,
            total_ngrams: 0,
            _phantom: PhantomData,
        }
    }

    pub fn new_tri_gram() -> Self {
        Self::new(3)
    }

    // Строим индекс
    pub fn build<F>(&mut self, items: &[Arc<T>], extractor: F)
    where
        F: Fn(&T) -> String + Send + Sync,
    {
        if items.is_empty() {
            return;
        }
        self.total_items = items.len();
        
        // Extract texts
        let texts: Vec<String> = items
            .par_iter()
            .map(|item| extractor(item).to_lowercase())
            .collect();
        
        let estimated_capacity = match texts.len() {
            0..=1_000 => 300,
            1_001..=10_000 => 800,
            10_001..=100_000 => 1_500,
            100_001..=1_000_000 => 3_000,
            1_000_001..=5_000_000 => 6_000,
            _ => 10_000,
        };
        
        // PHASE 1: Параллельно строим локальные HashMap'ы
        let num_threads = rayon::current_num_threads();
        let chunk_size = if texts.len() < 100_000 {
            (texts.len() / num_threads).max(1000)
        } else {
            (texts.len() / (num_threads * 4)).max(2000).min(10000)
        };
        
        let total_ngrams = AtomicUsize::new(0);
        
        // Каждый thread собирает свой локальный HashMap
        let local_maps: Vec<AHashMap<String, Vec<usize>>> = texts
            .par_chunks(chunk_size)
            .enumerate()
            .map(|(chunk_idx, chunk)| {
                let base_idx = chunk_idx * chunk_size;
                
                // Локальный HashMap (без contention!)
                let mut local_map: AHashMap<String, Vec<usize>> = 
                    AHashMap::with_capacity(estimated_capacity / num_threads);
                let mut ngrams_buffer: SmallVec<[String; 64]> = SmallVec::new();
                
                for (offset, text) in chunk.iter().enumerate() {
                    let idx = base_idx + offset;
                    
                    ngrams_buffer.clear();
                    self.extract_ngrams_to_buffer(text, &mut ngrams_buffer);
                    
                    total_ngrams.fetch_add(ngrams_buffer.len(), Ordering::Relaxed);
                    
                    for ngram in &ngrams_buffer {
                        local_map
                            .entry(ngram.clone())
                            .or_insert_with(Vec::new)
                            .push(idx);
                    }
                }
                
                local_map
            })
            .collect();
        
        // PHASE 2: Последовательно мержим локальные HashMap'ы
        let mut ngrams_map: AHashMap<String, Vec<usize>> = 
            AHashMap::with_capacity(estimated_capacity);
        
        for local_map in local_maps {
            for (ngram, mut indices) in local_map {
                ngrams_map
                    .entry(ngram)
                    .or_insert_with(Vec::new)
                    .append(&mut indices);
            }
        }
        
        // PHASE 3: Параллельная конвертация в BitIndex
        let mut entries: Vec<(String, Vec<usize>)> = ngrams_map.into_iter().collect();
        entries.sort_by(|a, b| b.1.len().cmp(&a.1.len()));
        
        let pairs: Vec<(String, BitIndex)> = entries
            .into_par_iter()
            .map(|(ngram, mut indices)| {
                indices.sort_unstable();
                indices.dedup();
                let bit_index = BitIndex::from_indices(&indices, self.total_items);
                (ngram, bit_index)
            })
            .collect();
        
        let mut ngrams_bit = AHashMap::with_capacity(pairs.len());
        ngrams_bit.extend(pairs);
        
        self.unique_ngrams = ngrams_bit.len();
        self.total_ngrams = total_ngrams.load(Ordering::Relaxed);
        self.ngrams = Arc::new(ngrams_bit);
        self.item_texts = Arc::new(texts);
    }

    // Извлекаем все n-граммы в буфер
    #[inline]
    fn extract_ngrams_to_buffer(&self, text: &str, buffer: &mut SmallVec<[String; 64]>) {
        if text.len() < self.n {
            if !text.is_empty() {
                buffer.push(text.to_string());
            }
            return;
        }
        // Быстрый путь для ASCII
        if text.is_ascii() {
            let bytes = text.as_bytes();
            for i in 0..=bytes.len().saturating_sub(self.n) {
                // SAFETY: ASCII is valid UTF-8
                let ngram = unsafe {
                    std::str::from_utf8_unchecked(&bytes[i..i + self.n])
                };
                buffer.push(ngram.to_string());
            }
        } else {
            // Fallback для Unicode
            let chars: Vec<char> = text.chars().collect();
            for i in 0..=chars.len().saturating_sub(self.n) {
                let ngram: String = chars[i..i + self.n].iter().collect();
                buffer.push(ngram);
            }
        }
    }

    /// Извлекаем все n-граммы из текста (для обратной совместимости)
    #[inline]
    fn extract_ngrams(&self, text: &str) -> Vec<String> {
        let mut buffer = SmallVec::<[String; 64]>::new();
        self.extract_ngrams_to_buffer(text, &mut buffer);
        buffer.into_vec()
    }

    /// Быстрый substring search через n-граммы
    /// 
    /// # Алгоритм
    /// 1. Извлекаем n-граммы из query
    /// 2. Используем BitIndex.multi_operation для пересечения
    /// 3. Проверяем кандидатов полным substring match
    pub fn search(&self, query: &str) -> Vec<usize> {
        if query.is_empty() {
            return Vec::new();
        }
        let query_lower = query.to_lowercase();
        // Для очень коротких query - linear search
        if query_lower.len() < self.n {
            return self.linear_search(&query_lower);
        }
        // Извлекаем n-граммы из query
        let query_ngrams = self.extract_ngrams(&query_lower);
        if query_ngrams.is_empty() {
            return Vec::new();
        }
        // Находим кандидатов через BitIndex операции
        let candidates = self.find_candidates_with_bitindex(&query_ngrams);
        if candidates.is_empty() {
            return Vec::new();
        }
        // Фильтруем кандидатов с полным substring match
        // Выбираем алгоритм в зависимости от размера результата
        if candidates.len() == 1 {
            // Для 1 результата - простая проверка
            if self.item_texts[candidates[0]].contains(&query_lower) {
                return candidates;
            } else {
                return Vec::new();
            }
        } else if candidates.len() < 100 {
            // Для малого количества - обычная contains (меньше overhead)
            return candidates
                .into_iter()
                .filter(|&idx| self.item_texts[idx].contains(&query_lower))
                .collect();
        } else {
            // Для большого количества - SIMD
            let finder = Finder::new(query_lower.as_bytes());
            return candidates
                .into_par_iter()
                .filter(|&idx| {
                    finder.find(self.item_texts[idx].as_bytes()).is_some()
                })
                .collect();
        }
    }

     /// Линейный поиск для коротких query
    fn linear_search(&self, query: &str) -> Vec<usize> {
        let finder = Finder::new(query.as_bytes());
        (0..self.total_items)
            .into_par_iter()
            .filter(|&idx| {
                finder.find(self.item_texts[idx].as_bytes()).is_some()
            })
            .collect()
    }

    /// Комплексный поиск по полным словам с логическими операторами
    /// 
    /// # Arguments
    ///  `or_words` - Слова для OR (любое должно присутствовать)
    ///  `and_words` - Слова для AND (все должны присутствовать)
    ///  `not_words` - Слова для NOT (не должны присутствовать)
    /// 
    /// # Example
    /// 
    /// // Найти: (payment OR transaction) AND failed AND NOT success
    /// let results = index.search_complex_words(
    ///     &["payment", "transaction"],  // OR
    ///     &["failed"],                  // AND
    ///     &["success"]                  // NOT
    /// );
    /// 
    pub fn search_complex_words(
        &self,
        or_words: &[&str],
        and_words: &[&str],
        not_words: &[&str],
    ) -> Vec<usize> {
        // ШАГ 1: BATCH SEARCH - параллельно получаем RoaringBitmap напрямую
        let all_words: Vec<&str> = or_words.iter()
            .chain(and_words.iter())
            .chain(not_words.iter())
            .copied()
            .collect();
        // Parallel search - сразу в RoaringBitmap (без промежуточных структур)
        let word_bitmaps: HashMap<String, RoaringBitmap> = all_words
            .par_iter()
            .map(|&word| {
                let indices = self.search(word);
                // Прямая конвертация - минимум overhead
                let bitmap: RoaringBitmap = indices.iter().map(|&i| i as u32).collect();
                (word.to_string(), bitmap)
            })
            .collect();
        // ШАГ 2: OR операции - прямые битовые операции
        let mut result = if !or_words.is_empty() {
            let mut combined = RoaringBitmap::new();
            for word in or_words {
                if let Some(bitmap) = word_bitmaps.get(*word) {
                    combined |= bitmap;  // In-place operation - быстро!
                }
            }
            if combined.is_empty() {
                return Vec::new();
            }
            combined
        } else {
            // Все элементы
            (0..self.total_items as u32).collect()
        };
        // ШАГ 3: AND операции - прямые битовые операции
        for word in and_words {
            if let Some(bitmap) = word_bitmaps.get(*word) {
                result &= bitmap;  // In-place AND - быстро!
                // Early exit
                if result.is_empty() {
                    return Vec::new();
                }
            } else {
                return Vec::new();
            }
        }
        // ШАГ 4: NOT операции - прямые битовые операции
        for word in not_words {
            if let Some(bitmap) = word_bitmaps.get(*word) {
                result -= bitmap;  // In-place MINUS - быстро!
            }
        }
        // Конвертируем в индексы (один раз в конце)
        result.iter().map(|i| i as usize).collect()
    }


    // Находим кандидатов
    fn find_candidates_with_bitindex(&self, query_ngrams: &[String]) -> Vec<usize> {
        if query_ngrams.is_empty() {
            return Vec::new();
        }
        // Получаем BitIndex для первой n-граммы
        let first_bit = match self.ngrams.get(&query_ngrams[0]) {
            Some(bit) => bit,
            None => return Vec::new(),
        };
        if query_ngrams.len() == 1 {
            return first_bit.to_indices();
        }
        // Собираем остальные для multi_operation (AND всех n-грамм)
        let operations: Vec<(&BitIndex, BitOp)> = query_ngrams[1..]
            .iter()
            .filter_map(|ngram| {
                self.ngrams.get(ngram).map(|bit| (bit, BitOp::And))
            })
            .collect();
        if operations.len() != query_ngrams.len() - 1 {
            // Какая-то n-грамма не найдена
            return Vec::new();
        }
        // Используем BitIndex.multi_operation! ⚡
        let result = first_bit.multi_operation(&operations);
        result.to_indices()
    }

    // Получить BitIndex для n-граммы (для сложный операций)
    #[allow(dead_code)]
    pub fn get_ngram_bitindex(&self, ngram: &str) -> Option<&BitIndex> {
        self.ngrams.get(ngram)
    }

    /// Комплексный поиск с BitIndex операциями
    /// 
    /// # Example
    ///
    /// // Найти (ngram1 OR ngram2) AND ngram3 AND NOT ngram4
    /// let result = index.complex_search(
    ///     &["pay", "tra"],  // OR
    ///     &["ent"],         // AND
    ///     &["err"]          // NOT
    /// );
    /// 
    #[allow(dead_code)]
    pub fn complex_search(
        &self,
        or_ngrams: &[&str],
        and_ngrams: &[&str],
        not_ngrams: &[&str],
    ) -> Vec<usize> {
        // Шаг 1: OR операции
        let mut result = if !or_ngrams.is_empty() {
            // Первая n-грамма
            let first = match self.ngrams.get(or_ngrams[0]) {
                Some(bit) => bit,
                None => return Vec::new(),
            };
            if or_ngrams.len() == 1 {
                first.get_result()
            } else {
                // OR остальных
                let operations: Vec<(&BitIndex, BitOp)> = or_ngrams[1..]
                    .iter()
                    .filter_map(|&ng| self.ngrams.get(ng).map(|b| (b, BitOp::Or)))
                    .collect();
                first.multi_operation(&operations)
            }
        } else {
            // Все элементы
            let full = BitIndex::from_indices(
                &(0..self.total_items).collect::<Vec<_>>(),
                self.total_items
            );
            full.get_result()
        };
        // Шаг 2: AND операции
        for &ngram in and_ngrams {
            if let Some(bit) = self.ngrams.get(ngram) {
                let current_bitmap = result.bitmap().clone();
                let and_bitmap = bit.bitmap();
                let new_bitmap = &current_bitmap & and_bitmap;
                let count = new_bitmap.len() as usize;
                result = BitOpResult::from_owned(new_bitmap, count);
                if result.len() == 0 {
                    return Vec::new();
                }
            } else {
                return Vec::new();
            }
        }
        // Шаг 3: NOT операции
        for &ngram in not_ngrams {
            if let Some(bit) = self.ngrams.get(ngram) {
                let current_bitmap = result.bitmap().clone();
                let not_bitmap = bit.bitmap();
                let new_bitmap = &current_bitmap - not_bitmap;
                let count = new_bitmap.len() as usize;
                result = BitOpResult::from_owned(new_bitmap, count);
            }
        }
        result.to_indices()
    }

    // Статистика индекса
    pub fn stats(&self) -> TextIndexStats {
        let memory_bytes = self.estimate_memory();
        TextIndexStats {
            n: self.n,
            total_items: self.total_items,
            unique_ngrams: self.unique_ngrams,
            total_ngrams: self.total_ngrams,
            avg_ngrams_per_item: if self.total_items > 0 {
                self.total_ngrams as f64 / self.total_items as f64
            } else {
                0.0
            },
            memory_kb: memory_bytes / 1024,
        }
    }

    fn estimate_memory(&self) -> usize {
        // Memory от BitIndex
        let ngrams_memory: usize = self.ngrams
            .values()
            .map(|bit_index| bit_index.memory_size())
            .sum();
        let texts_memory: usize = self.item_texts
            .iter()
            .map(|text| text.len())
            .sum();
        ngrams_memory + texts_memory
    }

    ///Получить статистику по конкретной n-грамме
    pub fn ngram_stats(&self, ngram: &str) -> Option<String> {
        self.ngrams.get(ngram).map(|bit| bit.to_string())
    }

    // Список всех n-грамм
    pub fn list_ngrams(&self) -> Vec<String> {
        self.ngrams.keys().cloned().collect()
    }

    // Top-N самых частых n-грамм
    pub fn top_ngrams(&self, n: usize) -> Vec<(String, usize)> {
        let mut ngrams: Vec<(String, usize)> = self.ngrams
            .iter()
            .map(|(ngram, bit)| (ngram.clone(), bit.count_ones()))
            .collect();
        ngrams.sort_by(|a, b| b.1.cmp(&a.1));
        ngrams.truncate(n);
        ngrams
    }

    #[allow(dead_code)]
    pub fn get_text(&self, index: usize) -> Option<&str> {
        self.item_texts.get(index).map(|s| s.as_str())
    }
    
    #[allow(dead_code)]
    pub fn get_item_ngrams(&self, index: usize) -> Vec<String> {
        if let Some(text) = self.item_texts.get(index) {
            self.extract_ngrams(text)
        } else {
            Vec::new()
        }
    }
}

#[derive(Debug, Clone)]
pub struct TextIndexStats {
    pub n: usize,
    pub total_items: usize,
    pub unique_ngrams: usize,
    pub total_ngrams: usize,
    pub avg_ngrams_per_item: f64,
    pub memory_kb: usize,
}

impl Display for TextIndexStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "N-gram Index Stats (n={}):\n\
             Total items: {}\n\
             Unique n-grams: {}\n\
             Total n-grams: {}\n\
             Avg n-grams per item: {:.1}\n\
             Memory: {} KB",
            self.n,
            self.total_items,
            self.unique_ngrams,
            self.total_ngrams,
            self.avg_ngrams_per_item,
            self.memory_kb
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct TestItem {
        text: String,
    }

    #[test]
    fn test_trigram_extraction() {
        let index = TextIndex::<TestItem>::new(3);
        let ngrams = index.extract_ngrams("payment");
        assert_eq!(ngrams, vec!["pay", "aym", "yme", "men", "ent"]);
        let ngrams = index.extract_ngrams("hi");
        assert_eq!(ngrams, vec!["hi"]);
        let ngrams = index.extract_ngrams("");
        assert_eq!(ngrams.len(), 0);
    }

    #[test]
    fn test_basic_search_with_bitindex() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed".into() }),
            Arc::new(TestItem { text: "user_id: 12345".into() }),
            Arc::new(TestItem { text: "timeout error".into() }),
        ];
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        let results = index.search("payment");
        assert_eq!(results, vec![0]);
        let results = index.search("user_id");
        assert_eq!(results, vec![1]);
        let results = index.search("timeout");
        assert_eq!(results, vec![2]);
        let results = index.search("notfound");
        assert!(results.is_empty());
    }

    #[test]
    fn test_partial_match() {
        let items = vec![
            Arc::new(TestItem { text: "user_id: 12345".into() }),
            Arc::new(TestItem { text: "user_id: 99999".into() }),
        ];

        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        let results = index.search("user_id: 123");
        assert_eq!(results, vec![0]);
        let results = index.search("user_id: 999");
        assert_eq!(results, vec![1]);
        let results = index.search("user_id:");
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_case_insensitive() {
        let items = vec![
            Arc::new(TestItem { text: "Payment Failed".into() }),
        ];

        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        let results = index.search("payment");
        assert_eq!(results, vec![0]);
        let results = index.search("PAYMENT");
        assert_eq!(results, vec![0]);
        let results = index.search("PaYmEnT");
        assert_eq!(results, vec![0]);
    }

    #[test]
    fn test_complex_words_or_only() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed".into() }),       // 0
            Arc::new(TestItem { text: "transaction success".into() }),  // 1
            Arc::new(TestItem { text: "timeout error".into() }),        // 2
            Arc::new(TestItem { text: "user login".into() }),           // 3
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // OR: payment OR transaction
        let results = index.search_complex_words(
            &["payment", "transaction"],
            &[],
            &[]
        );
        
        println!("OR results: {:?}", results);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&0)); // payment
        assert!(results.contains(&1)); // transaction
    }

    #[test]
    fn test_complex_words_and_only() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed error".into() }),  // 0
            Arc::new(TestItem { text: "payment success".into() }),       // 1
            Arc::new(TestItem { text: "transaction failed".into() }),    // 2
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // AND: payment AND failed
        let results = index.search_complex_words(
            &[],
            &["payment", "failed"],
            &[]
        );
        
        println!("AND results: {:?}", results);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 0); // только "payment failed error"
    }

    #[test]
    fn test_complex_words_not_only() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed".into() }),   // 0
            Arc::new(TestItem { text: "payment success".into() }),  // 1
            Arc::new(TestItem { text: "transaction".into() }),      // 2
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // NOT: все кроме payment
        let results = index.search_complex_words(
            &[],
            &[],
            &["payment"]
        );
        
        println!("NOT results: {:?}", results);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 2); // только "transaction"
    }

    #[test]
    fn test_complex_words_or_and() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed error".into() }),      // 0
            Arc::new(TestItem { text: "payment success".into() }),           // 1
            Arc::new(TestItem { text: "transaction failed".into() }),        // 2
            Arc::new(TestItem { text: "transaction success".into() }),       // 3
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // (payment OR transaction) AND failed
        let results = index.search_complex_words(
            &["payment", "transaction"],
            &["failed"],
            &[]
        );
        
        println!("OR+AND results: {:?}", results);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&0)); // payment failed error
        assert!(results.contains(&2)); // transaction failed
    }

    #[test]
    fn test_complex_words_or_not() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed".into() }),    // 0
            Arc::new(TestItem { text: "payment success".into() }),   // 1
            Arc::new(TestItem { text: "transaction failed".into() }), // 2
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // (payment OR transaction) AND NOT success
        let results = index.search_complex_words(
            &["payment", "transaction"],
            &[],
            &["success"]
        );
        
        println!("OR+NOT results: {:?}", results);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&0)); // payment failed
        assert!(results.contains(&2)); // transaction failed
    }

    #[test]
    fn test_complex_words_and_not() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed error".into() }),  // 0
            Arc::new(TestItem { text: "payment failed".into() }),        // 1
            Arc::new(TestItem { text: "payment success".into() }),       // 2
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // payment AND failed AND NOT error
        let results = index.search_complex_words(
            &[],
            &["payment", "failed"],
            &["error"]
        );
        
        println!("AND+NOT results: {:?}", results);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 1); // только "payment failed" (без error)
    }

    #[test]
    fn test_complex_words_all_three() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed error".into() }),      // 0
            Arc::new(TestItem { text: "payment failed warning".into() }),    // 1
            Arc::new(TestItem { text: "transaction failed error".into() }),  // 2
            Arc::new(TestItem { text: "transaction success".into() }),       // 3
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // (payment OR transaction) AND failed AND NOT warning
        let results = index.search_complex_words(
            &["payment", "transaction"],
            &["failed"],
            &["warning"]
        );
        
        println!("OR+AND+NOT results: {:?}", results);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&0)); // payment failed error
        assert!(results.contains(&2)); // transaction failed error
    }

    #[test]
    fn test_complex_words_empty_or() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed".into() }),
            Arc::new(TestItem { text: "transaction success".into() }),
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // Пустой OR - должны вернуться все элементы (если нет AND/NOT)
        let results = index.search_complex_words(
            &[],
            &[],
            &[]
        );
        
        println!("Empty OR results: {:?}", results);
        assert_eq!(results.len(), 2); // все элементы
    }

    #[test]
    fn test_complex_words_nonexistent_or() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed".into() }),
            Arc::new(TestItem { text: "transaction success".into() }),
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // OR с несуществующим словом
        let results = index.search_complex_words(
            &["nonexistent"],
            &[],
            &[]
        );
        
        println!("Nonexistent OR results: {:?}", results);
        assert_eq!(results.len(), 0); // пусто
    }

    #[test]
    fn test_complex_words_nonexistent_and() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed".into() }),
            Arc::new(TestItem { text: "transaction success".into() }),
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // AND с несуществующим словом
        let results = index.search_complex_words(
            &["payment"],
            &["nonexistent"],
            &[]
        );
        
        println!("Nonexistent AND results: {:?}", results);
        assert_eq!(results.len(), 0); // пусто, т.к. AND не выполнен
    }

    #[test]
    fn test_complex_words_partial_or() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed".into() }),
            Arc::new(TestItem { text: "transaction success".into() }),
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // OR где одно слово не найдено
        let results = index.search_complex_words(
            &["payment", "nonexistent"],
            &[],
            &[]
        );
        
        println!("Partial OR results: {:?}", results);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 0); // только payment
    }

    #[test]
    fn test_complex_words_not_nonexistent() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed".into() }),
            Arc::new(TestItem { text: "transaction success".into() }),
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // NOT с несуществующим словом (не должно ничего исключить)
        let results = index.search_complex_words(
            &["payment"],
            &[],
            &["nonexistent"]
        );
        
        println!("NOT nonexistent results: {:?}", results);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 0); // payment остался
    }

    #[test]
    fn test_complex_words_case_insensitive() {
        let items = vec![
            Arc::new(TestItem { text: "Payment Failed".into() }),
            Arc::new(TestItem { text: "TRANSACTION SUCCESS".into() }),
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // Lowercase поиск по uppercase тексту
        let results = index.search_complex_words(
            &["payment", "transaction"],
            &[],
            &[]
        );
        
        println!("Case insensitive results: {:?}", results);
        assert_eq!(results.len(), 2); // оба найдены
    }

    #[test]
    fn test_complex_words_duplicate_or() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed".into() }),
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // OR с дубликатами
        let results = index.search_complex_words(
            &["payment", "payment", "payment"],
            &[],
            &[]
        );
        
        println!("Duplicate OR results: {:?}", results);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 0);
    }

    #[test]
    fn test_complex_words_all_excluded() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed".into() }),
            Arc::new(TestItem { text: "payment success".into() }),
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // Исключаем все результаты
        let results = index.search_complex_words(
            &["payment"],
            &[],
            &["payment"]
        );
        
        println!("All excluded results: {:?}", results);
        assert_eq!(results.len(), 0); // все исключены
    }

    #[test]
    fn test_complex_words_multiple_and() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed error timeout".into() }), // 0
            Arc::new(TestItem { text: "payment failed error".into() }),         // 1
            Arc::new(TestItem { text: "payment failed".into() }),               // 2
            Arc::new(TestItem { text: "payment".into() }),                      // 3
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // Множественный AND
        let results = index.search_complex_words(
            &[],
            &["payment", "failed", "error", "timeout"],
            &[]
        );
        
        println!("Multiple AND results: {:?}", results);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 0); // только первый содержит все слова
    }

    #[test]
    fn test_complex_words_multiple_not() {
        let items = vec![
            Arc::new(TestItem { text: "payment".into() }),              // 0
            Arc::new(TestItem { text: "payment failed".into() }),       // 1
            Arc::new(TestItem { text: "payment error".into() }),        // 2
            Arc::new(TestItem { text: "payment failed error".into() }), // 3
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // Множественный NOT
        let results = index.search_complex_words(
            &["payment"],
            &[],
            &["failed", "error"]
        );
        
        println!("Multiple NOT results: {:?}", results);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 0); // только "payment" без failed/error
    }

    #[test]
    fn test_complex_words_real_world_example() {
        let items = vec![
            Arc::new(TestItem { 
                text: "2024-01-01 ERROR payment-service: Payment request failed with timeout".into() 
            }), // 0
            Arc::new(TestItem { 
                text: "2024-01-01 INFO payment-service: Payment completed successfully".into() 
            }), // 1
            Arc::new(TestItem { 
                text: "2024-01-01 ERROR transaction-service: Transaction failed due to insufficient funds".into() 
            }), // 2
            Arc::new(TestItem { 
                text: "2024-01-01 WARN payment-service: Payment retry initiated".into() 
            }), // 3
            Arc::new(TestItem { 
                text: "2024-01-01 ERROR auth-service: Authentication timeout".into() 
            }), // 4
        ];
        
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        
        // Real-world query: (payment OR transaction) AND error AND NOT timeout
        let results = index.search_complex_words(
            &["payment", "transaction"],
            &["error"],
            &["timeout"]
        );
        
        println!("Real-world results: {:?}", results);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 2); // только "transaction failed" с error без timeout
    }

    #[test]
    fn test_get_ngram_bitindex() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed".into() }),
            Arc::new(TestItem { text: "payment success".into() }),
        ];

        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        // Получаем BitIndex для "pay"
        let pay_bit = index.get_ngram_bitindex("pay").unwrap();
        assert_eq!(pay_bit.count_ones(), 2); // оба содержат "pay"
        // Получаем BitIndex для "fai"
        let fai_bit = index.get_ngram_bitindex("fai").unwrap();
        assert_eq!(fai_bit.count_ones(), 1); // только первый
        // Можем использовать BitIndex операции!
        let result = pay_bit.and(fai_bit);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_complex_search() {
        let items = vec![
            Arc::new(TestItem { text: "payment failed error".into() }),
            Arc::new(TestItem { text: "payment success".into() }),
            Arc::new(TestItem { text: "transaction failed".into() }),
        ];
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        // (payment OR transaction) AND failed
        let results = index.complex_search(
            &["pay", "tra"],  // OR
            &["fai"],         // AND
            &[]               // NOT
        );
        assert_eq!(results.len(), 2); // items 0 и 2
        // payment AND NOT success
        let results = index.complex_search(
            &["pay"],         // OR (только один)
            &[],              // AND
            &["suc"]          // NOT
        );
        assert_eq!(results.len(), 1); // только item 0
    }

    #[test]
    fn test_stats() {
        let items = vec![
            Arc::new(TestItem { text: "payment".into() }),
            Arc::new(TestItem { text: "timeout".into() }),
        ];
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        let stats = index.stats();
        println!("{}", stats);
        assert_eq!(stats.n, 3);
        assert_eq!(stats.total_items, 2);
        assert!(stats.unique_ngrams > 0);
    }

    #[test]
    fn test_top_ngrams() {
        let items = vec![
            Arc::new(TestItem { text: "payment".into() }),
            Arc::new(TestItem { text: "payday".into() }),
            Arc::new(TestItem { text: "timeout".into() }),
        ];
        let mut index = TextIndex::new(3);
        index.build(&items, |item| item.text.clone());
        let top = index.top_ngrams(10);
        println!("Top n-grams: {:?}", top);
        // "pay" встречается в 2 документах (payment, payday)
        let pay_count = top.iter()
            .find(|(ng, _)| ng == "pay")
            .map(|(_, count)| *count)
            .unwrap_or(0);
        assert!(pay_count >= 2, "Expected 'pay' in at least 2 documents, got {}", pay_count);
    }
}