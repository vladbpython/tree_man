#[derive(Debug, Clone)]
pub struct MemoryStats {
    pub current_level: usize,
    pub stored_levels: usize,
    pub current_level_items: usize,
    pub total_stored_items: usize,
    pub useful_items: usize,  // ← Новое: уровни 0..=current
    pub wasted_items: usize,  // ← Уровни > current
}

impl MemoryStats {
    // Проверка на отсутствие мусора
    pub fn is_clean(&self) -> bool {
        self.wasted_items == 0
    }

    // Эффективность = полезные данные / все данные
    // Полезные данные = уровни 0..=current (нужны для навигации)
    // Мусор = уровни > current (остались после возврата назад)
    pub fn efficiency(&self) -> f64 {
        if self.total_stored_items == 0 {
            return 1.0;
        }
        self.useful_items as f64 / self.total_stored_items as f64
    }

    // Процент использования текущего уровня
    pub fn current_level_ratio(&self) -> f64 {
        if self.total_stored_items == 0 {
            return 1.0;
        }
        self.current_level_items as f64 / self.total_stored_items as f64
    }

    // Процент мусора
    pub fn waste_ratio(&self) -> f64 {
        if self.total_stored_items == 0 {
            return 0.0;
        }
        self.wasted_items as f64 / self.total_stored_items as f64
    }
}