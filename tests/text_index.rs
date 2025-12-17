#[cfg(test)]
mod text_tests {
    use tree_man::{
        FieldOperation,
        Op,
        filter::FilterData
    };

    #[derive(Clone)]
    struct LogEntry {
        message: String,
        level: String,
    }

    #[derive(Clone, Debug)]
    struct LogEntryAdvanced {
        level: String,
        service: String,
        message: String,
    }

    #[test]
    fn test_text_index_creation() {
        let logs = vec![
            LogEntry {
                message: "Payment request failed".into(),
                level: "ERROR".into(),
            },
            LogEntry {
                message: "user_id: 12345 timeout".into(),
                level: "WARNING".into(),
            },
            LogEntry {
                message: "Payment successful".into(),
                level: "INFO".into(),
            },
        ];

        let data = FilterData::from_vec(logs);
        data.create_text_index("search", |log| log.message.clone()).unwrap();
        // Проверяем что индекс создан
        assert!(data.has_index("search"));
        // Проверяем статистику
        let stats = data.text_index_stats("search").unwrap();
        assert_eq!(stats.n, 3);
        assert_eq!(stats.total_items, 3);
        assert!(stats.unique_ngrams > 0);
        println!("{}", stats);
    }

    #[test]
    fn test_text_search() {
        let logs = vec![
            LogEntry {
                message: "Payment request for user_id: 12345 failed".into(),
                level: "ERROR".into(),
            },
            LogEntry {
                message: "Payment request for user_id: 99999 success".into(),
                level: "INFO".into(),
            },
            LogEntry {
                message: "Timeout error occurred".into(),
                level: "ERROR".into(),
            },
        ];

        let data = FilterData::from_vec(logs);
        data.create_text_index("search", |log| log.message.clone()).unwrap();
        // Test 1: Search for "user_id: 12345"
        data.search_with_text("search", "user_id: 12345").unwrap();
        let results = data.items();
        assert_eq!(results.len(), 1);
        assert!(results[0].message.contains("12345"));
        // Reset
        data.reset_to_source();
        // Test 2: Search for "Payment"
        data.search_with_text("search", "Payment").unwrap();
        let results = data.items();
        assert_eq!(results.len(), 2);
        // Reset
        data.reset_to_source();
        // Test 3: Search for "Timeout"
        data.search_with_text("search", "Timeout").unwrap();
        let results = data.items();
        assert_eq!(results.len(), 1);
        // Reset
        data.reset_to_source();
        // Test 4: Not found
        let result = data.search_with_text("search", "notfound");
        assert!(result.is_err()); // Должно вернуть ошибку DataNotFoundByIndex
    }

    #[test]
    fn test_text_drill_down() {
        let logs = vec![
            LogEntry {
                message: "Payment failed for user_id: 12345".into(),
                level: "ERROR".into(),
            },
            LogEntry {
                message: "Payment success for user_id: 99999".into(),
                level: "INFO".into(),
            },
            LogEntry {
                message: "Timeout for user_id: 12345".into(),
                level: "ERROR".into(),
            },
        ];

        let data = FilterData::from_vec(logs);
        data.create_text_index("search", |log| log.message.clone()).unwrap();
        data.create_field_index("level", |log| log.level.clone()).unwrap();
        // Drill-down: ERROR level + user_id: 12345
        data.filter_by_field_ops("level", &[(FieldOperation::eq("ERROR".to_string()),Op::And)]).unwrap();
        assert_eq!(data.len(), 2);
        data.search_with_text("search", "user_id: 12345").unwrap();
        assert_eq!(data.len(), 2);
        data.reset_to_source();
        assert_eq!(data.len(), 3);
    }

    #[test]
    fn test_apply_complex_words_basic() {
        let items = vec![
            LogEntryAdvanced {
                level: "ERROR".into(),
                service: "payment".into(),
                message: "payment failed error".into(),
            }, // 0
            LogEntryAdvanced {
                level: "INFO".into(),
                service: "payment".into(),
                message: "payment success".into(),
            }, // 1
            LogEntryAdvanced {
                level: "ERROR".into(),
                service: "transaction".into(),
                message: "transaction failed".into(),
            }, // 2
            LogEntryAdvanced {
                level: "INFO".into(),
                service: "auth".into(),
                message: "user login".into(),
            }, // 3
        ];

        let data = FilterData::from_vec(items);
        // Создаем текстовый индекс
        data.create_text_index("messages", |log| log.message.clone()).unwrap();
        // (payment OR transaction) AND failed
        data.search_complex_words_text(
            "messages",
            &["payment", "transaction"],
            &["failed"],
            &[]
        ).unwrap();
        let results = data.items();
        println!("Results: {}", results.len());
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].message, "payment failed error");
        assert_eq!(results[1].message, "transaction failed");
    }

    #[test]
    fn test_apply_complex_words_with_not() {
        let items = vec![
            LogEntryAdvanced {
                level: "ERROR".into(),
                service: "payment".into(),
                message: "payment failed error".into(),
            }, // 0
            LogEntryAdvanced {
                level: "ERROR".into(),
                service: "payment".into(),
                message: "payment failed".into(),
            }, // 1
            LogEntryAdvanced {
                level: "INFO".into(),
                service: "payment".into(),
                message: "payment success".into(),
            }, // 2
        ];
        
        let data = FilterData::from_vec(items);
        data.create_text_index("messages", |log| log.message.clone()).unwrap();
        
        // payment AND failed AND NOT error
        data.search_complex_words_text(
            "messages",
            &[],
            &["payment", "failed"],
            &["error"]
        ).unwrap();
        
        let results = data.items();
        println!("Results: {}", results.len());
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].message, "payment failed");
    }

    #[test]
    fn test_apply_complex_words_drill_down() {
        let items = vec![
            LogEntryAdvanced {
                level: "ERROR".into(),
                service: "payment".into(),
                message: "payment failed error".into(),
            }, // 0
            LogEntryAdvanced {
                level: "ERROR".into(),
                service: "transaction".into(),
                message: "transaction failed".into(),
            }, // 1
            LogEntryAdvanced {
                level: "INFO".into(),
                service: "payment".into(),
                message: "payment success".into(),
            }, // 2
            LogEntryAdvanced {
                level: "WARN".into(),
                service: "payment".into(),
                message: "payment failed warning".into(),
            }, // 3
        ];
        
        let data = FilterData::from_vec(items);
        // Создаем индексы
        data.create_field_index("level", |log| log.level.clone()).unwrap();
        data.create_text_index("messages", |log| log.message.clone()).unwrap();
        println!("\n=== Step 1: Filter by ERROR ===");
        data.filter_by_field_ops("level", &[(FieldOperation::eq("ERROR".to_string()),Op::And)]).unwrap();
        assert_eq!(data.len(), 2); // 0, 1
        println!("\n=== Step 2: Complex text search ===");
        // payment AND failed
        data.search_complex_words_text(
            "messages",
            &["payment"],
            &["failed"],
            &[]
        ).unwrap();
        let results = data.items();
        println!("Final results: {}", results.len());
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].message, "payment failed error");
        // Сброс
        data.reset_to_source();
        assert_eq!(data.len(), 4);
    }

    #[test]
    fn test_apply_complex_words_empty_result() {
        let items = vec![
            LogEntryAdvanced {
                level: "INFO".into(),
                service: "payment".into(),
                message: "payment success".into(),
            },
        ];
        let data = FilterData::from_vec(items);
        data.create_text_index("messages", |log| log.message.clone()).unwrap();
        // Поиск несуществующих слов должен вернуть ошибку
        let result = data.search_complex_words_text(
            "messages",
            &["nonexistent"],
            &[],
            &[]
        );
        assert!(result.is_err()); // Должна быть ошибка DataNotFoundByIndex
    }

    #[test]
    fn test_apply_complex_words_multiple_drill_down() {
        let items = vec![
            LogEntryAdvanced {
                level: "ERROR".into(),
                service: "payment".into(),
                message: "payment failed timeout error".into(),
            }, // 0
            LogEntryAdvanced {
                level: "ERROR".into(),
                service: "payment".into(),
                message: "payment failed error".into(),
            }, // 1
            LogEntryAdvanced {
                level: "ERROR".into(),
                service: "transaction".into(),
                message: "transaction failed timeout".into(),
            }, // 2
            LogEntryAdvanced {
                level: "WARN".into(),
                service: "payment".into(),
                message: "payment failed".into(),
            }, // 3
        ];
        
        let data = FilterData::from_vec(items);
        // Создаем индексы
        data.create_field_index("level", |log| log.level.clone()).unwrap();
        data.create_field_index("service", |log| log.service.clone()).unwrap();
        data.create_text_index("messages", |log| log.message.clone()).unwrap();
        println!("\n=== Initial count: {} ===", data.len());
        // Шаг 1: Только ERROR
        data.filter_by_field_ops("level", &[(FieldOperation::eq("ERROR".to_string()),Op::And)]).unwrap();
        println!("After ERROR filter: {}", data.len());
        assert_eq!(data.len(), 3); // 0, 1, 2
        // Шаг 2: Только payment service
        data.filter_by_field_ops("service", &[(FieldOperation::eq("payment".to_string()),Op::And)]).unwrap();
        println!("After payment filter: {}", data.len());
        assert_eq!(data.len(), 2); // 0, 1
        // Шаг 3: Комплексный текстовый поиск
        // failed AND NOT timeout
        data.search_complex_words_text(
            "messages",
            &[],
            &["failed"],
            &["timeout"]
        ).unwrap();
        let results = data.items();
        println!("Final results: {}", results.len());
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].message, "payment failed error");
    }

    #[test]
    fn test_format_complex_query_desc() {
        // Test OR only
        let desc = FilterData::<LogEntryAdvanced>::format_complex_query_desc(
            &["payment", "transaction"],
            &[],
            &[]
        );
        assert_eq!(desc, "(payment OR transaction)");
        // Test AND only
        let desc = FilterData::<LogEntryAdvanced>::format_complex_query_desc(
            &[],
            &["failed", "error"],
            &[]
        );
        assert_eq!(desc, "failed AND error");
        // Test NOT only
        let desc = FilterData::<LogEntryAdvanced>::format_complex_query_desc(
            &[],
            &[],
            &["timeout", "warning"]
        );
        assert_eq!(desc, "NOT timeout NOT warning");
        // Test OR + AND
        let desc = FilterData::<LogEntryAdvanced>::format_complex_query_desc(
            &["payment", "transaction"],
            &["failed"],
            &[]
        );
        assert_eq!(desc, "(payment OR transaction) AND failed");
        // Test OR + AND + NOT
        let desc = FilterData::<LogEntryAdvanced>::format_complex_query_desc(
            &["payment", "transaction"],
            &["failed"],
            &["timeout"]
        );
        assert_eq!(desc, "(payment OR transaction) AND failed AND NOT timeout");
        // Test single word OR (no parentheses)
        let desc = FilterData::<LogEntryAdvanced>::format_complex_query_desc(
            &["payment"],
            &["failed"],
            &[]
        );
        assert_eq!(desc, "payment AND failed");
        // Test empty (all)
        let desc = FilterData::<LogEntryAdvanced>::format_complex_query_desc(
            &[],
            &[],
            &[]
        );
        assert_eq!(desc, "all");
    }

    #[test]
    fn test_search_complex_words_consistency() {
        let items = vec![
            LogEntryAdvanced {
                level: "ERROR".into(),
                service: "payment".into(),
                message: "payment failed".into(),
            }, // 0
            LogEntryAdvanced {
                level: "INFO".into(),
                service: "payment".into(),
                message: "payment success".into(),
            }, // 1
        ];
        
        let data = FilterData::from_vec(items);
        data.create_text_index("messages", |log| log.message.clone()).unwrap();
        // search_complex_words_text выполняет поиск и меняет состояние
        data.search_complex_words_text(
            "messages",
            &["payment"],
            &["failed"],
            &[]
        ).unwrap();
        assert_eq!(data.len(), 1);
        let results = data.items();
        assert_eq!(results[0].message, "payment failed");
        // Сброс для повторного теста
        data.reset_to_source();
        assert_eq!(data.len(), 2);
        // Повторный поиск должен дать тот же результат
        data.search_complex_words_text(
            "messages",
            &["payment"],
            &["failed"],
            &[]
        ).unwrap();
        assert_eq!(data.len(), 1);
        let results2 = data.items();
        assert_eq!(results2[0].message, "payment failed");
    }

    #[test]
    fn test_text_index_ngram_stats() {
        let logs = vec![
            LogEntry {
                message: "payment payment payment".into(),
                level: "ERROR".into(),
            },
            LogEntry {
                message: "timeout".into(),
                level: "WARNING".into(),
            },
        ];
        let data = FilterData::from_vec(logs);
        data.create_text_index("search", |log| log.message.clone()).unwrap();
        // Получаем топ n-грамм
        let top_ngrams = data.top_text("search", 5).unwrap();
        assert!(!top_ngrams.is_empty());
        println!("Top ngrams: {:?}", top_ngrams);
        // Получаем список всех n-грамм
        let all_ngrams = data.list_text_ngrams("search").unwrap();
        assert!(all_ngrams.len() > 0);
        println!("Total ngrams: {}", all_ngrams.len());
        // Получаем статистику по конкретной n-грамме
        if let Some(first_ngram) = all_ngrams.first() {
            let ngram_stats = data.text_stats("search", first_ngram).unwrap();
            println!("Stats for '{}': {:?}", first_ngram, ngram_stats);
            assert!(ngram_stats.is_some());
        }
    }

    #[test]
    fn test_text_search_case_sensitivity() {
        let logs = vec![
            LogEntry {
                message: "Payment Failed".into(),
                level: "ERROR".into(),
            },
            LogEntry {
                message: "payment failed".into(),
                level: "ERROR".into(),
            },
        ];
        let data = FilterData::from_vec(logs);
        data.create_text_index("search", |log| log.message.clone()).unwrap();
        // Поиск должен быть case-insensitive (если реализован lowercase в TextIndex)
        data.search_with_text("search", "payment").unwrap();
        let results = data.items();
        // Оба сообщения должны найтись (если lowercase работает)
        println!("Found {} results", results.len());
        // assert_eq!(results.len(), 2); // Раскомментировать если lowercase работает
    }
}