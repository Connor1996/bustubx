test_p1_task1:
	cargo test test_lru_k_replacer -- --nocapture

test_p1_task2:
	cargo test test_buffer_pool_manager -- --nocapture

test_p1_task3:
	cargo test test_page_guard -- --nocapture