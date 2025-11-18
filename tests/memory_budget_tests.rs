//! Memory budget enforcement tests

use emsqrt_core::budget::{BudgetGuard, MemoryBudget};
use emsqrt_mem::MemoryBudgetImpl;
use std::sync::Arc;
use std::thread;

#[test]
fn test_budget_acquire_release() {
    let budget = MemoryBudgetImpl::new(1024 * 1024); // 1MB

    // Initially no memory used
    assert_eq!(budget.used_bytes(), 0);

    // Acquire 100KB
    let guard = budget
        .try_acquire(100 * 1024, "test")
        .expect("Acquire failed");
    assert_eq!(budget.used_bytes(), 100 * 1024);
    assert_eq!(guard.bytes(), 100 * 1024);

    // Release explicitly
    drop(guard);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn test_budget_exhaustion() {
    let budget = MemoryBudgetImpl::new(500 * 1024); // 500KB

    // Acquire 400KB
    let guard1 = budget
        .try_acquire(400 * 1024, "test")
        .expect("First acquire failed");
    assert_eq!(budget.used_bytes(), 400 * 1024);

    // Try to acquire another 200KB - should fail (total would be 600KB > 500KB)
    let result = budget.try_acquire(200 * 1024, "test");
    assert!(result.is_none(), "Should fail to acquire beyond capacity");

    // Should still have 400KB in use
    assert_eq!(budget.used_bytes(), 400 * 1024);

    // Release the first guard
    drop(guard1);
    assert_eq!(budget.used_bytes(), 0);

    // Now should be able to acquire 200KB
    let guard2 = budget
        .try_acquire(200 * 1024, "test")
        .expect("Acquire after release failed");
    assert_eq!(budget.used_bytes(), 200 * 1024);

    drop(guard2);
}

#[test]
fn test_budget_guard_drop() {
    let budget = MemoryBudgetImpl::new(1024 * 1024);

    {
        let _guard1 = budget
            .try_acquire(100 * 1024, "test")
            .expect("Acquire failed");
        assert_eq!(budget.used_bytes(), 100 * 1024);

        {
            let _guard2 = budget
                .try_acquire(200 * 1024, "test")
                .expect("Acquire failed");
            assert_eq!(budget.used_bytes(), 300 * 1024);

            // guard2 drops here
        }

        // Should have released guard2's memory
        assert_eq!(budget.used_bytes(), 100 * 1024);

        // guard1 drops here
    }

    // All memory should be released
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn test_budget_concurrent_access() {
    let budget = Arc::new(MemoryBudgetImpl::new(1024 * 1024)); // 1MB shared
    let mut handles = vec![];

    // Spawn 10 threads, each acquiring and releasing 50KB
    for i in 0..10 {
        let budget_clone: Arc<MemoryBudgetImpl> = Arc::clone(&budget);
        let handle = thread::spawn(move || {
            // Try to acquire 50KB
            if let Some(guard) = budget_clone.try_acquire(50 * 1024, "test") {
                // Hold it briefly
                thread::sleep(std::time::Duration::from_millis(10));
                assert_eq!(guard.bytes(), 50 * 1024);
                // Guard drops here
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // All memory should be released
    assert_eq!(budget.used_bytes(), 0);

    // Verify we never exceeded capacity by trying a full allocation
    let full_guard = budget
        .try_acquire(1024 * 1024, "test")
        .expect("Should be able to acquire full budget");
    assert_eq!(budget.used_bytes(), 1024 * 1024);
    drop(full_guard);
}

#[test]
fn test_budget_try_resize() {
    let budget = MemoryBudgetImpl::new(1024 * 1024); // 1MB

    // Acquire 100KB
    let mut guard = budget
        .try_acquire(100 * 1024, "test")
        .expect("Initial acquire failed");
    assert_eq!(budget.used_bytes(), 100 * 1024);
    assert_eq!(guard.bytes(), 100 * 1024);

    // Resize to 200KB
    let resized = guard.try_resize(200 * 1024);
    assert!(resized, "Resize should succeed");
    assert_eq!(budget.used_bytes(), 200 * 1024);
    assert_eq!(guard.bytes(), 200 * 1024);

    // Try to resize to 2MB (exceeds capacity)
    let resized_fail = guard.try_resize(2 * 1024 * 1024);
    assert!(!resized_fail, "Resize should fail when exceeding capacity");
    // Should still have 200KB
    assert_eq!(guard.bytes(), 200 * 1024);
    assert_eq!(budget.used_bytes(), 200 * 1024);

    // Resize down to 50KB
    let resized_down = guard.try_resize(50 * 1024);
    assert!(resized_down, "Resize down should succeed");
    assert_eq!(guard.bytes(), 50 * 1024);
    assert_eq!(budget.used_bytes(), 50 * 1024);

    drop(guard);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn test_budget_multiple_guards() {
    let budget = MemoryBudgetImpl::new(1024 * 1024);

    let guard1 = budget
        .try_acquire(100 * 1024, "test")
        .expect("Acquire 1 failed");
    let guard2 = budget
        .try_acquire(200 * 1024, "test")
        .expect("Acquire 2 failed");
    let guard3 = budget
        .try_acquire(300 * 1024, "test")
        .expect("Acquire 3 failed");

    assert_eq!(budget.used_bytes(), 600 * 1024);

    // Release in different order than acquired
    drop(guard2);
    assert_eq!(budget.used_bytes(), 400 * 1024);

    drop(guard1);
    assert_eq!(budget.used_bytes(), 300 * 1024);

    drop(guard3);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn test_budget_zero_size_allocation() {
    let budget = MemoryBudgetImpl::new(1024 * 1024);

    // Acquire 0 bytes (should succeed but be no-op)
    let guard = budget.try_acquire(0, "test");
    assert!(guard.is_some(), "Zero-size allocation should succeed");

    let guard = guard.unwrap();
    assert_eq!(guard.bytes(), 0);
    assert_eq!(budget.used_bytes(), 0);

    drop(guard);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn test_budget_exact_capacity() {
    let budget = MemoryBudgetImpl::new(1024);

    // Acquire exactly the capacity
    let guard = budget
        .try_acquire(1024, "test")
        .expect("Should acquire exact capacity");
    assert_eq!(budget.used_bytes(), 1024);

    // No more room
    let result = budget.try_acquire(1, "test");
    assert!(
        result.is_none(),
        "Should not be able to acquire even 1 byte"
    );

    drop(guard);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn test_budget_fragmentation_handling() {
    let budget = MemoryBudgetImpl::new(1000);

    // Acquire and release in a pattern that could cause fragmentation
    let g1 = budget.try_acquire(300, "test").expect("Acquire 1");
    let g2 = budget.try_acquire(300, "test").expect("Acquire 2");
    let g3 = budget.try_acquire(300, "test").expect("Acquire 3");

    assert_eq!(budget.used_bytes(), 900);

    // Release middle guard
    drop(g2);
    assert_eq!(budget.used_bytes(), 600);

    // Should still be able to allocate in the "freed" space
    let g4 = budget
        .try_acquire(300, "test")
        .expect("Acquire 4 in freed space");
    assert_eq!(budget.used_bytes(), 900);

    drop(g1);
    drop(g3);
    drop(g4);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn test_budget_high_contention() {
    let budget = Arc::new(MemoryBudgetImpl::new(100 * 1024)); // 100KB total
    let num_threads = 20;
    let mut handles = vec![];

    for _ in 0..num_threads {
        let budget_clone: Arc<MemoryBudgetImpl> = Arc::clone(&budget);
        let handle = thread::spawn(move || {
            for _ in 0..10 {
                // Try to acquire 10KB repeatedly
                if let Some(guard) = budget_clone.try_acquire(10 * 1024, "test") {
                    // Simulate some work
                    thread::sleep(std::time::Duration::from_micros(100));
                    drop(guard);
                } else {
                    // Failed to acquire, retry
                    thread::sleep(std::time::Duration::from_micros(50));
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // All memory should be released
    assert_eq!(budget.used_bytes(), 0);
}
