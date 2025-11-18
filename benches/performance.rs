use criterion::{criterion_group, criterion_main, Criterion};
use emsqrt_core::types::{Column, RowBatch, Scalar};
use emsqrt_mem::guard::MemoryBudgetImpl;
use emsqrt_operators::{
    window::{WindowFnKind, WindowFnSpec, WindowOp},
    Operator,
};

fn make_batch(rows: usize) -> RowBatch {
    let mut groups = Vec::with_capacity(rows);
    let mut orders = Vec::with_capacity(rows);
    let mut values = Vec::with_capacity(rows);
    for i in 0..rows {
        groups.push(Scalar::Str(format!("group-{}", i % 4)));
        orders.push(Scalar::I64(i as i64));
        values.push(Scalar::F64((i % 10) as f64));
    }
    RowBatch {
        columns: vec![
            Column {
                name: "group".into(),
                values: groups,
            },
            Column {
                name: "order".into(),
                values: orders,
            },
            Column {
                name: "value".into(),
                values,
            },
        ],
    }
}

fn bench_window_operator(c: &mut Criterion) {
    let batch = make_batch(1024);
    let window = WindowOp {
        partitions: vec!["group".into()],
        order_by: vec!["order".into()],
        functions: vec![
            WindowFnSpec {
                alias: "row_num".into(),
                kind: WindowFnKind::RowNumber,
            },
            WindowFnSpec {
                alias: "sum_value".into(),
                kind: WindowFnKind::Sum {
                    column: "value".into(),
                },
            },
        ],
    };
    let budget = MemoryBudgetImpl::new(4 * 1024 * 1024);
    c.bench_function("window_op", |b| {
        b.iter(|| {
            let _ = window.eval_block(&[batch.clone()], &budget).unwrap();
        })
    });
}

criterion_group!(windows, bench_window_operator);
criterion_main!(windows);
