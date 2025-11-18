use emsqrt_core::types::{Column, RowBatch, Scalar};
use emsqrt_mem::guard::MemoryBudgetImpl;
use emsqrt_operators::{
    window::{WindowFnKind, WindowFnSpec, WindowOp},
    Operator,
};

fn mk_column(name: &str, values: Vec<Scalar>) -> Column {
    Column {
        name: name.to_string(),
        values,
    }
}

#[test]
fn test_window_row_number_and_sum() {
    let row_batch = RowBatch {
        columns: vec![
            mk_column(
                "group",
                vec![
                    Scalar::Str("a".into()),
                    Scalar::Str("a".into()),
                    Scalar::Str("b".into()),
                    Scalar::Str("b".into()),
                ],
            ),
            mk_column(
                "order",
                vec![
                    Scalar::I64(1),
                    Scalar::I64(2),
                    Scalar::I64(1),
                    Scalar::I64(2),
                ],
            ),
            mk_column(
                "value",
                vec![
                    Scalar::F64(10.0),
                    Scalar::F64(20.0),
                    Scalar::F64(5.0),
                    Scalar::F64(15.0),
                ],
            ),
        ],
    };

    let window = WindowOp {
        partitions: vec!["group".into()],
        order_by: vec!["order".into()],
        functions: vec![
            WindowFnSpec {
                kind: WindowFnKind::RowNumber,
                alias: "rn".into(),
            },
            WindowFnSpec {
                kind: WindowFnKind::Sum {
                    column: "value".into(),
                },
                alias: "sum_value".into(),
            },
        ],
    };

    let result = window
        .eval_block(&[row_batch.clone()], &MemoryBudgetImpl::new(1024))
        .expect("window execution");

    assert_eq!(result.columns.len(), 5);
    let rn_vals = &result.columns[3].values;
    let sum_vals = &result.columns[4].values;
    assert_eq!(
        rn_vals,
        &vec![
            Scalar::I64(1),
            Scalar::I64(2),
            Scalar::I64(1),
            Scalar::I64(2)
        ]
    );
    assert_eq!(
        sum_vals,
        &vec![
            Scalar::F64(10.0),
            Scalar::F64(30.0),
            Scalar::F64(5.0),
            Scalar::F64(20.0)
        ]
    );
}
