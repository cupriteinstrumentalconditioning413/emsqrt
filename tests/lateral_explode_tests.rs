use emsqrt_core::types::{Column, RowBatch, Scalar};
use emsqrt_mem::guard::MemoryBudgetImpl;
use emsqrt_operators::{window::LateralExplodeOp, Operator};

fn mk_column(name: &str, values: Vec<Scalar>) -> Column {
    Column {
        name: name.to_string(),
        values,
    }
}

#[test]
fn test_lateral_explode() {
    let batch = RowBatch {
        columns: vec![
            mk_column("id", vec![Scalar::I64(1), Scalar::I64(2)]),
            mk_column(
                "tags",
                vec![Scalar::Str("a,b,c".into()), Scalar::Str("x,y".into())],
            ),
        ],
    };

    let op = LateralExplodeOp {
        column: "tags".into(),
        alias: "tag".into(),
        delimiter: ",".into(),
    };

    let result = op
        .eval_block(&[batch], &MemoryBudgetImpl::new(1024))
        .expect("lateral explode");

    assert_eq!(result.num_rows(), 5);
    let tag_col = result
        .columns
        .iter()
        .find(|c| c.name == "tag")
        .expect("alias column");
    assert_eq!(
        tag_col.values,
        vec![
            Scalar::Str("a".into()),
            Scalar::Str("b".into()),
            Scalar::Str("c".into()),
            Scalar::Str("x".into()),
            Scalar::Str("y".into()),
        ]
    );
}
