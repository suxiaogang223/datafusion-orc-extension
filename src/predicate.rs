// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Predicate conversion from DataFusion PhysicalExpr to orc-rust Predicate
//!
//! This module provides utilities to convert DataFusion's physical expressions
//! into orc-rust predicates that can be used for stripe-level and row-group
//! filtering during ORC file reads.

use arrow::datatypes::Schema;
use datafusion_common::ScalarValue;
use datafusion_physical_expr::expressions::{
    BinaryExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NotExpr,
};
use datafusion_physical_plan::PhysicalExpr;
use orc_rust::predicate::{ComparisonOp, Predicate, PredicateValue};
use std::sync::Arc;

/// Convert a DataFusion PhysicalExpr to an orc-rust Predicate
///
/// This function attempts to convert supported expression types to their
/// orc-rust equivalents. Unsupported expressions return None.
///
/// # Supported Expressions
///
/// - Binary comparisons: `=`, `!=`, `<`, `<=`, `>`, `>=`
/// - Null checks: `IS NULL`, `IS NOT NULL`
/// - Logical operators: `AND`, `OR`, `NOT`
///
/// # Arguments
///
/// * `expr` - The DataFusion physical expression to convert
/// * `schema` - The Arrow schema for resolving column references
///
/// # Returns
///
/// `Some(Predicate)` if the expression can be converted, `None` otherwise
pub fn convert_physical_expr_to_predicate(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Option<Predicate> {
    // Try to downcast to specific expression types
    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        return convert_binary_expr(binary, schema);
    }

    if let Some(is_null) = expr.as_any().downcast_ref::<IsNullExpr>() {
        return convert_is_null_expr(is_null, schema);
    }

    if let Some(is_not_null) = expr.as_any().downcast_ref::<IsNotNullExpr>() {
        return convert_is_not_null_expr(is_not_null, schema);
    }

    if let Some(not) = expr.as_any().downcast_ref::<NotExpr>() {
        return convert_not_expr(not, schema);
    }

    // Unsupported expression type
    None
}

/// Convert a BinaryExpr to a Predicate
fn convert_binary_expr(expr: &BinaryExpr, schema: &Schema) -> Option<Predicate> {
    use datafusion_expr::Operator;

    let op = expr.op();
    let left = expr.left();
    let right = expr.right();

    // Handle logical operators (AND, OR)
    match op {
        Operator::And => {
            let left_pred = convert_physical_expr_to_predicate(left, schema)?;
            let right_pred = convert_physical_expr_to_predicate(right, schema)?;
            return Some(Predicate::and(vec![left_pred, right_pred]));
        }
        Operator::Or => {
            let left_pred = convert_physical_expr_to_predicate(left, schema)?;
            let right_pred = convert_physical_expr_to_predicate(right, schema)?;
            return Some(Predicate::or(vec![left_pred, right_pred]));
        }
        _ => {}
    }

    // Handle comparison operators (col op value or value op col)
    let comparison_op = match op {
        Operator::Eq => Some(ComparisonOp::Equal),
        Operator::NotEq => Some(ComparisonOp::NotEqual),
        Operator::Lt => Some(ComparisonOp::LessThan),
        Operator::LtEq => Some(ComparisonOp::LessThanOrEqual),
        Operator::Gt => Some(ComparisonOp::GreaterThan),
        Operator::GtEq => Some(ComparisonOp::GreaterThanOrEqual),
        _ => None,
    }?;

    // Try: column op literal
    if let (Some(column), Some(literal)) = (
        left.as_any().downcast_ref::<Column>(),
        right.as_any().downcast_ref::<Literal>(),
    ) {
        let column_name = column.name();
        let value = convert_scalar_to_predicate_value(literal.value())?;
        return Some(Predicate::comparison(column_name, comparison_op, value));
    }

    // Try: literal op column (swap operands and flip operator)
    if let (Some(literal), Some(column)) = (
        left.as_any().downcast_ref::<Literal>(),
        right.as_any().downcast_ref::<Column>(),
    ) {
        let column_name = column.name();
        let value = convert_scalar_to_predicate_value(literal.value())?;
        // Flip the comparison operator for reversed operands
        let flipped_op = match comparison_op {
            ComparisonOp::LessThan => ComparisonOp::GreaterThan,
            ComparisonOp::LessThanOrEqual => ComparisonOp::GreaterThanOrEqual,
            ComparisonOp::GreaterThan => ComparisonOp::LessThan,
            ComparisonOp::GreaterThanOrEqual => ComparisonOp::LessThanOrEqual,
            other => other, // Equal and NotEqual are symmetric
        };
        return Some(Predicate::comparison(column_name, flipped_op, value));
    }

    None
}

/// Convert an IsNullExpr to a Predicate
fn convert_is_null_expr(expr: &IsNullExpr, _schema: &Schema) -> Option<Predicate> {
    let arg = expr.arg();
    if let Some(column) = arg.as_any().downcast_ref::<Column>() {
        return Some(Predicate::is_null(column.name()));
    }
    None
}

/// Convert an IsNotNullExpr to a Predicate
fn convert_is_not_null_expr(expr: &IsNotNullExpr, _schema: &Schema) -> Option<Predicate> {
    let arg = expr.arg();
    if let Some(column) = arg.as_any().downcast_ref::<Column>() {
        return Some(Predicate::is_not_null(column.name()));
    }
    None
}

/// Convert a NotExpr to a Predicate
fn convert_not_expr(expr: &NotExpr, schema: &Schema) -> Option<Predicate> {
    let arg = expr.arg();
    let inner = convert_physical_expr_to_predicate(arg, schema)?;
    Some(Predicate::not(inner))
}

/// Convert a DataFusion ScalarValue to an orc-rust PredicateValue
///
/// # Supported Types
///
/// - Boolean
/// - Int8, Int16, Int32, Int64
/// - UInt8, UInt16, UInt32, UInt64
/// - Float32, Float64
/// - Utf8, LargeUtf8
/// - Date32, Date64
/// - Timestamp (various units)
pub fn convert_scalar_to_predicate_value(value: &ScalarValue) -> Option<PredicateValue> {
    match value {
        // Boolean
        ScalarValue::Boolean(v) => Some(PredicateValue::Boolean(*v)),

        // Signed integers
        ScalarValue::Int8(v) => Some(PredicateValue::Int8(*v)),
        ScalarValue::Int16(v) => Some(PredicateValue::Int16(*v)),
        ScalarValue::Int32(v) => Some(PredicateValue::Int32(*v)),
        ScalarValue::Int64(v) => Some(PredicateValue::Int64(*v)),

        // Unsigned integers - convert to signed for orc-rust
        ScalarValue::UInt8(v) => v.map(|x| PredicateValue::Int16(Some(x as i16))),
        ScalarValue::UInt16(v) => v.map(|x| PredicateValue::Int32(Some(x as i32))),
        ScalarValue::UInt32(v) => v.map(|x| PredicateValue::Int64(Some(x as i64))),
        ScalarValue::UInt64(v) => {
            // UInt64 may overflow i64, handle carefully
            v.and_then(|x| i64::try_from(x).ok())
                .map(|x| PredicateValue::Int64(Some(x)))
        }

        // Floating point
        ScalarValue::Float32(v) => Some(PredicateValue::Float32(*v)),
        ScalarValue::Float64(v) => Some(PredicateValue::Float64(*v)),

        // Strings
        ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => Some(PredicateValue::Utf8(v.clone())),

        // Date types - convert to days since epoch
        ScalarValue::Date32(v) => Some(PredicateValue::Int32(*v)),
        ScalarValue::Date64(v) => {
            // Date64 is milliseconds since epoch, convert to days
            v.map(|ms| (ms / 86_400_000) as i32)
                .map(|days| PredicateValue::Int32(Some(days)))
                .unwrap_or(PredicateValue::Int32(None))
                .into()
        }

        // Timestamp types - convert to appropriate format
        ScalarValue::TimestampSecond(v, _) => Some(PredicateValue::Int64(*v)),
        ScalarValue::TimestampMillisecond(v, _) => Some(PredicateValue::Int64(*v)),
        ScalarValue::TimestampMicrosecond(v, _) => Some(PredicateValue::Int64(*v)),
        ScalarValue::TimestampNanosecond(v, _) => Some(PredicateValue::Int64(*v)),

        // Decimal types
        ScalarValue::Decimal128(v, _precision, scale) => {
            // For simple cases, try to convert to f64
            v.map(|d| {
                let scale_factor = 10_f64.powi(*scale as i32);
                PredicateValue::Float64(Some(d as f64 / scale_factor))
            })
            .or_else(|| {
                if v.is_none() {
                    Some(PredicateValue::Float64(None))
                } else {
                    None
                }
            })
        }

        // Null value
        ScalarValue::Null => Some(PredicateValue::Boolean(None)),

        // Other types not supported
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use datafusion_physical_expr::expressions::{binary, col, lit};

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
            Field::new("score", DataType::Float64, true),
        ])
    }

    #[test]
    fn test_convert_eq_predicate() {
        let schema = test_schema();
        let expr = binary(
            col("id", &schema).unwrap(),
            datafusion_expr::Operator::Eq,
            lit(42i64),
            &schema,
        )
        .unwrap();

        let predicate = convert_physical_expr_to_predicate(&expr, &schema);
        assert!(predicate.is_some());

        let pred = predicate.unwrap();
        assert_eq!(pred, Predicate::eq("id", PredicateValue::Int64(Some(42))));
    }

    #[test]
    fn test_convert_gt_predicate() {
        let schema = test_schema();
        let expr = binary(
            col("age", &schema).unwrap(),
            datafusion_expr::Operator::Gt,
            lit(18i32),
            &schema,
        )
        .unwrap();

        let predicate = convert_physical_expr_to_predicate(&expr, &schema);
        assert!(predicate.is_some());

        let pred = predicate.unwrap();
        assert_eq!(pred, Predicate::gt("age", PredicateValue::Int32(Some(18))));
    }

    #[test]
    fn test_convert_and_predicate() {
        let schema = test_schema();
        let left = binary(
            col("age", &schema).unwrap(),
            datafusion_expr::Operator::GtEq,
            lit(18i32),
            &schema,
        )
        .unwrap();

        let right = binary(
            col("age", &schema).unwrap(),
            datafusion_expr::Operator::Lt,
            lit(65i32),
            &schema,
        )
        .unwrap();

        let and_expr = binary(left, datafusion_expr::Operator::And, right, &schema).unwrap();

        let predicate = convert_physical_expr_to_predicate(&and_expr, &schema);
        assert!(predicate.is_some());

        let pred = predicate.unwrap();
        assert_eq!(
            pred,
            Predicate::and(vec![
                Predicate::gte("age", PredicateValue::Int32(Some(18))),
                Predicate::lt("age", PredicateValue::Int32(Some(65))),
            ])
        );
    }

    #[test]
    fn test_convert_string_predicate() {
        let schema = test_schema();
        let expr = binary(
            col("name", &schema).unwrap(),
            datafusion_expr::Operator::Eq,
            lit("Alice"),
            &schema,
        )
        .unwrap();

        let predicate = convert_physical_expr_to_predicate(&expr, &schema);
        assert!(predicate.is_some());

        let pred = predicate.unwrap();
        assert_eq!(
            pred,
            Predicate::eq("name", PredicateValue::Utf8(Some("Alice".to_string())))
        );
    }

    #[test]
    fn test_convert_is_null_predicate() {
        let schema = test_schema();
        let col_expr = col("name", &schema).unwrap();
        let is_null = Arc::new(IsNullExpr::new(col_expr)) as Arc<dyn PhysicalExpr>;

        let predicate = convert_physical_expr_to_predicate(&is_null, &schema);
        assert!(predicate.is_some());

        let pred = predicate.unwrap();
        assert_eq!(pred, Predicate::is_null("name"));
    }

    #[test]
    fn test_convert_is_not_null_predicate() {
        let schema = test_schema();
        let col_expr = col("name", &schema).unwrap();
        let is_not_null = Arc::new(IsNotNullExpr::new(col_expr)) as Arc<dyn PhysicalExpr>;

        let predicate = convert_physical_expr_to_predicate(&is_not_null, &schema);
        assert!(predicate.is_some());

        let pred = predicate.unwrap();
        assert_eq!(pred, Predicate::is_not_null("name"));
    }

    #[test]
    fn test_convert_reversed_operands() {
        let schema = test_schema();
        // 10 < age => age > 10
        let expr = binary(
            lit(10i32),
            datafusion_expr::Operator::Lt,
            col("age", &schema).unwrap(),
            &schema,
        )
        .unwrap();

        let predicate = convert_physical_expr_to_predicate(&expr, &schema);
        assert!(predicate.is_some());

        let pred = predicate.unwrap();
        // Should flip to: age > 10
        assert_eq!(pred, Predicate::gt("age", PredicateValue::Int32(Some(10))));
    }

    #[test]
    fn test_scalar_value_conversions() {
        assert_eq!(
            convert_scalar_to_predicate_value(&ScalarValue::Boolean(Some(true))),
            Some(PredicateValue::Boolean(Some(true)))
        );

        assert_eq!(
            convert_scalar_to_predicate_value(&ScalarValue::Int32(Some(42))),
            Some(PredicateValue::Int32(Some(42)))
        );

        assert_eq!(
            convert_scalar_to_predicate_value(&ScalarValue::Float64(Some(3.14))),
            Some(PredicateValue::Float64(Some(3.14)))
        );

        assert_eq!(
            convert_scalar_to_predicate_value(&ScalarValue::Utf8(Some("hello".to_string()))),
            Some(PredicateValue::Utf8(Some("hello".to_string())))
        );

        // Null values
        assert_eq!(
            convert_scalar_to_predicate_value(&ScalarValue::Int32(None)),
            Some(PredicateValue::Int32(None))
        );
    }
}
