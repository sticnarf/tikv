// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tipb::expression::FieldType;

use super::expr::{RpnExpression, RpnExpressionNode};
use super::RpnFnCallPayload;
use crate::coprocessor::codec::batch::LazyBatchColumnVec;
use crate::coprocessor::codec::data_type::{ScalarValue, VectorValue};
use crate::coprocessor::codec::data_type::{ScalarValueRef, VectorLikeValueRef};
use crate::coprocessor::codec::mysql::time::Tz;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::Result;

/// Represents a vector value node in the RPN stack.
///
/// It can be either an owned node or a reference node.
///
/// When node comes from a column reference, it is a reference node (both value and field_type
/// are references).
///
/// When nodes comes from an evaluated result, it is an owned node.
#[derive(Debug)]
pub enum RpnStackNodeVectorValue<'a> {
    /// There can be frequent stack push & pops, so we wrap this field in a `Box` to reduce move
    /// cost.
    // TODO: Check whether it is more efficient to just remove the box.
    Owned(Box<VectorValue>),
    Ref(&'a VectorValue),
}

impl<'a> std::ops::Deref for RpnStackNodeVectorValue<'a> {
    type Target = VectorValue;

    fn deref(&self) -> &Self::Target {
        match self {
            RpnStackNodeVectorValue::Owned(value) => &value,
            RpnStackNodeVectorValue::Ref(value) => *value,
        }
    }
}

/// A type for each node in the RPN evaluation stack. It can be one of a scalar value node or a
/// vector value node. The vector value node can be either an owned vector value or a reference.
#[derive(Debug)]
pub enum RpnStackNode<'a> {
    /// Represents a scalar value. Comes from a constant node in expression list.
    Scalar {
        value: &'a ScalarValue,
        field_type: &'a FieldType,
    },

    /// Represents a vector value. Comes from a column reference or evaluated result.
    Vector {
        value: RpnStackNodeVectorValue<'a>,
        field_type: &'a FieldType,
    },
}

impl<'a> RpnStackNode<'a> {
    /// Gets the field type.
    #[inline]
    pub fn field_type(&self) -> &FieldType {
        match self {
            RpnStackNode::Scalar { field_type, .. } => field_type,
            RpnStackNode::Vector { field_type, .. } => field_type,
        }
    }

    /// Borrows the inner scalar value for `Scalar` variant.
    #[inline]
    pub fn scalar_value(&self) -> Option<&ScalarValue> {
        match self {
            RpnStackNode::Scalar { value, .. } => Some(*value),
            RpnStackNode::Vector { .. } => None,
        }
    }

    /// Borrows the inner vector value for `Vector` variant.
    #[inline]
    pub fn vector_value(&self) -> Option<&VectorValue> {
        match self {
            RpnStackNode::Scalar { .. } => None,
            RpnStackNode::Vector { value, .. } => Some(&value),
        }
    }

    /// Borrows the inner scalar or vector value as a vector like value.
    #[inline]
    pub fn as_vector_like(&self) -> VectorLikeValueRef<'_> {
        match self {
            RpnStackNode::Scalar { value, .. } => value.as_vector_like(),
            RpnStackNode::Vector { value, .. } => value.as_vector_like(),
        }
    }

    /// Whether this is a `Scalar` variant.
    #[inline]
    pub fn is_scalar(&self) -> bool {
        match self {
            RpnStackNode::Scalar { .. } => true,
            _ => false,
        }
    }

    /// Whether this is a `Vector` variant.
    #[inline]
    pub fn is_vector(&self) -> bool {
        match self {
            RpnStackNode::Vector { .. } => true,
            _ => false,
        }
    }

    /// Gets a reference of the element in corresponding index.
    ///
    /// If this is a `Scalar` variant, the returned reference will be the same for any index.
    ///
    /// # Panics
    ///
    /// Panics if index is out of range and this is a `Vector` variant.
    #[inline]
    pub fn get_scalar_ref(&self, index: usize) -> ScalarValueRef<'_> {
        match self {
            RpnStackNode::Vector { value, .. } => value.get_scalar_ref(index),
            RpnStackNode::Scalar { value, .. } => value.as_scalar_value_ref(),
        }
    }
}

impl RpnExpression {
    /// Evaluates the expression into a vector.
    ///
    /// If referred columns are not decoded, they will be decoded according to the given schema.
    ///
    /// # Panics
    ///
    /// Panics if the expression is not valid.
    ///
    /// Panics when referenced column does not have equal length as specified in `rows`.
    pub fn eval<'a>(
        &'a self,
        context: &mut EvalContext,
        rows: usize,
        schema: &'a [FieldType],
        columns: &'a mut LazyBatchColumnVec,
    ) -> Result<RpnStackNode<'a>> {
        // We iterate two times. The first time we decode all referred columns. The second time
        // we evaluate. This is to make Rust's borrow checker happy because there will be
        // mutable reference during the first iteration and we can't keep these references.
        self.ensure_columns_decoded(&context.cfg.tz, schema, columns)?;
        self.eval_unchecked(context, rows, schema, columns)
    }

    /// Evaluates the expression into a boolean vector.
    ///
    /// # Panics
    ///
    /// Panics if the expression is not valid.
    ///
    /// Panics if the boolean vector output buffer is not large enough to contain all values.
    pub fn eval_as_mysql_bools(
        &self,
        context: &mut EvalContext,
        rows: usize,
        schema: &[FieldType],
        columns: &mut LazyBatchColumnVec,
        outputs: &mut [bool], // modify an existing buffer to avoid repeated allocation
    ) -> Result<()> {
        use crate::coprocessor::codec::data_type::AsMySQLBool;

        assert!(outputs.len() >= rows);
        let values = self.eval(context, rows, schema, columns)?;
        match values {
            RpnStackNode::Scalar { value, .. } => {
                let b = value.as_mysql_bool(context)?;
                for i in 0..rows {
                    outputs[i] = b;
                }
            }
            RpnStackNode::Vector { value, .. } => {
                assert_eq!(value.len(), rows);
                value.eval_as_mysql_bools(context, outputs)?;
            }
        }
        Ok(())
    }

    /// Decodes all referred columns which are not decoded. Then we ensure
    /// all referred columns are decoded.
    pub fn ensure_columns_decoded<'a>(
        &'a self,
        tz: &Tz,
        schema: &'a [FieldType],
        columns: &'a mut LazyBatchColumnVec,
    ) -> Result<()> {
        for node in self.as_ref() {
            if let RpnExpressionNode::ColumnRef { offset, .. } = node {
                columns[*offset].ensure_decoded(tz, &schema[*offset])?;
            }
        }
        Ok(())
    }

    /// Evaluates the expression into a vector.
    ///
    /// It differs from `eval` in that `eval_unchecked` needn't receive a mutable reference
    /// to `LazyBatchColumnVec`. However, since `eval_unchecked` doesn't decode columns,
    /// it will panic if referred columns are not decoded.
    ///
    /// # Panics
    ///
    /// Panics if the expression is not valid.
    ///
    /// Panics if referred columns are not decoded.
    ///
    /// Panics when referenced column does not have equal length as specified in `rows`.
    pub fn eval_unchecked<'a>(
        &'a self,
        context: &mut EvalContext,
        rows: usize,
        schema: &'a [FieldType],
        columns: &'a LazyBatchColumnVec,
    ) -> Result<RpnStackNode<'a>> {
        assert!(rows > 0);
        let mut stack = Vec::with_capacity(self.len());

        for node in self.as_ref() {
            match node {
                RpnExpressionNode::Constant {
                    ref value,
                    ref field_type,
                } => {
                    stack.push(RpnStackNode::Scalar {
                        value: &value,
                        field_type,
                    });
                }
                RpnExpressionNode::ColumnRef { offset } => {
                    let field_type = &schema[*offset];
                    let decoded_column = columns[*offset].decoded();
                    assert_eq!(decoded_column.len(), rows);
                    stack.push(RpnStackNode::Vector {
                        value: RpnStackNodeVectorValue::Ref(&decoded_column),
                        field_type,
                    });
                }
                RpnExpressionNode::FnCall {
                    ref func,
                    ref field_type,
                } => {
                    // Suppose that we have function call `Foo(A, B, C)`, the RPN nodes looks like
                    // `[A, B, C, Foo]`.
                    // Now we receives a function call `Foo`, so there are `[A, B, C]` in the stack
                    // as the last several elements. We will directly use the last N (N = number of
                    // arguments) elements in the stack as function arguments.
                    assert!(stack.len() >= func.args_len);
                    let stack_slice_begin = stack.len() - func.args_len;
                    let stack_slice = &stack[stack_slice_begin..];
                    let call_info = RpnFnCallPayload {
                        raw_args: stack_slice,
                        ret_field_type: field_type,
                    };
                    let ret = (func.fn_ptr)(rows, context, call_info)?;
                    stack.truncate(stack_slice_begin);
                    stack.push(RpnStackNode::Vector {
                        value: RpnStackNodeVectorValue::Owned(Box::new(ret)),
                        field_type,
                    });
                }
            }
        }

        assert_eq!(stack.len(), 1);
        Ok(stack.into_iter().next().unwrap())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::float_cmp)]

    use super::*;

    use cop_codegen::rpn_fn;
    use cop_datatype::{EvalType, FieldTypeAccessor, FieldTypeTp};
    use tipb::expression::FieldType;

    use crate::coprocessor::codec::batch::LazyBatchColumn;
    use crate::coprocessor::codec::data_type::Real;
    use crate::coprocessor::codec::datum::{Datum, DatumEncoder};
    use crate::coprocessor::dag::expr::EvalContext;
    use crate::coprocessor::dag::rpn_expr::{RpnExpressionBuilder, RpnFn};
    use crate::coprocessor::Result;

    /// Single constant node
    #[test]
    fn test_eval_single_constant_node() {
        let exp = RpnExpressionBuilder::new().push_constant(1.5f64).build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let result = exp.eval(&mut ctx, 10, &[], &mut columns);
        let val = result.unwrap();
        assert!(val.is_scalar());
        assert_eq!(*val.scalar_value().unwrap().as_real(), Real::new(1.5).ok());
        assert_eq!(val.field_type().tp(), FieldTypeTp::Double);
    }

    /// Creates fixture to be used in `test_eval_single_column_node_xxx`.
    fn new_single_column_node_fixture() -> (LazyBatchColumnVec, [FieldType; 2]) {
        let columns = LazyBatchColumnVec::from(vec![
            {
                // this column is not referenced
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(5, EvalType::Real);
                col.mut_decoded().push_real(Real::new(1.0).ok());
                col.mut_decoded().push_real(None);
                col.mut_decoded().push_real(Real::new(7.5).ok());
                col.mut_decoded().push_real(None);
                col.mut_decoded().push_real(None);
                col
            },
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(5, EvalType::Int);
                col.mut_decoded().push_int(Some(1));
                col.mut_decoded().push_int(Some(5));
                col.mut_decoded().push_int(None);
                col.mut_decoded().push_int(None);
                col.mut_decoded().push_int(Some(42));
                col
            },
        ]);
        let schema = [FieldTypeTp::Double.into(), FieldTypeTp::LongLong.into()];
        (columns, schema)
    }

    /// Single column node
    #[test]
    fn test_eval_single_column_node_normal() {
        let (columns, schema) = new_single_column_node_fixture();

        let mut c = columns.clone();
        let exp = RpnExpressionBuilder::new().push_column_ref(1).build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 5, &schema, &mut c);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_int_slice(),
            [Some(1), Some(5), None, None, Some(42)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::LongLong);

        let mut c = columns.clone();
        let exp = RpnExpressionBuilder::new().push_column_ref(0).build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 5, &schema, &mut c);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_real_slice(),
            [Real::new(1.0).ok(), None, Real::new(7.5).ok(), None, None]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::Double);
    }

    /// Single column node but row numbers in `eval()` does not match column length, should panic.
    #[test]
    fn test_eval_single_column_node_mismatch_rows() {
        let (columns, schema) = new_single_column_node_fixture();

        let mut c = columns.clone();
        let exp = RpnExpressionBuilder::new().push_column_ref(1).build();
        let mut ctx = EvalContext::default();
        let hooked_eval = ::panic_hook::recover_safe(|| {
            // smaller row number
            let _ = exp.eval(&mut ctx, 4, &schema, &mut c);
        });
        assert!(hooked_eval.is_err());

        let mut c = columns.clone();
        let exp = RpnExpressionBuilder::new().push_column_ref(1).build();
        let mut ctx = EvalContext::default();
        let hooked_eval = ::panic_hook::recover_safe(|| {
            // larger row number
            let _ = exp.eval(&mut ctx, 6, &schema, &mut c);
        });
        assert!(hooked_eval.is_err());
    }

    /// Single function call node (i.e. nullary function)
    #[test]
    fn test_eval_single_fn_call_node() {
        #[rpn_fn]
        fn foo() -> Result<Option<i64>> {
            Ok(Some(42))
        }

        let exp = RpnExpressionBuilder::new()
            .push_fn_call(foo_fn(), FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let result = exp.eval(&mut ctx, 4, &[], &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_int_slice(),
            [Some(42), Some(42), Some(42), Some(42)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::LongLong);
    }

    /// Unary function (argument is scalar)
    #[test]
    fn test_eval_unary_function_scalar() {
        /// foo(v) performs v * 2.
        #[rpn_fn]
        fn foo(v: &Option<Real>) -> Result<Option<Real>> {
            Ok(v.map(|v| v * 2.0))
        }

        let exp = RpnExpressionBuilder::new()
            .push_constant(1.5f64)
            .push_fn_call(foo_fn(), FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let result = exp.eval(&mut ctx, 3, &[], &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_real_slice(),
            [
                Real::new(3.0).ok(),
                Real::new(3.0).ok(),
                Real::new(3.0).ok()
            ]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::Double);
    }

    /// Unary function (argument is vector)
    #[test]
    fn test_eval_unary_function_vector() {
        /// foo(v) performs v + 5.
        #[rpn_fn]
        fn foo(v: &Option<i64>) -> Result<Option<i64>> {
            Ok(v.map(|v| v + 5))
        }

        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Int);
            col.mut_decoded().push_int(Some(1));
            col.mut_decoded().push_int(Some(5));
            col.mut_decoded().push_int(None);
            col
        }]);
        let schema = &[FieldTypeTp::LongLong.into()];

        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_fn_call(foo_fn(), FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_int_slice(),
            [Some(6), Some(10), None]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::LongLong);
    }

    /// Unary function (argument is raw column). The column should be decoded.
    #[test]
    fn test_eval_unary_function_raw_column() {
        /// foo(v) performs v + 5.
        #[rpn_fn]
        fn foo(v: &Option<i64>) -> Result<Option<i64>> {
            Ok(Some(v.unwrap() + 5))
        }

        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::raw_with_capacity(3);

            let mut datum_raw = Vec::new();
            DatumEncoder::encode(&mut datum_raw, &[Datum::I64(-5)], false).unwrap();
            col.mut_raw().push(&datum_raw);

            let mut datum_raw = Vec::new();
            DatumEncoder::encode(&mut datum_raw, &[Datum::I64(-7)], false).unwrap();
            col.mut_raw().push(&datum_raw);

            let mut datum_raw = Vec::new();
            DatumEncoder::encode(&mut datum_raw, &[Datum::I64(3)], false).unwrap();
            col.mut_raw().push(&datum_raw);

            col
        }]);
        let schema = &[FieldTypeTp::LongLong.into()];

        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_fn_call(foo_fn(), FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_int_slice(),
            [Some(0), Some(-2), Some(8)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::LongLong);
    }

    /// Binary function (arguments are scalar, scalar)
    #[test]
    fn test_eval_binary_function_scalar_scalar() {
        /// foo(v) performs v1 + float(v2) - 1.
        #[rpn_fn]
        fn foo(v1: &Option<Real>, v2: &Option<i64>) -> Result<Option<Real>> {
            Ok(Some(v1.unwrap() + v2.unwrap() as f64 - 1.0))
        }

        let exp = RpnExpressionBuilder::new()
            .push_constant(1.5f64)
            .push_constant(3i64)
            .push_fn_call(foo_fn(), FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let result = exp.eval(&mut ctx, 3, &[], &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_real_slice(),
            [
                Real::new(3.5).ok(),
                Real::new(3.5).ok(),
                Real::new(3.5).ok()
            ]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::Double);
    }

    /// Binary function (arguments are vector, scalar)
    #[test]
    fn test_eval_binary_function_vector_scalar() {
        /// foo(v) performs v1 - v2.
        #[rpn_fn]
        fn foo(v1: &Option<Real>, v2: &Option<Real>) -> Result<Option<Real>> {
            Ok(Some(v1.unwrap() - v2.unwrap()))
        }

        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Real);
            col.mut_decoded().push_real(Real::new(1.0).ok());
            col.mut_decoded().push_real(Real::new(5.5).ok());
            col.mut_decoded().push_real(Real::new(-4.3).ok());
            col
        }]);
        let schema = &[FieldTypeTp::Double.into()];

        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_constant(1.5f64)
            .push_fn_call(foo_fn(), FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_real_slice(),
            [
                Real::new(-0.5).ok(),
                Real::new(4.0).ok(),
                Real::new(-5.8).ok()
            ]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::Double);
    }

    /// Binary function (arguments are scalar, vector)
    #[test]
    fn test_eval_binary_function_scalar_vector() {
        /// foo(v) performs v1 - float(v2).
        #[rpn_fn]
        fn foo(v1: &Option<Real>, v2: &Option<i64>) -> Result<Option<Real>> {
            Ok(Some(v1.unwrap() - v2.unwrap() as f64))
        }

        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Int);
            col.mut_decoded().push_int(Some(1));
            col.mut_decoded().push_int(Some(5));
            col.mut_decoded().push_int(Some(-4));
            col
        }]);
        let schema = &[FieldTypeTp::LongLong.into()];

        let exp = RpnExpressionBuilder::new()
            .push_constant(1.5f64)
            .push_column_ref(0)
            .push_fn_call(foo_fn(), FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_real_slice(),
            [
                Real::new(0.5).ok(),
                Real::new(-3.5).ok(),
                Real::new(5.5).ok()
            ]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::Double);
    }

    /// Binary function (arguments are vector, vector)
    #[test]
    fn test_eval_binary_function_vector_vector() {
        /// foo(v) performs int(v1*2.5 - float(v2)*3.5).
        #[rpn_fn]
        fn foo(v1: &Option<Real>, v2: &Option<i64>) -> Result<Option<i64>> {
            Ok(Some(
                (v1.unwrap().into_inner() * 2.5 - (v2.unwrap() as f64) * 3.5) as i64,
            ))
        }

        let mut columns = LazyBatchColumnVec::from(vec![
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Int);
                col.mut_decoded().push_int(Some(1));
                col.mut_decoded().push_int(Some(5));
                col.mut_decoded().push_int(Some(-4));
                col
            },
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Real);
                col.mut_decoded().push_real(Real::new(0.5).ok());
                col.mut_decoded().push_real(Real::new(-0.1).ok());
                col.mut_decoded().push_real(Real::new(3.5).ok());
                col
            },
        ]);
        let schema = &[FieldTypeTp::LongLong.into(), FieldTypeTp::Double.into()];

        // foo(col1, col0)
        let exp = RpnExpressionBuilder::new()
            .push_column_ref(1)
            .push_column_ref(0)
            .push_fn_call(foo_fn(), FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_int_slice(),
            [Some(-2), Some(-17), Some(22)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::LongLong);
    }

    /// Binary function (arguments are both raw columns). The same column is referred multiple times
    /// and it should be Ok.
    #[test]
    fn test_eval_binary_function_raw_column() {
        /// foo(v1, v2) performs v1 * v2.
        #[rpn_fn]
        fn foo(v1: &Option<i64>, v2: &Option<i64>) -> Result<Option<i64>> {
            Ok(Some(v1.unwrap() * v2.unwrap()))
        }

        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::raw_with_capacity(3);

            let mut datum_raw = Vec::new();
            DatumEncoder::encode(&mut datum_raw, &[Datum::I64(-5)], false).unwrap();
            col.mut_raw().push(&datum_raw);

            let mut datum_raw = Vec::new();
            DatumEncoder::encode(&mut datum_raw, &[Datum::I64(-7)], false).unwrap();
            col.mut_raw().push(&datum_raw);

            let mut datum_raw = Vec::new();
            DatumEncoder::encode(&mut datum_raw, &[Datum::I64(3)], false).unwrap();
            col.mut_raw().push(&datum_raw);

            col
        }]);
        let schema = &[FieldTypeTp::LongLong.into(), FieldTypeTp::LongLong.into()];

        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_column_ref(0)
            .push_fn_call(foo_fn(), FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_int_slice(),
            [Some(25), Some(49), Some(9)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::LongLong);
    }

    /// Ternary function (arguments are vector, scalar, vector)
    #[test]
    fn test_eval_ternary_function() {
        /// foo(v) performs v1 - v2 * v3.
        #[rpn_fn]
        fn foo(v1: &Option<i64>, v2: &Option<i64>, v3: &Option<i64>) -> Result<Option<i64>> {
            Ok(Some(v1.unwrap() - v2.unwrap() * v3.unwrap()))
        }

        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Int);
            col.mut_decoded().push_int(Some(1));
            col.mut_decoded().push_int(Some(5));
            col.mut_decoded().push_int(Some(-4));
            col
        }]);
        let schema = &[FieldTypeTp::LongLong.into()];

        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_constant(3i64)
            .push_column_ref(0)
            .push_fn_call(foo_fn(), FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_int_slice(),
            [Some(-2), Some(-10), Some(8)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::LongLong);
    }

    // Comprehensive expression:
    //      fn_a(
    //          Col0,
    //          fn_b(),
    //          fn_c(
    //              fn_d(Col1, Const0),
    //              Const1
    //          )
    //      )
    //
    // RPN: Col0, fn_b, Col1, Const0, fn_d, Const1, fn_c, fn_a
    #[test]
    fn test_eval_comprehensive() {
        /// fn_a(v1, v2, v3) performs v1 * v2 - v3.
        #[rpn_fn]
        fn fn_a(v1: &Option<Real>, v2: &Option<Real>, v3: &Option<Real>) -> Result<Option<Real>> {
            Ok(Some(v1.unwrap() * v2.unwrap() - v3.unwrap()))
        }

        /// fn_b() returns 42.0.
        #[rpn_fn]
        fn fn_b() -> Result<Option<Real>> {
            Ok(Real::new(42.0).ok())
        }

        /// fn_c(v1, v2) performs float(v2 - v1).
        #[rpn_fn]
        fn fn_c(v1: &Option<i64>, v2: &Option<i64>) -> Result<Option<Real>> {
            Ok(Real::new((v2.unwrap() - v1.unwrap()) as f64).ok())
        }

        /// fn_d(v1, v2) performs v1 + v2 * 2.
        #[rpn_fn]
        fn fn_d(v1: &Option<i64>, v2: &Option<i64>) -> Result<Option<i64>> {
            Ok(Some(v1.unwrap() + v2.unwrap() * 2))
        }

        let mut columns = LazyBatchColumnVec::from(vec![
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Real);
                col.mut_decoded().push_real(Real::new(0.5).ok());
                col.mut_decoded().push_real(Real::new(-0.1).ok());
                col.mut_decoded().push_real(Real::new(3.5).ok());
                col
            },
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Int);
                col.mut_decoded().push_int(Some(1));
                col.mut_decoded().push_int(Some(5));
                col.mut_decoded().push_int(Some(-4));
                col
            },
        ]);
        let schema = &[FieldTypeTp::Double.into(), FieldTypeTp::LongLong.into()];

        // Col0, fn_b, Col1, Const0, fn_d, Const1, fn_c, fn_a
        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_fn_call(fn_b_fn(), FieldTypeTp::Double)
            .push_column_ref(1)
            .push_constant(7i64)
            .push_fn_call(fn_d_fn(), FieldTypeTp::LongLong)
            .push_constant(11i64)
            .push_fn_call(fn_c_fn(), FieldTypeTp::Double)
            .push_fn_call(fn_a_fn(), FieldTypeTp::Double)
            .build();

        //      fn_a(
        //          [0.5, -0.1, 3.5],
        //          42.0,
        //          fn_c(
        //              fn_d([1, 5, -4], 7),
        //              11
        //          )
        //      )
        //      => [25.0, 3.8, 146.0]

        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_real_slice(),
            [
                Real::new(25.0).ok(),
                Real::new(3.8).ok(),
                Real::new(146.0).ok()
            ]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::Double);
    }

    /// Unary function, but supplied zero arguments. Should panic.
    #[test]
    fn test_eval_fail_1() {
        #[rpn_fn]
        fn foo(_v: &Option<i64>) -> Result<Option<i64>> {
            unreachable!()
        }

        let exp = RpnExpressionBuilder::new()
            .push_fn_call(foo_fn(), FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let hooked_eval = ::panic_hook::recover_safe(|| {
            let _ = exp.eval(&mut ctx, 3, &[], &mut columns);
        });
        assert!(hooked_eval.is_err());
    }

    /// Irregular RPN expression (contains unused node). Should panic.
    #[test]
    fn test_eval_fail_2() {
        /// foo(v) performs v * 2.
        #[rpn_fn]
        fn foo(v: &Option<Real>) -> Result<Option<Real>> {
            Ok(v.map(|v| v * 2.0))
        }

        // foo() only accepts 1 parameter but we will give 2.

        let exp = RpnExpressionBuilder::new()
            .push_constant(3.0f64)
            .push_constant(1.5f64)
            .push_fn_call(foo_fn(), FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let hooked_eval = ::panic_hook::recover_safe(|| {
            let _ = exp.eval(&mut ctx, 3, &[], &mut columns);
        });
        assert!(hooked_eval.is_err());
    }

    /// Eval type does not match. Should panic.
    /// Note: When field type is not matching, it doesn't panic.
    #[test]
    fn test_eval_fail_3() {
        /// Expects real argument, receives int argument.
        #[rpn_fn]
        fn foo(v: &Option<Real>) -> Result<Option<Real>> {
            Ok(v.map(|v| v * 2.5))
        }

        let exp = RpnExpressionBuilder::new()
            .push_constant(7i64)
            .push_fn_call(foo_fn(), FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let hooked_eval = ::panic_hook::recover_safe(|| {
            let _ = exp.eval(&mut ctx, 3, &[], &mut columns);
        });
        assert!(hooked_eval.is_err());
    }

    /// Parse from an expression tree then evaluate.
    #[test]
    fn test_parse_and_eval() {
        use tipb::expression::ScalarFuncSig;
        use tipb::expression::{Expr, ExprType};

        use tikv_util::codec::number::NumberEncoder;

        // We will build an expression tree from:
        //      fn_d(
        //          fn_a(
        //              Const1,
        //              fn_b(Col1, fn_c()),
        //              Col0
        //          )
        //      )

        /// fn_a(a: int, b: float, c: int) performs: float(a) - b * float(c)
        #[rpn_fn]
        fn fn_a(a: &Option<i64>, b: &Option<Real>, c: &Option<i64>) -> Result<Option<Real>> {
            Ok(Real::new(a.unwrap() as f64 - b.unwrap().into_inner() * c.unwrap() as f64).ok())
        }

        /// fn_b(a: float, b: int) performs: a * (float(b) - 1.5)
        #[rpn_fn]
        fn fn_b(a: &Option<Real>, b: &Option<i64>) -> Result<Option<Real>> {
            Ok(Real::new(a.unwrap().into_inner() * (b.unwrap() as f64 - 1.5)).ok())
        }

        /// fn_c() returns: int(42)
        #[rpn_fn]
        fn fn_c() -> Result<Option<i64>> {
            Ok(Some(42))
        }

        /// fn_d(a: float) performs: int(a)
        #[rpn_fn]
        fn fn_d(a: &Option<Real>) -> Result<Option<i64>> {
            Ok(Some(a.unwrap().into_inner() as i64))
        }

        fn fn_mapper(value: ScalarFuncSig, _children: &[Expr]) -> Result<RpnFn> {
            // fn_a: CastIntAsInt
            // fn_b: CastIntAsReal
            // fn_c: CastIntAsString
            // fn_d: CastIntAsDecimal
            Ok(match value {
                ScalarFuncSig::CastIntAsInt => fn_a_fn(),
                ScalarFuncSig::CastIntAsReal => fn_b_fn(),
                ScalarFuncSig::CastIntAsString => fn_c_fn(),
                ScalarFuncSig::CastIntAsDecimal => fn_d_fn(),
                _ => unreachable!(),
            })
        }

        let node_fn_b = {
            // Col1
            let mut node_col_1 = Expr::new();
            node_col_1.set_tp(ExprType::ColumnRef);
            node_col_1
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::Double);
            node_col_1.mut_val().encode_i64(1).unwrap();

            // fn_c
            let mut node_fn_c = Expr::new();
            node_fn_c.set_tp(ExprType::ScalarFunc);
            node_fn_c.set_sig(ScalarFuncSig::CastIntAsString);
            node_fn_c
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);

            // fn_b
            let mut node_fn_b = Expr::new();
            node_fn_b.set_tp(ExprType::ScalarFunc);
            node_fn_b.set_sig(ScalarFuncSig::CastIntAsReal);
            node_fn_b
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::Double);
            node_fn_b.mut_children().push(node_col_1);
            node_fn_b.mut_children().push(node_fn_c);
            node_fn_b
        };

        let node_fn_a = {
            // Const1
            let mut node_const_1 = Expr::new();
            node_const_1.set_tp(ExprType::Int64);
            node_const_1
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            node_const_1.mut_val().encode_i64(7).unwrap();

            // Col0
            let mut node_col_0 = Expr::new();
            node_col_0.set_tp(ExprType::ColumnRef);
            node_col_0
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            node_col_0.mut_val().encode_i64(0).unwrap();

            // fn_a
            let mut node_fn_a = Expr::new();
            node_fn_a.set_tp(ExprType::ScalarFunc);
            node_fn_a.set_sig(ScalarFuncSig::CastIntAsInt);
            node_fn_a
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::Double);
            node_fn_a.mut_children().push(node_const_1);
            node_fn_a.mut_children().push(node_fn_b);
            node_fn_a.mut_children().push(node_col_0);
            node_fn_a
        };

        // fn_d
        let mut node_fn_d = Expr::new();
        node_fn_d.set_tp(ExprType::ScalarFunc);
        node_fn_d.set_sig(ScalarFuncSig::CastIntAsDecimal);
        node_fn_d
            .mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::LongLong);
        node_fn_d.mut_children().push(node_fn_a);

        // Build RPN expression from this expression tree.
        let exp =
            RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node_fn_d, fn_mapper, 2)
                .unwrap();

        let mut columns = LazyBatchColumnVec::from(vec![
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Int);
                col.mut_decoded().push_int(Some(1));
                col.mut_decoded().push_int(Some(5));
                col.mut_decoded().push_int(Some(-4));
                col
            },
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Real);
                col.mut_decoded().push_real(Real::new(0.5).ok());
                col.mut_decoded().push_real(Real::new(-0.1).ok());
                col.mut_decoded().push_real(Real::new(3.5).ok());
                col
            },
        ]);
        let schema = &[FieldTypeTp::LongLong.into(), FieldTypeTp::Double.into()];

        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_int_slice(),
            [Some(-13), Some(27), Some(574)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::LongLong);
    }
}

#[cfg(test)]
mod benches {
    #![allow(clippy::float_cmp)]

    use test::{black_box, Bencher};

    use super::*;

    use cop_codegen::rpn_fn;
    use cop_datatype::{EvalType, FieldTypeAccessor, FieldTypeTp};
    use tipb::expression::FieldType;

    use crate::coprocessor::codec::batch::LazyBatchColumn;
    use crate::coprocessor::codec::data_type::Real;
    use crate::coprocessor::codec::datum::{Datum, DatumEncoder};
    use crate::coprocessor::dag::expr::EvalContext;
    use crate::coprocessor::dag::rpn_expr::{RpnExpressionBuilder, RpnFn};
    use crate::coprocessor::Result;

    // Comprehensive expression:
    //      FnA(
    //          Col0,
    //          FnB(),
    //          FnC(
    //              FnD(Col1, Const0),
    //              Const1
    //          )
    //      )
    //
    // RPN: Col0, FnB, Col1, Const0, FnD, Const1, FnC, FnA
    #[bench]
    fn bench_eval_comprehensive(b: &mut Bencher) {
        /// fn_a(v1, v2, v3) performs v1 * v2 - v3.
        #[rpn_fn]
        fn fn_a(v1: &Option<Real>, v2: &Option<Real>, v3: &Option<Real>) -> Result<Option<Real>> {
            Ok(Some(v1.unwrap() * v2.unwrap() - v3.unwrap()))
        }

        /// fn_b() returns 42.0.
        #[rpn_fn]
        fn fn_b() -> Result<Option<Real>> {
            Ok(Real::new(42.0).ok())
        }

        /// fn_c(v1, v2) performs float(v2 - v1).
        #[rpn_fn]
        fn fn_c(v1: &Option<i64>, v2: &Option<i64>) -> Result<Option<Real>> {
            Ok(Real::new((v2.unwrap() - v1.unwrap()) as f64).ok())
        }

        /// fn_d(v1, v2) performs v1 + v2 * 2.
        #[rpn_fn]
        fn fn_d(v1: &Option<i64>, v2: &Option<i64>) -> Result<Option<i64>> {
            Ok(Some(v1.unwrap() + v2.unwrap() * 2))
        }

        let mut columns = LazyBatchColumnVec::from(vec![
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(1024, EvalType::Real);
                for i in -512..512 {
                    col.mut_decoded()
                        .push_real(Real::new(i as f64 / 128.0).ok());
                }
                col
            },
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(1024, EvalType::Int);
                for i in -512..512 {
                    col.mut_decoded().push_int(Some(i));
                }
                col
            },
        ]);
        let schema = &[FieldTypeTp::Double.into(), FieldTypeTp::LongLong.into()];

        // Col0, fn_b, Col1, Const0, fn_d, Const1, fn_c, fn_a
        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_fn_call(fn_b_fn(), FieldTypeTp::Double)
            .push_column_ref(1)
            .push_constant(7i64)
            .push_fn_call(fn_d_fn(), FieldTypeTp::LongLong)
            .push_constant(11i64)
            .push_fn_call(fn_c_fn(), FieldTypeTp::Double)
            .push_fn_call(fn_a_fn(), FieldTypeTp::Double)
            .build();

        let mut ctx = EvalContext::default();
        b.iter(|| {
            black_box(exp.eval(&mut ctx, 1024, schema, &mut columns));
        });
    }
}
