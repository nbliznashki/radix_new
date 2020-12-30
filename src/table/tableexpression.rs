use std::collections::HashMap;

use radix_column::{ColumnDataF, ColumnWrapper, ErrorDesc};
use radix_operations::{Dictionary, InputTypes, Signature};

use crate::column_buffer::ColumnBuffer;

pub enum ExpressionInput<'a> {
    Column(usize),
    Const(&'a ColumnWrapper<'a>),
    Expr(TableExpression<'a>),
}

pub struct TableExpression<'a> {
    pub op: String,
    pub input: Vec<ExpressionInput<'a>>,
}

pub enum ExpressionInputEvaluated {
    Column(usize),
    ExprEvaluated(ColumnWrapper<'static>),
}

impl<'a> TableExpression<'a> {
    pub fn new(op: &str, col_ids: &[usize]) -> Self {
        Self {
            op: op.to_string(),
            input: col_ids
                .iter()
                .map(|i| ExpressionInput::Column(*i))
                .collect(),
        }
    }
    pub fn expand_node(
        &mut self,
        old_col_id: usize,
        new_op: &str,
        new_col_ids: &[usize],
    ) -> Result<(), ErrorDesc> {
        let mut found = false;
        for i in self.input.iter_mut() {
            match i {
                ExpressionInput::Column(col_id) => {
                    if *col_id == old_col_id {
                        let new_table_expression = ExpressionInput::Expr(TableExpression::new(
                            new_op.clone(),
                            new_col_ids,
                        ));
                        *i = new_table_expression;
                        found = true;
                        break;
                    }
                }
                ExpressionInput::Expr(e) => {
                    if e.expand_node(old_col_id, new_op, new_col_ids).is_ok() {
                        found = true;
                        break;
                    }
                }
                ExpressionInput::Const(_) => {}
            }
        }
        match found {
            true => Ok(()),
            false => Err("Value to be expanded not found")?,
        }
    }

    pub fn expand_node_as_const(
        &mut self,
        old_col_id: usize,
        new_const_val: &mut Option<&'a ColumnWrapper<'a>>,
    ) -> Result<(), ErrorDesc> {
        let mut found = false;
        for i in self.input.iter_mut() {
            match i {
                ExpressionInput::Column(col_id) => {
                    if *col_id == old_col_id {
                        let new_table_expression =
                            ExpressionInput::Const(new_const_val.take().unwrap());
                        *i = new_table_expression;
                        found = true;
                        break;
                    }
                }
                ExpressionInput::Expr(e) => {
                    if e.expand_node_as_const(old_col_id, new_const_val).is_ok() {
                        found = true;
                        break;
                    }
                }
                ExpressionInput::Const(_) => {}
            }
        }
        match found {
            true => Ok(()),
            false => Err("Value to be expanded not found")?,
        }
    }

    pub fn eval(
        &self,
        dict: &Dictionary,
        buffer: &mut ColumnBuffer,
        columns: &[ColumnWrapper],
        indexes: &[ColumnDataF<usize>],
        columnindexmap: &HashMap<usize, usize>,
    ) -> Result<ColumnWrapper<'static>, ErrorDesc> {
        assert!(!dict.op_is_assign.get(&self.op).unwrap());
        let index_empty: ColumnDataF<usize> = ColumnDataF::None;

        let inp: Vec<InputTypes> = self
            .input
            .iter()
            .map(|inp| match inp {
                ExpressionInput::Column(col_id) => {
                    let col_index = match columnindexmap.get(col_id) {
                        Some(i) => &indexes[*i],
                        None => &index_empty,
                    };
                    InputTypes::Ref(&columns[*col_id], col_index)
                }
                ExpressionInput::Expr(expr) => InputTypes::Owned(
                    expr.eval(dict, buffer, columns, indexes, columnindexmap)
                        .unwrap(),
                    ColumnDataF::None,
                ),
                ExpressionInput::Const(c) => InputTypes::Ref(c, &index_empty),
            })
            .collect();

        let inp_types: Vec<_> = inp
            .iter()
            .map(|c| match c {
                InputTypes::Ref(c, _) => c.column().item_type_id(),
                InputTypes::Owned(c, _) => c.column().item_type_id(),
            })
            .collect();

        let signature = Signature::new(&self.op, inp_types);
        let op = dict.op.get(&signature).unwrap();

        let mut output = buffer.pop(dict, op.output_type_id)?;

        (op.f)(&mut output, &index_empty, &inp)?;
        inp.into_iter().for_each(|inp| {
            if let InputTypes::Owned(c, _) = inp {
                buffer.push(dict, c)
            }
        });

        Ok(output)
    }
}
