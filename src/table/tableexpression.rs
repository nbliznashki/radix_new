use std::collections::HashMap;

use radix_column::{
    ColumnDataF, ColumnDataIndex, ColumnWrapper, ErrorDesc, HashMapBuffer, NullableValue,
};
use radix_operations::{ColumnOperations, Dictionary, InputTypes, Signature};

use crate::column_buffer::ColumnBuffer;

pub enum ExpressionInput<'a> {
    Column(usize),
    Const(&'a ColumnWrapper<'a>),
    Expr(TableExpression<'a>),
}

pub struct TableExpression<'a> {
    pub op: String,
    pub input: Vec<ExpressionInput<'a>>,
    pub partition_by: Vec<ExpressionInput<'a>>,
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
            partition_by: vec![],
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

        for i in self.partition_by.iter_mut() {
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
        for i in self.partition_by.iter_mut() {
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
        hashmap_buffer: &mut HashMapBuffer,
        hashmap_binary: &mut HashMap<(usize, NullableValue<&[u8]>), usize, ahash::RandomState>,
        columns: &[ColumnWrapper],
        indexes: &[ColumnDataIndex],
        columnindexmap: &HashMap<usize, usize>,
    ) -> Result<InputTypes, ErrorDesc> {
        assert!(!dict.op_is_assign.get(&self.op).unwrap());
        let index_empty: ColumnDataIndex = ColumnDataIndex::None;

        let mut inp: Vec<InputTypes> = self
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
                ExpressionInput::Expr(expr) => expr
                    .eval(
                        dict,
                        buffer,
                        hashmap_buffer,
                        hashmap_binary,
                        columns,
                        indexes,
                        columnindexmap,
                    )
                    .unwrap(),
                ExpressionInput::Const(c) => InputTypes::Ref(c, &index_empty),
            })
            .collect();

        let mut inp_types: Vec<_> = inp
            .iter()
            .map(|c| match c {
                InputTypes::Ref(c, _) => c.column().item_type_id(),
                InputTypes::Owned(c, _) => c.column().item_type_id(),
            })
            .collect();

        let part_by: Vec<InputTypes> = self
            .partition_by
            .iter()
            .map(|inp| match inp {
                ExpressionInput::Column(col_id) => {
                    let col_index = match columnindexmap.get(col_id) {
                        Some(i) => &indexes[*i],
                        None => &index_empty,
                    };
                    InputTypes::Ref(&columns[*col_id], col_index)
                }
                ExpressionInput::Expr(expr) => expr
                    .eval(
                        dict,
                        buffer,
                        hashmap_buffer,
                        hashmap_binary,
                        columns,
                        indexes,
                        columnindexmap,
                    )
                    .unwrap(),
                ExpressionInput::Const(c) => InputTypes::Ref(c, &index_empty),
            })
            .collect();

        if !part_by.is_empty() {
            let buffer_group_ids = buffer.pop(dict, std::any::TypeId::of::<usize>())?;
            let mut buffer_group_ids = buffer_group_ids.get_inner().0.downcast_owned::<usize>()?;

            let _number_of_groups = buffer.pop(dict, std::any::TypeId::of::<usize>())?;

            part_by.iter().for_each(|c| match c {
                InputTypes::Ref(c, src_index) => c
                    .group_in(
                        dict,
                        src_index,
                        &mut buffer_group_ids,
                        hashmap_buffer,
                        hashmap_binary,
                    )
                    .unwrap(),
                InputTypes::Owned(c, src_index) => c
                    .group_in(
                        dict,
                        src_index,
                        &mut buffer_group_ids,
                        hashmap_buffer,
                        hashmap_binary,
                    )
                    .unwrap(),
            });

            let mut decrease_index_by = 0;
            let mut v = buffer_group_ids;
            (1..v.len()).into_iter().for_each(|i| {
                decrease_index_by += (v[i] != i) as usize;
                v[i] = v[v[i]];
                let diff = ((v[i] != i) as usize).wrapping_sub(1); //If equal then 0 else FFFFFF
                let diff = diff & decrease_index_by;
                v[i] -= diff;
            });
            let number_of_groups = v.len() - decrease_index_by;

            inp.push(InputTypes::Owned(
                ColumnWrapper::new_from_vec(dict, v),
                ColumnDataIndex::None,
            ));

            inp.push(InputTypes::Owned(
                ColumnWrapper::new_const(dict, number_of_groups),
                ColumnDataIndex::None,
            ));
        }

        let signature = Signature::new(&self.op, inp_types);
        let op = dict.op.get(&signature).unwrap();

        let mut output = buffer.pop(dict, op.output_type_id)?;
        let mut output_index = ColumnDataIndex::None;

        (op.f)(&mut output, &index_empty, &inp)?;
        if !part_by.is_empty() {
            inp.pop().unwrap();
            let v = inp.pop().unwrap();
            let v = match v {
                InputTypes::Owned(v, _) => v,
                _ => panic!(),
            };
            let (v, _) = v.get_inner();
            let v = v.downcast_owned::<usize>()?;
            output_index = ColumnDataIndex::new(v);
        }
        inp.into_iter().for_each(|inp| {
            if let InputTypes::Owned(c, _) = inp {
                buffer.push(dict, c)
            }
        });

        Ok(InputTypes::Owned(output, output_index))
    }
}
