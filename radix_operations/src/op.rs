use crate::Signature;
use radix_column::*;
use std::any::TypeId;

pub enum InputTypes<'a> {
    Ref(&'a ColumnWrapper<'a>, &'a ColumnDataF<'a, usize>),
    Owned(ColumnWrapper<'static>, ColumnDataF<'static, usize>),
}
#[derive(Clone)]
pub struct Operation {
    pub f: fn(&mut ColumnWrapper, &ColumnDataF<usize>, &[InputTypes]) -> Result<(), ErrorDesc>,
    pub output_type_id: TypeId,
    pub is_assign_op: bool,
    pub associated_assign_op: Option<String>,
    pub associated_input_switch_op: Option<String>,
}

pub trait ColumnOp {
    fn op(
        output: &mut ColumnWrapper,
        output_index: &ColumnDataF<usize>,
        input: &Vec<InputTypes>,
    ) -> Result<(), ErrorDesc>;
}

#[derive(Clone, PartialEq, Hash, Eq, Debug)]
pub enum Binding {
    RefColumn(usize),
    OwnedColumn,
    ConstValue(usize),
    Expr(Box<Expression>),
}

#[derive(Clone, PartialEq, Hash, Eq, Debug)]
pub struct Expression {
    op: Signature,
    output: Binding,
    input: Vec<Binding>,
}
impl Expression {
    pub fn new(sig: Signature, output: Binding, input: Vec<Binding>) -> Self {
        Self {
            op: sig,
            output,
            input,
        }
    }
    /*
    fn eval_recursive(
        &self,
        owned_columns: &mut Vec<&mut ColumnWrapper>,
        ref_columns: &Vec<&ColumnWrapper>,
        const_values: &Vec<&ColumnWrapper>,
        dict: &Dictionary,
    ) -> Result<(), ErrorDesc> {
        let mut output: &mut ColumnWrapper = match &self.output {
            Binding::OwnedColumn => owned_columns.pop().unwrap(),
            Binding::Expr(expr) => {
                (*expr).eval_recursive(owned_columns, ref_columns, const_values, dict)?;
                owned_columns.pop().unwrap()
            }
            Binding::RefColumn(_) | Binding::ConstValue(_) => {
                panic!("Incorrect expression output type")
            }
        };

        let input: Result<Vec<InputTypes>, ErrorDesc> =
            self.input
                .iter()
                .try_fold(Vec::with_capacity(self.input.len()), |mut acc, b| {
                    match b {
                        Binding::OwnedColumn => {
                            acc.push(InputTypes::Ref(owned_columns.pop().unwrap()))
                        }
                        Binding::RefColumn(i) => acc.push(InputTypes::Ref(ref_columns[*i])),
                        Binding::ConstValue(i) => acc.push(InputTypes::Ref(const_values[*i])),
                        Binding::Expr(expr) => {
                            (*expr).eval_recursive(
                                owned_columns,
                                ref_columns,
                                const_values,
                                dict,
                            )?;
                            acc.push(InputTypes::Ref(owned_columns.pop().unwrap()))
                        }
                    };
                    Ok(acc)
                });
        let input = input?;
        let op = dict.op.get(&self.op).unwrap();
        (op.f)(&mut output, input)?;
        owned_columns.push(output);
        Ok(())
    }

    pub fn eval(
        &self,
        owned_columns: &mut Vec<&mut ColumnWrapper>,
        ref_columns: &Vec<&ColumnWrapper>,
        const_values: &Vec<&ColumnWrapper>,
        dict: &Dictionary,
    ) -> Result<(), ErrorDesc> {
        self.eval_recursive(owned_columns, ref_columns, const_values, dict)?;
        if owned_columns.len() == 1 {
            Ok(())
        } else {
            Err(format!(
                "Following expression returned more than 1 output columns: {:?}",
                &self
            ))?
        }
    }
    */
    /*
    fn compile_recursive(
        &self,
        dict: &Dictionary,
        ops: &mut Vec<Operation>,
        owned_columns: &mut Vec<ColumnWrapper>,
    ) -> Result<(), ErrorDesc> {
        self.input.iter().rev().try_for_each(|inp| match inp {
            Binding::OwnedColumn | Binding::RefColumn(_) | Binding::ConstValue(_) => Ok(()),
            Binding::Expr(expr) => (*expr).compile_recursive(dict, ops, owned_columns),
        })?;

        match &self.output {
            Binding::OwnedColumn => {
                let output_sig = &self.op.as_output_sig(dict)?;
                let f = dict.init.get(output_sig);
                if let Some(f) = f {
                    owned_columns.push(f());
                } else {
                    Err(format!(
                        "Following operation not found in dictionary: {:?}",
                        output_sig
                    ))?
                }

                //println!("{}", output_sig.op_name())
            }
            Binding::Expr(expr) => {
                (*expr).compile_recursive(dict, ops, owned_columns)?;
            }
            Binding::RefColumn(_) | Binding::ConstValue(_) => {
                Err("Incorrect expression output type")?
            }
        }
        let op = dict.op.get(&self.op);

        if let Some(op) = op {
            ops.push(op.clone());
            Ok(())
        } else {
            Err(format!(
                "Following operation not found in dictionary: {:?}",
                &self.op
            ))?
        }
    }

    pub fn compile(
        &self,
        dict: &Dictionary,
    ) -> Result<(Vec<Operation>, Vec<ColumnWrapper>), ErrorDesc> {
        let mut ops: Vec<Operation> = Vec::new();
        let mut owned_columns: Vec<ColumnWrapper> = Vec::new();
        self.compile_recursive(dict, &mut ops, &mut owned_columns)?;
        Ok((ops, owned_columns))
    }

    pub fn output_type(&self, dict: &Dictionary) -> Result<TypeId, ErrorDesc> {
        if let Some(t) = self.op.as_output_sig(dict)?.to_input().pop() {
            Ok(t)
        } else {
            Err(format!(
                "Output of the following operation can't be determined: {:?}",
                &self.op
            ))?
        }
    }*/
}
