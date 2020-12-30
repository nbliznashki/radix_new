use std::collections::HashMap;

use crate::{load_columninternal_dict, ColumnInternalOp, Operation, Signature};

pub type ColumnInternalDictionary = HashMap<Signature, Box<dyn ColumnInternalOp + Sync>>;
pub type OpDictionary = HashMap<Signature, Operation>;
pub struct Dictionary {
    pub columninternal: ColumnInternalDictionary,
    pub op: OpDictionary,
    pub op_is_assign: HashMap<String, bool>,
}

impl Dictionary {
    pub fn new() -> Self {
        let mut columninternal: ColumnInternalDictionary = HashMap::new();
        let mut op: OpDictionary = HashMap::new();
        let mut op_is_assign: HashMap<String, bool> = HashMap::new();
        load_columninternal_dict(&mut columninternal);
        crate::c_addassign::load_op_dict(&mut op);
        crate::c_add::load_op_dict(&mut op);
        crate::c_eq::load_op_dict(&mut op);
        crate::c_gt::load_op_dict(&mut op);
        crate::c_gteq::load_op_dict(&mut op);
        crate::c_lt::load_op_dict(&mut op);
        crate::c_lteq::load_op_dict(&mut op);

        op.iter().for_each(|(signature, op)| {
            let val = op_is_assign
                .entry(signature.op_name().clone())
                .or_insert(op.is_assign_op);
            if *val != op.is_assign_op {
                panic!(
                    "Inconsistent assign types for operation: {}",
                    signature.op_name()
                );
            }
        });

        Self {
            columninternal,
            op,
            op_is_assign,
        }
    }
}
