use radix_column::ErrorDesc;
use std::any::TypeId;

use crate::Dictionary;

#[derive(Clone, Hash, Debug)]
pub struct Signature {
    op_name: String,
    input: Vec<TypeId>,
}

impl PartialEq for Signature {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input && self.op_name == other.op_name
    }
}

impl Eq for Signature {}

impl Signature {
    pub fn new(op: &str, input: Vec<TypeId>) -> Self {
        Self {
            op_name: op.to_string(),
            input,
        }
    }
    pub fn new_op(op: &str) -> Self {
        Self {
            op_name: op.to_string(),
            input: vec![],
        }
    }
    pub fn to_input(self) -> Vec<TypeId> {
        self.input
    }
    pub fn add_input<T: 'static + ?Sized>(&mut self) {
        self.input.push(TypeId::of::<T>());
    }

    pub fn input_len(&self) -> usize {
        self.input.len()
    }

    pub fn op_name(&self) -> &String {
        &self.op_name
    }

    pub fn as_output_sig(&self, dict: &Dictionary) -> Result<Self, ErrorDesc> {
        let op = dict.op.get(&self);
        if let Some(op) = op {
            Ok(Self {
                op_name: "new".to_string(),
                input: vec![op.output_type_id],
            })
        } else {
            Err(format!(
                "Following operation not found in dictionary: {:?}",
                &self
            ))?
        }
    }
}

#[macro_export]
macro_rules! sig {
    ($elem:expr) => (
        Signature::new_op(
            $elem,
        )
        );
    ($elem:expr; $($x:ty),+ $(,)?) => (
    {
        let mut s=Signature::new_op(
            $elem,
        );
        $(s.add_input::<$x>();)+
        s
    }
    );
}
