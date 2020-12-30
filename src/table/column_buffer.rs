use std::{any::TypeId, collections::VecDeque};

use radix_column::{ColumnWrapper, ErrorDesc};
use radix_operations::{ColumnOperations, Dictionary};

pub struct ColumnBuffer {
    stored: VecDeque<ColumnWrapper<'static>>,
}

impl<'a> ColumnBuffer {
    pub fn new() -> Self {
        Self {
            stored: VecDeque::new(),
        }
    }
    pub fn push(&mut self, dict: &Dictionary, mut c: ColumnWrapper<'static>) {
        if c.truncate(dict).is_ok() {
            if !c.bitmap().is_some() {
                self.stored.push_back(c);
            } else {
                let b = c.bitmap_mut().downcast_vec();
                if b.is_ok() {
                    b.unwrap().truncate(0);
                    self.stored.push_back(c);
                }
            }
        };
    }
    pub fn pop(
        &mut self,
        dict: &Dictionary,
        item_type_id: TypeId,
    ) -> Result<ColumnWrapper<'static>, ErrorDesc> {
        let pos = self
            .stored
            .iter()
            .position(|c| c.column().item_type_id() == item_type_id);
        match pos {
            Some(i) => self
                .stored
                .swap_remove_front(i)
                .ok_or_else(|| unreachable!()),
            None => ColumnWrapper::new_owned_with_capacity(dict, item_type_id, false, 0, 0),
        }
    }
}
