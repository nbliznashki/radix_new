use std::{any::TypeId, collections::HashMap, mem::MaybeUninit};

use crate::*;
use radix_column::*;

pub trait ColumnOperations<'a> {
    fn new_owned_with_capacity(
        dict: &Dictionary,
        item_type_id: TypeId,
        with_bitmap: bool,
        capacity: usize,
        binary_capacity: usize,
    ) -> Result<ColumnWrapper<'static>, ErrorDesc>;

    fn new_const<T>(dict: &Dictionary, data: T) -> Self
    where
        T: 'static;

    fn new_from_slice<'b, T>(dict: &Dictionary, data: &'b [T]) -> Self
    where
        T: 'static + Send + Sync,
        'b: 'a;

    fn new_from_slice_mut<'b, T>(dict: &Dictionary, data: &'b mut [T]) -> Self
    where
        T: 'static + Send + Sync,
        'b: 'a;

    fn new_from_vec<T>(dict: &Dictionary, data: Vec<T>) -> Self
    where
        T: 'static;
    fn len(&self, dict: &Dictionary) -> Result<usize, ErrorDesc>;

    fn hash_in(
        &self,
        dict: &Dictionary,
        src_index: &ColumnDataIndex<'a>,
        hash_column: &mut Vec<u64>,
    ) -> Result<(), ErrorDesc>;

    fn truncate(&mut self, dict: &Dictionary) -> Result<(), ErrorDesc>;

    fn copy_to(
        &self,
        dict: &Dictionary,
        dst: &mut ColumnWrapper<'a>,
        src_index: &ColumnDataIndex<'a>,
    ) -> Result<(), ErrorDesc>;

    fn op(
        &mut self,
        dict: &Dictionary,
        op: &str,
        c1_index: &ColumnDataIndex<'a>,
        input: &[InputTypes],
    ) -> Result<(), ErrorDesc>;

    fn as_string(
        &self,
        dict: &Dictionary,
        src_index: &ColumnDataIndex<'a>,
    ) -> Result<Vec<String>, ErrorDesc>;

    fn to_const<T: 'static + Send + Sync>(&self, dict: &Dictionary) -> Result<T, ErrorDesc>;
    fn group_in(
        &self,
        dict: &Dictionary,
        src_index: &ColumnDataIndex,
        dst: &mut Vec<usize>,
        hashmap_buffer: &mut HashMapBuffer,
        hashmap_binary: &mut HashMap<(usize, NullableValue<&[u8]>), usize, ahash::RandomState>,
    ) -> Result<(), ErrorDesc>;
}

impl<'a> ColumnOperations<'a> for ColumnWrapper<'a> {
    fn new_owned_with_capacity(
        dict: &Dictionary,
        item_type_id: TypeId,
        with_bitmap: bool,
        capacity: usize,
        binary_capacity: usize,
    ) -> Result<ColumnWrapper<'static>, ErrorDesc> {
        let signature = Signature::new("" as &str, vec![item_type_id]);
        let internaloperator = dict.columninternal.get(&signature);
        let c = match internaloperator {
            Some(iop) => iop.new_owned_with_capacity(capacity, binary_capacity, with_bitmap),
            None => Err(format!(
                "The following internal column operation not found in dictionary: {:?}",
                signature
            ))?,
        };
        Ok(c)
    }

    fn new_const<T>(dict: &Dictionary, data: T) -> Self
    where
        T: 'static,
    {
        let cd: ColumnData = ColumnData::new_const(&dict, data).unwrap();
        ColumnWrapper::new_from_columndata(cd)
    }

    fn new_from_slice<'b, T>(dict: &Dictionary, data: &'b [T]) -> Self
    where
        T: 'static + Send + Sync,
        'b: 'a,
    {
        let cd: ColumnData = ColumnData::new_from_slice(&dict, data).unwrap();
        ColumnWrapper::new_from_columndata(cd)
    }

    fn new_from_slice_mut<'b, T>(dict: &Dictionary, data: &'b mut [T]) -> Self
    where
        T: 'static + Send + Sync,
        'b: 'a,
    {
        let cd: ColumnData = ColumnData::new_from_slice(&dict, data).unwrap();
        ColumnWrapper::new_from_columndata(cd)
    }

    fn new_from_vec<T>(dict: &Dictionary, data: Vec<T>) -> Self
    where
        T: 'static,
    {
        let cd: ColumnData = ColumnData::new(&dict, data).unwrap();
        ColumnWrapper::new_from_columndata(cd)
    }
    fn len(&self, dict: &Dictionary) -> Result<usize, ErrorDesc> {
        let signature = Signature::new("" as &str, vec![self.column().item_type_id()]);
        let internaloperator = dict.columninternal.get(&signature);
        match internaloperator {
            Some(iop) => iop.len(&self),
            None => Err(format!(
                "The following internal column operation not found in dictionary: {:?}",
                signature
            ))?,
        }
    }

    fn hash_in(
        &self,
        dict: &Dictionary,
        src_index: &ColumnDataIndex<'a>,
        hash_column: &mut Vec<u64>,
    ) -> Result<(), ErrorDesc> {
        let signature = Signature::new("" as &str, vec![self.column().item_type_id()]);
        let internaloperator = dict.columninternal.get(&signature);
        match internaloperator {
            Some(iop) => iop.hash_in(&self, src_index, hash_column),
            None => Err(format!(
                "The following internal column operation not found in dictionary: {:?}",
                signature
            ))?,
        }
    }

    fn group_in(
        &self,
        dict: &Dictionary,
        src_index: &ColumnDataIndex,
        dst: &mut Vec<usize>,
        hashmap_buffer: &mut HashMapBuffer,
        hashmap_binary: &mut HashMap<(usize, NullableValue<&[u8]>), usize, ahash::RandomState>,
    ) -> Result<(), ErrorDesc> {
        let signature = Signature::new("" as &str, vec![self.column().item_type_id()]);
        let internaloperator = dict.columninternal.get(&signature);
        match internaloperator {
            Some(iop) => iop.group_in(&self, src_index, dst, hashmap_buffer, hashmap_binary),
            None => Err(format!(
                "The following internal column operation not found in dictionary: {:?}",
                signature
            ))?,
        }
    }

    fn truncate(&mut self, dict: &Dictionary) -> Result<(), ErrorDesc> {
        match self.column_mut() {
            ColumnData::BinaryOwned(c) => {
                c.truncate();
            }
            ColumnData::Owned(c) => {
                let signature = Signature::new("" as &str, vec![c.item_type_id()]);
                let internaloperator = dict.columninternal.get(&signature);
                match internaloperator {
                    Some(iop) => iop.truncate(self)?,
                    None => Err(format!(
                        "The following internal column operation not found in dictionary: {:?}",
                        signature
                    ))?,
                }
            }
            _ => Err("Truncate of columns other than Owned and BinaryOwned is not allowed")?,
        }

        if let ColumnDataF::Owned(b) = self.bitmap_mut() {
            b.truncate(0);
        } else {
            self.bitmap_set(ColumnDataF::None);
        };

        Ok(())
    }

    fn copy_to(
        &self,
        dict: &Dictionary,
        dst: &mut ColumnWrapper<'a>,
        src_index: &ColumnDataIndex<'a>,
    ) -> Result<(), ErrorDesc> {
        let signature = Signature::new("" as &str, vec![self.column().item_type_id()]);
        let internaloperator = dict.columninternal.get(&signature);
        match internaloperator {
            Some(iop) => iop.copy_to(&self, dst, src_index),
            None => Err(format!(
                "The following internal column operation not found in dictionary: {:?}",
                signature
            ))?,
        }
    }

    fn op(
        &mut self,
        dict: &Dictionary,
        op: &str,
        c1_index: &ColumnDataIndex<'a>,
        input: &[InputTypes],
    ) -> Result<(), ErrorDesc> {
        let is_assign_op = dict
            .op_is_assign
            .get(op)
            .ok_or(format!("Operation {} missing from the dictionary", op))?;

        let mut input_types: Vec<TypeId> = Vec::new();
        if *is_assign_op {
            input_types.push(self.column().item_type_id())
        };

        input_types.extend(input.iter().map(|input| match input {
            InputTypes::Ref(c, _) => c.column().item_type_id(),
            InputTypes::Owned(c, _) => c.column().item_type_id(),
        }));

        let signature = Signature::new(op, input_types);
        let op = dict.op.get(&signature);
        match op {
            Some(op) => (op.f)(self, c1_index, input),
            None => Err(format!(
                "The following internal column operation not found in dictionary: {:?}",
                signature
            ))?,
        }
    }

    fn as_string(
        &self,
        dict: &Dictionary,
        src_index: &ColumnDataIndex<'a>,
    ) -> Result<Vec<String>, ErrorDesc> {
        let signature = Signature::new("" as &str, vec![self.column().item_type_id()]);
        let internaloperator = dict.columninternal.get(&signature);
        match internaloperator {
            Some(iop) => iop.as_string(&self, src_index),
            None => Err(format!(
                "The following internal column operation not found in dictionary: {:?}",
                signature
            ))?,
        }
    }

    fn to_const<T: 'static + Send + Sync>(&self, dict: &Dictionary) -> Result<T, ErrorDesc> {
        let signature = Signature::new("" as &str, vec![self.column().item_type_id()]);
        let internaloperator = dict.columninternal.get(&signature);

        if !self.column().is_const() {
            Err("Cannot cast a non-const column to const value")?
        }

        let mut output = vec![MaybeUninit::<T>::uninit()];
        let mut output_as_col = ColumnWrapper::new_from_columndata(ColumnData::SliceMut(
            SliceRefMut::new(output.as_mut_slice()),
        ));

        match internaloperator {
            Some(iop) => {
                iop.copy_to(&self, &mut output_as_col, &ColumnDataIndex::None)?;
                Ok(unsafe { output.pop().unwrap().assume_init() })
            }
            None => Err(format!(
                "The following internal column operation not found in dictionary: {:?}",
                signature
            ))?,
        }
    }
}

pub trait ColumnDataOperations<'a> {
    fn new<T>(dict: &Dictionary, data: Vec<T>) -> Result<ColumnData<'static>, ErrorDesc>
    where
        T: 'static;

    fn new_from_slice<'b, T>(dict: &Dictionary, data: &'b [T]) -> Result<ColumnData<'a>, ErrorDesc>
    where
        T: 'static + Send + Sync,
        'b: 'a;

    fn new_from_slice_mut<'b, T>(
        dict: &Dictionary,
        data: &'b mut [T],
    ) -> Result<ColumnData<'a>, ErrorDesc>
    where
        T: 'static + Send + Sync,
        'b: 'a;
    fn new_const<T>(dict: &Dictionary, data: T) -> Result<ColumnData<'static>, ErrorDesc>
    where
        T: 'static;
}

impl<'a> ColumnDataOperations<'a> for ColumnData<'a> {
    fn new<T>(dict: &Dictionary, data: Vec<T>) -> Result<ColumnData<'static>, ErrorDesc>
    where
        T: 'static,
    {
        let signature = Signature::new("" as &str, vec![std::any::TypeId::of::<T>()]);
        let internaloperator = dict.columninternal.get(&signature);
        match internaloperator {
            Some(iop) => iop.new(Box::new(data)),
            None => Err(format!(
                "The following internal column operation not found in dictionary: {:?}",
                signature
            ))?,
        }
    }

    fn new_from_slice<'b, T>(dict: &Dictionary, data: &'b [T]) -> Result<ColumnData<'a>, ErrorDesc>
    where
        T: 'static + Send + Sync,
        'b: 'a,
    {
        let signature = Signature::new("" as &str, vec![std::any::TypeId::of::<T>()]);
        let internaloperator = dict.columninternal.get(&signature);
        match internaloperator {
            Some(iop) => iop.new_ref(SliceRef::new(data)),
            None => Err(format!(
                "The following internal column operation not found in dictionary: {:?}",
                signature
            ))?,
        }
    }

    fn new_from_slice_mut<'b, T>(
        dict: &Dictionary,
        data: &'b mut [T],
    ) -> Result<ColumnData<'a>, ErrorDesc>
    where
        T: 'static + Send + Sync,
        'b: 'a,
    {
        let signature = Signature::new("" as &str, vec![std::any::TypeId::of::<T>()]);
        let internaloperator = dict.columninternal.get(&signature);
        match internaloperator {
            Some(iop) => iop.new_mut(SliceRefMut::new(data)),
            None => Err(format!(
                "The following internal column operation not found in dictionary: {:?}",
                signature
            ))?,
        }
    }
    fn new_const<T>(dict: &Dictionary, data: T) -> Result<ColumnData<'static>, ErrorDesc>
    where
        T: 'static,
    {
        let signature = Signature::new("" as &str, vec![std::any::TypeId::of::<T>()]);
        let internaloperator = dict.columninternal.get(&signature);
        match internaloperator {
            Some(iop) => {
                let c = iop.new(Box::new(vec![data]))?;
                match c {
                    ColumnData::Owned(c) => Ok(ColumnData::Const(c)),
                    ColumnData::BinaryOwned(c) => Ok(ColumnData::BinaryConst(c)),
                    _ => Err("Const column can only be constructed from an owned column")?,
                }
            }
            None => Err(format!(
                "The following internal column operation not found in dictionary: {:?}",
                signature
            ))?,
        }
    }
}
