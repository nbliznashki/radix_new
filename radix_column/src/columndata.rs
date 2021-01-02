use std::any::TypeId;

pub type ErrorDesc = Box<dyn std::error::Error>;

use crate::OwnedColumn;

use super::{
    binarycolumn::BinarySlice,
    binarycolumn::{BinarySliceMut, OnwedBinaryColumn},
    SliceRef, SliceRefMut,
};

/// Source code copied from std::boxed::into_boxed_slice()
/// As of 13.09.2020, the feature is not stabilized. Tracking issue = "71582".
/// Converts a `Box<T>` into a `Box<[T]>`
///
/// This conversion does not allocate on the heap and happens in place.
///

#[derive(Debug)]
pub enum ColumnData<'a> {
    Owned(OwnedColumn),
    Slice(SliceRef<'a>),
    SliceMut(SliceRefMut<'a>),
    Const(OwnedColumn),
    BinaryOwned(OnwedBinaryColumn),
    BinarySlice(BinarySlice<'a>),
    BinarySliceMut(BinarySliceMut<'a>),
    BinaryConst(OnwedBinaryColumn),
}

impl<'a> ColumnData<'a> {
    pub fn item_type_id(&self) -> TypeId {
        {
            match self {
                ColumnData::Owned(c) => c.item_type_id(),
                ColumnData::SliceMut(c) => c.item_type_id(),
                ColumnData::Slice(c) => c.item_type_id(),
                ColumnData::Const(c) => c.item_type_id(),
                ColumnData::BinaryOwned(c) => c.item_type_id(),
                ColumnData::BinarySliceMut(c) => c.item_type_id(),
                ColumnData::BinarySlice(c) => c.item_type_id(),
                ColumnData::BinaryConst(c) => c.item_type_id(),
            }
        }
    }

    pub fn is_const(&self) -> bool {
        {
            match self {
                ColumnData::Owned(_) => false,
                ColumnData::SliceMut(_) => false,
                ColumnData::Slice(_) => false,
                ColumnData::Const(_) => true,
                ColumnData::BinaryOwned(_) => false,
                ColumnData::BinarySliceMut(_) => false,
                ColumnData::BinarySlice(_) => false,
                ColumnData::BinaryConst(_) => true,
            }
        }
    }

    pub fn is_owned(&self) -> bool {
        {
            match self {
                ColumnData::Owned(_) => true,
                ColumnData::SliceMut(_) => false,
                ColumnData::Slice(_) => false,
                ColumnData::Const(_) => false,
                ColumnData::BinaryOwned(_) => true,
                ColumnData::BinarySliceMut(_) => false,
                ColumnData::BinarySlice(_) => false,
                ColumnData::BinaryConst(_) => false,
            }
        }
    }

    pub fn is_binary(&self) -> bool {
        {
            match self {
                ColumnData::Owned(_) => false,
                ColumnData::SliceMut(_) => false,
                ColumnData::Slice(_) => false,
                ColumnData::Const(_) => false,
                ColumnData::BinaryOwned(_) => true,
                ColumnData::BinarySliceMut(_) => true,
                ColumnData::BinarySlice(_) => true,
                ColumnData::BinaryConst(_) => true,
            }
        }
    }

    pub fn is_sized(&self) -> bool {
        {
            match self {
                ColumnData::Owned(_) => true,
                ColumnData::SliceMut(_) => true,
                ColumnData::Slice(_) => true,
                ColumnData::Const(_) => true,
                ColumnData::BinaryOwned(_) => false,
                ColumnData::BinarySliceMut(_) => false,
                ColumnData::BinarySlice(_) => false,
                ColumnData::BinaryConst(_) => false,
            }
        }
    }

    pub fn is<T: 'static>(&self) -> bool {
        self.item_type_id() == std::any::TypeId::of::<T>()
    }

    pub fn downcast_owned<T>(self) -> Result<Vec<T>, ErrorDesc>
    where
        T: Send + Sync + 'static,
    {
        match self {
            ColumnData::Owned(c) => c.downcast_owned::<T>(),
            ColumnData::SliceMut(_) => Err(format!(
                "Downcast failed. downcast_owned not possible for ColumnData::SliceRef",
            ))?,
            ColumnData::Slice(_) => Err(format!(
                "Downcast failed. downcast_owned not possible for ColumnData::Slice",
            ))?,
            ColumnData::Const(c) => c.downcast_owned::<T>(),
            ColumnData::BinaryOwned(_) => Err(format!(
                "Downcast failed. downcast_owned not possible for ColumnData::BinaryOwned",
            ))?,
            ColumnData::BinarySliceMut(_) => Err(format!(
                "Downcast failed. downcast_owned not possible for ColumnData::BinarySliceMut",
            ))?,
            ColumnData::BinarySlice(_) => Err(format!(
                "Downcast failed. downcast_owned not possible for ColumnData::BinarySlice",
            ))?,
            ColumnData::BinaryConst(_) => Err(format!(
                "Downcast failed. downcast_owned not possible for ColumnData::BinaryConst",
            ))?,
        }
    }
    pub fn downcast_vec<T>(&mut self) -> Result<&mut Vec<T>, ErrorDesc>
    where
        T: Send + Sync + 'static,
    {
        match self {
            ColumnData::Owned(c) => c.downcast_vec::<T>(),
            ColumnData::SliceMut(_) => Err(format!(
                "Downcast failed. downcast_vec not possible for ColumnData::SliceRef",
            ))?,
            ColumnData::Slice(_) => Err(format!(
                "Downcast failed. downcast_vec not possible for ColumnData::Slice",
            ))?,
            ColumnData::Const(c) => c.downcast_vec::<T>(),
            ColumnData::BinaryOwned(_) => Err(format!(
                "Downcast failed. downcast_vec not possible for ColumnData::BinaryOwned",
            ))?,
            ColumnData::BinarySliceMut(_) => Err(format!(
                "Downcast failed. downcast_vec not possible for ColumnData::BinarySliceMut",
            ))?,
            ColumnData::BinarySlice(_) => Err(format!(
                "Downcast failed. downcast_vec not possible for ColumnData::BinarySlice",
            ))?,
            ColumnData::BinaryConst(_) => Err(format!(
                "Downcast failed. downcast_vec not possible for ColumnData::BinaryConst",
            ))?,
        }
    }
    pub fn downcast_mut<T>(&mut self) -> Result<&mut [T], ErrorDesc>
    where
        T: Send + Sync + 'static,
    {
        match self {
            ColumnData::Owned(c) => c.downcast_mut::<T>(),

            ColumnData::SliceMut(c) => c.downcast_mut::<T>(),
            ColumnData::Slice(_) => Err(format!(
                "Downcast failed. downcast_mut not possible for ColumnData::Slice",
            ))?,
            ColumnData::Const(c) => c.downcast_mut::<T>(),
            ColumnData::BinaryOwned(_) => Err(format!(
                "Downcast failed. downcast_mut not possible for ColumnData::SliceMut",
            ))?,
            ColumnData::BinarySliceMut(_) => Err(format!(
                "Downcast failed. downcast_mut not possible for ColumnData::BinarySliceMut",
            ))?,
            ColumnData::BinarySlice(_) => Err(format!(
                "Downcast failed. downcast_mut not possible for ColumnData::BinarySlice",
            ))?,
            ColumnData::BinaryConst(_) => Err(format!(
                "Downcast failed. downcast_mut not possible for ColumnData::BinaryConst",
            ))?,
        }
    }
    pub fn downcast_ref<T>(&self) -> Result<&[T], ErrorDesc>
    where
        T: Send + Sync + 'static,
    {
        match self {
            ColumnData::Owned(c) => c.downcast_ref::<T>(),
            ColumnData::SliceMut(c) => c.downcast_ref::<T>(),
            ColumnData::Slice(c) => c.downcast_ref::<T>(),
            ColumnData::Const(c) => c.downcast_ref::<T>(),
            ColumnData::BinaryOwned(_) => Err(format!(
                "Downcast failed. downcast_ref not possible for ColumnData::BinaryOwned",
            ))?,
            ColumnData::BinarySliceMut(_) => Err(format!(
                "Downcast failed. downcast_ref not possible for ColumnData::BinarySliceMut",
            ))?,
            ColumnData::BinarySlice(_) => Err(format!(
                "Downcast failed. downcast_ref not possible for ColumnData::BinarySlice",
            ))?,
            ColumnData::BinaryConst(_) => Err(format!(
                "Downcast failed. downcast_ref not possible for ColumnData::BinaryConst",
            ))?,
        }
    }

    pub fn data_len<T>(&self) -> Result<usize, ErrorDesc>
    where
        T: Send + Sync + 'static,
    {
        let len = match self {
            ColumnData::Owned(c) => c.downcast_ref::<T>()?.len(),
            ColumnData::SliceMut(c) => c.downcast_ref::<T>()?.len(),
            ColumnData::Slice(c) => c.downcast_ref::<T>()?.len(),
            ColumnData::Const(c) => c.downcast_ref::<T>()?.len(),
            ColumnData::BinaryOwned(c) => c.downcast_binary_ref::<T>()?.1.len(),
            ColumnData::BinarySliceMut(c) => c.downcast_binary_ref::<T>()?.1.len(),
            ColumnData::BinarySlice(c) => c.downcast_binary_ref::<T>()?.1.len(),
            ColumnData::BinaryConst(c) => c.downcast_binary_ref::<T>()?.1.len(),
        };
        Ok(len)
    }

    pub fn downcast_binary_owned<T>(
        self,
    ) -> Result<(Vec<u8>, Vec<usize>, Vec<usize>, usize), ErrorDesc>
    where
        T: Send + Sync + 'static,
    {
        match self{
                ColumnData::Owned(_) => Err(format!(
                    "Downcast failed. downcast_binary_owned not possible for ColumnData::Owned",
                ))?,
                ColumnData::SliceMut(_)=>Err(format!(
                    "Downcast failed. downcast_binary_owned not possible for ColumnData::SliceRef",
                ))?,
                ColumnData::Slice(_)=>Err(format!(
                    "Downcast failed. downcast_binary_owned not possible for ColumnData::Slice",
                ))?,
                ColumnData::Const(_) => Err(format!(
                    "Downcast failed. downcast_binary_owned not possible for ColumnData::Const",
                ))?,
                ColumnData::BinaryOwned(c)=>c.downcast_binary_owned::<T>(),
                ColumnData::BinarySliceMut(_)=>Err(format!(
                    "Downcast failed. downcast_binary_owned not possible for ColumnData::BinarySliceMut",
                ))?,
                ColumnData::BinarySlice(_)=>Err(format!(
                    "Downcast failed. downcast_binary_owned not possible for ColumnData::BinarySlice",
                ))?,
                ColumnData::BinaryConst(c)=>c.downcast_binary_owned::<T>(),
            }
    }

    pub fn downcast_binary_vec<T>(
        &mut self,
    ) -> Result<(&mut Vec<u8>, &mut Vec<usize>, &mut Vec<usize>, &mut usize), ErrorDesc>
    where
        T: Send + Sync + 'static,
    {
        match self {
            ColumnData::Owned(_) => Err(format!(
                "Downcast failed. downcast_binary_vec not possible for ColumnData::Owned",
            ))?,
            ColumnData::SliceMut(_) => Err(format!(
                "Downcast failed. downcast_binary_vec not possible for ColumnData::SliceRef",
            ))?,
            ColumnData::Slice(_) => Err(format!(
                "Downcast failed. downcast_binary_vec not possible for ColumnData::Slice",
            ))?,
            ColumnData::Const(_) => Err(format!(
                "Downcast failed. downcast_binary_vec not possible for ColumnData::Const",
            ))?,
            ColumnData::BinaryOwned(c) => c.downcast_binary_vec::<T>(),
            ColumnData::BinarySliceMut(_) => Err(format!(
                "Downcast failed. downcast_binary_vec not possible for ColumnData::BinarySliceMut",
            ))?,
            ColumnData::BinarySlice(_) => Err(format!(
                "Downcast failed. downcast_binary_vec not possible for ColumnData::BinarySlice",
            ))?,
            ColumnData::BinaryConst(c) => c.downcast_binary_vec::<T>(),
        }
    }

    pub fn downcast_binary_mut<T>(
        &mut self,
    ) -> Result<(&mut [u8], &mut [usize], &mut [usize], &mut usize), ErrorDesc>
    where
        T: Send + Sync + 'static,
    {
        match self {
            ColumnData::Owned(_) => Err(format!(
                "Downcast failed. downcast_binary_mut not possible for ColumnData::Owned",
            ))?,
            ColumnData::SliceMut(_) => Err(format!(
                "Downcast failed. downcast_binary_mut not possible for ColumnData::SliceRef",
            ))?,
            ColumnData::Slice(_) => Err(format!(
                "Downcast failed. downcast_binary_mut not possible for ColumnData::Slice",
            ))?,
            ColumnData::Const(_) => Err(format!(
                "Downcast failed. downcast_binary_mut not possible for ColumnData::Const",
            ))?,
            ColumnData::BinaryOwned(c) => c.downcast_binary_mut::<T>(),
            ColumnData::BinarySliceMut(c) => c.downcast_binary_mut::<T>(),
            ColumnData::BinarySlice(_) => Err(format!(
                "Downcast failed. downcast_binary_mut not possible for ColumnData::BinarySlice",
            ))?,
            ColumnData::BinaryConst(c) => c.downcast_binary_mut::<T>(),
        }
    }

    pub fn downcast_binary_ref<'b, T>(
        &'b self,
    ) -> Result<(&'b [u8], &'b [usize], &'b [usize], &'b usize), ErrorDesc>
    where
        T: Send + Sync + 'static,
        'a: 'b,
    {
        match self {
            ColumnData::Owned(_) => Err(format!(
                "Downcast failed. downcast_binary_ref not possible for ColumnData::Owned",
            ))?,
            ColumnData::SliceMut(_) => Err(format!(
                "Downcast failed. downcast_binary_ref not possible for ColumnData::SliceRef",
            ))?,
            ColumnData::Slice(_) => Err(format!(
                "Downcast failed. downcast_binary_ref not possible for ColumnData::Slice",
            ))?,
            ColumnData::Const(_) => Err(format!(
                "Downcast failed. downcast_binary_ref not possible for ColumnData::Const",
            ))?,
            ColumnData::BinaryOwned(c) => c.downcast_binary_ref::<T>(),
            ColumnData::BinarySliceMut(c) => c.downcast_binary_ref::<T>(),
            ColumnData::BinarySlice(c) => c.downcast_binary_ref::<T>(),
            ColumnData::BinaryConst(c) => c.downcast_binary_ref::<T>(),
        }
    }

    pub fn get_binary_offset(&self) -> Result<usize, ErrorDesc> {
        match self {
            ColumnData::Owned(_) => Err(format!(
                "Getting binary offset not possible for ColumnData::Owned",
            ))?,
            ColumnData::SliceMut(_) => Err(format!(
                "Getting binary offset not possible for ColumnData::SliceRef",
            ))?,
            ColumnData::Slice(_) => Err(format!(
                "Getting binary offset not possible for ColumnData::Slice",
            ))?,
            ColumnData::Const(_) => Err(format!(
                "Getting binary offset not possible for ColumnData::Const",
            ))?,
            ColumnData::BinaryOwned(_) => Ok(0),
            ColumnData::BinarySliceMut(c) => Ok(c.offset()),
            ColumnData::BinarySlice(c) => Ok(c.offset()),
            ColumnData::BinaryConst(_) => Ok(0),
        }
    }

    pub fn binary_to_const(self) -> Result<Self, ErrorDesc> {
        match self {
            ColumnData::BinaryOwned(c) => {
                if c.len() == 1 {
                    let col = ColumnData::BinaryConst(c);
                    Ok(col)
                } else {
                    Err("Binary column must have exactly one element in order to be transformed to a const")?
                }
            }
            _ => Err("Operation not supported for columns which are not BinaryOwned")?,
        }
    }

    pub unsafe fn assume_init<T: 'static + Send + Sync>(self) -> Result<Self, ErrorDesc> {
        match self {
            ColumnData::Owned(c) => c.assume_init::<T>().map(|c| ColumnData::Owned(c)),
            ColumnData::SliceMut(c) => c.assume_init::<T>().map(|c| ColumnData::SliceMut(c)),
            ColumnData::Slice(_) => Err(format!("Assume init not possible for mutable columns",))?,
            ColumnData::Const(_) => Err(format!("Assume init not possible for mutable columns",))?,
            ColumnData::BinaryOwned(_) => Ok(self),
            ColumnData::BinarySliceMut(_) => Ok(self),
            ColumnData::BinarySlice(_) => {
                Err(format!("Assume init not possible for mutable columns",))?
            }
            ColumnData::BinaryConst(_) => {
                Err(format!("Assume init not possible for mutable columns",))?
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ColumnDataF<'a, T> {
    //None,
    Owned(Vec<T>),
    Slice(&'a [T]),
    SliceMut(&'a mut [T]),
    None,
}

impl<'a, T> ColumnDataF<'a, T> {
    pub fn is_some(&self) -> bool {
        self.len().is_some()
    }

    pub fn is_empty(&self) -> bool {
        !self.len().is_some()
    }

    pub fn len(&self) -> Option<usize> {
        match &self {
            //ColumnDataF::None => None,
            ColumnDataF::Owned(v) => Some(v.len()),
            ColumnDataF::Slice(s) => Some(s.len()),
            ColumnDataF::SliceMut(s) => Some(s.len()),
            ColumnDataF::None => None,
        }
    }

    pub fn downcast_ref<'b>(&'b self) -> Result<&'b [T], ErrorDesc> {
        match &self {
            ColumnDataF::Owned(v) => Ok(v.as_slice()),
            ColumnDataF::Slice(s) => Ok(s),
            ColumnDataF::SliceMut(s) => Ok(s),
            ColumnDataF::None => Err("ColumnDataF is None and cannot be downcasted as a ref")?,
        }
    }

    pub fn downcast_mut<'b>(&'b mut self) -> Result<&'b mut [T], ErrorDesc> {
        match self {
            ColumnDataF::Owned(v) => Ok(v.as_mut_slice()),
            ColumnDataF::Slice(_) => Err("")?,
            ColumnDataF::SliceMut(s) => Ok(*s),
            ColumnDataF::None => Err("ColumnDataF is None and cannot be downcasted as a mut ref")?,
        }
    }

    pub fn downcast_vec<'b>(&'b mut self) -> Result<&'b mut Vec<T>, ErrorDesc>
    where
        'a: 'b,
    {
        match self {
            ColumnDataF::Owned(v) => Ok(v),
            ColumnDataF::Slice(_) => Err("")?,
            ColumnDataF::SliceMut(_) => Err("")?,
            ColumnDataF::None => Err("ColumnDataF is None and cannot be downcasted as a mut Vec")?,
        }
    }
    pub fn new(data: Vec<T>) -> Self {
        ColumnDataF::Owned(data)
    }
    pub fn new_from_slice(data: &'a [T]) -> Self {
        ColumnDataF::Slice(data)
    }
    pub fn new_from_slice_mut(data: &'a mut [T]) -> Self {
        ColumnDataF::SliceMut(data)
    }
    pub fn is_owned(&self) -> bool {
        match self {
            ColumnDataF::Owned(_) => true,
            ColumnDataF::Slice(_) => false,
            ColumnDataF::SliceMut(_) => false,
            ColumnDataF::None => false,
        }
    }
}
