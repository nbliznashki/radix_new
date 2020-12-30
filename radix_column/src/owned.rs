use std::any::{Any, TypeId};

use crate::ErrorDesc;

fn copy_of_into_boxed_slice<T>(boxed: Box<T>) -> Box<[T]> {
    // *mut T and *mut [T; 1] have the same size and alignment
    unsafe { Box::from_raw(Box::into_raw(boxed) as *mut [T; 1]) }
}

#[derive(Debug)]
pub struct OwnedColumn {
    item_type_id: std::any::TypeId,
    data: Box<dyn Any + Send + Sync>,
}

impl OwnedColumn {
    pub fn new<T>(data: Vec<T>) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self {
            item_type_id: std::any::TypeId::of::<T>(),
            data: Box::new(data),
        }
    }
    pub fn item_type_id(&self) -> TypeId {
        self.item_type_id
    }
    pub fn is<T>(&self) -> bool
    where
        T: 'static,
    {
        let t = std::any::TypeId::of::<T>();
        // Get `TypeId` of the type in the trait object (`self`).
        let concrete = self.item_type_id;
        // Compare both `TypeId`s on equality.
        t == concrete
    }
    pub fn downcast_owned<T>(self) -> Result<Vec<T>, ErrorDesc>
    where
        T: Send + Sync + 'static,
    {
        let (col, item_type_id) = (self.data as Box<dyn Any>, self.item_type_id);
        let col = col.downcast::<Vec<T>>().map_err(|_| {
            format!(
                "Downcast failed. Source type is {:?}, target type is ({:?}, {})",
                item_type_id,
                std::any::TypeId::of::<T>(),
                std::any::type_name::<T>()
            )
        })?;
        let col = copy_of_into_boxed_slice(col);
        let mut res: Vec<Vec<T>> = col.into();
        //Should never fail
        let res = res.pop().unwrap();
        Ok(res)
    }
    pub fn downcast_mut<T>(&mut self) -> Result<&mut [T], ErrorDesc>
    where
        T: 'static,
    {
        match self.data.downcast_mut::<Vec<T>>().map(|v| v.as_mut_slice()) {
            Some(s) => Ok(s),
            None => Err(format!(
                "Downcast failed. Source type is {:?}, target type is ({:?}, {})",
                self.item_type_id,
                std::any::TypeId::of::<T>(),
                std::any::type_name::<T>()
            ))?,
        }
    }
    pub fn downcast_ref<T>(&self) -> Result<&[T], ErrorDesc>
    where
        T: 'static,
    {
        match self.data.downcast_ref::<Vec<T>>().map(|v| v.as_slice()) {
            Some(s) => Ok(s),
            None => Err(format!(
                "Downcast failed. Source type is {:?}, target type is ({:?}, {})",
                self.item_type_id,
                std::any::TypeId::of::<T>(),
                std::any::type_name::<T>()
            ))?,
        }
    }
    pub fn downcast_vec<T>(&mut self) -> Result<&mut Vec<T>, ErrorDesc>
    where
        T: 'static,
    {
        match self.data.downcast_mut::<Vec<T>>() {
            Some(s) => Ok(s),
            None => Err(format!(
                "Downcast failed. Source type is {:?}, target type is ({:?}, {})",
                self.item_type_id,
                std::any::TypeId::of::<T>(),
                std::any::type_name::<T>()
            ))?,
        }
    }
}
