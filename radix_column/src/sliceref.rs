use std::marker::PhantomData;

use super::ErrorDesc;

#[derive(Debug)]
pub struct SliceRef<'a> {
    item_type_id: std::any::TypeId,
    len: usize,
    ptr: *const u8,
    phantom: std::marker::PhantomData<&'a u8>,
}

pub unsafe trait SliceItemType<V: ?Sized> {
    type ItemType;
}

pub unsafe trait VectorItemType<V: ?Sized> {
    type ItemType;
}

unsafe impl<T: 'static + Sync> SliceItemType<[T]> for [T] {
    type ItemType = T;
}

unsafe impl<T: 'static + Sync> VectorItemType<[T]> for Vec<T> {
    type ItemType = T;
}

impl<'a> SliceRef<'a> {
    pub fn item_type_id(&self) -> std::any::TypeId {
        self.item_type_id
    }
    pub fn new<T>(s: &'a [T]) -> Self
    where
        T: 'static + Sync,
    {
        SliceRef {
            item_type_id: std::any::TypeId::of::<T>(),
            len: s.len(),
            ptr: s.as_ptr() as *const u8,
            phantom: PhantomData,
        }
    }
    pub(crate) fn is<T>(&self) -> bool
    where
        T: 'static,
    {
        let t = std::any::TypeId::of::<T>();
        let concrete = self.item_type_id;
        t == concrete
    }

    pub fn downcast_ref<T>(&self) -> Result<&[T], ErrorDesc>
    where
        T: 'static + Sync,
    {
        if self.is::<T>() {
            let ptr = self.ptr;
            let len = self.len;
            // SAFETY: just checked whether we are pointing to the correct type, and we can rely on
            // that check for memory safety because we have implemented Any for all types; no other
            // impls can exist as they would conflict with our impl.
            unsafe {
                let ptr = ptr as *const T;
                Ok(std::slice::from_raw_parts(ptr, len))
            }
        } else {
            Err(format!(
                "Downcast failed. Source type is {:?}, target type is ({:?}, {})",
                self.item_type_id,
                std::any::TypeId::of::<T>(),
                std::any::type_name::<T>()
            ))?
        }
    }
    pub fn split_off_left<T>(&mut self, pos: usize) -> Result<Self, ErrorDesc>
    where
        T: 'static + Sync,
    {
        if pos > self.len {
            Err(format!(
                "Attempt to split at slice at position {}, while the source slice has length {}",
                pos, self.len,
            ))?
        } else if self.is::<T>() {
            let l = Self {
                item_type_id: self.item_type_id,
                len: pos,
                ptr: self.ptr,
                phantom: self.phantom,
            };

            self.len = self.len - pos;
            self.ptr = unsafe { (self.ptr as *const T).offset(pos as isize) as *const u8 };
            Ok(l)
        } else {
            Err(format!(
                "Downcast failed. Source type is {:?}, target type is ({:?}, {})",
                self.item_type_id,
                std::any::TypeId::of::<T>(),
                std::any::type_name::<T>()
            ))?
        }
    }
}

unsafe impl<'a> Sync for SliceRef<'a> {}
unsafe impl<'a> Sync for SliceRefMut<'a> {}

unsafe impl<'a> Send for SliceRef<'a> {}
unsafe impl<'a> Send for SliceRefMut<'a> {}

#[derive(Debug)]
pub struct SliceRefMut<'a> {
    item_type_id: std::any::TypeId,
    len: usize,
    ptr: *mut u8,
    phantom: std::marker::PhantomData<&'a u8>,
}

impl<'a> SliceRefMut<'a> {
    pub fn item_type_id(&self) -> std::any::TypeId {
        self.item_type_id
    }
    pub fn new<T>(s: &'a mut [T]) -> Self
    where
        T: 'static + Sync,
    {
        SliceRefMut {
            item_type_id: std::any::TypeId::of::<T>(),
            len: s.len(),
            ptr: s.as_mut_ptr() as *mut u8,
            phantom: PhantomData,
        }
    }
    pub(crate) fn is<T>(&self) -> bool
    where
        T: 'static,
    {
        // Get `TypeId` of the type this function is instantiated with.
        let t = std::any::TypeId::of::<T>();

        // Get `TypeId` of the type in the trait object (`self`).
        let concrete = self.item_type_id;

        // Compare both `TypeId`s on equality.
        t == concrete
    }

    pub fn downcast_ref<T>(&self) -> Result<&[T], ErrorDesc>
    where
        T: 'static + Sync,
    {
        if self.is::<T>() {
            let ptr = self.ptr;
            let len = self.len;
            // SAFETY: just checked whether we are pointing to the correct type, and we can rely on
            // that check for memory safety because we have implemented Any for all types; no other
            // impls can exist as they would conflict with our impl.
            unsafe {
                let ptr = ptr as *const T;
                Ok(std::slice::from_raw_parts(ptr, len))
            }
        } else {
            Err(format!(
                "Downcast failed. Source type is {:?}, target type is ({:?}, {})",
                self.item_type_id,
                std::any::TypeId::of::<T>(),
                std::any::type_name::<T>()
            ))?
        }
    }
    pub fn downcast_mut<T>(&mut self) -> Result<&mut [T], ErrorDesc>
    where
        T: 'static + Sync,
    {
        if self.is::<T>() {
            let ptr = self.ptr;
            let len = self.len;
            // SAFETY: just checked whether we are pointing to the correct type, and we can rely on
            // that check for memory safety because we have implemented Any for all types; no other
            // impls can exist as they would conflict with our impl.
            unsafe {
                let ptr = ptr as *mut T;
                Ok(std::slice::from_raw_parts_mut(ptr, len))
            }
        } else {
            Err(format!(
                "Downcast failed. Source type is {:?}, target type is ({:?}, {})",
                self.item_type_id,
                std::any::TypeId::of::<T>(),
                std::any::type_name::<T>()
            ))?
        }
    }
    pub fn split_off_left<T>(&mut self, pos: usize) -> Result<Self, ErrorDesc>
    where
        T: 'static + Sync,
    {
        if pos > self.len {
            Err(format!(
                "Attempt to split at slice at position {}, while the source slice has length {}",
                pos, self.len,
            ))?
        } else if self.is::<T>() {
            let l = Self {
                item_type_id: self.item_type_id,
                len: pos,
                ptr: self.ptr,
                phantom: self.phantom,
            };

            self.len = self.len - pos;
            self.ptr = unsafe { (self.ptr as *mut T).offset(pos as isize) as *mut u8 };
            Ok(l)
        } else {
            Err(format!(
                "Downcast failed. Source type is {:?}, target type is ({:?}, {})",
                self.item_type_id,
                std::any::TypeId::of::<T>(),
                std::any::type_name::<T>()
            ))?
        }
    }
}
