use std::any::TypeId;

use super::{asbytes::AsBytes, ErrorDesc};

#[derive(Debug)]
pub struct OnwedBinaryColumn {
    item_type_id: std::any::TypeId,
    offset: usize,
    data: Vec<u8>,
    start_pos: Vec<usize>,
    len: Vec<usize>,
}

impl OnwedBinaryColumn {
    pub fn item_type_id(&self) -> TypeId {
        self.item_type_id
    }

    pub fn len(&self) -> usize {
        self.len.len()
    }

    pub fn truncate(&mut self) {
        self.offset = 0;
        self.data.truncate(0);
        self.start_pos.truncate(0);
        self.len.truncate(0);
    }

    pub fn new<T: 'static + AsBytes>(data: &[T]) -> Self {
        OnwedBinaryColumn::new_with_capacity(data, 0, 0)
    }
    pub fn new_with_capacity<T: 'static + AsBytes>(
        data: &[T],
        _capacity: usize,
        binarycapacity: usize,
    ) -> Self {
        let mut cur_pos = 0;
        let data_len = data.len();

        let mut start_pos: Vec<usize> = Vec::with_capacity(data_len);
        let mut len: Vec<usize> = Vec::with_capacity(data_len);
        let mut datau8: Vec<u8> = Vec::with_capacity(binarycapacity);

        data.iter().for_each(|t| {
            let t_as_u8 = <T as AsBytes>::as_bytes(t);
            len.push(t_as_u8.len());
            start_pos.push(cur_pos);
            datau8.extend_from_slice(t_as_u8);
            cur_pos += t_as_u8.len();
        });

        Self {
            item_type_id: std::any::TypeId::of::<T>(),
            offset: 0,
            data: datau8,
            start_pos,
            len,
        }
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
    pub fn downcast_binary_owned<T>(
        self,
    ) -> Result<(Vec<u8>, Vec<usize>, Vec<usize>, usize), ErrorDesc>
    where
        T: Send + Sync + 'static,
    {
        if self.is::<T>() {
            Ok((self.data, self.start_pos, self.len, self.offset))
        } else {
            Err(format!(
                "Downcast failed. Source type is {:?}, target type is ({:?}, {})",
                self.item_type_id,
                std::any::TypeId::of::<T>(),
                std::any::type_name::<T>()
            ))?
        }
    }

    pub fn downcast_binary_vec<T>(
        &mut self,
    ) -> Result<(&mut Vec<u8>, &mut Vec<usize>, &mut Vec<usize>, &mut usize), ErrorDesc>
    where
        T: Send + Sync + 'static,
    {
        if self.is::<T>() {
            Ok((
                &mut self.data,
                &mut self.start_pos,
                &mut self.len,
                &mut self.offset,
            ))
        } else {
            Err(format!(
                "Downcast failed. Source type is {:?}, target type is ({:?}, {})",
                self.item_type_id,
                std::any::TypeId::of::<T>(),
                std::any::type_name::<T>()
            ))?
        }
    }
    pub fn downcast_binary_mut<T>(
        &mut self,
    ) -> Result<(&mut [u8], &mut [usize], &mut [usize], &mut usize), ErrorDesc>
    where
        T: 'static,
    {
        if self.is::<T>() {
            Ok((
                self.data.as_mut_slice(),
                self.start_pos.as_mut_slice(),
                self.len.as_mut_slice(),
                &mut self.offset,
            ))
        } else {
            Err(format!(
                "Downcast failed. Source type is {:?}, target type is ({:?}, {})",
                self.item_type_id,
                std::any::TypeId::of::<T>(),
                std::any::type_name::<T>()
            ))?
        }
    }
    pub fn downcast_binary_ref<'b, T>(
        &'b self,
    ) -> Result<(&'b [u8], &'b [usize], &'b [usize], &'b usize), ErrorDesc>
    where
        T: 'static,
    {
        if self.is::<T>() {
            Ok((
                self.data.as_slice(),
                self.start_pos.as_slice(),
                self.len.as_slice(),
                &self.offset,
            ))
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

#[derive(Debug)]
pub struct BinarySliceMut<'a> {
    item_type_id: std::any::TypeId,
    offset: usize,
    data: &'a mut [u8],
    start_pos: &'a mut [usize],
    len: &'a mut [usize],
}

impl<'a> BinarySliceMut<'a> {
    pub fn new<T: 'static>(
        offset: usize,
        data: &'a mut [u8],
        start_pos: &'a mut [usize],
        len: &'a mut [usize],
    ) -> Self {
        assert_eq!(start_pos.len(), len.len());
        if !start_pos.is_empty() {
            let end_pos_last_element = start_pos[start_pos.len() - 1] + len[len.len() - 1];
            assert!(data.len() >= end_pos_last_element);
        }
        Self {
            item_type_id: std::any::TypeId::of::<T>(),
            offset,
            data,
            start_pos,
            len,
        }
    }

    pub fn offset(&self) -> usize {
        self.offset
    }
    pub fn item_type_id(&self) -> std::any::TypeId {
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
    pub fn downcast_binary_mut<T>(
        &mut self,
    ) -> Result<(&mut [u8], &mut [usize], &mut [usize], &mut usize), ErrorDesc>
    where
        T: 'static,
    {
        if self.is::<T>() {
            Ok((self.data, self.start_pos, self.len, &mut self.offset))
        } else {
            Err(format!(
                "Downcast failed. Source type is {:?}, target type is ({:?}, {})",
                self.item_type_id,
                std::any::TypeId::of::<T>(),
                std::any::type_name::<T>()
            ))?
        }
    }
    pub fn downcast_binary_ref<T>(&self) -> Result<(&[u8], &[usize], &[usize], &usize), ErrorDesc>
    where
        T: 'static,
    {
        if self.is::<T>() {
            Ok((self.data, self.start_pos, self.len, &self.offset))
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
        if pos > self.start_pos.len() {
            Err(format!(
                "Attempt to split at slice at position {}, while the source slice has length {}",
                pos,
                self.start_pos.len(),
            ))?
        } else if self.is::<T>() {
            let tmp = std::mem::replace(&mut self.start_pos, &mut []);
            let (s, tail) = tmp.split_at_mut(pos);
            self.start_pos = tail;

            let tmp = std::mem::replace(&mut self.len, &mut []);
            let (l, tail) = tmp.split_at_mut(pos);
            self.len = tail;

            let mut data_len = 0;
            if !s.is_empty() {
                data_len = s[s.len() - 1] + l[l.len() - 1] - self.offset;
            }

            let tmp = std::mem::replace(&mut self.data, &mut []);
            let (d, tail) = tmp.split_at_mut(data_len);
            self.data = tail;

            let l = Self {
                item_type_id: self.item_type_id,
                offset: self.offset,
                data: d,
                start_pos: s,
                len: l,
            };

            self.offset += data_len;
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

#[derive(Debug)]
pub struct BinarySlice<'a> {
    item_type_id: std::any::TypeId,
    offset: usize,
    data: &'a [u8],
    start_pos: &'a [usize],
    len: &'a [usize],
}

impl<'a> BinarySlice<'a> {
    pub fn new<T: 'static>(
        offset: usize,
        data: &'a [u8],
        start_pos: &'a [usize],
        len: &'a [usize],
    ) -> Self {
        assert_eq!(start_pos.len(), len.len());
        if !start_pos.is_empty() {
            let end_pos_last_element = start_pos[start_pos.len() - 1] + len[len.len() - 1];
            assert!(data.len() >= end_pos_last_element);
        }
        Self {
            item_type_id: std::any::TypeId::of::<T>(),
            offset,
            data,
            start_pos,
            len,
        }
    }
    pub fn offset(&self) -> usize {
        self.offset
    }
    pub fn item_type_id(&self) -> std::any::TypeId {
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
    pub fn downcast_binary_ref<T>(&self) -> Result<(&[u8], &[usize], &[usize], &usize), ErrorDesc>
    where
        T: 'static,
    {
        if self.is::<T>() {
            Ok((self.data, self.start_pos, self.len, &self.offset))
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
        if pos > self.start_pos.len() {
            Err(format!(
                "Attempt to split at slice at position {}, while the source slice has length {}",
                pos,
                self.start_pos.len(),
            ))?
        } else if self.is::<T>() {
            let (s, tmp) = self.start_pos.split_at(pos);
            self.start_pos = tmp;

            let (l, tmp) = self.len.split_at(pos);
            self.len = tmp;

            let mut data_len = 0;
            if !s.is_empty() {
                data_len = s[s.len() - 1] + l[l.len() - 1] - self.offset;
            }
            let (d, tmp) = self.data.split_at(data_len);
            self.data = tmp;

            let l = Self {
                item_type_id: self.item_type_id,
                offset: self.offset,
                data: d,
                start_pos: s,
                len: l,
            };

            self.offset += data_len;
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
