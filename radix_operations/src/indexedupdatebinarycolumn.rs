use std::marker::PhantomData;

use radix_column::{ColumnData, ColumnDataF, ColumnDataIndex, ColumnWrapper};

////////////////////////////////////
////                            ////
////  Update Binary Column      ////
////                            ////
////////////////////////////////////

pub struct IUCBinaryNoBitmapNoIndex<'a, T> {
    pub data: &'a mut [u8],
    pub start_pos: &'a [usize],
    pub len: &'a [usize],
    pub offset: usize,
    _phantom: PhantomData<T>,
}
impl<'a, T> IUCBinaryNoBitmapNoIndex<'a, T> {
    #[inline]
    pub fn update<F, I>(&mut self, iter: I, mut f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(&mut [u8], &mut bool, I::Item),
    {
        self.start_pos
            .iter()
            .zip(self.len)
            .zip(iter)
            .for_each(|((s, l), source)| {
                f(
                    &mut self.data[*s - self.offset..*s - self.offset + *l],
                    &mut true,
                    source,
                )
            });
    }
}
pub struct IUCBinaryBitmapNoIndex<'a, T> {
    pub data: &'a mut [u8],
    pub start_pos: &'a [usize],
    pub len: &'a [usize],
    pub offset: usize,
    pub bitmap: &'a mut [bool],
    _phantom: PhantomData<T>,
}
impl<'a, T> IUCBinaryBitmapNoIndex<'a, T> {
    #[inline]
    pub fn update<F, I>(&mut self, iter: I, mut f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(&mut [u8], &mut bool, I::Item),
    {
        let (data, start_pos, len, offset, bitmap) = (
            &mut *self.data,
            self.start_pos,
            self.len,
            self.offset,
            &mut *self.bitmap,
        );
        start_pos
            .iter()
            .zip(len)
            .zip(bitmap.iter_mut())
            .zip(iter)
            .for_each(|(((s, l), b), source)| {
                f(&mut data[*s - offset..*s - offset + *l], b, source)
            });
    }
}
pub struct IUCBinaryNoBitmapIndex<'a, T> {
    pub data: &'a mut [u8],
    pub start_pos: &'a [usize],
    pub len: &'a [usize],
    pub offset: usize,
    pub index: &'a [usize],
    _phantom: PhantomData<T>,
}

impl<'a, T> IUCBinaryNoBitmapIndex<'a, T> {
    #[inline]
    pub fn update<F, I>(&mut self, iter: I, mut f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(&mut [u8], &mut bool, I::Item),
    {
        self.index.iter().zip(iter).for_each(|(i, source)| {
            f(
                &mut self.data[self.start_pos[*i] - self.offset
                    ..self.start_pos[*i] - self.offset + self.len[*i]],
                &mut true,
                source,
            )
        });
    }
}

pub struct IUCBinaryBitmapIndex<'a, T> {
    pub data: &'a mut [u8],
    pub start_pos: &'a [usize],
    pub len: &'a [usize],
    pub offset: usize,
    pub index: &'a [usize],
    pub bitmap: &'a mut [bool],
    _phantom: PhantomData<T>,
}

impl<'a, T> IUCBinaryBitmapIndex<'a, T> {
    #[inline]
    pub fn update<F, I>(&mut self, iter: I, mut f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(&mut [u8], &mut bool, I::Item),
    {
        self.index.iter().zip(iter).for_each(|(i, source)| {
            f(
                &mut self.data[self.start_pos[*i] - self.offset
                    ..self.start_pos[*i] - self.offset + self.len[*i]],
                &mut self.bitmap[*i],
                source,
            )
        });
    }
}

pub enum UpdateBinaryColumn<'a, T> {
    BitmapIndex(IUCBinaryBitmapIndex<'a, T>),
    BitmapNoIndex(IUCBinaryBitmapNoIndex<'a, T>),
    NoBitmapIndex(IUCBinaryNoBitmapIndex<'a, T>),
    NoBitmapNoIndex(IUCBinaryNoBitmapNoIndex<'a, T>),
}

impl<'a, 'b, T: 'static + Send + Sync>
    From<(
        &'a mut ColumnData<'b>,
        &'a mut ColumnDataF<'b, bool>,
        &'a ColumnDataIndex<'a>,
    )> for UpdateBinaryColumn<'a, T>
{
    fn from(
        (data, bitmap, index): (
            &'a mut ColumnData<'b>,
            &'a mut ColumnDataF<'b, bool>,
            &'a ColumnDataIndex<'a>,
        ),
    ) -> Self {
        let (data, start_pos, len, offset) = data.downcast_binary_mut::<T>().unwrap();
        match (bitmap.is_some(), index.is_some()) {
            (true, true) => Self::BitmapIndex(IUCBinaryBitmapIndex {
                data,
                start_pos,
                len,
                offset: *offset,
                bitmap: bitmap.downcast_mut().unwrap(),
                index: index.downcast_ref().unwrap(),
                _phantom: PhantomData::<T>,
            }),
            (true, false) => Self::BitmapNoIndex(IUCBinaryBitmapNoIndex {
                data,
                start_pos,
                len,
                offset: *offset,
                bitmap: bitmap.downcast_mut().unwrap(),
                _phantom: PhantomData::<T>,
            }),
            (false, true) => Self::NoBitmapIndex(IUCBinaryNoBitmapIndex {
                data,
                start_pos,
                len,
                offset: *offset,
                index: index.downcast_ref().unwrap(),
                _phantom: PhantomData::<T>,
            }),
            (false, false) => Self::NoBitmapNoIndex(IUCBinaryNoBitmapNoIndex {
                data,
                start_pos,
                len,
                offset: *offset,
                _phantom: PhantomData::<T>,
            }),
        }
    }
}

impl<'a, T> UpdateBinaryColumn<'a, T> {
    pub fn len(&self) -> usize {
        match self {
            Self::BitmapIndex(c) => c.index.len(),
            Self::BitmapNoIndex(c) => c.len.len(),
            Self::NoBitmapIndex(c) => c.index.len(),
            Self::NoBitmapNoIndex(c) => c.len.len(),
        }
    }
    pub fn from_destination(c: &'a mut ColumnWrapper, c_index: &'a ColumnDataIndex) -> Self
    where
        T: 'static + Send + Sync,
    {
        let (c_col, c_bitmap) = c.get_inner_mut();
        let c_read_column = UpdateBinaryColumn::from((c_col, c_bitmap, c_index));
        c_read_column
    }

    pub fn update<F, I>(&mut self, iter: I, f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(&mut [u8], &mut bool, I::Item),
    {
        match self {
            Self::BitmapIndex(c) => c.update(iter, f),
            Self::BitmapNoIndex(c) => c.update(iter, f),
            Self::NoBitmapIndex(c) => c.update(iter, f),
            Self::NoBitmapNoIndex(c) => c.update(iter, f),
        }
    }
}
