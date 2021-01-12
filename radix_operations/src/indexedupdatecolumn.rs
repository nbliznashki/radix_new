use radix_column::{
    ColumnData, ColumnDataF, ColumnDataFMut, ColumnDataIndex, ColumnDataIndexRef, ColumnWrapper,
};

/////////////////////////////
////                     ////
////  Update Column      ////
////                     ////
/////////////////////////////

//Index Read Column types (IUC)
pub struct IUCNoBitmapNoIndex<'a, T> {
    pub data: &'a mut [T],
}
impl<'a, T> IUCNoBitmapNoIndex<'a, T> {
    #[inline]
    pub fn update<F, I>(&mut self, iter: I, mut f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(&mut T, &mut bool, I::Item),
    {
        &mut self
            .data
            .iter_mut()
            .zip(iter)
            .for_each(|(d, source)| f(d, &mut true, source));
    }
    #[inline]
    pub fn assign<F, I>(&mut self, iter: I, mut f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(I::Item) -> (bool, T),
    {
        &mut self
            .data
            .iter_mut()
            .zip(iter)
            .for_each(|(d, source)| *d = f(source).1);
    }
}
pub struct IUCBitmapNoIndex<'a, T> {
    pub data: &'a mut [T],
    pub bitmap: &'a mut [bool],
}
impl<'a, T> IUCBitmapNoIndex<'a, T> {
    #[inline]
    pub fn update<F, I>(&mut self, iter: I, mut f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(&mut T, &mut bool, I::Item),
    {
        &mut self
            .data
            .iter_mut()
            .zip(self.bitmap.iter_mut())
            .zip(iter)
            .for_each(|((data, bitmap), source)| f(data, bitmap, source));
    }
    #[inline]
    pub fn assign<F, I>(&mut self, iter: I, mut f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(I::Item) -> (bool, T),
    {
        &mut self
            .data
            .iter_mut()
            .zip(self.bitmap.iter_mut())
            .zip(iter)
            .for_each(|((data, bitmap), source)| {
                let (b_new, d_new) = f(source);
                *data = d_new;
                *bitmap = b_new;
            });
    }
}
pub struct IUCNoBitmapIndex<'a, T> {
    pub data: &'a mut [T],
    pub index: &'a [usize],
}

impl<'a, T> IUCNoBitmapIndex<'a, T> {
    #[inline]
    pub fn update<F, I>(&mut self, iter: I, mut f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(&mut T, &mut bool, I::Item),
    {
        &mut self
            .index
            .iter()
            .zip(iter)
            .for_each(|(i, source)| f(&mut self.data[*i], &mut true, source));
    }
    #[inline]
    pub fn assign<F, I>(&mut self, iter: I, mut f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(I::Item) -> (bool, T),
    {
        &mut self.index.iter().zip(iter).for_each(|(i, source)| {
            let (_, d_new) = f(source);
            self.data[*i] = d_new;
        });
    }
}

pub struct IUCBitmapIndex<'a, T> {
    pub data: &'a mut [T],
    pub index: &'a [usize],
    pub bitmap: &'a mut [bool],
}

impl<'a, T> IUCBitmapIndex<'a, T> {
    #[inline]
    pub fn update<F, I>(&mut self, iter: I, mut f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(&mut T, &mut bool, I::Item),
    {
        &mut self
            .index
            .iter()
            .zip(iter)
            .for_each(|(i, source)| f(&mut self.data[*i], &mut self.bitmap[*i], source));
    }
    #[inline]
    pub fn assign<F, I>(&mut self, iter: I, mut f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(I::Item) -> (bool, T),
    {
        &mut self.index.iter().zip(iter).for_each(|(i, source)| {
            let (b_new, d_new) = f(source);
            self.data[*i] = d_new;
            self.bitmap[*i] = b_new;
        });
    }
}

pub enum UpdateColumn<'a, T> {
    BitmapIndex(IUCBitmapIndex<'a, T>),
    BitmapNoIndex(IUCBitmapNoIndex<'a, T>),
    NoBitmapIndex(IUCNoBitmapIndex<'a, T>),
    NoBitmapNoIndex(IUCNoBitmapNoIndex<'a, T>),
}

impl<'a, 'b, T: Send + Sync + 'static>
    From<(
        &'a mut ColumnData<'b>,
        &'a mut ColumnDataF<'b, bool>,
        &'a ColumnDataIndex<'a>,
    )> for UpdateColumn<'a, T>
{
    fn from(
        (data, bitmap, index): (
            &'a mut ColumnData<'b>,
            &'a mut ColumnDataF<'b, bool>,
            &'a ColumnDataIndex<'a>,
        ),
    ) -> Self {
        let data = data.downcast_mut::<T>().unwrap();
        let bitmap = bitmap.to_mut();
        let index = index.to_ref();

        match (bitmap, index) {
            (ColumnDataFMut::Some(bitmap), ColumnDataIndexRef::Some(index)) => {
                Self::BitmapIndex(IUCBitmapIndex {
                    data,
                    bitmap,
                    index,
                })
            }
            (ColumnDataFMut::Some(_bitmap), ColumnDataIndexRef::SomeOption(_index)) => {
                panic!("Update column indexed by Option<usize> is not supported")
            }
            (ColumnDataFMut::Some(bitmap), ColumnDataIndexRef::None) => {
                Self::BitmapNoIndex(IUCBitmapNoIndex { data, bitmap })
            }
            (ColumnDataFMut::None, ColumnDataIndexRef::Some(index)) => {
                Self::NoBitmapIndex(IUCNoBitmapIndex { data, index })
            }
            (ColumnDataFMut::None, ColumnDataIndexRef::SomeOption(_index)) => {
                panic!("Update column indexed by Option<usize> is not supported")
            }
            (ColumnDataFMut::None, ColumnDataIndexRef::None) => {
                Self::NoBitmapNoIndex(IUCNoBitmapNoIndex { data })
            }
        }
    }
}

impl<'a, T> UpdateColumn<'a, T> {
    pub fn len(&self) -> usize {
        match self {
            Self::BitmapIndex(c) => c.index.len(),
            Self::BitmapNoIndex(c) => c.data.len(),
            Self::NoBitmapIndex(c) => c.index.len(),
            Self::NoBitmapNoIndex(c) => c.data.len(),
        }
    }
    pub fn from_destination(c: &'a mut ColumnWrapper, c_index: &'a ColumnDataIndex) -> Self
    where
        T: 'static + Send + Sync,
    {
        let (c_col, c_bitmap) = c.get_inner_mut();
        let c_read_column: UpdateColumn<T> = UpdateColumn::from((c_col, c_bitmap, c_index));
        c_read_column
    }

    pub fn update<F, I>(&mut self, iter: I, f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(&mut T, &mut bool, I::Item),
    {
        match self {
            Self::BitmapIndex(c) => c.update(iter, f),
            Self::BitmapNoIndex(c) => c.update(iter, f),
            Self::NoBitmapIndex(c) => c.update(iter, f),
            Self::NoBitmapNoIndex(c) => c.update(iter, f),
        }
    }
    pub fn assign<F, I>(&mut self, iter: I, f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(I::Item) -> (bool, T),
    {
        match self {
            Self::BitmapIndex(c) => c.assign(iter, f),
            Self::BitmapNoIndex(c) => c.assign(iter, f),
            Self::NoBitmapIndex(c) => c.assign(iter, f),
            Self::NoBitmapNoIndex(c) => c.assign(iter, f),
        }
    }
}
