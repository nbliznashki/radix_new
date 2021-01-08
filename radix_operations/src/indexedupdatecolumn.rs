use radix_column::{ColumnData, ColumnDataF, ColumnDataIndex, ColumnWrapper};

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
    pub fn apply<F, I>(&mut self, iter: I, mut f: F)
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
}
pub struct IUCBitmapNoIndex<'a, T> {
    pub data: &'a mut [T],
    pub bitmap: &'a mut [bool],
}
impl<'a, T> IUCBitmapNoIndex<'a, T> {
    #[inline]
    pub fn apply<F, I>(&mut self, iter: I, mut f: F)
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
}
pub struct IUCNoBitmapIndex<'a, T> {
    pub data: &'a mut [T],
    pub index: &'a [usize],
}

impl<'a, T> IUCNoBitmapIndex<'a, T> {
    #[inline]
    pub fn apply<F, I>(&mut self, iter: I, mut f: F)
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
}

pub struct IUCBitmapIndex<'a, T> {
    pub data: &'a mut [T],
    pub index: &'a [usize],
    pub bitmap: &'a mut [bool],
}

impl<'a, T> IUCBitmapIndex<'a, T> {
    #[inline]
    pub fn apply<F, I>(&mut self, iter: I, mut f: F)
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
}

pub enum UpdateColumn<'a, T> {
    BitmapIndex(IUCBitmapIndex<'a, T>),
    BitmapNoIndex(IUCBitmapNoIndex<'a, T>),
    NoBitmapIndex(IUCNoBitmapIndex<'a, T>),
    NoBitmapNoIndex(IUCNoBitmapNoIndex<'a, T>),
}

impl<'a, T: Send + Sync + 'static>
    From<(
        &'a mut ColumnData<'a>,
        &'a mut ColumnDataF<'a, bool>,
        &'a ColumnDataIndex<'a>,
    )> for UpdateColumn<'a, T>
{
    fn from(
        (data, bitmap, index): (
            &'a mut ColumnData,
            &'a mut ColumnDataF<'a, bool>,
            &'a ColumnDataIndex<'a>,
        ),
    ) -> Self {
        let data = data.downcast_mut::<T>().unwrap();
        match (bitmap.is_some(), index.is_some()) {
            (true, true) => Self::BitmapIndex(IUCBitmapIndex {
                data: data,
                bitmap: bitmap.downcast_mut().unwrap(),
                index: index.downcast_ref().unwrap(),
            }),
            (true, false) => Self::BitmapNoIndex(IUCBitmapNoIndex {
                data: data,
                bitmap: bitmap.downcast_mut().unwrap(),
            }),
            (false, true) => Self::NoBitmapIndex(IUCNoBitmapIndex {
                data: data,
                index: index.downcast_ref().unwrap(),
            }),
            (false, false) => Self::NoBitmapNoIndex(IUCNoBitmapNoIndex { data: data }),
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
    pub fn from_destination(c: &'a mut ColumnWrapper<'a>, c_index: &'a ColumnDataIndex) -> Self
    where
        T: 'static + Send + Sync,
    {
        let (c_col, c_bitmap) = c.get_inner_mut();
        let c_read_column: UpdateColumn<T> = UpdateColumn::from((c_col, c_bitmap, c_index));
        c_read_column
    }

    pub fn apply<F, I>(&mut self, iter: I, mut f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(&mut T, &mut bool, I::Item),
    {
        match self {
            Self::BitmapIndex(c) => c.apply(iter, f),
            Self::BitmapNoIndex(c) => c.apply(iter, f),
            Self::NoBitmapIndex(c) => c.apply(iter, f),
            Self::NoBitmapNoIndex(c) => c.apply(iter, f),
        }
    }
}
