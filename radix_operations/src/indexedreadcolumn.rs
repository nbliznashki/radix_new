use radix_column::{AsBytes, ColumnData, ColumnDataF, ColumnDataIndex};

use crate::InputTypes;

/////////////////////////////
////                     ////
////  Read Column        ////
////                     ////
/////////////////////////////

//Index Read Column types (IRC)
pub struct IRCNoBitmapNoIndex<'a, T> {
    pub data: &'a [T],
}
impl<'a, T> IRCNoBitmapNoIndex<'a, T> {
    #[inline]
    pub fn as_iter<'i>(&'i self) -> impl ExactSizeIterator<Item = (&T, &bool)> + 'i + Clone {
        self.data.iter().map(|t| (t, &true))
    }
    #[inline]
    pub fn as_binary_iter<'i>(
        &'i self,
    ) -> impl ExactSizeIterator<Item = (&[u8], &bool)> + 'i + Clone
    where
        T: AsBytes,
    {
        self.data.iter().map(|t| (t.as_bytes(), &true))
    }
}
pub struct IRCBitmapNoIndex<'a, T> {
    pub data: &'a [T],
    pub bitmap: &'a [bool],
}
impl<'a, T> IRCBitmapNoIndex<'a, T> {
    #[inline]
    pub fn as_iter<'i>(&'i self) -> impl ExactSizeIterator<Item = (&T, &bool)> + 'i + Clone {
        self.data.iter().zip(self.bitmap)
    }
    #[inline]
    pub fn as_binary_iter<'i>(
        &'i self,
    ) -> impl ExactSizeIterator<Item = (&[u8], &bool)> + 'i + Clone
    where
        T: AsBytes,
    {
        self.data.iter().map(|t| (t.as_bytes(), &true))
    }
}
pub struct IRCNoBitmapIndex<'a, T> {
    pub data: &'a [T],
    pub index: &'a [usize],
}

impl<'a, T> IRCNoBitmapIndex<'a, T> {
    #[inline]
    pub fn as_iter<'i>(&'i self) -> impl ExactSizeIterator<Item = (&T, &bool)> + 'i + Clone {
        self.index.iter().map(move |i| (&self.data[*i], &true))
    }
    #[inline]
    pub fn as_binary_iter<'i>(
        &'i self,
    ) -> impl ExactSizeIterator<Item = (&[u8], &bool)> + 'i + Clone
    where
        T: AsBytes,
    {
        self.index
            .iter()
            .map(move |i| (self.data[*i].as_bytes(), &true))
    }
}

pub struct IRCBitmapIndex<'a, T> {
    pub data: &'a [T],
    pub index: &'a [usize],
    pub bitmap: &'a [bool],
}

impl<'a, T> IRCBitmapIndex<'a, T> {
    #[inline]
    pub fn as_iter<'i>(&'i self) -> impl ExactSizeIterator<Item = (&T, &bool)> + 'i + Clone {
        self.index
            .iter()
            .map(move |i| (&self.data[*i], &self.bitmap[*i]))
    }
    #[inline]
    pub fn as_binary_iter<'i>(
        &'i self,
    ) -> impl ExactSizeIterator<Item = (&[u8], &bool)> + 'i + Clone
    where
        T: AsBytes,
    {
        self.index
            .iter()
            .map(move |i| (self.data[*i].as_bytes(), &self.bitmap[*i]))
    }
}

pub struct IRCConst<'a, T> {
    pub data: &'a T,
    pub bitmap: bool,
    pub target_len: usize,
}

impl<'a, T> IRCConst<'a, T> {
    #[inline]
    pub fn as_iter<'i>(&'i self) -> impl ExactSizeIterator<Item = (&T, &bool)> + 'i + Clone {
        (0..self.target_len)
            .into_iter()
            .map(move |_| (self.data, &self.bitmap))
    }
    #[inline]
    pub fn as_binary_iter<'i>(
        &'i self,
    ) -> impl ExactSizeIterator<Item = (&[u8], &bool)> + 'i + Clone
    where
        T: AsBytes,
    {
        (0..self.target_len)
            .into_iter()
            .map(move |_| (self.data.as_bytes(), &self.bitmap))
    }
}

pub enum ReadColumn<'a, T> {
    BitmapIndex(IRCBitmapIndex<'a, T>),
    BitmapNoIndex(IRCBitmapNoIndex<'a, T>),
    NoBitmapIndex(IRCNoBitmapIndex<'a, T>),
    NoBitmapNoIndex(IRCNoBitmapNoIndex<'a, T>),
    Const(IRCConst<'a, T>),
}

impl<'a, T: Send + Sync + 'static>
    From<(
        &'a ColumnData<'a>,
        &'a ColumnDataF<'a, bool>,
        &'a ColumnDataIndex<'a>,
        usize,
    )> for ReadColumn<'a, T>
{
    fn from(
        (data, bitmap, index, target_len): (
            &'a ColumnData,
            &'a ColumnDataF<'a, bool>,
            &'a ColumnDataIndex<'a>,
            usize,
        ),
    ) -> Self {
        let is_const = data.is_const();
        let data = data.downcast_ref::<T>().unwrap();
        if is_const {
            if bitmap.is_some() {
                Self::Const(IRCConst {
                    data: &data[0],
                    bitmap: bitmap.downcast_ref().unwrap()[0],
                    target_len: target_len,
                })
            } else {
                Self::Const(IRCConst {
                    data: &data[0],
                    bitmap: true,
                    target_len: target_len,
                })
            }
        } else {
            match (bitmap.is_some(), index.is_some()) {
                (true, true) => Self::BitmapIndex(IRCBitmapIndex {
                    data: &data,
                    bitmap: bitmap.downcast_ref().unwrap(),
                    index: index.downcast_ref().unwrap(),
                }),
                (true, false) => Self::BitmapNoIndex(IRCBitmapNoIndex {
                    data: &data,
                    bitmap: bitmap.downcast_ref().unwrap(),
                }),
                (false, true) => Self::NoBitmapIndex(IRCNoBitmapIndex {
                    data: &data,
                    index: index.downcast_ref().unwrap(),
                }),
                (false, false) => Self::NoBitmapNoIndex(IRCNoBitmapNoIndex { data: &data }),
            }
        }
    }
}

impl<'a, T> ReadColumn<'a, T> {
    pub fn len(&self) -> usize {
        match self {
            Self::BitmapIndex(c) => c.index.len(),
            Self::BitmapNoIndex(c) => c.data.len(),
            Self::NoBitmapIndex(c) => c.index.len(),
            Self::NoBitmapNoIndex(c) => c.data.len(),
            Self::Const(c) => c.target_len,
        }
    }
    pub fn update_len_if_const(&mut self, new_len: usize) {
        match self {
            Self::BitmapIndex(_) => {}
            Self::BitmapNoIndex(_) => {}
            Self::NoBitmapIndex(_) => {}
            Self::NoBitmapNoIndex(_) => {}
            Self::Const(c) => c.target_len = new_len,
        }
    }
    pub fn from_input(c: &'a InputTypes) -> Self
    where
        T: 'static + Send + Sync,
    {
        let (c, c_index) = match c {
            InputTypes::Ref(c, i) => (*c, *i),
            InputTypes::Owned(c, i) => (c, i),
        };

        let (c_col, c_bitmap) = c.get_inner_ref();
        let c_read_column: ReadColumn<T> = ReadColumn::from((c_col, c_bitmap, c_index, 1));
        c_read_column
    }
}