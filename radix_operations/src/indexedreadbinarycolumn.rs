use radix_column::{ColumnData, ColumnDataF, ColumnDataIndex};

use crate::{
    IRCBitmapIndex, IRCBitmapNoIndex, IRCConst, IRCNoBitmapIndex, IRCNoBitmapNoIndex, InputTypes,
    ReadColumn,
};

/////////////////////////////
////                     ////
////  Binary Read Column ////
////                     ////
/////////////////////////////

//Index Read Column types (IRC)
pub struct IRCBinaryNoBitmapNoIndex<'a> {
    pub data: &'a [u8],
    pub start_pos: &'a [usize],
    pub len: &'a [usize],
    pub offset: usize,
}

impl<'a> IRCBinaryNoBitmapNoIndex<'a> {
    #[inline]
    pub fn as_binary_iter<'i>(
        &'i self,
    ) -> impl ExactSizeIterator<Item = (&[u8], &bool)> + 'i + Clone {
        self.start_pos
            .iter()
            .zip(self.len)
            .map(move |(s, l)| (&self.data[s - self.offset..s + l - self.offset], &true))
    }
}
pub struct IRCBinaryBitmapNoIndex<'a> {
    pub data: &'a [u8],
    pub start_pos: &'a [usize],
    pub len: &'a [usize],
    pub offset: usize,
    pub bitmap: &'a [bool],
}
impl<'a> IRCBinaryBitmapNoIndex<'a> {
    #[inline]
    pub fn as_binary_iter<'i>(
        &'i self,
    ) -> impl ExactSizeIterator<Item = (&[u8], &bool)> + 'i + Clone {
        self.start_pos
            .iter()
            .zip(self.len)
            .zip(self.bitmap)
            .map(move |((s, l), b)| (&self.data[s - self.offset..s + l - self.offset], b))
    }
}
pub struct IRCBinaryNoBitmapIndex<'a> {
    pub data: &'a [u8],
    pub start_pos: &'a [usize],
    pub len: &'a [usize],
    pub offset: usize,
    pub index: &'a [usize],
}

impl<'a> IRCBinaryNoBitmapIndex<'a> {
    #[inline]
    pub fn as_binary_iter<'i>(
        &'i self,
    ) -> impl ExactSizeIterator<Item = (&[u8], &bool)> + 'i + Clone {
        self.index.iter().map(move |i| {
            (
                &self.data[self.start_pos[*i] - self.offset
                    ..self.start_pos[*i] + self.len[*i] - self.offset],
                &true,
            )
        })
    }
}

pub struct IRCBinaryBitmapIndex<'a> {
    pub data: &'a [u8],
    pub start_pos: &'a [usize],
    pub len: &'a [usize],
    pub offset: usize,
    pub index: &'a [usize],
    pub bitmap: &'a [bool],
}

impl<'a> IRCBinaryBitmapIndex<'a> {
    #[inline]
    pub fn as_binary_iter<'i>(
        &'i self,
    ) -> impl ExactSizeIterator<Item = (&[u8], &bool)> + 'i + Clone {
        self.index.iter().zip(self.bitmap).map(move |(i, b)| {
            (
                &self.data[self.start_pos[*i] - self.offset
                    ..self.start_pos[*i] + self.len[*i] - self.offset],
                b,
            )
        })
    }
}

pub struct IRCBinaryConst<'a> {
    pub data: &'a [u8],
    pub bitmap: bool,
    pub target_len: usize,
}

impl<'a> IRCBinaryConst<'a> {
    #[inline]
    pub fn as_binary_iter<'i>(
        &'i self,
    ) -> impl ExactSizeIterator<Item = (&[u8], &bool)> + 'i + Clone {
        (0..self.target_len)
            .into_iter()
            .map(move |_| (self.data, &self.bitmap))
    }
}

pub enum ReadBinaryColumn<'a, T> {
    BitmapIndex(IRCBinaryBitmapIndex<'a>),
    BitmapNoIndex(IRCBinaryBitmapNoIndex<'a>),
    NoBitmapIndex(IRCBinaryNoBitmapIndex<'a>),
    NoBitmapNoIndex(IRCBinaryNoBitmapNoIndex<'a>),
    Const(IRCBinaryConst<'a>),
    BitmapIndexOrig(IRCBitmapIndex<'a, T>),
    BitmapNoIndexOrig(IRCBitmapNoIndex<'a, T>),
    NoBitmapIndexOrig(IRCNoBitmapIndex<'a, T>),
    NoBitmapNoIndexOrig(IRCNoBitmapNoIndex<'a, T>),
    ConstOrig(IRCConst<'a, T>),
}

impl<'a, T: Send + Sync + 'static>
    From<(
        &'a ColumnData<'a>,
        &'a ColumnDataF<'a, bool>,
        &'a ColumnDataIndex<'a>,
        usize,
    )> for ReadBinaryColumn<'a, T>
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
        let (data, start_pos, len, offset) = data.downcast_binary_ref::<T>().unwrap();
        let offset = *offset;
        if is_const {
            if bitmap.is_some() {
                Self::Const(IRCBinaryConst {
                    data: &data[start_pos[0] - offset..start_pos[0] - offset + len[0]],
                    bitmap: bitmap.downcast_ref().unwrap()[0],
                    target_len: target_len,
                })
            } else {
                Self::Const(IRCBinaryConst {
                    data: &data[start_pos[0] - offset..start_pos[0] - offset + len[0]],
                    bitmap: true,
                    target_len: target_len,
                })
            }
        } else {
            match (bitmap.is_some(), index.is_some()) {
                (true, true) => Self::BitmapIndex(IRCBinaryBitmapIndex {
                    data,
                    start_pos,
                    len,
                    offset,
                    bitmap: bitmap.downcast_ref().unwrap(),
                    index: index.downcast_ref().unwrap(),
                }),
                (true, false) => Self::BitmapNoIndex(IRCBinaryBitmapNoIndex {
                    data,
                    start_pos,
                    len,
                    offset,
                    bitmap: bitmap.downcast_ref().unwrap(),
                }),
                (false, true) => Self::NoBitmapIndex(IRCBinaryNoBitmapIndex {
                    data,
                    start_pos,
                    len,
                    offset,
                    index: index.downcast_ref().unwrap(),
                }),
                (false, false) => Self::NoBitmapNoIndex(IRCBinaryNoBitmapNoIndex {
                    data,
                    start_pos,
                    len,
                    offset,
                }),
            }
        }
    }
}

impl<'a, T> ReadBinaryColumn<'a, T> {
    pub fn len(&self) -> usize {
        match self {
            Self::BitmapIndex(c) => c.index.len(),
            Self::BitmapNoIndex(c) => c.start_pos.len(),
            Self::NoBitmapIndex(c) => c.index.len(),
            Self::NoBitmapNoIndex(c) => c.start_pos.len(),
            Self::Const(c) => c.target_len,
            Self::BitmapIndexOrig(c) => c.index.len(),
            Self::BitmapNoIndexOrig(c) => c.data.len(),
            Self::NoBitmapIndexOrig(c) => c.index.len(),
            Self::NoBitmapNoIndexOrig(c) => c.data.len(),
            Self::ConstOrig(c) => c.target_len,
        }
    }
    pub fn update_len_if_const(&mut self, new_len: usize) {
        match self {
            Self::BitmapIndex(_) => {}
            Self::BitmapNoIndex(_) => {}
            Self::NoBitmapIndex(_) => {}
            Self::NoBitmapNoIndex(_) => {}
            Self::Const(c) => c.target_len = new_len,
            Self::BitmapIndexOrig(_) => {}
            Self::BitmapNoIndexOrig(_) => {}
            Self::NoBitmapIndexOrig(_) => {}
            Self::NoBitmapNoIndexOrig(_) => {}
            Self::ConstOrig(c) => c.target_len = new_len,
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
        if c.is_binary() {
            let c_read_column: ReadBinaryColumn<T> =
                ReadBinaryColumn::from((c_col, c_bitmap, c_index, 1));
            c_read_column
        } else {
            let c_read_column: ReadColumn<T> = ReadColumn::from((c_col, c_bitmap, c_index, 1));
            match c_read_column {
                ReadColumn::BitmapIndex(c) => ReadBinaryColumn::BitmapIndexOrig(c),
                ReadColumn::BitmapNoIndex(c) => ReadBinaryColumn::BitmapNoIndexOrig(c),
                ReadColumn::NoBitmapIndex(c) => ReadBinaryColumn::NoBitmapIndexOrig(c),
                ReadColumn::NoBitmapNoIndex(c) => ReadBinaryColumn::NoBitmapNoIndexOrig(c),
                ReadColumn::Const(c) => ReadBinaryColumn::ConstOrig(c),
            }
        }
    }
}
