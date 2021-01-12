use radix_column::{
    AsBytes, ColumnData, ColumnDataF, ColumnDataFRef, ColumnDataIndex, ColumnDataIndexRef,
};

use crate::{
    IRCBitmapIndex, IRCBitmapIndexOption, IRCBitmapNoIndex, IRCConst, IRCNoBitmapIndex,
    IRCNoBitmapIndexOption, IRCNoBitmapNoIndex, InputTypes, ReadColumn,
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
    #[inline]
    pub fn index(&self, i: usize) -> (bool, &[u8]) {
        (
            true,
            &self.data
                [self.start_pos[i] - self.offset..self.start_pos[i] + self.len[i] - self.offset],
        )
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
    #[inline]
    pub fn index(&self, i: usize) -> (bool, &[u8]) {
        (
            self.bitmap[i],
            &self.data
                [self.start_pos[i] - self.offset..self.start_pos[i] + self.len[i] - self.offset],
        )
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
    #[inline]
    pub fn index(&self, i: usize) -> (bool, &[u8]) {
        let i = self.index[i];
        (
            true,
            &self.data
                [self.start_pos[i] - self.offset..self.start_pos[i] + self.len[i] - self.offset],
        )
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
    pub fn index(&self, i: usize) -> (bool, &[u8]) {
        let i = self.index[i];
        (
            self.bitmap[i],
            &self.data
                [self.start_pos[i] - self.offset..self.start_pos[i] + self.len[i] - self.offset],
        )
    }
}
///////////////////////////////////////
pub struct IRCBinaryNoBitmapIndexOption<'a> {
    pub data: &'a [u8],
    pub start_pos: &'a [usize],
    pub len: &'a [usize],
    pub offset: usize,
    pub index: &'a [Option<usize>],
}

impl<'a> IRCBinaryNoBitmapIndexOption<'a> {
    #[inline]
    pub fn as_binary_iter<'i>(
        &'i self,
    ) -> impl ExactSizeIterator<Item = (&[u8], &bool)> + 'i + Clone {
        self.index.iter().map(move |i| {
            i.map_or((&self.data[0..0], &false), |i| {
                (
                    &self.data[self.start_pos[i] - self.offset
                        ..self.start_pos[i] + self.len[i] - self.offset],
                    &true,
                )
            })
        })
    }
    #[inline]
    pub fn index(&self, i: usize) -> (bool, &[u8]) {
        let i = self.index[i];
        i.map_or((false, &self.data[0..0]), |i| {
            (
                true,
                &self.data[self.start_pos[i] - self.offset
                    ..self.start_pos[i] + self.len[i] - self.offset],
            )
        })
    }
}

pub struct IRCBinaryBitmapIndexOption<'a> {
    pub data: &'a [u8],
    pub start_pos: &'a [usize],
    pub len: &'a [usize],
    pub offset: usize,
    pub index: &'a [Option<usize>],
    pub bitmap: &'a [bool],
}

impl<'a> IRCBinaryBitmapIndexOption<'a> {
    #[inline]
    pub fn as_binary_iter<'i>(
        &'i self,
    ) -> impl ExactSizeIterator<Item = (&[u8], &bool)> + 'i + Clone {
        self.index.iter().zip(self.bitmap).map(move |(i, b)| {
            i.map_or((&self.data[0..0], &false), |i| {
                (
                    &self.data[self.start_pos[i] - self.offset
                        ..self.start_pos[i] + self.len[i] - self.offset],
                    b,
                )
            })
        })
    }
    pub fn index(&self, i: usize) -> (bool, &[u8]) {
        let i = self.index[i];
        i.map_or((false, &self.data[0..0]), |i| {
            (
                self.bitmap[i],
                &self.data[self.start_pos[i] - self.offset
                    ..self.start_pos[i] + self.len[i] - self.offset],
            )
        })
    }
}
///////////////////////////////////////
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
    pub fn index(&self, _i: usize) -> (bool, &[u8]) {
        (self.bitmap, &self.data)
    }
}

pub enum ReadBinaryColumn<'a, T> {
    BitmapIndex(IRCBinaryBitmapIndex<'a>),
    BitmapNoIndex(IRCBinaryBitmapNoIndex<'a>),
    NoBitmapIndex(IRCBinaryNoBitmapIndex<'a>),
    NoBitmapNoIndex(IRCBinaryNoBitmapNoIndex<'a>),
    BitmapIndexOption(IRCBinaryBitmapIndexOption<'a>),
    NoBitmapIndexOption(IRCBinaryNoBitmapIndexOption<'a>),
    Const(IRCBinaryConst<'a>),
    BitmapIndexOrig(IRCBitmapIndex<'a, T>),
    BitmapNoIndexOrig(IRCBitmapNoIndex<'a, T>),
    NoBitmapIndexOrig(IRCNoBitmapIndex<'a, T>),
    NoBitmapNoIndexOrig(IRCNoBitmapNoIndex<'a, T>),
    BitmapIndexOptionOrig(IRCBitmapIndexOption<'a, T>),
    NoBitmapIndexOptionOrig(IRCNoBitmapIndexOption<'a, T>),
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
        let (is_const, (data, start_pos, len, offset)) = match data {
            ColumnData::Owned(_) => panic!("wrong type"),
            ColumnData::Slice(_) => panic!("wrong type"),
            ColumnData::SliceMut(_) => panic!("wrong type"),
            ColumnData::Const(_) => panic!("wrong type"),
            ColumnData::BinaryConst(c) => (true, c.downcast_binary_ref::<T>().unwrap()),
            ColumnData::BinaryOwned(c) => (false, c.downcast_binary_ref::<T>().unwrap()),
            ColumnData::BinarySlice(c) => (false, c.downcast_binary_ref::<T>().unwrap()),
            ColumnData::BinarySliceMut(c) => (false, c.downcast_binary_ref::<T>().unwrap()),
        };

        let offset = *offset;
        let bitmap = bitmap.to_ref();
        let index = index.to_ref();

        if is_const {
            let data = &data[start_pos[0] - offset..start_pos[0] - offset + len[0]];
            let bitmap = if let ColumnDataIndexRef::SomeOption(o) = index {
                o[0].is_some()
            } else if let ColumnDataFRef::Some(b) = bitmap {
                b[0]
            } else {
                true
            };
            Self::Const(IRCBinaryConst {
                data,
                bitmap,
                target_len,
            })
        } else {
            match (bitmap, index) {
                (ColumnDataFRef::Some(bitmap), ColumnDataIndexRef::Some(index)) => {
                    Self::BitmapIndex(IRCBinaryBitmapIndex {
                        data,
                        start_pos,
                        len,
                        offset,
                        bitmap,
                        index,
                    })
                }
                (ColumnDataFRef::Some(bitmap), ColumnDataIndexRef::SomeOption(index)) => {
                    Self::BitmapIndexOption(IRCBinaryBitmapIndexOption {
                        data,
                        start_pos,
                        len,
                        offset,
                        bitmap,
                        index,
                    })
                }
                (ColumnDataFRef::Some(bitmap), ColumnDataIndexRef::None) => {
                    Self::BitmapNoIndex(IRCBinaryBitmapNoIndex {
                        data,
                        start_pos,
                        len,
                        offset,
                        bitmap,
                    })
                }
                (ColumnDataFRef::None, ColumnDataIndexRef::Some(index)) => {
                    Self::NoBitmapIndex(IRCBinaryNoBitmapIndex {
                        data,
                        start_pos,
                        len,
                        offset,
                        index,
                    })
                }
                (ColumnDataFRef::None, ColumnDataIndexRef::SomeOption(index)) => {
                    Self::NoBitmapIndexOption(IRCBinaryNoBitmapIndexOption {
                        data,
                        start_pos,
                        len,
                        offset,
                        index,
                    })
                }
                (ColumnDataFRef::None, ColumnDataIndexRef::None) => {
                    Self::NoBitmapNoIndex(IRCBinaryNoBitmapNoIndex {
                        data,
                        start_pos,
                        len,
                        offset,
                    })
                }
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
            Self::BitmapIndexOption(c) => c.index.len(),
            Self::NoBitmapIndexOption(c) => c.index.len(),
            Self::Const(c) => c.target_len,
            Self::BitmapIndexOrig(c) => c.index.len(),
            Self::BitmapNoIndexOrig(c) => c.data.len(),
            Self::NoBitmapIndexOrig(c) => c.index.len(),
            Self::NoBitmapNoIndexOrig(c) => c.data.len(),
            Self::BitmapIndexOptionOrig(c) => c.index.len(),
            Self::NoBitmapIndexOptionOrig(c) => c.index.len(),
            Self::ConstOrig(c) => c.target_len,
        }
    }
    pub fn index(&self, i: usize) -> (bool, &[u8])
    where
        T: AsBytes,
    {
        match self {
            Self::BitmapIndex(c) => c.index(i),
            Self::BitmapNoIndex(c) => c.index(i),
            Self::NoBitmapIndex(c) => c.index(i),
            Self::NoBitmapNoIndex(c) => c.index(i),
            Self::BitmapIndexOption(c) => c.index(i),
            Self::NoBitmapIndexOption(c) => c.index(i),
            Self::Const(c) => c.index(i),
            Self::BitmapIndexOrig(c) => {
                let (a, b) = c.index(i);
                (a, b.as_bytes())
            }
            Self::BitmapNoIndexOrig(c) => {
                let (a, b) = c.index(i);
                (a, b.as_bytes())
            }
            Self::NoBitmapIndexOrig(c) => {
                let (a, b) = c.index(i);
                (a, b.as_bytes())
            }
            Self::NoBitmapNoIndexOrig(c) => {
                let (a, b) = c.index(i);
                (a, b.as_bytes())
            }
            Self::BitmapIndexOptionOrig(c) => {
                let (a, b) = c.index(i);
                (a, b.as_bytes())
            }
            Self::NoBitmapIndexOptionOrig(c) => {
                let (a, b) = c.index(i);
                (a, b.as_bytes())
            }
            Self::ConstOrig(c) => {
                let (a, b) = c.index(i);
                (a, b.as_bytes())
            }
        }
    }

    pub fn index_orig(&self, i: usize) -> (bool, T)
    where
        T: AsBytes + Clone,
    {
        match self {
            Self::BitmapIndex(c) => {
                let (a, b) = c.index(i);
                (a, AsBytes::from_bytes(b))
            }
            Self::BitmapNoIndex(c) => {
                let (a, b) = c.index(i);
                (a, AsBytes::from_bytes(b))
            }
            Self::NoBitmapIndex(c) => {
                let (a, b) = c.index(i);
                (a, AsBytes::from_bytes(b))
            }
            Self::NoBitmapNoIndex(c) => {
                let (a, b) = c.index(i);
                (a, AsBytes::from_bytes(b))
            }
            Self::BitmapIndexOption(c) => {
                let (a, b) = c.index(i);
                (a, AsBytes::from_bytes(b))
            }
            Self::NoBitmapIndexOption(c) => {
                let (a, b) = c.index(i);
                (a, AsBytes::from_bytes(b))
            }
            Self::Const(c) => {
                let (a, b) = c.index(i);
                (a, AsBytes::from_bytes(b))
            }
            Self::BitmapIndexOrig(c) => {
                let (a, b) = c.index(i);
                (a, b.clone())
            }
            Self::BitmapNoIndexOrig(c) => {
                let (a, b) = c.index(i);
                (a, b.clone())
            }
            Self::NoBitmapIndexOrig(c) => {
                let (a, b) = c.index(i);
                (a, b.clone())
            }
            Self::NoBitmapNoIndexOrig(c) => {
                let (a, b) = c.index(i);
                (a, b.clone())
            }
            Self::BitmapIndexOptionOrig(c) => {
                let (a, b) = c.index(i);
                (a, b.clone())
            }
            Self::NoBitmapIndexOptionOrig(c) => {
                let (a, b) = c.index(i);
                (a, b.clone())
            }
            Self::ConstOrig(c) => {
                let (a, b) = c.index(i);
                (a, b.clone())
            }
        }
    }

    pub fn update_len_if_const(&mut self, new_len: usize) {
        match self {
            Self::BitmapIndex(_) => {}
            Self::BitmapNoIndex(_) => {}
            Self::NoBitmapIndex(_) => {}
            Self::NoBitmapNoIndex(_) => {}
            Self::BitmapIndexOption(_) => {}
            Self::NoBitmapIndexOption(_) => {}
            Self::Const(c) => c.target_len = new_len,
            Self::BitmapIndexOrig(_) => {}
            Self::BitmapNoIndexOrig(_) => {}
            Self::NoBitmapIndexOrig(_) => {}
            Self::NoBitmapNoIndexOrig(_) => {}
            Self::BitmapIndexOptionOrig(_) => {}
            Self::NoBitmapIndexOptionOrig(_) => {}
            Self::ConstOrig(c) => c.target_len = new_len,
        }
    }

    #[inline]
    pub fn for_each<F>(&self, f: F)
    where
        F: FnMut((&[u8], &bool)),
        T: AsBytes,
    {
        match self {
            Self::BitmapIndex(c) => c.as_binary_iter().for_each(f),
            Self::BitmapNoIndex(c) => c.as_binary_iter().for_each(f),
            Self::NoBitmapIndex(c) => c.as_binary_iter().for_each(f),
            Self::NoBitmapNoIndex(c) => c.as_binary_iter().for_each(f),
            Self::BitmapIndexOption(c) => c.as_binary_iter().for_each(f),
            Self::NoBitmapIndexOption(c) => c.as_binary_iter().for_each(f),
            Self::Const(c) => c.as_binary_iter().for_each(f),
            Self::BitmapIndexOrig(c) => c.as_binary_iter().for_each(f),
            Self::BitmapNoIndexOrig(c) => c.as_binary_iter().for_each(f),
            Self::NoBitmapIndexOrig(c) => c.as_binary_iter().for_each(f),
            Self::NoBitmapNoIndexOrig(c) => c.as_binary_iter().for_each(f),
            Self::BitmapIndexOptionOrig(c) => c.as_binary_iter().for_each(f),
            Self::NoBitmapIndexOptionOrig(c) => c.as_binary_iter().for_each(f),
            Self::ConstOrig(c) => c.as_binary_iter().for_each(f),
        }
    }

    #[inline]
    pub fn enumerate_and_for_each<F>(&self, f: F)
    where
        F: FnMut((usize, (&[u8], &bool))),
        T: AsBytes,
    {
        match self {
            Self::BitmapIndex(c) => c.as_binary_iter().enumerate().for_each(f),
            Self::BitmapNoIndex(c) => c.as_binary_iter().enumerate().for_each(f),
            Self::NoBitmapIndex(c) => c.as_binary_iter().enumerate().for_each(f),
            Self::NoBitmapNoIndex(c) => c.as_binary_iter().enumerate().for_each(f),
            Self::BitmapIndexOption(c) => c.as_binary_iter().enumerate().for_each(f),
            Self::NoBitmapIndexOption(c) => c.as_binary_iter().enumerate().for_each(f),
            Self::Const(c) => c.as_binary_iter().enumerate().for_each(f),
            Self::BitmapIndexOrig(c) => c.as_binary_iter().enumerate().for_each(f),
            Self::BitmapNoIndexOrig(c) => c.as_binary_iter().enumerate().for_each(f),
            Self::NoBitmapIndexOrig(c) => c.as_binary_iter().enumerate().for_each(f),
            Self::NoBitmapNoIndexOrig(c) => c.as_binary_iter().enumerate().for_each(f),
            Self::BitmapIndexOptionOrig(c) => c.as_binary_iter().enumerate().for_each(f),
            Self::NoBitmapIndexOptionOrig(c) => c.as_binary_iter().enumerate().for_each(f),
            Self::ConstOrig(c) => c.as_binary_iter().enumerate().for_each(f),
        }
    }

    #[inline]
    pub fn zip_and_for_each<I, F>(&self, iter: I, f: F)
    where
        I: ExactSizeIterator,
        F: FnMut(((&[u8], &bool), <I as Iterator>::Item)),
        T: AsBytes,
    {
        match self {
            Self::BitmapIndex(c) => c.as_binary_iter().zip(iter).for_each(f),
            Self::BitmapNoIndex(c) => c.as_binary_iter().zip(iter).for_each(f),
            Self::NoBitmapIndex(c) => c.as_binary_iter().zip(iter).for_each(f),
            Self::NoBitmapNoIndex(c) => c.as_binary_iter().zip(iter).for_each(f),
            Self::BitmapIndexOption(c) => c.as_binary_iter().zip(iter).for_each(f),
            Self::NoBitmapIndexOption(c) => c.as_binary_iter().zip(iter).for_each(f),
            Self::Const(c) => c.as_binary_iter().zip(iter).for_each(f),
            Self::BitmapIndexOrig(c) => c.as_binary_iter().zip(iter).for_each(f),
            Self::BitmapNoIndexOrig(c) => c.as_binary_iter().zip(iter).for_each(f),
            Self::NoBitmapIndexOrig(c) => c.as_binary_iter().zip(iter).for_each(f),
            Self::NoBitmapNoIndexOrig(c) => c.as_binary_iter().zip(iter).for_each(f),
            Self::BitmapIndexOptionOrig(c) => c.as_binary_iter().zip(iter).for_each(f),
            Self::NoBitmapIndexOptionOrig(c) => c.as_binary_iter().zip(iter).for_each(f),
            Self::ConstOrig(c) => c.as_binary_iter().zip(iter).for_each(f),
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
                ReadColumn::BitmapIndexOption(c) => ReadBinaryColumn::BitmapIndexOptionOrig(c),
                ReadColumn::NoBitmapIndexOption(c) => ReadBinaryColumn::NoBitmapIndexOptionOrig(c),
                ReadColumn::Const(c) => ReadBinaryColumn::ConstOrig(c),
            }
        }
    }
}
