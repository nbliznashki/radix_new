use std::marker::PhantomData;

use radix_column::{ColumnData, ColumnDataF, ColumnWrapper};

/////////////////////////////
////                     ////
////  Insert Column      ////
////                     ////
/////////////////////////////

//Index Read Column types (IIC)
pub struct ICBinaryBitmap<'a, T> {
    pub data: &'a mut Vec<u8>,
    pub start_pos: &'a mut Vec<usize>,
    pub len: &'a mut Vec<usize>,
    pub offset: usize,
    pub bitmap: &'a mut Vec<bool>,
    _phantom: PhantomData<T>,
}

impl<'a, T> ICBinaryBitmap<'a, T> {
    #[inline]
    pub fn insert<F, I>(&mut self, iter: I, f: &F)
    where
        I: ExactSizeIterator,
        F: Fn(I::Item) -> (bool, &'a [u8]),
    {
        let mut cur_start_pos = self.data.len() + self.offset;
        let new_items_count = iter.len();
        self.start_pos.reserve(new_items_count);
        self.len.reserve(new_items_count);
        self.bitmap.reserve(new_items_count);

        iter.for_each(|i| {
            let (b, d) = f(i);
            self.data.extend_from_slice(d);
            self.len.push(d.len());
            self.start_pos.push(cur_start_pos);

            self.bitmap.push(b);

            cur_start_pos += d.len();
        });
    }
}

pub struct ICBinaryNoBitmap<'a, T> {
    pub data: &'a mut Vec<u8>,
    pub start_pos: &'a mut Vec<usize>,
    pub len: &'a mut Vec<usize>,
    pub offset: usize,
    _phantom: PhantomData<T>,
}

impl<'a, T> ICBinaryNoBitmap<'a, T> {
    #[inline]
    pub fn insert<F, I>(&mut self, iter: I, f: F)
    where
        I: ExactSizeIterator,
        F: Fn(I::Item) -> (bool, &'a [u8]),
    {
        let mut cur_start_pos = self.data.len() + self.offset;

        let new_items_count = iter.len();
        self.start_pos.reserve(new_items_count);
        self.len.reserve(new_items_count);

        iter.for_each(|i| {
            let (_, d) = f(i);
            self.data.extend_from_slice(d);
            self.len.push(d.len());
            self.start_pos.push(cur_start_pos);
            cur_start_pos += d.len();
        });
    }
}

pub enum InsertBinaryColumn<'a, T> {
    Bitmap(ICBinaryBitmap<'a, T>),
    NoBitmap(ICBinaryNoBitmap<'a, T>),
}

impl<'a, 'b, T: Send + Sync + 'static>
    From<(
        &'a mut ColumnData<'b>,
        &'a mut ColumnDataF<'b, bool>,
        bool,
        usize,
    )> for InsertBinaryColumn<'a, T>
{
    fn from(
        (data, bitmap, bitmap_update_required, target_length): (
            &'a mut ColumnData,
            &'a mut ColumnDataF<bool>,
            bool,
            usize,
        ),
    ) -> Self {
        let (data, start_pos, len, offset) = data.downcast_binary_vec::<T>().unwrap();
        data.reserve(target_length);

        match (bitmap_update_required, bitmap.is_some()) {
            (true, true) => {
                let bitmap = bitmap.downcast_vec().unwrap();
                bitmap.reserve(target_length);
                Self::Bitmap(ICBinaryBitmap {
                    data,
                    start_pos,
                    len,
                    offset: *offset,
                    bitmap,
                    _phantom: PhantomData::<T>,
                })
            }
            (false, true) => {
                *bitmap = ColumnDataF::None;
                Self::NoBitmap(ICBinaryNoBitmap {
                    data,
                    start_pos,
                    len,
                    offset: *offset,
                    _phantom: PhantomData::<T>,
                })
            }
            (true, false) => {
                *bitmap = ColumnDataF::new(Vec::with_capacity(target_length));
                let bitmap = bitmap.downcast_vec().unwrap();
                Self::Bitmap(ICBinaryBitmap {
                    data,
                    start_pos,
                    len,
                    offset: *offset,
                    bitmap,
                    _phantom: PhantomData::<T>,
                })
            }
            (false, false) => Self::NoBitmap(ICBinaryNoBitmap {
                data,
                start_pos,
                len,
                offset: *offset,
                _phantom: PhantomData::<T>,
            }),
        }
    }
}

impl<'a, T> InsertBinaryColumn<'a, T> {
    pub fn len(&self) -> usize {
        match self {
            Self::Bitmap(c) => c.data.len(),
            Self::NoBitmap(c) => c.data.len(),
        }
    }
    pub fn from_destination<'b>(
        c: &'a mut ColumnWrapper<'b>,
        bitmap_update_required: bool,
        target_len: usize,
    ) -> Self
    where
        T: 'static + Send + Sync,
    {
        let (c_col, c_bitmap) = c.get_inner_mut();
        let c_read_column: InsertBinaryColumn<T> =
            InsertBinaryColumn::from((c_col, c_bitmap, bitmap_update_required, target_len));
        c_read_column
    }

    #[inline]
    pub fn insert<F, I>(&mut self, iter: I, f: &F)
    where
        I: ExactSizeIterator,
        F: Fn(I::Item) -> (bool, &'a [u8]),
    {
        match self {
            Self::Bitmap(c) => c.insert(iter, f),
            Self::NoBitmap(c) => c.insert(iter, f),
        }
    }
}
