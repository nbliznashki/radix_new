use radix_column::{ColumnData, ColumnDataF, ColumnWrapper};

/////////////////////////////
////                     ////
////  Insert Column      ////
////                     ////
/////////////////////////////

//Index Read Column types (IIC)
pub struct ICBitmap<'a, T> {
    pub data: &'a mut Vec<T>,
    pub bitmap: &'a mut Vec<bool>,
}

impl<'a, T> ICBitmap<'a, T> {
    #[inline]
    pub fn insert<F, I>(&mut self, iter: I, f: &F)
    where
        I: ExactSizeIterator,
        F: Fn(I::Item) -> (bool, T),
    {
        self.data.reserve(iter.len());
        self.bitmap.reserve(iter.len());
        iter.map(f).for_each(|(b, d)| {
            self.bitmap.push(b);
            self.data.push(d);
        });
    }
}

pub struct ICNoBitmap<'a, T> {
    pub data: &'a mut Vec<T>,
}

impl<'a, T> ICNoBitmap<'a, T> {
    #[inline]
    pub fn insert<F, I>(&mut self, iter: I, f: F)
    where
        I: ExactSizeIterator,
        F: Fn(I::Item) -> (bool, T),
    {
        self.data.extend(iter.map(f).map(|a| a.1));
    }
}

pub enum InsertColumn<'a, T> {
    Bitmap(ICBitmap<'a, T>),
    NoBitmap(ICNoBitmap<'a, T>),
}

impl<'a, 'b, T: Send + Sync + 'static>
    From<(
        &'a mut ColumnData<'b>,
        &'a mut ColumnDataF<'b, bool>,
        bool,
        usize,
    )> for InsertColumn<'a, T>
{
    fn from(
        (data, bitmap, bitmap_update_required, target_length): (
            &'a mut ColumnData,
            &'a mut ColumnDataF<bool>,
            bool,
            usize,
        ),
    ) -> Self {
        let data = data.downcast_vec::<T>().unwrap();
        data.reserve(target_length);

        match (bitmap_update_required, bitmap.is_some()) {
            (true, true) => {
                let bitmap = bitmap.downcast_vec().unwrap();
                bitmap.reserve(target_length);
                Self::Bitmap(ICBitmap { data, bitmap })
            }
            (false, true) => {
                *bitmap = ColumnDataF::None;
                Self::NoBitmap(ICNoBitmap { data })
            }
            (true, false) => {
                *bitmap = ColumnDataF::new(Vec::with_capacity(target_length));
                let bitmap = bitmap.downcast_vec().unwrap();
                Self::Bitmap(ICBitmap { data, bitmap })
            }
            (false, false) => Self::NoBitmap(ICNoBitmap { data }),
        }
    }
}

impl<'a, T> InsertColumn<'a, T> {
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
        let c_read_column: InsertColumn<T> =
            InsertColumn::from((c_col, c_bitmap, bitmap_update_required, target_len));
        c_read_column
    }

    #[inline]
    pub fn insert<F, I>(&mut self, iter: I, f: &F)
    where
        I: ExactSizeIterator,
        F: Fn(I::Item) -> (bool, T),
    {
        match self {
            Self::Bitmap(c) => c.insert(iter, f),
            Self::NoBitmap(c) => c.insert(iter, f),
        }
    }
}
