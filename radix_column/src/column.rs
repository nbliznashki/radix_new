use crate::ColumnDataF;

use super::columndata::ColumnData;
#[derive(Debug)]
pub struct ColumnWrapper<'a> {
    column: super::ColumnData<'a>,
    bitmap: super::ColumnDataF<'a, bool>,
}

impl<'a> ColumnWrapper<'a> {
    pub fn is_owned(&self) -> bool {
        self.column().is_owned() && self.bitmap().is_owned()
    }

    pub fn is_binary(&self) -> bool {
        self.column().is_binary()
    }

    pub fn is_sized(&self) -> bool {
        self.column().is_sized()
    }

    pub fn new_from_columndata(data: ColumnData<'a>) -> Self {
        ColumnWrapper {
            column: data,
            bitmap: ColumnDataF::None,
        }
    }

    pub fn with_bitmap_slice(&mut self, bmap: &'a [bool]) {
        self.bitmap = ColumnDataF::Slice(bmap)
    }

    pub fn get_inner(self) -> (ColumnData<'a>, ColumnDataF<'a, bool>) {
        (self.column, self.bitmap)
    }

    pub fn get_inner_ref(&self) -> (&ColumnData<'a>, &ColumnDataF<'a, bool>) {
        (&self.column, &self.bitmap)
    }

    pub fn get_inner_mut(&mut self) -> (&mut ColumnData<'a>, &mut ColumnDataF<'a, bool>) {
        (&mut self.column, &mut self.bitmap)
    }

    pub fn column<'b>(&self) -> &ColumnData<'b>
    where
        'a: 'b,
    {
        &self.column
    }

    pub fn column_mut(&mut self) -> &mut ColumnData<'a> {
        &mut self.column
    }

    pub fn bitmap<'b>(&self) -> &ColumnDataF<'b, bool>
    where
        'a: 'b,
    {
        &self.bitmap
    }

    pub fn bitmap_mut(&mut self) -> &mut ColumnDataF<'a, bool> {
        &mut self.bitmap
    }

    pub fn bitmap_set(&mut self, b: ColumnDataF<'a, bool>) {
        self.bitmap = b;
    }
}
