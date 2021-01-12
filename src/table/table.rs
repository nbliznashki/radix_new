use rayon::prelude::*;
use std::{cell::UnsafeCell, collections::HashMap, mem::MaybeUninit};

use crate::{
    column_buffer::ColumnBuffer, filter, part_with_sizes, part_with_sizes_mut,
    tabletotable::TableToTableMap, PartitionedColumn, TableExpression,
};
use radix_column::*;
use radix_operations::*;

//Small table, no parallel execution
#[derive(Debug)]
pub struct Table<'a> {
    table_rows: usize,
    columns: Vec<ColumnWrapper<'a>>,
    columns_nullable: Vec<bool>,
    pub indexes: Vec<ColumnDataIndex<'a>>,
    columnindexmap: HashMap<usize, usize>,
    //buffer_columns: BufferColumns<'a>,
}

impl<'a> Table<'a> {
    pub fn new() -> Self {
        Self {
            table_rows: 0,
            columns: vec![],
            columns_nullable: vec![],
            indexes: vec![],
            columnindexmap: HashMap::new(), //buffer_columns: BufferColumns::new(),
        }
    }
    //TRAIT
    pub fn number_of_columns(&self) -> usize {
        self.columns.len()
    }
    //TRAIT
    pub fn push<'b, T>(&mut self, dict: &Dictionary, data: &'b [T]) -> Result<(), ErrorDesc>
    where
        T: 'static + Send + Sync,
        'b: 'a,
    {
        if (self.table_rows == 0) & (self.number_of_columns() == 0) {
            self.table_rows = data.len();
        } else if self.table_rows != data.len() {
            Err(format!(
                "Attempt to add a column with {} rows to a table with {} rows",
                data.len(),
                self.table_rows,
            ))?
        }

        self.columns
            .push(ColumnWrapper::new_from_columndata(ColumnData::Slice(
                SliceRef::new(data),
            )));
        self.columns_nullable.push(false);
        Ok(())
    }
    //Should be removed in the future
    pub fn push_index(
        &mut self,
        index: ColumnDataIndex<'a>,
        applies_for_columns: &[usize],
    ) -> Result<(), ErrorDesc> {
        //If the columns should have no index, remove the entries from the columnindexmap
        if let ColumnDataIndex::None = index {
            applies_for_columns.iter().for_each(|i| {
                self.columnindexmap.remove_entry(i);
            });
        //TO-DO: un-used indexes should be dropped
        } else {
            let number_of_columns = self.number_of_columns();
            self.indexes.push(index);
            let index_id = self.indexes.len() - 1;
            let res: Result<(), ErrorDesc> = applies_for_columns.iter().try_for_each(|column_id| {
                if *column_id >= number_of_columns {
                    Err("Column id out of bounds")?
                } else {
                    self.columnindexmap.insert(*column_id, index_id);
                    Ok(())
                }
            });
            res?
        }
        Ok(())
    }

    pub fn push_with_bitmap<'b, T>(
        &mut self,
        dict: &Dictionary,
        data: &'b [T],
        bitmap: &'b [bool],
    ) -> Result<(), ErrorDesc>
    where
        T: 'static + Send + Sync,
        'b: 'a,
    {
        if (self.table_rows == 0) & (self.number_of_columns() == 0) {
            self.table_rows = data.len();
        } else if self.table_rows != data.len() {
            Err(format!(
                "Attempt to add a column with {} rows to a table with {} rows",
                data.len(),
                self.table_rows,
            ))?
        }
        let mut c = ColumnWrapper::new_from_columndata(ColumnData::Slice(SliceRef::new(data)));
        c.bitmap_set(ColumnDataF::Slice(bitmap));

        self.columns.push(c);
        self.columns_nullable.push(true);
        Ok(())
    }

    pub fn push_mut<'b, T>(&mut self, data: &'b mut [T]) -> Result<(), ErrorDesc>
    where
        T: 'static + Send + Sync,
        'b: 'a,
    {
        if (self.table_rows == 0) & (self.number_of_columns() == 0) {
            self.table_rows = data.len();
        } else if self.table_rows != data.len() {
            Err(format!(
                "Attempt to add a column with {} rows to a table with {} rows",
                data.len(),
                self.table_rows,
            ))?
        }

        self.columns
            .push(ColumnWrapper::new_from_columndata(ColumnData::SliceMut(
                SliceRefMut::new(data),
            )));
        self.columns_nullable.push(false);
        Ok(())
    }

    pub fn push_mut_with_bitmap<'b, T>(
        &mut self,
        data: &'b mut [T],
        bitmap: &'b mut [bool],
    ) -> Result<(), ErrorDesc>
    where
        T: 'static + Send + Sync,
        'b: 'a,
    {
        if (self.table_rows == 0) & (self.number_of_columns() == 0) {
            self.table_rows = data.len();
        } else if self.table_rows != data.len() {
            Err(format!(
                "Attempt to add a column with {} rows to a table with {} rows",
                data.len(),
                self.table_rows,
            ))?
        }
        let mut c =
            ColumnWrapper::new_from_columndata(ColumnData::SliceMut(SliceRefMut::new(data)));
        c.bitmap_set(ColumnDataF::SliceMut(bitmap));

        self.columns.push(c);
        self.columns_nullable.push(true);
        Ok(())
    }

    pub(crate) fn is_const(&self, column_id: &usize) -> Result<bool, ErrorDesc> {
        let number_of_columns = self.number_of_columns();
        if number_of_columns <= *column_id {
            Err(format!(
                "Column index out of bounds: {} while the table has only {} columns",
                column_id, number_of_columns
            ))?
        };
        Ok(self.columns[*column_id].column().is_const())
    }

    pub(crate) fn materialize_const<T: 'static + Send + Sync + Clone>(
        &self,
        dict: &Dictionary,
        column_id: &usize,
    ) -> Result<(Vec<T>, ColumnDataF<'static, bool>), ErrorDesc> {
        if !self.is_const(column_id)? {
            Err(format!(
                "materialize_const expects a const column, but column {} is not constant",
                column_id
            ))?
        };

        let cw_value = &self.columns[*column_id];
        let value = cw_value.to_const::<T>(dict)?;
        let bitmap = cw_value.bitmap().to_ref();

        let total_len: usize = self.table_rows;
        let mut output_data: Vec<MaybeUninit<T>> = Vec::with_capacity(total_len);
        unsafe { output_data.set_len(total_len) };

        output_data
            .par_iter_mut()
            .for_each(|t| *t = MaybeUninit::new(value.clone()));
        let output_data: Vec<T> = unsafe { std::mem::transmute(output_data) };

        let output_bitmap = match bitmap {
            ColumnDataFRef::None => ColumnDataF::None,
            ColumnDataFRef::Some(bitmap) => ColumnDataF::Owned(bitmap.to_vec()),
        };

        Ok((output_data, output_bitmap))
    }

    pub fn materialize<T: 'static + Send + Sync + Clone>(
        &self,
        dict: &Dictionary,
        column_id: &usize,
    ) -> Result<(Vec<T>, ColumnDataF<'static, bool>), ErrorDesc> {
        let p_index = self.columnindexmap.get(column_id);
        let p_column = &self.columns[*column_id];

        self.materialize_common(dict, column_id, p_column, p_index)
    }

    //TO-DO - switch to a more general execution framework
    fn materialize_common<T: 'static + Send + Sync + Clone>(
        &self,
        dict: &Dictionary,
        column_id: &usize,
        column: &ColumnWrapper,
        p_index: Option<&usize>,
    ) -> Result<(Vec<T>, ColumnDataF<'static, bool>), ErrorDesc> {
        if self.is_const(column_id)? {
            return self.materialize_const(dict, column_id);
        }

        //let p_column = p_column.column_vec();
        let total_len: usize = self.table_rows;

        let has_bitmap = column.bitmap().is_some();

        //Reserve memory for the result column

        let mut output_data: Vec<MaybeUninit<T>> = Vec::with_capacity(total_len);

        //SAFETY: OK to do as the memory has been reserved, and moreover the type is assumed to be not initialized
        unsafe {
            output_data.set_len(total_len);
        }

        let mut output_slice = output_data.as_mut_slice();

        let mut output_col = ColumnWrapper::new_from_columndata(ColumnData::SliceMut(
            SliceRefMut::new(output_slice),
        ));

        let mut b: Vec<bool> = Vec::new();

        if has_bitmap {
            b = vec![false; self.table_rows];
            output_col.bitmap_set(ColumnDataF::SliceMut(b.as_mut_slice()));
        }

        match p_index {
            Some(index) => column.copy_to(dict, &mut output_col, &self.indexes[*index])?,
            None => column.copy_to(dict, &mut output_col, &ColumnDataIndex::None)?,
        };

        let output_data: Vec<T> = unsafe { std::mem::transmute(output_data) };
        let output_bitmap = if has_bitmap {
            ColumnDataF::new(b)
        } else {
            ColumnDataF::None
        };

        Ok((output_data, output_bitmap))
    }

    pub fn materialize_as_string(
        &self,
        dict: &Dictionary,
        column_id: &usize,
    ) -> Result<Vec<String>, ErrorDesc> {
        let column = &self.columns[*column_id];

        let index = self.columnindexmap.get(column_id);
        let index_empty = ColumnDataIndex::None;
        let index = match index {
            Some(i) => &self.indexes[*i],
            None => &index_empty,
        };

        let (c_data, c_bool) = (
            column.as_string(dict, &ColumnDataIndex::None).unwrap(),
            column.bitmap(),
        );

        let c = ColumnData::Slice(SliceRef::new(c_data.as_slice()));
        let c = ReadColumn::from((&c, c_bool, index, self.table_rows));
        let mut res: Vec<String> = Vec::with_capacity(self.table_rows);
        c.for_each(|(data, b): (&String, &bool)| {
            let s = if *b {
                data.clone()
            } else {
                "(null)".to_string()
            };
            res.push(s);
        });
        Ok(res)
    }
    pub fn print(&self, dict: &Dictionary) -> Result<(), ErrorDesc> {
        let v: Vec<Vec<String>> = (0usize..self.number_of_columns())
            .into_iter()
            .enumerate()
            .map(|(i, _)| self.materialize_as_string(dict, &i).unwrap())
            .collect();

        let mut table = prettytable::Table::new();

        if !v.is_empty() {
            let len = v[0].len();
            (0..len).into_iter().for_each(|i| {
                let cells: Vec<_> = v.iter().map(|v| prettytable::Cell::new(&v[i])).collect();
                table.add_row(prettytable::Row::new(cells));
            });
        }

        table.printstd();

        Ok(())
    }

    //TO-DO - switch to a more general execution framework
    pub fn filter(&mut self, dict: &Dictionary, expr: &TableExpression) -> Result<(), ErrorDesc> {
        let number_of_columns = self.number_of_columns();
        let columnindexmap = &self.columnindexmap;

        let columns_without_index: Vec<_> = (0..number_of_columns)
            .into_iter()
            .filter(|i| self.columnindexmap.get(i).is_none())
            .collect();

        let mut buffer = ColumnBuffer::new();
        let mut hashmap_buffer = HashMapBuffer::new();
        let mut hashmap_binary: HashMap<(usize, NullableValue<&[u8]>), usize, ahash::RandomState> =
            HashMap::with_capacity_and_hasher(100, ahash::RandomState::default());

        let result = expr
            .eval(
                dict,
                &mut buffer,
                &mut hashmap_buffer,
                &mut hashmap_binary,
                &self.columns,
                &self.indexes,
                &self.columnindexmap,
            )
            .unwrap();

        //TO-DO: use index!
        let result = match result {
            InputTypes::Owned(res, _index) => res,
            _ => panic!(),
        };
        let b = result.column().downcast_ref::<bool>().unwrap();
        let bitmap = result.bitmap();

        let mut hint_size: Option<usize> = None;
        let (indexes, table_rows) = (&mut self.indexes, &mut self.table_rows);
        indexes.iter_mut().for_each(|i| {
            let new_index_len = filter(i, b, bitmap, &hint_size).unwrap();
            if hint_size.is_none() {
                hint_size = Some(new_index_len);
                *table_rows = new_index_len;
            }
        });
        if !columns_without_index.is_empty() {
            let mut ind: ColumnDataIndex = ColumnDataIndex::None;
            filter(&mut ind, b, bitmap, &hint_size).unwrap();
            self.indexes.push(ind);
        }
        buffer.push(dict, result);

        if !columns_without_index.is_empty() {
            let insert_pos = self.indexes.len() - 1;
            columns_without_index.iter().for_each(|i| {
                self.columnindexmap.insert(*i, insert_pos);
            });
        }

        Ok(())
    }

    //TO-DO - switch to a more general execution framework
    pub fn add_expression_as_new_column(&mut self, dict: &Dictionary, expr: &TableExpression) {
        let (indexes, columnindexmap) = (&mut self.indexes, &mut self.columnindexmap);

        let indexes_num = indexes.len();

        let mut buffer = ColumnBuffer::new();
        let mut hashmap_buffer = HashMapBuffer::new();
        let mut hashmap_binary: HashMap<(usize, NullableValue<&[u8]>), usize, ahash::RandomState> =
            HashMap::with_capacity_and_hasher(100, ahash::RandomState::default());

        let res = expr
            .eval(
                dict,
                &mut buffer,
                &mut hashmap_buffer,
                &mut hashmap_binary,
                &self.columns,
                indexes,
                columnindexmap,
            )
            .unwrap();
        //TO-DO: use index!
        match res {
            InputTypes::Owned(res, index) => {
                self.columns.push(res);
                if index != ColumnDataIndex::None {
                    indexes.push(index);
                }
            }
            _ => panic!(),
        }
        if indexes_num != indexes.len() {
            columnindexmap.insert(self.columns.len() - 1, indexes_num);
        }

        //TO-DO - fix this!!!
        self.columns_nullable.push(true);
    }

    pub fn build_hash(&self, dict: &Dictionary, input_ids: &[usize]) -> Vec<u64> {
        //TO-DO: Switch to general execution framework
        let mut output: Vec<u64> = Vec::with_capacity(self.table_rows);
        let index_empty = ColumnDataIndex::None;
        input_ids.iter().for_each(|col_id| {
            let signature = Signature::new(
                "" as &str,
                vec![self.columns[*col_id].column().item_type_id()],
            );
            let iop = dict.columninternal.get(&signature).unwrap();
            let c_index = self.columnindexmap.get(col_id);

            let c_index = match c_index {
                Some(i) => &self.indexes[*i],
                None => &index_empty,
            };
            let c = &self.columns[*col_id];
            iop.hash_in(c, c_index, &mut output).unwrap();
        });

        output
    }

    pub fn build_groups(&self, dict: &Dictionary, input_ids: &[usize]) -> (Vec<usize>, usize) {
        //TO-DO: Switch to general execution framework
        let mut output: (Vec<usize>, usize) = (vec![0usize; self.table_rows], self.table_rows);
        let index_empty = ColumnDataIndex::None;

        input_ids.iter().for_each(|col_id| {
            let signature = Signature::new(
                "" as &str,
                vec![self.columns[*col_id].column().item_type_id()],
            );
            let iop = dict.columninternal.get(&signature).unwrap();
            let c_index = self.columnindexmap.get(col_id);

            let mut hashmap_buffer = HashMapBuffer::new();
            let mut hashmap_binary: HashMap<
                (usize, NullableValue<&[u8]>),
                usize,
                ahash::RandomState,
            > = HashMap::with_capacity_and_hasher(100, ahash::RandomState::default());

            let c_index = match c_index {
                Some(i) => &self.indexes[*i],
                None => &index_empty,
            };
            let c = &self.columns[*col_id];
            iop.group_in(
                c,
                c_index,
                &mut output.0,
                &mut hashmap_buffer,
                &mut hashmap_binary,
            )
            .unwrap()
        });

        let (v, number_of_groups) = (&mut output.0, &mut output.1);

        let mut decrease_index_by = 0;
        (1..v.len()).into_iter().for_each(|i| {
            decrease_index_by += (v[i] != i) as usize;
            v[i] = v[v[i]];
            let diff = ((v[i] != i) as usize).wrapping_sub(1); //If equal then 0 else FFFFFF
            let diff = diff & decrease_index_by;
            v[i] -= diff;

            *number_of_groups -= decrease_index_by;
        });
        output
    }
}
