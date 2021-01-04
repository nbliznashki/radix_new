use rayon::prelude::*;
use std::{cell::UnsafeCell, collections::HashMap, mem::MaybeUninit};

use crate::{
    column_buffer::ColumnBuffer, filter, part_with_sizes, part_with_sizes_mut,
    tabletotable::TableToTableMap, TableExpression,
};
use radix_column::*;
use radix_operations::*;

type PartitionedColumn<'a> = Vec<&'a ColumnWrapper<'a>>;
type PartitionedColumnMut<'a> = Vec<&'a mut ColumnWrapper<'a>>;

pub type PartitionedIndex<'a> = Vec<ColumnDataF<'a, usize>>;

#[derive(Debug)]
pub struct Table<'a> {
    partition_sizes: Vec<usize>,
    columns: Vec<Vec<ColumnWrapper<'a>>>,
    columns_nullable: Vec<bool>,
    indexes: Vec<Vec<ColumnDataF<'a, usize>>>,
    columnindexmap: HashMap<usize, usize>,
    //buffer_columns: BufferColumns<'a>,
}

impl<'a> Table<'a> {
    pub fn new(partition_sizes: Vec<usize>) -> Self {
        let columns = partition_sizes.iter().map(|_| vec![]).collect();
        let indexes = partition_sizes.iter().map(|_| vec![]).collect();
        Self {
            partition_sizes,
            columns,
            columns_nullable: vec![],
            indexes,
            columnindexmap: HashMap::new(), //buffer_columns: BufferColumns::new(),
        }
    }
    pub fn push<'b, T>(&mut self, dict: &Dictionary, data: &'b [T]) -> Result<(), ErrorDesc>
    where
        T: 'static + Send + Sync,
        'b: 'a,
    {
        let p_column = part_with_sizes(dict, data, &[], &self.partition_sizes)?;
        self.columns
            .iter_mut()
            .zip(p_column.into_iter())
            .for_each(|(v, c)| v.push(c));
        self.columns_nullable.push(false);
        Ok(())
    }

    pub fn number_of_columns(&self) -> Result<usize, ErrorDesc> {
        if self.columns.is_empty() {
            Err("The table was not properly initialized, therefore the number of columns is unknown")?
        } else {
            Ok(self.columns[0].len())
        }
    }

    pub fn push_index(
        &mut self,
        p_index: PartitionedIndex<'a>,
        applies_for_columns: &[usize],
    ) -> Result<(), ErrorDesc> {
        if self.partition_sizes.len() != p_index.len() {
            Err(format!(
                "Mismatch while adding an index to a table: index has {} partitions, while the table has {} partitions",
                p_index.len(),
                self.partition_sizes.len()
            ))?
        }

        let res: Result<(), ErrorDesc>=self.partition_sizes.iter().zip(p_index.iter()).enumerate()
        .try_for_each(|(partition_id, (p_size,index))| if index.is_empty() || (index.len()==Some(*p_size)) {Ok(())}
        else {
            Err(format!("Mismatch while adding an index to a table: index partition {} has lenght {:?}, while the table partitio has length {} ",partition_id, index.len(), p_size))?
        });
        res?;

        let cur_index_pos = self.indexes[0].len();

        self.indexes
            .iter_mut()
            .zip(p_index)
            .for_each(|(v, i)| v.push(i));

        let number_of_columns = self.number_of_columns()?;

        let res: Result<(), ErrorDesc> = applies_for_columns.iter().try_for_each(|i| {
            if *i < number_of_columns {
                self.columnindexmap.insert(*i, cur_index_pos);
                Ok(())
            } else {
                Err(format!(
                    "Index should apply for column {}, while the table has only {} columns",
                    i, number_of_columns
                ))?
            }
        });
        res
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
        let p_column = part_with_sizes(dict, data, bitmap, &self.partition_sizes)?;
        self.columns
            .iter_mut()
            .zip(p_column.into_iter())
            .for_each(|(v, c)| v.push(c));
        self.columns_nullable.push(true);
        Ok(())
    }

    pub fn push_mut<'b, T>(&mut self, data: &'b mut [T]) -> Result<(), ErrorDesc>
    where
        T: 'static + Send + Sync,
        'b: 'a,
    {
        let p_column = part_with_sizes_mut(data, &mut [], &self.partition_sizes)?;
        self.columns
            .iter_mut()
            .zip(p_column.into_iter())
            .for_each(|(v, c)| v.push(c));
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
        let p_column = part_with_sizes_mut(data, bitmap, &self.partition_sizes)?;
        self.columns
            .iter_mut()
            .zip(p_column.into_iter())
            .for_each(|(v, c)| v.push(c));
        self.columns_nullable.push(true);
        Ok(())
    }

    pub(crate) fn get_part_col(&self, column_id: &usize) -> Result<PartitionedColumn, ErrorDesc> {
        if *column_id <= self.number_of_columns()? {
            Ok(self.columns.iter().map(|v| &v[*column_id]).collect())
        } else {
            Err(format!(
                "Index {} requested, but table has only {} columns",
                column_id,
                self.number_of_columns()?
            ))?
        }
    }

    pub(crate) fn is_const(&self, column_id: &usize) -> Result<bool, ErrorDesc> {
        let number_of_columns = self.number_of_columns()?;
        if number_of_columns <= *column_id {
            Err(format!(
                "Column index out of bounds: {} while the table has only {} columns",
                column_id, number_of_columns
            ))?
        };
        let res: bool = self
            .columns
            .iter()
            .map(|c| c[*column_id].column().is_const())
            .fold(true, |a, b| a && b);
        Ok(res)
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

        let cw_value = &self.columns[0][*column_id];
        let value = cw_value.to_const::<T>(dict)?;
        let bitmap = cw_value.bitmap();

        let total_len: usize = self.partition_sizes.iter().sum();
        let mut output_data: Vec<MaybeUninit<T>> = Vec::with_capacity(total_len);
        unsafe { output_data.set_len(total_len) };

        output_data
            .par_iter_mut()
            .for_each(|t| *t = MaybeUninit::new(value.clone()));
        let output_data: Vec<T> = unsafe { std::mem::transmute(output_data) };

        let output_bitmap = if bitmap.is_some() {
            let bitmap_value = bitmap.downcast_ref().unwrap()[0];
            let mut output_bitmap: Vec<MaybeUninit<bool>> = Vec::with_capacity(total_len);
            unsafe { output_bitmap.set_len(total_len) };

            output_bitmap
                .par_iter_mut()
                .for_each(|t| *t = MaybeUninit::new(bitmap_value));
            let output_bitmap: Vec<bool> = unsafe { std::mem::transmute(output_bitmap) };
            ColumnDataF::new(output_bitmap)
        } else {
            ColumnDataF::None
        };

        Ok((output_data, output_bitmap))
    }

    pub fn materialize<T: 'static + Send + Sync + Clone>(
        &self,
        dict: &Dictionary,
        column_id: &usize,
    ) -> Result<(Vec<T>, ColumnDataF<'static, bool>), ErrorDesc> {
        let p_index = self.columnindexmap.get(column_id);
        let p_column = self.get_part_col(column_id)?;

        self.materialize_common(dict, column_id, p_column, p_index)
    }

    //TO-DO - switch to a more general execution framework
    fn materialize_common<T: 'static + Send + Sync + Clone>(
        &self,
        dict: &Dictionary,
        column_id: &usize,
        p_column: PartitionedColumn,
        p_index: Option<&usize>,
    ) -> Result<(Vec<T>, ColumnDataF<'static, bool>), ErrorDesc> {
        if self.is_const(column_id)? {
            return self.materialize_const(dict, column_id);
        }

        //let p_column = p_column.column_vec();
        let total_len: usize = self.partition_sizes.iter().sum();

        let type_check: Result<(), &str> = p_column.iter().try_for_each(|c| {
            if c.column().is::<T>() {
                Ok(())
            } else {
                Err("Materialize called with type different from the column type")
            }
        });
        type_check?;

        let has_bitmap = p_column.iter().find(|c| c.bitmap().is_some()).is_some();

        //Reserve memory for the result column

        let mut output_data: Vec<MaybeUninit<T>> = Vec::with_capacity(total_len);

        //SAFETY: OK to do as the memory has been reserved, and moreover the type is assumed to be not initialized
        unsafe {
            output_data.set_len(total_len);
        }

        let mut output_slice = output_data.as_mut_slice();

        let mut output_vec: Vec<ColumnWrapper> = self
            .partition_sizes
            .iter()
            .map(|i| {
                let tmp = std::mem::replace(&mut output_slice, &mut []);
                let (l, r) = tmp.split_at_mut(*i);
                let _ = std::mem::replace(&mut output_slice, r);
                ColumnWrapper::new_from_columndata(ColumnData::SliceMut(SliceRefMut::new(l)))
            })
            .collect();

        let mut b: Vec<MaybeUninit<bool>> = Vec::new();

        if has_bitmap {
            b.reserve(total_len);
            //SAFETY: OK to do as the memory has been reserved, and moreover the type is assumed to be not initialized
            unsafe {
                b.set_len(total_len);
            }

            //Initialize b
            b.par_iter_mut().for_each(|b| *b = MaybeUninit::new(false));

            //SAFETY: OK to do as b has been initialized in the previous step
            let mut b_slice: &mut [bool] = unsafe { std::mem::transmute(b.as_mut_slice()) };

            output_vec
                .iter_mut()
                .zip(self.partition_sizes.iter())
                .for_each(|(c, i)| {
                    let tmp = std::mem::replace(&mut b_slice, &mut []);
                    let (l, r) = tmp.split_at_mut(*i);
                    let _ = std::mem::replace(&mut b_slice, r);
                    c.bitmap_set(ColumnDataF::new_from_slice_mut(l));
                });
        }

        if let Some(ind) = p_index {
            p_column
                .par_iter()
                .zip_eq(&self.indexes)
                .zip_eq(output_vec.par_iter_mut())
                .for_each(|((src, src_index), dst)| {
                    src.copy_to(dict, dst, &src_index[*ind]).unwrap();
                });
        } else {
            p_column
                .par_iter()
                .zip_eq(output_vec.par_iter_mut())
                .for_each(|(src, dst)| src.copy_to(dict, dst, &ColumnDataF::None).unwrap());
        };

        //SAFETY: b is either empty or fully initilized at this point
        let b: Vec<bool> = unsafe { std::mem::transmute(b) };
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
        let p_column = self.get_part_col(column_id)?;
        let is_const = self.is_const(column_id)?;

        let p_index = self.columnindexmap.get(column_id);

        let p_column_str: Vec<_> = p_column
            .iter()
            .map(|c| (c.as_string(dict, &ColumnDataF::None).unwrap(), c.bitmap()))
            .map(|(mut s, b)| {
                let mut s = if !is_const {
                    ColumnWrapper::new_from_vec(dict, s)
                } else {
                    ColumnWrapper::new_const(dict, s.pop().unwrap())
                };
                if b.is_some() {
                    s.bitmap_set(ColumnDataF::new_from_slice(b.downcast_ref().unwrap()));
                }
                s
            })
            .collect();

        let p_column_str_ref: Vec<_> = p_column_str.iter().collect();

        let (mut v, b) =
            self.materialize_common::<String>(dict, column_id, p_column_str_ref, p_index)?;
        if b.is_some() {
            v.par_iter_mut()
                .zip_eq(b.downcast_ref().unwrap().par_iter())
                .for_each(|(s, b)| {
                    if !b {
                        *s = "(null)".to_string()
                    }
                });
        }
        Ok(v)
    }
    pub fn print(&self, dict: &Dictionary) -> Result<(), ErrorDesc> {
        let v: Vec<Vec<String>> = (0usize..self.number_of_columns()?)
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
    pub fn op(
        &mut self,
        dict: &Dictionary,
        op: &str,
        c1_id: &usize,
        input_ids: &[usize],
    ) -> Result<(), ErrorDesc> {
        let (indexes, columnindexmap) = (&self.indexes, &self.columnindexmap);

        if input_ids.iter().find(|c| *c == c1_id).is_some() {
            Err(format!("In a write operation, the output column {} cannot also be equal to one of the inputs ({:?})", c1_id, &input_ids))?
        };

        let number_of_columns = self.number_of_columns()?;
        if (*c1_id >= number_of_columns)
            || input_ids
                .iter()
                .find(|c| **c >= number_of_columns)
                .is_some()
        {
            Err(format!(
                "Column index out of bounds: write column: {}, column inputs {:?}, while table has {} columns",
                c1_id, &input_ids, number_of_columns
            ))?
        };

        self.columns
            .par_iter_mut()
            .zip_eq(indexes.par_iter())
            .for_each(|(v_col, v_ind)| {
                let (s_left, s_right) = v_col.split_at_mut(*c1_id);
                let (c1, s_right) = s_right.split_at_mut(1);
                let c1 = &mut c1[0];
                let index_empty: ColumnDataF<usize> = ColumnDataF::None;

                let c1_index = match columnindexmap.get(c1_id) {
                    Some(i) => &v_ind[*i],
                    None => &index_empty,
                };

                let input: Vec<_> = input_ids
                    .iter()
                    .map(|c_id| {
                        let c = if c_id < c1_id {
                            &s_left[*c_id]
                        } else {
                            {
                                &s_right[c_id - c1_id - 1]
                            }
                        };
                        let c_index = match columnindexmap.get(c_id) {
                            Some(i) => &v_ind[*i],
                            None => &index_empty,
                        };

                        InputTypes::Ref(c, c_index)
                    })
                    .collect();

                c1.op(dict, op, c1_index, &input).unwrap();
            });

        Ok(())
    }

    //TO-DO - switch to a more general execution framework
    pub fn filter(&mut self, dict: &Dictionary, expr: &TableExpression) -> Result<(), ErrorDesc> {
        let number_of_columns = self.number_of_columns()?;
        let columnindexmap = &self.columnindexmap;

        let columns_without_index: Vec<_> = (0..number_of_columns)
            .into_iter()
            .filter(|i| self.columnindexmap.get(i).is_none())
            .collect();

        let num_of_cpus = num_cpus::get();
        let chunk_size = (self.partition_sizes.len() + num_of_cpus - 1) / num_of_cpus;
        self.indexes
            .par_chunks_mut(chunk_size)
            .zip_eq(self.columns.par_chunks(chunk_size))
            .zip_eq(self.partition_sizes.par_chunks_mut(chunk_size))
            .for_each(|((indexes, columns), partition_size)| {
                let mut buffer = ColumnBuffer::new();
                indexes
                    .iter_mut()
                    .zip(columns.iter())
                    .zip(partition_size.iter_mut())
                    .for_each(|((indexes, columns), partition_size)| {
                        let result = expr
                            .eval(dict, &mut buffer, columns, indexes, columnindexmap)
                            .unwrap();
                        let b = result.column().downcast_ref::<bool>().unwrap();
                        let bitmap = result.bitmap();

                        let mut hint_size: Option<usize> = None;
                        indexes.iter_mut().for_each(|i| {
                            let new_index_len = filter(i, b, bitmap, &hint_size).unwrap();
                            if hint_size.is_none() {
                                hint_size = Some(new_index_len);
                                *partition_size = new_index_len;
                            }
                        });
                        if !columns_without_index.is_empty() {
                            let mut ind: ColumnDataF<usize> = ColumnDataF::None;
                            filter(&mut ind, b, bitmap, &hint_size).unwrap();
                            indexes.push(ind);
                        }
                        buffer.push(dict, result);
                    });
            });

        if !columns_without_index.is_empty() {
            let insert_pos = self.indexes[0].len() - 1;
            columns_without_index.iter().for_each(|i| {
                self.columnindexmap.insert(*i, insert_pos);
            });
        }

        Ok(())
    }

    //TO-DO - switch to a more general execution framework
    pub fn add_expression_as_new_column(&mut self, dict: &Dictionary, expr: &TableExpression) {
        let (indexes, columnindexmap) = (&self.indexes, &self.columnindexmap);
        let num_of_cpus = num_cpus::get();
        let chunk_size = (self.partition_sizes.len() + num_of_cpus - 1) / num_of_cpus;
        self.columns
            .par_chunks_mut(chunk_size)
            .zip_eq(indexes.par_chunks(chunk_size))
            .for_each(|(c, i)| {
                let mut buffer = ColumnBuffer::new();
                c.iter_mut().zip(i.iter()).for_each(|(c, i)| {
                    let res = expr.eval(dict, &mut buffer, c, i, columnindexmap).unwrap();
                    c.push(res);
                });
            });
        //TO-DO - fix this!!!
        self.columns_nullable.push(true);
    }

    pub fn build_hash(&self, dict: &Dictionary, input_ids: &[usize]) -> Vec<Vec<u64>> {
        let num_of_cpus = num_cpus::get();
        let chunk_size = (self.partition_sizes.len() + num_of_cpus - 1) / num_of_cpus;

        //TO-DO: Switch to general execution framework
        let mut output: Vec<Vec<u64>> = self
            .partition_sizes
            .par_iter()
            .map(|i| Vec::with_capacity(*i))
            .collect();
        let index_empty = ColumnDataF::<usize>::None;

        input_ids.iter().for_each(|col_id| {
            let signature = Signature::new(
                "" as &str,
                vec![self.columns[0][*col_id].column().item_type_id()],
            );
            let iop = dict.columninternal.get(&signature).unwrap();
            let c_index = self.columnindexmap.get(col_id);
            self.columns
                .par_chunks(chunk_size)
                .zip_eq(self.indexes.par_chunks(chunk_size))
                .zip_eq(output.par_chunks_mut(chunk_size))
                .for_each(|((columns, indexes), output)| {
                    columns.iter().zip(indexes).zip(output).for_each(
                        |((columns, indexes), output)| {
                            let c_index = match c_index {
                                Some(i) => &indexes[*i],
                                None => &index_empty,
                            };
                            let c = &columns[*col_id];
                            iop.hash_in(c, c_index, output).unwrap()
                        },
                    );
                });
        });
        output
    }

    pub fn build_groups(&self, dict: &Dictionary, input_ids: &[usize]) -> Vec<Vec<usize>> {
        let num_of_cpus = num_cpus::get();
        let chunk_size = (self.partition_sizes.len() + num_of_cpus - 1) / num_of_cpus;

        //TO-DO: Switch to general execution framework
        let mut output: Vec<Vec<usize>> = self
            .partition_sizes
            .par_iter()
            .map(|i| vec![0; *i])
            .collect();
        let index_empty = ColumnDataF::<usize>::None;

        input_ids.iter().for_each(|col_id| {
            let signature = Signature::new(
                "" as &str,
                vec![self.columns[0][*col_id].column().item_type_id()],
            );
            let iop = dict.columninternal.get(&signature).unwrap();
            let c_index = self.columnindexmap.get(col_id);
            self.columns
                .par_chunks(chunk_size)
                .zip_eq(self.indexes.par_chunks(chunk_size))
                .zip_eq(output.par_chunks_mut(chunk_size))
                .for_each(|((columns, indexes), output)| {
                    let mut hashmap_buffer = HashMapBuffer::new();
                    let mut hashmap_binary: HashMap<
                        NullableValue<&[u8]>,
                        usize,
                        ahash::RandomState,
                    > = HashMap::with_capacity_and_hasher(100, ahash::RandomState::default());

                    columns.iter().zip(indexes).zip(output).for_each(
                        |((columns, indexes), output)| {
                            let c_index = match c_index {
                                Some(i) => &indexes[*i],
                                None => &index_empty,
                            };
                            let c = &columns[*col_id];
                            iop.group_in(
                                c,
                                c_index,
                                output,
                                &mut hashmap_buffer,
                                &mut hashmap_binary,
                            )
                            .unwrap()
                        },
                    );
                });
        });
        output
    }
    pub unsafe fn column_repartition(
        &self,
        dict: &Dictionary,
        hash: &Vec<Vec<u64>>,
        tmap: &TableToTableMap,
        col_id: &usize,
    ) -> Vec<ColumnWrapper> {
        struct UnsafeOutput {
            data: UnsafeCell<Vec<ColumnWrapper<'static>>>,
        }
        //SAFETY: check below
        unsafe impl Sync for UnsafeOutput {}

        let chunk_size = tmap.target_partition_size;

        let signature = Signature::new(
            "" as &str,
            vec![self.columns[0][*col_id].column().item_type_id()],
        );
        let iop = dict.columninternal.get(&signature).unwrap();
        let with_bitmap = self.columns_nullable[*col_id];

        let is_binary = self.columns[0][*col_id].is_binary();
        let c_index = self.columnindexmap.get(col_id);

        let number_of_buckets = tmap.number_of_buckets;
        let bucket_mask = (number_of_buckets - 1) as u64;

        if !is_binary {
            let output: Vec<ColumnWrapper> = tmap
                .bucket_number_of_elements
                .par_iter()
                .map(|i| iop.new_uninit(*i, 0, with_bitmap))
                .collect();

            let unsafe_output = UnsafeOutput {
                data: UnsafeCell::new(output),
            };

            self.columns
                .par_chunks(chunk_size)
                .zip_eq(self.indexes.par_chunks(chunk_size))
                .zip_eq(tmap.write_offsets.par_iter())
                .zip_eq(hash.par_chunks(chunk_size))
                .for_each(|(((columns, indexes), write_offsets), h)| {
                    let output = &mut *unsafe_output.data.get();
                    iop.copy_to_buckets_part1(
                        h,
                        bucket_mask,
                        columns,
                        indexes,
                        *col_id,
                        &c_index,
                        write_offsets,
                        output,
                        with_bitmap,
                    )
                    .unwrap();
                });
            let output = unsafe_output.data.into_inner();

            let output: Vec<_> = output
                .into_par_iter()
                .map(|c| iop.assume_init(c).unwrap())
                .collect();
            output
        } else {
            let output: Vec<ColumnWrapper> = tmap
                .bucket_number_of_elements
                .par_iter()
                .map(|i| iop.new_uninit(*i, 0, with_bitmap))
                .collect();

            let unsafe_output = UnsafeOutput {
                data: UnsafeCell::new(output),
            };

            self.columns
                .par_chunks(chunk_size)
                .zip_eq(self.indexes.par_chunks(chunk_size))
                .zip_eq(tmap.write_offsets.par_iter())
                .zip_eq(hash.par_chunks(chunk_size))
                .for_each(|(((columns, indexes), write_offsets), h)| {
                    let output = &mut *unsafe_output.data.get();
                    iop.copy_to_buckets_part1(
                        h,
                        bucket_mask,
                        columns,
                        indexes,
                        *col_id,
                        &c_index,
                        write_offsets,
                        output,
                        with_bitmap,
                    )
                    .unwrap();
                });
            let mut output = unsafe_output.data.into_inner();

            output.iter_mut().for_each(|c| {
                iop.copy_to_buckets_part2(c).unwrap();
            });

            let unsafe_output = UnsafeOutput {
                data: UnsafeCell::new(output),
            };

            self.columns
                .par_chunks(chunk_size)
                .zip_eq(self.indexes.par_chunks(chunk_size))
                .zip_eq(tmap.write_offsets.par_iter())
                .zip_eq(hash.par_chunks(chunk_size))
                .for_each(|(((columns, indexes), write_offsets), h)| {
                    let output = &mut *unsafe_output.data.get();
                    iop.copy_to_buckets_part3(
                        h,
                        bucket_mask,
                        columns,
                        indexes,
                        *col_id,
                        &c_index,
                        write_offsets,
                        output,
                    )
                    .unwrap();
                });
            let output = unsafe_output.data.into_inner();

            output
        }
    }
}
