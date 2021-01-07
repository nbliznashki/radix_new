use radix_column::*;
use radix_operations::*;
use rayon::prelude::*;

pub(crate) fn part_with_sizes<'a, T>(
    dict: &Dictionary,
    inp_data: &'a [T],
    inp_bitmap: &'a [bool],
    len_vec: &Vec<usize>,
) -> Result<Vec<ColumnWrapper<'a>>, ErrorDesc>
where
    T: Send + Sync + 'static,
{
    let total_len: usize = len_vec.par_iter().sum();
    if inp_data.len() != total_len {
        Err(format!(
            "Attempt to partition a slice with {} elements into partitions with total elements {}",
            inp_data.len(),
            total_len
        ))?
    }
    if inp_bitmap.len() != total_len && inp_bitmap.len() > 0 {
        Err(format!(
            "Attempt to partition a bitmap with {} elements into partitions with total elements {}",
            inp_data.len(),
            total_len
        ))?
    }

    let mut cur_pos: usize = 0;

    let output_vec: Vec<ColumnWrapper> = len_vec
        .iter()
        .map(|i| {
            let mut s = <ColumnWrapper as ColumnOperations>::new_from_slice(
                dict,
                &inp_data[cur_pos..cur_pos + *i],
            );
            if inp_bitmap.len() != 0 {
                s.bitmap_set(ColumnDataF::new_from_slice(
                    &inp_bitmap[cur_pos..cur_pos + *i],
                ))
            };
            cur_pos += i;
            s
        })
        .collect();

    let p_column = output_vec;
    Ok(p_column)
}

pub(crate) fn part_with_sizes_mut<'a, T>(
    inp_data: &'a mut [T],
    inp_bitmap: &'a mut [bool],
    len_vec: &Vec<usize>,
) -> Result<Vec<ColumnWrapper<'a>>, ErrorDesc>
where
    T: Send + Sync + 'static,
{
    let total_len: usize = len_vec.par_iter().sum();
    if inp_data.len() != total_len {
        Err(format!(
            "Attempt to partition a slice with {} elements into partitions with total elements {}",
            inp_data.len(),
            total_len
        ))?
    }
    if inp_bitmap.len() != total_len && inp_bitmap.len() > 0 {
        Err(format!(
            "Attempt to partition a bitmap with {} elements into partitions with total elements {}",
            inp_data.len(),
            total_len
        ))?
    }

    let has_bitamp = inp_bitmap.len() != 0;
    let mut inp_data = inp_data;
    let mut inp_bitmap = inp_bitmap;

    let output_vec: Vec<ColumnWrapper> = len_vec
        .iter()
        .map(|i| {
            let tmp = std::mem::replace(&mut inp_data, &mut []);
            let (l, r) = tmp.split_at_mut(*i);
            let _ = std::mem::replace(&mut inp_data, r);
            let mut c =
                ColumnWrapper::new_from_columndata(ColumnData::SliceMut(SliceRefMut::new(l)));

            if has_bitamp {
                let tmp = std::mem::replace(&mut inp_bitmap, &mut []);
                let (l, r) = tmp.split_at_mut(*i);
                let _ = std::mem::replace(&mut inp_bitmap, r);
                c.bitmap_set(ColumnDataF::new_from_slice_mut(l));
            }
            c
        })
        .collect();
    Ok(output_vec)
}

pub(crate) fn filter(
    index: &mut ColumnDataIndex,
    keep: &[bool],
    bitmap: &ColumnDataF<bool>,
    size_hint: &Option<usize>,
) -> Result<usize, ErrorDesc> {
    let size_hint = size_hint.unwrap_or(keep.len() / 2);
    if index.is_some() {
        assert_eq!(index.len().unwrap(), keep.len());
        if index.is_owned() {
            let index = index.downcast_vec()?;
            let mut del = 0;
            if bitmap.is_some() {
                keep.iter()
                    .zip(bitmap.downcast_ref()?.iter())
                    .enumerate()
                    .for_each(|(i, (b, bitmap))| {
                        let b = *b && *bitmap;
                        let i_new = i - (b as usize) * del;
                        del += !b as usize;
                        index.swap(i_new, i);
                    });
            } else {
                keep.iter().enumerate().for_each(|(i, b)| {
                    let i_new = i - (*b as usize) * del;
                    del += !b as usize;
                    index.swap(i_new, i);
                });
            }
            index.truncate(index.len() - del);
        } else {
            let mut index_new: Vec<usize> = Vec::with_capacity(size_hint);
            if bitmap.is_some() {
                index_new.extend(
                    index
                        .downcast_ref()?
                        .iter()
                        .zip(keep.iter())
                        .zip(bitmap.downcast_ref().unwrap())
                        .filter(|((_, b), bitmap)| **b && **bitmap)
                        .map(|((i, _), _)| *i),
                );
            } else {
                index_new.extend(
                    index
                        .downcast_ref()
                        .unwrap()
                        .iter()
                        .zip(keep.iter())
                        .filter(|(_, b)| **b)
                        .map(|(i, _)| *i),
                );
            }
            *index = ColumnDataIndex::new(index_new);
        }
    } else {
        let mut index_new = if index.is_owned() && index.downcast_vec()?.capacity() > 0 {
            std::mem::replace(index.downcast_vec()?, vec![])
        } else {
            Vec::<usize>::with_capacity(size_hint)
        };
        if bitmap.is_some() {
            index_new.extend(
                keep.iter()
                    .zip(bitmap.downcast_ref()?.iter())
                    .enumerate()
                    .filter(|(_, (b, bitmap))| **b && **bitmap)
                    .map(|(i, _)| i),
            );
        } else {
            index_new.extend(keep.iter().enumerate().filter(|(_, b)| **b).map(|(i, _)| i));
        }
        *index = ColumnDataIndex::new(index_new);
    }
    Ok(index.len().unwrap())
}
