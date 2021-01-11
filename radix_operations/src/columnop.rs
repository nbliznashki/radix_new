use std::mem::MaybeUninit;
use std::{any::Any, collections::VecDeque};
use std::{
    collections::HashMap,
    hash::{BuildHasher, Hash, Hasher},
};

use paste::paste;

use crate::Signature;
use crate::*;
use radix_column::*;

fn copy_of_into_boxed_slice<T>(boxed: Box<T>) -> Box<[T]> {
    // *mut T and *mut [T; 1] have the same size and alignment
    unsafe { Box::from_raw(Box::into_raw(boxed) as *mut [T; 1]) }
}

fn copy_to_buckets_part<T: 'static + Copy + Send + Sync>(
    hash: &Vec<u64>,
    buckets_mask: u64,
    src: &[T],
    src_index: &ColumnDataIndex,
    offsets: &mut VecDeque<usize>,
    dst: &mut [&mut [T]],
) -> Result<usize, ErrorDesc> {
    let mut items_written = 0;
    let src: ReadColumn<T> = ReadColumn::from((src, src_index));

    src.zip_and_for_each(hash.iter(), |((val, bitmap), h)| {
        let bucket_id = (*h & buckets_mask) as usize;
        dst[bucket_id][offsets[bucket_id]] = *val;
        offsets[bucket_id] += 1;
        items_written += 1;
    });
    Ok(items_written)
}

fn copy_to_buckets_data_uninit_part<T: 'static + Copy + Send + Sync>(
    hash: &Vec<u64>,
    buckets_mask: u64,
    src: &ColumnWrapper,
    src_index: &ColumnDataIndex,
    offsets: &mut VecDeque<usize>,
    dst: &mut [&mut [MaybeUninit<T>]],
) -> Result<usize, ErrorDesc> {
    let mut items_written = 0;
    let src: ReadColumn<T> = ReadColumn::from((src.column(), src.bitmap(), src_index, hash.len()));

    src.zip_and_for_each(hash.iter(), |((val, bitmap), h)| {
        let bucket_id = (*h & buckets_mask) as usize;
        dst[bucket_id][offsets[bucket_id]] = MaybeUninit::new(*val);
        offsets[bucket_id] += 1;
        items_written += 1;
    });
    Ok(items_written)
}
fn copy_to_buckets_bitmap_part<T: 'static + Copy + Send + Sync>(
    hash: &Vec<u64>,
    buckets_mask: u64,
    src: &ColumnWrapper,
    src_index: &ColumnDataIndex,
    offsets: &mut VecDeque<usize>,
    dst: &mut [&mut [bool]],
) -> Result<usize, ErrorDesc> {
    let mut items_written = 0;
    let src: ReadColumn<T> = ReadColumn::from((src.column(), src.bitmap(), src_index, hash.len()));

    src.zip_and_for_each(hash.iter(), |((val, bitmap), h)| {
        let bucket_id = (*h & buckets_mask) as usize;
        dst[bucket_id][offsets[bucket_id]] = *bitmap;
        offsets[bucket_id] += 1;
        items_written += 1;
    });
    Ok(items_written)
}

fn copy_to_buckets_binary_part<T: 'static + AsBytes + Send + Sync>(
    hash: &Vec<u64>,
    buckets_mask: u64,
    src: &ColumnWrapper,
    src_index: &ColumnDataIndex,
    offsets: &mut VecDeque<usize>,
    dst: &mut [(&mut [u8], &[usize], &[usize], usize)],
) -> Result<usize, ErrorDesc> {
    let mut bytes_written: usize = 0;
    let src: ReadBinaryColumn<T> =
        ReadBinaryColumn::from((src.column(), src.bitmap(), src_index, hash.len()));
    src.zip_and_for_each(hash.iter(), |((val, bitmap), h)| {
        let bucket_id = (*h & buckets_mask) as usize;
        let (dst_datau8, dst_start_pos, dst_len, dst_offset) = &mut dst[bucket_id];

        let start_pos_write = dst_start_pos[offsets[bucket_id]] - *dst_offset;
        let end_pos_write = start_pos_write + dst_len[offsets[bucket_id]];
        let slice_write = &mut dst_datau8[start_pos_write..end_pos_write];

        bytes_written += slice_write
            .iter_mut()
            .zip(val.iter())
            .map(|(t, s)| {
                *t = *s;
                1usize
            })
            .sum::<usize>();
        offsets[bucket_id] += 1;
    });
    Ok(bytes_written)
}

fn copy_to_buckets_binary_bitmap_part<T: 'static + Copy + Send + Sync>(
    hash: &Vec<u64>,
    buckets_mask: u64,
    src: &ColumnWrapper,
    src_index: &ColumnDataIndex,
    offsets: &mut VecDeque<usize>,
    dst: &mut [&mut [bool]],
) -> Result<usize, ErrorDesc> {
    let mut items_written = 0;
    let src: ReadColumn<T> = ReadColumn::from((src.column(), src.bitmap(), src_index, hash.len()));

    src.zip_and_for_each(hash.iter(), |((val, bitmap), h)| {
        let bucket_id = (*h & buckets_mask) as usize;
        dst[bucket_id][offsets[bucket_id]] = *bitmap;
        offsets[bucket_id] += 1;
        items_written += 1;
    });
    Ok(items_written)
}
///Operations which all columns must implement
pub trait ColumnInternalOp {
    fn len(&self, inp: &ColumnWrapper) -> Result<usize, ErrorDesc>;
    fn truncate(&self, inp: &mut ColumnWrapper) -> Result<(), ErrorDesc>;
    fn new<'a>(&self, data: Box<dyn Any>) -> Result<ColumnData<'a>, ErrorDesc>;
    fn new_ref<'a: 'b, 'b>(&self, data: SliceRef<'a>) -> Result<ColumnData<'b>, ErrorDesc>;
    fn new_mut<'a: 'b, 'b>(&self, data: SliceRefMut<'a>) -> Result<ColumnData<'b>, ErrorDesc>;
    fn copy_to<'a>(
        &self,
        src: &ColumnWrapper<'a>,
        dst: &mut ColumnWrapper<'a>,
        src_index: &ColumnDataIndex<'a>,
    ) -> Result<(), ErrorDesc>;
    fn as_string<'a>(
        &self,
        src: &ColumnWrapper<'a>,
        src_index: &ColumnDataIndex<'a>,
    ) -> Result<Vec<String>, ErrorDesc>;
    fn new_owned_with_capacity(
        &self,
        capacity: usize,
        binary_capacity: usize,
        with_bitmap: bool,
    ) -> ColumnWrapper<'static>;
    fn new_uninit(
        &self,
        number_of_items: usize,
        binary_storage: usize,
        with_bitmap: bool,
    ) -> ColumnWrapper<'static>;
    //SAFETY: The caller must take care that the column is fully initialized
    unsafe fn assume_init<'b>(&self, c: ColumnWrapper<'b>) -> Result<ColumnWrapper<'b>, ErrorDesc>;
    fn hash_in(
        &self,
        src: &ColumnWrapper,
        src_index: &ColumnDataIndex,
        dst: &mut Vec<u64>,
    ) -> Result<(), ErrorDesc>;

    fn copy_to_buckets_part1(
        &self,
        hash: &[Vec<u64>],
        buckets_mask: u64,
        src_columns: &[Vec<ColumnWrapper>],
        src_indexes: &[Vec<ColumnDataIndex>],
        col_id: usize,
        index_id: &Option<&usize>,
        offsets: &VecDeque<usize>,
        dst: &mut [ColumnWrapper<'static>],
        is_nullable: bool,
    ) -> Result<usize, ErrorDesc>;
    fn copy_to_buckets_part2(&self, dst: &mut ColumnWrapper<'static>) -> Result<usize, ErrorDesc>;
    fn copy_to_buckets_part3(
        &self,
        hash: &[Vec<u64>],
        buckets_mask: u64,
        src_columns: &[Vec<ColumnWrapper>],
        src_indexes: &[Vec<ColumnDataIndex>],
        col_id: usize,
        index_id: &Option<&usize>,
        offsets: &VecDeque<usize>,
        dst: &mut [ColumnWrapper<'static>],
    ) -> Result<usize, ErrorDesc>;
    fn group_in(
        &self,
        src: &ColumnWrapper,
        src_index: &ColumnDataIndex,
        dst: &mut Vec<usize>,
        hashmap_buffer: &mut HashMapBuffer,
        hashmap_binary: &mut HashMap<(usize, NullableValue<&[u8]>), usize, ahash::RandomState>,
    ) -> Result<(), ErrorDesc>;
}

const OP: &str = "";

macro_rules! sized_types_load {
    ($dict:ident; $($tr:ty)+) => ($(
            let signature=sig![OP;$tr];
            $dict.insert(
                signature, Box::new(
                    {
                        paste!{[<ColumnInternalOp $tr>]::new()}
                    }
                )
            );
    )+)
}

macro_rules! binary_types_load {
    ($dict:ident; $($tr:ty)+) => ($(
            let signature=sig![OP;$tr];
            $dict.insert(
                signature, Box::new(
                    {
                        paste!{[<ColumnBinaryInternalOp $tr>]::new()}
                    }
                )
            );
    )+)
}

macro_rules! sized_types_impl {
    ($($tr:ty)+) => ($(
        paste!{
            struct [<ColumnInternalOp $tr>]{}
            impl [<ColumnInternalOp $tr>]{
                fn new()->Self{
                    Self{}
                }
            }
            impl ColumnInternalOp for [<ColumnInternalOp $tr>]
            {
                fn len(&self, inp: &ColumnWrapper)->Result<usize, ErrorDesc>
                {
                    type T=$tr;
                    inp.column().data_len::<T>()
                }
                fn truncate(&self, inp: &mut ColumnWrapper) -> Result<(), ErrorDesc>{
                    type T=$tr;
                    inp.column_mut().truncate::<T>()
                }

                fn new<'a>(&self, data: Box<dyn Any>)->Result<ColumnData<'a>, ErrorDesc>{
                    type T=$tr;
                    let col = data.downcast::<Vec<T>>().map_err(|_| {
                        format!(
                            "Downcast failed. Target type is Vec<{}>",
                          std::any::type_name::<T>()
                        )
                    })?;
                    let col = copy_of_into_boxed_slice(col);
                    let mut res: Vec<Vec<T>> = col.into();
                    let res = res.pop().unwrap();
                    Ok(ColumnData::Owned(OwnedColumn::new(res)))
                }
                fn new_ref<'a: 'b,'b>(&self, data: SliceRef<'a>)->Result<ColumnData<'b>, ErrorDesc>
                {
                    type T=$tr;
                    if data.item_type_id()==std::any::TypeId::of::<T>(){
                        Ok(ColumnData::Slice(data))
                    } else {
                        //TO-DO: Better error message
                        Err("Wrong type")?
                    }

                }
                fn new_mut<'a: 'b, 'b>(&self, data: SliceRefMut<'a>) -> Result<ColumnData<'b>, ErrorDesc>{
                    type T=$tr;
                    if data.item_type_id()==std::any::TypeId::of::<T>(){
                        Ok(ColumnData::SliceMut(data))
                    } else {
                        //TO-DO: Better error message
                        Err("Wrong type")?
                    }
                }
                fn copy_to<'a>(
                    &self,
                    src: &ColumnWrapper<'a>,
                    dst: &mut ColumnWrapper<'a>,
                    src_index: &ColumnDataIndex<'a>,
                ) -> Result<(), ErrorDesc>
                {
                    type T=$tr;

                    let input=vec![InputTypes::Ref(src, src_index)];


                    assign_2_sized_sized_unroll::<MaybeUninit<T>, T, _>(dst, &input, |c1_data,c1_bool|
                        (*c1_bool, MaybeUninit::new(*c1_data))
                   )


                }

                fn as_string<'a>(
                    &self,
                    src: &ColumnWrapper<'a>,
                    src_index: &ColumnDataIndex<'a>,
                ) -> Result<Vec<String>, ErrorDesc>
                {
                    type T=$tr;

                    let input=[InputTypes::Ref(src, src_index)];
                    let output: Vec<String>=Vec::new();

                    let output_col=OwnedColumn::new(output);
                    let output_col=ColumnData::Owned(output_col);
                    let mut output_col=ColumnWrapper::new_from_columndata(output_col);
                    let bitmap_update_required=false;

                    insert_2_sized_sized_unroll::<String, T, _>(&mut output_col, &input, &bitmap_update_required, |c1_data,c1_bool|
                        (*c1_bool, format!("{}", *c1_data))
                    )?;

                    let output=output_col.get_inner().0.downcast_owned::<String>()?;
                    Ok(output)

                }
                fn new_owned_with_capacity(&self, number_of_items: usize, _binary_capacity: usize, with_bitmap: bool) -> ColumnWrapper<'static>{
                    type T=$tr;
                    let c=ColumnData::Owned(OwnedColumn::new(Vec::<T>::with_capacity(number_of_items)));
                    let mut c=ColumnWrapper::new_from_columndata (c);
                    if with_bitmap{
                        c.bitmap_set(ColumnDataF::new(vec![false; number_of_items]))
                    };
                    c
                }
                fn new_uninit(&self, number_of_items: usize, _binary_storage: usize, with_bitmap: bool) -> ColumnWrapper<'static>{
                    type T=$tr;
                    let c=ColumnData::Owned(OwnedColumn::new_uninit::<T>(number_of_items));
                    let mut c=ColumnWrapper::new_from_columndata (c);
                    if with_bitmap{
                        c.bitmap_set(ColumnDataF::new(vec![false; number_of_items]))
                    };
                    c
                }

                unsafe fn assume_init<'b>(&self, c: ColumnWrapper<'b>) -> Result<ColumnWrapper<'b>, ErrorDesc>{
                    type T=$tr;
                    let (column, bitmap)=c.get_inner();
                    let column=column.assume_init::<T>()?;
                    let mut c=ColumnWrapper::new_from_columndata (column);
                    c.bitmap_set(bitmap);
                    Ok(c)
                }

                fn hash_in(&self, src: &ColumnWrapper, src_index: &ColumnDataIndex, dst: &mut Vec<u64>)-> Result<(), ErrorDesc>{


                    type T=$tr;

                    let input=[InputTypes::Ref(src, src_index)];
                    let v_empty: Vec<u64>=Vec::new();
                    let output: Vec<u64>=std::mem::replace(dst, v_empty);

                    let insert_required=output.len()==0;

                    let output_col=OwnedColumn::new(output);
                    let output_col=ColumnData::Owned(output_col);
                    let mut output_col=ColumnWrapper::new_from_columndata(output_col);
                    let bitmap_update_required=false;

                    let s=ahash::RandomState::with_seeds(2194717786824016851,7391161229587532433,8421638162391593347, 13425712476683680973);

                    if insert_required {
                        insert_2_sized_sized_unroll::<u64, T, _>(&mut output_col, &input, &bitmap_update_required,
                            |c1_data,c1_bool|
                           {
                               let mut h = s.build_hasher();
                               c1_data.hash(&mut h);
                               (true, h.finish()|(*c1_bool as u64).wrapping_sub(1))
                           }
                       )?;
                    } else {
                        update_2_sized_sized_unroll::<u64, T, _>(&mut output_col, &ColumnDataIndex::None, &input,
                            |data, _bitmap, (c1_data,c1_bool)|
                           {
                               let mut h = s.build_hasher();
                               c1_data.hash(&mut h);
                               *data=data.wrapping_add(h.finish()|(*c1_bool as u64).wrapping_sub(1));
                           }
                       )?;
                    }
                    let output=output_col.get_inner().0.downcast_owned::<u64>()?;
                    let _output: Vec<u64>=std::mem::replace(dst, output);

                    Ok(())

                }
                fn copy_to_buckets_part1(
                    &self,
                    hash: &[Vec<u64>],
                    buckets_mask: u64,
                    src_columns: &[Vec<ColumnWrapper>],
                    src_indexes: &[Vec<ColumnDataIndex>],
                    col_id: usize,
                    index_id: &Option<&usize>,
                    offsets: &VecDeque<usize>,
                    dst: &mut [ColumnWrapper<'static>],
                    is_nullable: bool,
                ) -> Result<usize, ErrorDesc>
                {
                    type T=$tr;

                    let mut dst_data: Vec<_>=dst.iter_mut().map(|c| c.column_mut()).map(|c| c.downcast_mut::<MaybeUninit<T>>().unwrap()).collect();
                    let mut offsets_tmp=offsets.clone();
                    let mut items_written=0;

                    let index_empty=ColumnDataIndex::None;

                    src_columns.iter().zip(src_indexes.iter()).zip(hash.iter()).for_each(|((src, src_index), hash)|{
                        let src=&src[col_id];
                        let src_index=match index_id {
                            Some(i) => &src_index[**i],
                            None => &index_empty,
                        };
                        items_written+=copy_to_buckets_data_uninit_part(hash, buckets_mask, src, src_index, &mut offsets_tmp, &mut dst_data).unwrap();
                    });

                    if is_nullable {
                        let mut offsets_tmp=offsets.clone();
                        let mut dst_bitmap: Vec<_>=dst.iter_mut().map(|c| c.bitmap_mut().downcast_mut().unwrap()).collect();

                        src_columns.iter().zip(src_indexes.iter()).zip(hash.iter()).for_each(|((src, src_index), hash)|{

                            let src=&src[col_id];
                            let src_index=match index_id {
                                Some(i) => &src_index[**i],
                                None => &index_empty,
                            };
                            copy_to_buckets_bitmap_part::<T>(hash, buckets_mask, src, src_index, &mut offsets_tmp, &mut dst_bitmap).unwrap();
                        });
                    }

                    Ok(items_written)
                }

                fn copy_to_buckets_part2(&self, _dst: &mut ColumnWrapper<'static>) -> Result<usize, ErrorDesc>{Err("copy_to_buckets_part2 called for a sized type")?}
                fn copy_to_buckets_part3(
                    &self,
                    _hash: &[Vec<u64>],
                    _buckets_mask: u64,
                    _src_columns: &[Vec<ColumnWrapper>],
                    _src_indexes: &[Vec<ColumnDataIndex>],
                    _col_id: usize,
                    _index_id: &Option<&usize>,
                    _offsets: &VecDeque<usize>,
                    _dst: &mut [ColumnWrapper<'static>],
                ) -> Result<usize, ErrorDesc>{Err("copy_to_buckets_part2 called for a sized type")?}

                fn group_in(
                    &self,
                    src: &ColumnWrapper,
                    src_index: &ColumnDataIndex,
                    dst: &mut Vec<usize>,
                    hashmap_buffer: &mut HashMapBuffer,
                    _hashmap_binary: &mut HashMap<(usize, NullableValue<&[u8]>), usize, ahash::RandomState>,
                ) -> Result<(), ErrorDesc>{
                    type T=$tr;
                    let src_is_const=src.column().is_const();
                    let src=ReadColumn::from((src.column(), src.bitmap(), src_index, dst.len()));
                    let mut h=hashmap_buffer.pop::<T>();

                    if dst.len()==0{
                        //We have to do an insert
                        src.enumerate_and_for_each(|(i,(value, bitmap))| {
                            let new_group_id: usize=*h.entry((0, NullableValue{value: *value, bitmap: *bitmap})).or_insert(i);
                            dst.push(new_group_id);
                        });
                    } else if !src_is_const{
                        //We have to do an update
                        src.enumerate_and_for_each(|(i,(value, bitmap))| {
                            let new_group_id: usize=*h.entry((0, NullableValue{value: *value, bitmap: *bitmap})).or_insert(i);
                            dst[i]=new_group_id;
                        });

                    }
                    hashmap_buffer.push(h);
                    Ok(())
                }
            }

    }




    )+)
}

macro_rules! binary_types_impl {
    ($($tr:ty)+) => ($(
        paste!{
            struct [<ColumnBinaryInternalOp $tr>]{}
            impl [<ColumnBinaryInternalOp $tr>]{
                fn new()->Self{
                    Self{}
                }
            }
            impl ColumnInternalOp for [<ColumnBinaryInternalOp $tr>]
            {
                fn len(&self, inp: &ColumnWrapper)->Result<usize, ErrorDesc>{
                    type T=$tr;
                    inp.column().downcast_binary_ref::<T>().map(|(_,_,c,_)| c.len())
                }

                fn truncate(&self, _inp: &mut ColumnWrapper) -> Result<(), ErrorDesc>{
                    Err("Truncate for binary columns should not be done by an internal operation")?
                }
                fn new<'a>(&self, data: Box<dyn Any>)->Result<ColumnData<'a>, ErrorDesc>{
                    type T=$tr;
                    let col = data.downcast::<Vec<T>>().map_err(|_| {
                        format!(
                            "Downcast failed. Target type is Vec<{}>",
                          std::any::type_name::<T>()
                        )
                    })?;
                    let col = copy_of_into_boxed_slice(col);
                    let mut res: Vec<Vec<T>> = col.into();
                    let res = res.pop().unwrap();
                    Ok(ColumnData::BinaryOwned(OnwedBinaryColumn::new(res.as_slice())))
                }

                fn new_ref<'a,'b>(&self, data: SliceRef<'a>)->Result<ColumnData<'b>, ErrorDesc>
                where
                    'a: 'b
                {
                    type T=$tr;
                    let col=data.downcast_ref::<T>()?;
                    Ok(ColumnData::BinaryOwned(OnwedBinaryColumn::new(col)))
                }
                fn new_mut<'a: 'b, 'b>(&self, data: SliceRefMut<'a>) -> Result<ColumnData<'b>, ErrorDesc>{
                    type T=$tr;
                    let col=data.downcast_ref::<T>()?;
                    Ok(ColumnData::BinaryOwned(OnwedBinaryColumn::new(col)))
                }
                fn copy_to<'a>(
                    &self,
                    src: &ColumnWrapper<'a>,
                    dst: &mut ColumnWrapper<'a>,
                    src_index: &ColumnDataIndex<'a>,
                ) -> Result<(), ErrorDesc>
                {
                    type T=$tr;


                    let input=vec![InputTypes::Ref(src, src_index)];


                    assign_2_sized_binary_unroll::<MaybeUninit<T>, T, _>(dst, &&*input,  |c1_data,c1_bool|
                        (*c1_bool, MaybeUninit::new(<T as AsBytes>::from_bytes(c1_data))))



                }

                fn as_string<'a>(
                    &self,
                    src: &ColumnWrapper<'a>,
                    src_index: &ColumnDataIndex<'a>,
                ) -> Result<Vec<String>, ErrorDesc>
                {
                    type T=$tr;

                    let input=[InputTypes::Ref(src, src_index)];
                    let output: Vec<String>=Vec::new();

                    let output_col=OwnedColumn::new(output);
                    let output_col=ColumnData::Owned(output_col);
                    let mut output_col=ColumnWrapper::new_from_columndata(output_col);
                    let bitmap_update_required=false;

                    insert_2_sized_binary_unroll::<String, T, _>(&mut output_col, &input, &bitmap_update_required, |c1_data,c1_bool|
                        (*c1_bool, AsBytes::from_bytes(c1_data))
                    )?;

                    let output=output_col.get_inner().0.downcast_owned::<String>()?;
                    Ok(output)

                }
                fn new_owned_with_capacity(&self, number_of_items: usize, binary_capacity: usize, with_bitmap: bool) -> ColumnWrapper<'static>{
                    type T=$tr;
                    let mut c=ColumnWrapper::new_from_columndata (ColumnData::BinaryOwned(OnwedBinaryColumn::new_with_capacity(&[] as &[T], number_of_items,binary_capacity)));
                    if with_bitmap{
                        c.bitmap_set(ColumnDataF::new(vec![false; number_of_items]))
                    };
                    c
                }

                fn new_uninit(&self, number_of_items: usize, binary_storage: usize, with_bitmap: bool) -> ColumnWrapper<'static>{
                    type T=$tr;
                    let mut c=ColumnWrapper::new_from_columndata (ColumnData::BinaryOwned(OnwedBinaryColumn::new_uninit::<T>(number_of_items,binary_storage)));
                    if with_bitmap{
                        c.bitmap_set(ColumnDataF::new(vec![false; number_of_items]))
                    };
                    c
                }

                unsafe fn assume_init<'b>(&self, c: ColumnWrapper<'b>) -> Result<ColumnWrapper<'b>, ErrorDesc>{
                    Ok(c)
                }


                fn hash_in(&self, src: &ColumnWrapper, src_index: &ColumnDataIndex, dst: &mut Vec<u64>)-> Result<(), ErrorDesc>{
                    type T=$tr;
                    let input=[InputTypes::Ref(src, src_index)];
                    let v_empty: Vec<u64>=Vec::new();
                    let output: Vec<u64>=std::mem::replace(dst, v_empty);

                    let insert_required=output.len()==0;

                    let output_col=OwnedColumn::new(output);
                    let output_col=ColumnData::Owned(output_col);
                    let mut output_col=ColumnWrapper::new_from_columndata(output_col);
                    let bitmap_update_required=false;

                    let s=ahash::RandomState::with_seeds(2194717786824016851,7391161229587532433,8421638162391593347, 13425712476683680973);

                    if insert_required {
                        insert_2_sized_binary_unroll::<u64, T, _>(&mut output_col, &input, &bitmap_update_required,
                            |c1_data,c1_bool|
                           {
                               let mut h = s.build_hasher();
                               c1_data.hash(&mut h);
                               (true, h.finish()|(*c1_bool as u64).wrapping_sub(1))
                           }
                       )?;
                    } else {
                        update_2_sized_binary_unroll::<u64, T, _>(&mut output_col, &ColumnDataIndex::None, &input,
                            |data, _bitmap, (c1_data,c1_bool)|
                           {
                               let mut h = s.build_hasher();
                               c1_data.hash(&mut h);
                               *data=data.wrapping_add(h.finish()|(*c1_bool as u64).wrapping_sub(1));
                           }
                       )?;
                    }
                    let output=output_col.get_inner().0.downcast_owned::<u64>()?;
                    let _output: Vec<u64>=std::mem::replace(dst, output);

                    Ok(())
                }
                fn copy_to_buckets_part1(
                    &self,
                    hash: &[Vec<u64>],
                    buckets_mask: u64,
                    src_columns: &[Vec<ColumnWrapper>],
                    src_indexes: &[Vec<ColumnDataIndex>],
                    col_id: usize,
                    index_id: &Option<&usize>,
                    offsets: &VecDeque<usize>,
                    dst: &mut [ColumnWrapper<'static>],
                    is_nullable: bool,
                ) -> Result<usize, ErrorDesc>
                {
                    type T=$tr;

                    let mut dst_data: Vec<_>=dst.iter_mut().map(|c| c.column_mut()).map(|c| c.downcast_binary_mut::<T>().unwrap().2).collect();
                    let mut offsets_tmp=offsets.clone();
                    let mut items_written=0;


                    let index_empty=ColumnDataIndex::None;

                    src_columns.iter().zip(src_indexes.iter()).zip(hash.iter()).for_each(|((src, src_index), hash)|{
                        let src=src[col_id].column().downcast_binary_ref::<T>().unwrap().2;
                        let src_index=match index_id {
                            Some(i) => &src_index[**i],
                            None => &index_empty,
                        };
                        items_written+=copy_to_buckets_part(hash, buckets_mask, src, src_index, &mut offsets_tmp, &mut dst_data).unwrap();
                    });

                    if is_nullable {
                        let mut offsets_tmp=offsets.clone();
                        let mut dst_bitmap: Vec<_>=dst.iter_mut().map(|c| c.bitmap_mut().downcast_mut().unwrap()).collect();

                        src_columns.iter().zip(src_indexes.iter()).zip(hash.iter()).for_each(|((src, src_index), hash)|{

                            let src=src[col_id].bitmap().downcast_ref().unwrap();
                            let src_index=match index_id {
                                Some(i) => &src_index[**i],
                                None => &index_empty,
                            };
                            items_written+=copy_to_buckets_part(hash, buckets_mask, src, src_index, &mut offsets_tmp, &mut dst_bitmap).unwrap();
                        });
                    }

                    Ok(items_written)
                }

                fn copy_to_buckets_part2(&self, dst: &mut ColumnWrapper<'static>) -> Result<usize, ErrorDesc>{
                    type T=$tr;
                    let  (datau8, start_pos, len, offset)=dst.column_mut().downcast_binary_vec::<T>()?;
                    let mut cur_start_pos=0;
                    len.iter().zip(start_pos.iter_mut()).for_each(|(l,s)| {*s=cur_start_pos; cur_start_pos+=l});
                    *datau8=vec![0; cur_start_pos];
                    *offset=0;
                    Ok(cur_start_pos)

                }
                fn copy_to_buckets_part3(
                    &self,
                    hash: &[Vec<u64>],
                    buckets_mask: u64,
                    src_columns: &[Vec<ColumnWrapper>],
                    src_indexes: &[Vec<ColumnDataIndex>],
                    col_id: usize,
                    index_id: &Option<&usize>,
                    offsets: &VecDeque<usize>,
                    dst: &mut [ColumnWrapper<'static>],
                ) -> Result<usize, ErrorDesc>{
                    type T=$tr;

                    let mut dst_data: Vec<_>=dst.iter_mut().map(|c| c.column_mut()).map(|c| c.downcast_binary_mut::<T>().unwrap())
                    .map(|(src_datau8, src_start_pos,src_len,src_offset)|(src_datau8, &*src_start_pos,&*src_len,*src_offset)).collect();
                    let mut offsets_tmp=offsets.clone();
                    let mut bytes_written=0;

                    let index_empty=ColumnDataIndex::None;

                    src_columns.iter().zip(src_indexes.iter()).zip(hash.iter()).for_each(|((src, src_index), hash)|{
                        let src=&src[col_id];
                        let src_index=match index_id {
                            Some(i) => &src_index[**i],
                            None => &index_empty,
                        };

                        bytes_written+=copy_to_buckets_binary_part::<T>(hash, buckets_mask, src , src_index, &mut offsets_tmp, &mut dst_data).unwrap();
                    });
                    Ok(bytes_written)
                }

                fn group_in(
                    &self,
                    src: &ColumnWrapper,
                    src_index: &ColumnDataIndex,
                    dst: &mut Vec<usize>,
                    _hashmap_buffer: &mut HashMapBuffer,
                    hashmap_binary: &mut HashMap<(usize, NullableValue<&[u8]>), usize, ahash::RandomState>,
                ) -> Result<(), ErrorDesc>{

                    type T=$tr;
                    let src_is_const=src.column().is_const();
                    let src=ReadBinaryColumn::<T>::from((src.column(), src.bitmap(), src_index, dst.len()));
                    hashmap_binary.clear();

                    if dst.len()==0{
                        //We have to do an insert
                        src.enumerate_and_for_each(|(i,(value, bitmap))| {

                            let nullableslice=NullableValue{
                                value: value,
                                bitmap: *bitmap
                            };
                            let val: (usize, NullableValue<&[u8]>)=(0,nullableslice);

                            //SAFETY: hashmap_binary would outlive the slice to src, however src is guaranteed to be live until the hashmap is cleared.
                            //        Once the hashmap is cleared, there should be no references to src, and therefore no reason why we need to insist on
                            //        having to drop hashmap_binary.
                            let val: (usize, NullableValue<&[u8]>)=unsafe{std::mem::transmute(val)};
                            let new_group_id=hashmap_binary.entry(val).or_insert(i);
                            dst.push(*new_group_id);
                        });
                    } else if !src_is_const{
                        //We have to do an update
                        src.enumerate_and_for_each(|(i,(value, bitmap))| {
                            let nullableslice=NullableValue{
                                value: value,
                                bitmap: *bitmap
                            };
                            let val: (usize, NullableValue<&[u8]>)=(0,nullableslice);

                            //SAFETY: hashmap_binary would outlive the slice to src, however src is guaranteed to be live until the hashmap is cleared.
                            //        Once the hashmap is cleared, there should be no references to src, and therefore no reason why we need to insist on
                            //        having to drop hashmap_binary.
                            let val: (usize, NullableValue<&[u8]>)=unsafe{std::mem::transmute(val)};
                            let new_group_id=hashmap_binary.entry(val).or_insert(i);
                            dst[i]=*new_group_id;
                        });

                    }
                    hashmap_binary.clear();
                    Ok(())


                }

            }


    }
    )+)
}
/*
fn copy_to_buckets_binary_part(
    hash: &Vec<u64>,
    buckets_mask: u64,
    src_datau8: &[u8],
    src_start_pos: &[usize],
    src_len: &[usize],
    src_offset: &usize,
    src_index: &ColumnDataIndex,
    offsets: &mut VecDeque<usize>,
    dst: &mut [(&mut [u8], &[usize], &[usize], usize)],
)
*/

sized_types_impl! {
u64 u32 u16 u8 bool usize
}

binary_types_impl! {
String
}

//{ usize u8 u16 u32 u64 u128 isize i8 i16 i32 i64 i128 f32 f64 }

//binary_operation_impl! { (u64,u8) (u64,u16) (u64,u32) (u64,u64) }

pub fn load_columninternal_dict(part_dict: &mut ColumnInternalDictionary) {
    //dict.insert(s, columnadd_onwedcolumnvecu64_vecu64);7
    sized_types_load! {part_dict;
        u64 u32 u16 u8 bool usize
    };

    binary_types_load! {part_dict;
        String
    };
}
