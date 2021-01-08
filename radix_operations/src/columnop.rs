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

fn copy_to_buckets_part<T: Copy>(
    hash: &Vec<u64>,
    buckets_mask: u64,
    src: &[T],
    src_index: &ColumnDataIndex,
    offsets: &mut VecDeque<usize>,
    dst: &mut [&mut [T]],
) -> Result<usize, ErrorDesc> {
    let mut items_written = 0;
    if let Ok(src_index) = src_index.downcast_ref() {
        src_index.iter().zip(hash.iter()).for_each(|(i, h)| {
            let bucket_id = (*h & buckets_mask) as usize;
            dst[bucket_id][offsets[bucket_id]] = src[*i];
            offsets[bucket_id] += 1;
            items_written += 1;
        });
    } else {
        src.iter().zip(hash.iter()).for_each(|(val, h)| {
            let bucket_id = (*h & buckets_mask) as usize;
            dst[bucket_id][offsets[bucket_id]] = *val;
            offsets[bucket_id] += 1;
            items_written += 1;
        });
    }
    Ok(items_written)
}

fn copy_to_buckets_part_uninit<T: Copy>(
    hash: &Vec<u64>,
    buckets_mask: u64,
    src: &[T],
    src_index: &ColumnDataIndex,
    offsets: &mut VecDeque<usize>,
    dst: &mut [&mut [MaybeUninit<T>]],
) -> Result<usize, ErrorDesc> {
    let mut items_written = 0;
    if let Ok(src_index) = src_index.downcast_ref() {
        src_index.iter().zip(hash.iter()).for_each(|(i, h)| {
            let bucket_id = (*h & buckets_mask) as usize;
            dst[bucket_id][offsets[bucket_id]] = MaybeUninit::new(src[*i]);
            offsets[bucket_id] += 1;
            items_written += 1;
        });
    } else {
        src.iter().zip(hash.iter()).for_each(|(val, h)| {
            let bucket_id = (*h & buckets_mask) as usize;

            dst[bucket_id][offsets[bucket_id]] = MaybeUninit::new(*val);
            offsets[bucket_id] += 1;
            items_written += 1;
        });
    }
    Ok(items_written)
}

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
) -> Result<usize, ErrorDesc> {
    let mut bytes_written: usize = 0;
    if let Ok(src_index) = src_index.downcast_ref() {
        src_index.iter().zip(hash.iter()).for_each(|(i, h)| {
            let bucket_id = (*h & buckets_mask) as usize;
            let (dst_datau8, dst_start_pos, dst_len, _a) = &mut dst[bucket_id];
            let start_u8 = src_start_pos[*i] - src_offset;
            let end_u8 = start_u8 + src_len[*i];
            let slice_read = &src_datau8[start_u8..end_u8];

            let start_pos_write = dst_start_pos[offsets[bucket_id]];
            let end_pos_write = start_pos_write + dst_len[offsets[bucket_id]];
            let slice_write = &mut dst_datau8[start_pos_write..end_pos_write];

            bytes_written += slice_write
                .iter_mut()
                .zip(slice_read.iter())
                .map(|(t, s)| {
                    *t = *s;
                    1usize
                })
                .sum::<usize>();
            offsets[bucket_id] += 1;
        });
    } else {
        src_start_pos
            .iter()
            .zip(src_len)
            .zip(hash)
            .for_each(|((start_pos, len), h)| {
                let bucket_id = (*h & buckets_mask) as usize;
                let (dst_datau8, dst_start_pos, dst_len, _a) = &mut dst[bucket_id];

                let start_u8 = start_pos - src_offset;
                let end_u8 = start_u8 + len;
                let slice_read = &src_datau8[start_u8..end_u8];

                let start_pos_write = dst_start_pos[offsets[bucket_id]];
                let end_pos_write = start_pos_write + dst_len[offsets[bucket_id]];
                let slice_write = &mut dst_datau8[start_pos_write..end_pos_write];

                bytes_written += slice_write
                    .iter_mut()
                    .zip(slice_read.iter())
                    .map(|(t, s)| {
                        *t = *s;
                        1usize
                    })
                    .sum::<usize>();
            });
    }
    Ok(bytes_written)
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
                    inp.column().downcast_ref::<T>().map(|c| c.len())
                }
                fn truncate(&self, inp: &mut ColumnWrapper) -> Result<(), ErrorDesc>{
                    type T=$tr;
                    if let ColumnData::Owned(c)=inp.column_mut(){
                        c.downcast_vec::<T>()?.truncate(0);
                        Ok(())
                    } else
                    {
                        Err("Only ColumnData::Owned can be truncated")?
                    }
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

                    let bitmap_update_required=src.bitmap().is_some();
                    let input=vec![InputTypes::Ref(src, src_index)];


                    insert_2_sized_sized_unroll::<MaybeUninit<T>, T, _,_>(dst, &input, &bitmap_update_required, |c1_data,_c1_bool|
                        MaybeUninit::new(*c1_data)
                    , |b| *b)


                }

                fn as_string<'a>(
                    &self,
                    src: &ColumnWrapper<'a>,
                    src_index: &ColumnDataIndex<'a>,
                ) -> Result<Vec<String>, ErrorDesc>
                {
                    type T=$tr;
                    let src=src.column().downcast_ref::<T>()?;
                    if src_index.is_some(){
                        let index=src_index.downcast_ref()?;
                        let out:Vec<_>=index.iter().map(|v| format!("{}",v)).collect();
                        Ok(out)
                    } else {
                        let out:Vec<_>=src.iter().map(|v| format!("{}",v)).collect();
                        Ok(out)
                    }

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
                    let src_data=src.column().downcast_ref::<T>()?;
                    let src_bitmap=src.bitmap();

                    let s=ahash::RandomState::with_seeds(2194717786824016851,7391161229587532433,8421638162391593347, 13425712476683680973);

                    if dst.len()==0{
                        //We have to do an insert
                        match (src_index.is_some(), src_bitmap.is_some()){
                            (true, true)=>{
                                let src_index=src_index.downcast_ref()?;
                                let src_bitmap=src_bitmap.downcast_ref()?;
                                let itr=src_index.iter().map(|i| {
                                    let data=src_data[*i];
                                    let bitmap=src_bitmap[*i];
                                    let mut h = s.build_hasher();
                                    data.hash(&mut h);
                                    h.finish()|(bitmap as u64).wrapping_sub(1)
                                });
                                dst.extend(itr);
                            },
                            (true, false)=>{
                                let src_index=src_index.downcast_ref()?;
                                let itr=src_index.iter().map(|i| {
                                    let data=src_data[*i];
                                    let mut h = s.build_hasher();
                                    data.hash(&mut h);
                                    h.finish()
                                });
                                dst.extend(itr);
                            },
                            (false, true)=>{
                                let src_bitmap=src_bitmap.downcast_ref()?;
                                let itr=src_data.iter().zip(src_bitmap).map(|(data, bitmap)| {
                                    let mut h = s.build_hasher();
                                    data.hash(&mut h);
                                    h.finish()|(*bitmap as u64).wrapping_sub(1)
                                });
                                dst.extend(itr);
                            },
                            (false, false)=>{
                                let itr=src_data.iter().map(|data| {
                                    let mut h = s.build_hasher();
                                    data.hash(&mut h);
                                    h.finish()
                                });
                                dst.extend(itr);
                            },
                        }
                        } else {
                            //We have to do an update

                            //First check if src is a const
                            if src.column().is_const(){
                                //in case of constant, we have to add its hash to the entire hash vector
                                let mut h = s.build_hasher();
                                src_data[0].hash(&mut h);
                                let mut hash_value=h.finish();
                                if src_bitmap.is_some() {
                                    if !src_bitmap.downcast_ref()?[0] {hash_value=u64::MAX};
                                };
                                let hash_value=hash_value;
                                dst.iter_mut().for_each(|h| *h=h.wrapping_add(hash_value));
                            } else {
                                //The source is not a constant value, therefore we have to make sure it has the same length as the hash vector


                                match (src_index.is_some(), src_bitmap.is_some()){
                                    (true, true)=>{
                                        let src_index=src_index.downcast_ref()?;
                                        assert_eq!(src_index.len(), dst.len());
                                        let src_bitmap=src_bitmap.downcast_ref()?;
                                        let itr=src_index.iter().map(|i| {
                                            let data=src_data[*i];
                                            let bitmap=src_bitmap[*i];
                                            let mut h = s.build_hasher();
                                            data.hash(&mut h);
                                            h.finish()|(bitmap as u64).wrapping_sub(1)
                                        });
                                        dst.iter_mut().zip(itr).for_each(|(h, hash_value)| *h=h.wrapping_add(hash_value));
                                    },
                                    (true, false)=>{
                                        let src_index=src_index.downcast_ref()?;
                                        assert_eq!(src_index.len(), dst.len());
                                        let itr=src_index.iter().map(|i| {
                                            let data=src_data[*i];
                                            let mut h = s.build_hasher();
                                            data.hash(&mut h);
                                            h.finish()
                                        });
                                        dst.iter_mut().zip(itr).for_each(|(h, hash_value)| *h=h.wrapping_add(hash_value));
                                    },
                                    (false, true)=>{
                                        let src_bitmap=src_bitmap.downcast_ref()?;
                                        assert_eq!(src_data.len(), dst.len());
                                        let itr=src_data.iter().zip(src_bitmap).map(|(data, bitmap)| {
                                            let mut h = s.build_hasher();
                                            data.hash(&mut h);
                                            h.finish()|(*bitmap as u64).wrapping_sub(1)
                                        });
                                        dst.iter_mut().zip(itr).for_each(|(h, hash_value)| *h=h.wrapping_add(hash_value));
                                    },
                                    (false, false)=>{
                                        assert_eq!(src_data.len(), dst.len());
                                        let itr=src_data.iter().map(|data| {
                                            let mut h = s.build_hasher();
                                            data.hash(&mut h);
                                            h.finish()
                                        });
                                        dst.iter_mut().zip(itr).for_each(|(h, hash_value)| *h=h.wrapping_add(hash_value));
                                    },
                                }
                            }
                        }
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
                        let src=src[col_id].column().downcast_ref::<T>().unwrap();
                        let src_index=match index_id {
                            Some(i) => &src_index[**i],
                            None => &index_empty,
                        };
                        items_written+=copy_to_buckets_part_uninit(hash, buckets_mask, src, src_index, &mut offsets_tmp, &mut dst_data).unwrap();
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
                    let src_data=src.column().downcast_ref::<T>()?;
                    let src_bitmap=src.bitmap();

                    let mut h=hashmap_buffer.pop::<T>();


                    if dst.len()==0{
                        //We have to do an insert
                        match (src_index.is_some(), src_bitmap.is_some()){
                            (true, true)=>{
                                let src_index=src_index.downcast_ref()?;
                                let src_bitmap=src_bitmap.downcast_ref()?;
                                let itr=src_index.iter().enumerate().map(|(i, index)| {
                                    let data=src_data[*index];
                                    let bitmap=src_bitmap[*index];

                                    let new_group_id: usize=*h.entry((0, NullableValue{value: data, bitmap})).or_insert(i);
                                    new_group_id

                                });
                                dst.extend(itr);
                            },
                            (true, false)=>{
                                let src_index=src_index.downcast_ref()?;
                                let itr=src_index.iter().enumerate().map(|(i, index)| {
                                    let data=src_data[*index];
                                    let bitmap=true;
                                    let new_group_id: usize=*h.entry((0, NullableValue{value: data, bitmap})).or_insert(i);
                                    new_group_id

                                });
                                dst.extend(itr);
                            },
                            (false, true)=>{
                                let src_bitmap=src_bitmap.downcast_ref()?;
                                let itr=src_data.iter().zip(src_bitmap).enumerate().map(|(i,(data, bitmap))| {
                                    let new_group_id: usize=*h.entry((0, NullableValue{value: *data, bitmap: *bitmap})).or_insert(i);
                                    new_group_id
                                });
                                dst.extend(itr);
                            },
                            (false, false)=>{
                                let itr=src_data.iter().enumerate().map(|(i,data)| {
                                    let new_group_id: usize=*h.entry((0, NullableValue{value: *data, bitmap: true})).or_insert(i);
                                    new_group_id
                                });
                                dst.extend(itr);
                            },
                        }
                        } else {
                            //We have to do an update

                            //First check if src is a const, do nothing
                            if src.column().is_const(){
                            } else {
                                match (src_index.is_some(), src_bitmap.is_some()){
                                    (true, true)=>{
                                        let src_index=src_index.downcast_ref()?;
                                        assert_eq!(src_index.len(), dst.len());
                                        let src_bitmap=src_bitmap.downcast_ref()?;
                                        src_index.iter().zip(dst.iter_mut()).enumerate().for_each(|(i, (index, current_group_id))| {
                                            let data=src_data[*index];
                                            let bitmap=src_bitmap[*index];

                                            let new_group_id: usize=*h.entry((*current_group_id, NullableValue{value: data, bitmap})).or_insert(i);
                                            *current_group_id=new_group_id;

                                        });

                                    },
                                    (true, false)=>{
                                        let src_index=src_index.downcast_ref()?;
                                        assert_eq!(src_index.len(), dst.len());
                                        src_index.iter().zip(dst.iter_mut()).enumerate().for_each(|(i, (index, current_group_id))| {
                                            let data=src_data[*index];
                                            let bitmap=true;
                                            let new_group_id: usize=*h.entry((*current_group_id, NullableValue{value: data, bitmap})).or_insert(i);
                                            *current_group_id=new_group_id;

                                        });

                                    },
                                    (false, true)=>{
                                        assert_eq!(src_data.len(), dst.len());
                                        let src_bitmap=src_bitmap.downcast_ref()?;
                                        src_data.iter().zip(src_bitmap).zip(dst.iter_mut()).enumerate().for_each(|(i,((data, bitmap), current_group_id))| {
                                            let new_group_id: usize=*h.entry((*current_group_id, NullableValue{value: *data, bitmap: *bitmap})).or_insert(i);
                                            *current_group_id=new_group_id;
                                        });

                                    },
                                    (false, false)=>{
                                        assert_eq!(src_data.len(), dst.len());
                                        src_data.iter().zip(dst.iter_mut()).enumerate().for_each(|(i,(data, current_group_id))| {
                                            let new_group_id: usize=*h.entry((*current_group_id, NullableValue{value: *data, bitmap: true})).or_insert(i);
                                            *current_group_id=new_group_id;
                                        });
                                    },
                                }
                            }
                        }
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

                    let bitmap_update_required=src.bitmap().is_some();
                    let input=vec![InputTypes::Ref(src, src_index)];


                    set_2_sized_binary_unroll::<MaybeUninit<T>, T, _,_>(dst, &&*input, &bitmap_update_required, |c1_data,_c1_bool|
                        MaybeUninit::new(<T as AsBytes>::from_bytes(c1_data))
                    , |b| *b)


                }

                fn as_string<'a>(
                    &self,
                    src: &ColumnWrapper<'a>,
                    src_index: &ColumnDataIndex<'a>,
                ) -> Result<Vec<String>, ErrorDesc>
                {
                    type T=$tr;
                    let (datau8, start_pos, len,offset)=src.column().downcast_binary_ref::<T>()?;

                    if src_index.is_some(){
                        let index=src_index.downcast_ref()?;

                        let v: Vec<String>= index.iter().map(|i|
                            {
                                let s=start_pos[*i]-offset;
                                let e=s+len[*i]-offset;
                                format!("{}", <T as AsBytes>::from_bytes(&datau8[s..e]))
                            }).collect();
                        Ok(v)
                    } else {
                        assert_eq!(start_pos.len(), len.len());
                        let v: Vec<_>=start_pos.iter().zip(len.iter()).map(|(s, l)|
                        {
                            let s=s-offset;
                            let e=s+l;
                            format!("{}", <T as AsBytes>::from_bytes(&datau8[s..e]))
                        }).collect();
                        Ok(v)
                    }

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
                    let (datau8, start_pos, len, offset) =src.column().downcast_binary_ref::<T>()?;
                    let src_bitmap=src.bitmap();

                    let s=ahash::RandomState::with_seeds(2194717786824016851,7391161229587532433,8421638162391593347, 13425712476683680973);

                    if dst.len()==0{
                        //We have to do an insert
                        match (src_index.is_some(), src_bitmap.is_some()){
                            (true, true)=>{
                                let src_index=src_index.downcast_ref()?;
                                let src_bitmap=src_bitmap.downcast_ref()?;
                                let itr=src_index.iter().map(|i| {
                                    let start=start_pos[*i]-offset;
                                    let end=start+len[*i];
                                    let data=&datau8[start..end];
                                    let bitmap=src_bitmap[*i];
                                    let mut h = s.build_hasher();
                                    data.hash(&mut h);
                                    h.finish()|(bitmap as u64).wrapping_sub(1)
                                });
                                dst.extend(itr);
                            },
                            (true, false)=>{
                                let src_index=src_index.downcast_ref()?;
                                let itr=src_index.iter().map(|i| {
                                    let start=start_pos[*i]-offset;
                                    let end=start+len[*i];
                                    let data=&datau8[start..end];
                                    let mut h = s.build_hasher();
                                    data.hash(&mut h);
                                    h.finish()
                                });
                                dst.extend(itr);
                            },
                            (false, true)=>{
                                let src_bitmap=src_bitmap.downcast_ref()?;
                                let itr=start_pos.iter().zip(len.iter()).zip(src_bitmap).map(|((start_pos, len), bitmap)| {
                                    let start=start_pos-offset;
                                    let end=start+len;
                                    let data=&datau8[start..end];
                                    let mut h = s.build_hasher();
                                    data.hash(&mut h);
                                    h.finish()|(*bitmap as u64).wrapping_sub(1)
                                });
                                dst.extend(itr);
                            },
                            (false, false)=>{
                                let itr=start_pos.iter().zip(len.iter()).map(|(start_pos, len)| {
                                    let start=start_pos-offset;
                                    let end=start+len;
                                    let data=&datau8[start..end];
                                    let mut h = s.build_hasher();
                                    data.hash(&mut h);
                                    h.finish()
                                });
                                dst.extend(itr);
                            },
                        }
                        } else {
                            //We have to do an update

                            //First check if src is a const
                            if src.column().is_const(){
                                //in case of constant, we have to add its hash to the entire hash vector

                                let start=start_pos[0]-offset;
                                let end=start+len[0];
                                let data=&datau8[start..end];

                                let mut h = s.build_hasher();
                                data.hash(&mut h);
                                let mut hash_value=h.finish();
                                if src_bitmap.is_some() {
                                    if !src_bitmap.downcast_ref()?[0] {hash_value=u64::MAX};
                                };
                                let hash_value=hash_value;
                                dst.iter_mut().for_each(|h| *h=h.wrapping_add(hash_value));
                            } else {
                                //The source is not a constant value, therefore we have to make sure it has the same length as the hash vector


                                match (src_index.is_some(), src_bitmap.is_some()){
                                    (true, true)=>{
                                        let src_index=src_index.downcast_ref()?;
                                        assert_eq!(src_index.len(), dst.len());
                                        let src_bitmap=src_bitmap.downcast_ref()?;
                                        let itr=src_index.iter().map(|i| {
                                            let start=start_pos[*i]-offset;
                                            let end=start+len[*i];
                                            let data=&datau8[start..end];
                                            let bitmap=src_bitmap[*i];
                                            let mut h = s.build_hasher();
                                            data.hash(&mut h);
                                            h.finish()|(bitmap as u64).wrapping_sub(1)
                                        });
                                        dst.iter_mut().zip(itr).for_each(|(h, hash_value)| *h=h.wrapping_add(hash_value));
                                    },
                                    (true, false)=>{
                                        let src_index=src_index.downcast_ref()?;
                                        assert_eq!(src_index.len(), dst.len());
                                        let itr=src_index.iter().map(|i| {
                                            let start=start_pos[*i]-offset;
                                            let end=start+len[*i];
                                            let data=&datau8[start..end];
                                            let mut h = s.build_hasher();
                                            data.hash(&mut h);
                                            h.finish()
                                        });
                                        dst.iter_mut().zip(itr).for_each(|(h, hash_value)| *h=h.wrapping_add(hash_value));
                                    },
                                    (false, true)=>{
                                        let src_bitmap=src_bitmap.downcast_ref()?;
                                        assert_eq!(dst.len(), start_pos.len());
                                        assert_eq!(dst.len(), len.len());
                                        let itr=start_pos.iter().zip(len.iter()).zip(src_bitmap).map(|((start_pos, len), bitmap)| {
                                            let start=start_pos-offset;
                                            let end=start+len;
                                            let data=&datau8[start..end];
                                            let mut h = s.build_hasher();
                                            data.hash(&mut h);
                                            h.finish()|(*bitmap as u64).wrapping_sub(1)
                                        });
                                        dst.iter_mut().zip(itr).for_each(|(h, hash_value)| *h=h.wrapping_add(hash_value));
                                    },
                                    (false, false)=>{
                                        assert_eq!(dst.len(), start_pos.len());
                                        assert_eq!(dst.len(), len.len());
                                        let itr=start_pos.iter().zip(len.iter()).map(|(start_pos, len)| {
                                            let start=start_pos-offset;
                                            let end=start+len;
                                            let data=&datau8[start..end];
                                            let mut h = s.build_hasher();
                                            data.hash(&mut h);
                                            h.finish()
                                        });
                                        dst.iter_mut().zip(itr).for_each(|(h, hash_value)| *h=h.wrapping_add(hash_value));
                                    },
                                }
                            }
                        }
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
                        let (src_datau8, src_start_pos,src_len,src_offset)=src[col_id].column().downcast_binary_ref::<T>().unwrap();
                        let src_index=match index_id {
                            Some(i) => &src_index[**i],
                            None => &index_empty,
                        };

                        bytes_written+=copy_to_buckets_binary_part(hash, buckets_mask, src_datau8,src_start_pos, src_len,src_offset , src_index, &mut offsets_tmp, &mut dst_data).unwrap();
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
                    let (datau8, start_pos,len,offset)=src.column().downcast_binary_ref::<T>().unwrap();
                    let src_bitmap=src.bitmap();
                    hashmap_binary.clear();


                    if dst.len()==0{
                        //We have to do an insert
                        match (src_index.is_some(), src_bitmap.is_some()){
                            (true, true)=>{
                                let src_index=src_index.downcast_ref()?;
                                let src_bitmap=src_bitmap.downcast_ref()?;
                                let itr=src_index.iter().enumerate().map(|(i, index)| {
                                    let start=start_pos[*index]-offset;
                                    let end=start+len[*index];
                                    let data=&datau8[start..end];
                                    let bitmap=src_bitmap[*index];
                                    let nullableslice=NullableValue{
                                        value: data,
                                        bitmap
                                    };

                                    let val: (usize, NullableValue<&[u8]>)=(0,nullableslice);

                                    //SAFETY: hashmap_binary would outlive the slice to src, however src is guaranteed to be live until the hashmap is cleared.
                                    //        Once the hashmap is cleared, there should be no references to src, and therefore no reason why we need to insist on
                                    //        having to drop hashmap_binary.
                                    let val: (usize, NullableValue<&[u8]>)=unsafe{std::mem::transmute(val)};
                                    let new_group_id=hashmap_binary.entry(val).or_insert(i);
                                    *new_group_id
                                });
                                dst.extend(itr);
                            },
                            (true, false)=>{
                                let src_index=src_index.downcast_ref()?;
                                let itr=src_index.iter().enumerate().map(|(i, index)| {
                                    let start=start_pos[*index]-offset;
                                    let end=start+len[*index];
                                    let data=&datau8[start..end];
                                    let nullableslice=NullableValue{
                                        value: data,
                                        bitmap: true,
                                    };
                                    let val: (usize, NullableValue<&[u8]>)=(0,nullableslice);

                                    //SAFETY: hashmap_binary would outlive the slice to src, however src is guaranteed to be live until the hashmap is cleared.
                                    //        Once the hashmap is cleared, there should be no references to src, and therefore no reason why we need to insist on
                                    //        having to drop hashmap_binary.
                                    let val: (usize, NullableValue<&[u8]>)=unsafe{std::mem::transmute(val)};
                                    let new_group_id=hashmap_binary.entry(val).or_insert(i);
                                    *new_group_id
                                });
                                dst.extend(itr);
                            },
                            (false, true)=>{
                                let src_bitmap=src_bitmap.downcast_ref()?;
                                let itr=start_pos.iter().zip(len.iter()).zip(src_bitmap).enumerate().map(|(i,((start_pos, len), bitmap))| {
                                    let start=start_pos-offset;
                                    let end=start+len;
                                    let data=&datau8[start..end];
                                    let nullableslice=NullableValue{
                                        value: data,
                                        bitmap: *bitmap,
                                    };
                                    let val: (usize, NullableValue<&[u8]>)=(0,nullableslice);

                                    //SAFETY: hashmap_binary would outlive the slice to src, however src is guaranteed to be live until the hashmap is cleared.
                                    //        Once the hashmap is cleared, there should be no references to src, and therefore no reason why we need to insist on
                                    //        having to drop hashmap_binary.
                                    let val: (usize, NullableValue<&[u8]>)=unsafe{std::mem::transmute(val)};
                                    let new_group_id=hashmap_binary.entry(val).or_insert(i);
                                    *new_group_id
                                });
                                dst.extend(itr);
                            },
                            (false, false)=>{
                                let itr=start_pos.iter().zip(len.iter()).enumerate().map(|(i,(start_pos, len))| {
                                    let start=start_pos-offset;
                                    let end=start+len;
                                    let data=&datau8[start..end];
                                    let nullableslice=NullableValue{
                                        value: data,
                                        bitmap: true,
                                    };
                                    let val: (usize, NullableValue<&[u8]>)=(0,nullableslice);

                                    //SAFETY: hashmap_binary would outlive the slice to src, however src is guaranteed to be live until the hashmap is cleared.
                                    //        Once the hashmap is cleared, there should be no references to src, and therefore no reason why we need to insist on
                                    //        having to drop hashmap_binary.
                                    let val: (usize, NullableValue<&[u8]>)=unsafe{std::mem::transmute(val)};
                                    let new_group_id=hashmap_binary.entry(val).or_insert(i);
                                    *new_group_id
                                });
                                dst.extend(itr);
                            },
                        }
                    } else {
                        //We have to do an update


                        //First check if src is a const
                        if src.column().is_const(){
                            //in case of constant, then we have nothing to do
                        } else {

                            match (src_index.is_some(), src_bitmap.is_some()){
                                (true, true)=>{
                                    let src_index=src_index.downcast_ref()?;
                                    let src_bitmap=src_bitmap.downcast_ref()?;
                                    src_index.iter().zip(dst.iter_mut()).enumerate().for_each(|(i, (index, current_group_id))| {
                                        let start=start_pos[*index]-offset;
                                        let end=start+len[*index];
                                        let data=&datau8[start..end];
                                        let bitmap=src_bitmap[*index];
                                        let nullableslice=NullableValue{
                                            value: data,
                                            bitmap
                                        };
                                        let val: (usize, NullableValue<&[u8]>)=(0,nullableslice);

                                        //SAFETY: hashmap_binary would outlive the slice to src, however src is guaranteed to be live until the hashmap is cleared.
                                        //        Once the hashmap is cleared, there should be no references to src, and therefore no reason why we need to insist on
                                        //        having to drop hashmap_binary.
                                        let val: (usize, NullableValue<&[u8]>)=unsafe{std::mem::transmute(val)};
                                        let new_group_id=hashmap_binary.entry(val).or_insert(i);
                                        *current_group_id=*new_group_id;
                                    });

                                },
                                (true, false)=>{
                                    let src_index=src_index.downcast_ref()?;
                                    src_index.iter().zip(dst.iter_mut()).enumerate().for_each(|(i, (index, current_group_id))| {
                                        let start=start_pos[*index]-offset;
                                        let end=start+len[*index];
                                        let data=&datau8[start..end];
                                        let nullableslice=NullableValue{
                                            value: data,
                                            bitmap: true,
                                        };
                                        let val: (usize, NullableValue<&[u8]>)=(0,nullableslice);

                                        //SAFETY: hashmap_binary would outlive the slice to src, however src is guaranteed to be live until the hashmap is cleared.
                                        //        Once the hashmap is cleared, there should be no references to src, and therefore no reason why we need to insist on
                                        //        having to drop hashmap_binary.
                                        let val: (usize, NullableValue<&[u8]>)=unsafe{std::mem::transmute(val)};
                                        let new_group_id=hashmap_binary.entry(val).or_insert(i);
                                        *current_group_id=*new_group_id;
                                    });

                                },
                                (false, true)=>{
                                    let src_bitmap=src_bitmap.downcast_ref()?;
                                    start_pos.iter().zip(len.iter()).zip(src_bitmap).zip(dst.iter_mut()).enumerate().for_each(|(i,(((start_pos, len), bitmap), current_group_id))| {
                                        let start=start_pos-offset;
                                        let end=start+len;
                                        let data=&datau8[start..end];
                                        let nullableslice=NullableValue{
                                            value: data,
                                            bitmap: *bitmap,
                                        };
                                        let val: (usize, NullableValue<&[u8]>)=(0,nullableslice);

                                        //SAFETY: hashmap_binary would outlive the slice to src, however src is guaranteed to be live until the hashmap is cleared.
                                        //        Once the hashmap is cleared, there should be no references to src, and therefore no reason why we need to insist on
                                        //        having to drop hashmap_binary.
                                        let val: (usize, NullableValue<&[u8]>)=unsafe{std::mem::transmute(val)};
                                        let new_group_id=hashmap_binary.entry(val).or_insert(i);
                                        *current_group_id=*new_group_id;
                                    });

                                },
                                (false, false)=>{
                                    start_pos.iter().zip(len.iter()).zip(dst.iter_mut()).enumerate().for_each(|(i,((start_pos, len), current_group_id))| {
                                        let start=start_pos-offset;
                                        let end=start+len;
                                        let data=&datau8[start..end];
                                        let nullableslice=NullableValue{
                                            value: data,
                                            bitmap: true,
                                        };
                                        let val: (usize, NullableValue<&[u8]>)=(0,nullableslice);

                                        //SAFETY: hashmap_binary would outlive the slice to src, however src is guaranteed to be live until the hashmap is cleared.
                                        //        Once the hashmap is cleared, there should be no references to src, and therefore no reason why we need to insist on
                                        //        having to drop hashmap_binary.
                                        let val: (usize, NullableValue<&[u8]>)=unsafe{std::mem::transmute(val)};
                                        let new_group_id=hashmap_binary.entry(val).or_insert(i);
                                        *current_group_id=*new_group_id;
                                    });

                                },
                            }
                        }
                    }





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
