use std::any::Any;
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem::MaybeUninit;

use paste::paste;

use crate::Signature;
use crate::*;
use radix_column::*;

fn copy_of_into_boxed_slice<T>(boxed: Box<T>) -> Box<[T]> {
    // *mut T and *mut [T; 1] have the same size and alignment
    unsafe { Box::from_raw(Box::into_raw(boxed) as *mut [T; 1]) }
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
        src_index: &ColumnDataF<'a, usize>,
    ) -> Result<(), ErrorDesc>;
    fn as_string<'a>(
        &self,
        src: &ColumnWrapper<'a>,
        src_index: &ColumnDataF<'a, usize>,
    ) -> Result<Vec<String>, ErrorDesc>;
    fn new_owned_with_capacity(
        &self,
        capacity: usize,
        binary_capacity: usize,
    ) -> ColumnWrapper<'static>;
    fn hash_in(
        &self,
        src: &ColumnWrapper,
        src_index: &ColumnDataF<usize>,
        dst: &mut Vec<u64>,
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
                    src_index: &ColumnDataF<'a, usize>,
                ) -> Result<(), ErrorDesc>
                {
                    type T=$tr;

                    let bitmap_update_required=src.bitmap().is_some();
                    let input=vec![InputTypes::Ref(src, src_index)];


                    set_2_sized_sized_unroll::<MaybeUninit<T>, T, _,_>(dst, &input, &bitmap_update_required, |c1_data,_c1_bool|
                        MaybeUninit::new(*c1_data)
                    , |b| *b)


                }

                fn as_string<'a>(
                    &self,
                    src: &ColumnWrapper<'a>,
                    src_index: &ColumnDataF<'a, usize>,
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
                fn new_owned_with_capacity(&self, capacity: usize, _binary_capacity: usize) -> ColumnWrapper<'static>{
                    type T=$tr;
                    ColumnWrapper::new_from_columndata (self.new(Box::new(Vec::<T>::with_capacity(capacity))).unwrap())
                }
                fn hash_in(&self, src: &ColumnWrapper, src_index: &ColumnDataF<usize>, dst: &mut Vec<u64>)-> Result<(), ErrorDesc>{
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
                                assert_eq!(dst.len(), src_data.len());

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
                                        dst.iter_mut().zip(itr).for_each(|(h, hash_value)| *h=h.wrapping_add(hash_value));
                                    },
                                    (true, false)=>{
                                        let src_index=src_index.downcast_ref()?;
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
                                        let itr=src_data.iter().zip(src_bitmap).map(|(data, bitmap)| {
                                            let mut h = s.build_hasher();
                                            data.hash(&mut h);
                                            h.finish()|(*bitmap as u64).wrapping_sub(1)
                                        });
                                        dst.iter_mut().zip(itr).for_each(|(h, hash_value)| *h=h.wrapping_add(hash_value));
                                    },
                                    (false, false)=>{
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
                    src_index: &ColumnDataF<'a, usize>,
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
                    src_index: &ColumnDataF<'a, usize>,
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
                fn new_owned_with_capacity(&self, capacity: usize, binary_capacity: usize) -> ColumnWrapper<'static>{
                    type T=$tr;
                    ColumnWrapper::new_from_columndata (ColumnData::BinaryOwned(OnwedBinaryColumn::new_with_capacity(&[] as &[T], capacity,binary_capacity)))
                }
                fn hash_in(&self, src: &ColumnWrapper, src_index: &ColumnDataF<usize>, dst: &mut Vec<u64>)-> Result<(), ErrorDesc>{
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
                                assert_eq!(dst.len(), start_pos.len());
                                assert_eq!(dst.len(), len.len());

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
                                        dst.iter_mut().zip(itr).for_each(|(h, hash_value)| *h=h.wrapping_add(hash_value));
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
                                        dst.iter_mut().zip(itr).for_each(|(h, hash_value)| *h=h.wrapping_add(hash_value));
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
                                        dst.iter_mut().zip(itr).for_each(|(h, hash_value)| *h=h.wrapping_add(hash_value));
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
                                        dst.iter_mut().zip(itr).for_each(|(h, hash_value)| *h=h.wrapping_add(hash_value));
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
