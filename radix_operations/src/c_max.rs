use crate::*;
use radix_column::*;

use paste::paste;

const OP: &str = "MAX";

macro_rules! operation_load{
    ($dict:ident; $($tr:ty)+) => ($(
            {
                type T=$tr;
                let signature=sig![OP; T];
                let op=Operation{
                    f:  paste!{[<max_ $tr:lower>]},
                    output_type_id: std::any::TypeId::of::<T>(),
                    is_assign_op: false,
                    associated_assign_op: None,
                    associated_input_switch_op: None,

                };
            $dict.insert(signature, op);
            }
    )+)
}

macro_rules! operation_impl_sized {
    ($( $tr:ty)+) => ($(
        paste!   {
            fn [<max_ $tr:lower>](c1: &mut ColumnWrapper, c1_index: &ColumnDataIndex, input:&[InputTypes])->Result<(),ErrorDesc>
            {
                type T=$tr;

                let (c3, c3_index) = match &input[1] {
                    InputTypes::Ref(c, i) => (*c, *i),
                    InputTypes::Owned(c, i) => (c, i),
                };


                let (c4, c4_index) = match &input[2] {
                    InputTypes::Ref(c, i) => (*c, *i),
                    InputTypes::Owned(c, i) => (c, i),
                };

                assert_eq!(c1_index, &ColumnDataIndex::None);
                assert_eq!(c1.bitmap(), &ColumnDataF::None);
                assert_eq!(c3_index, &ColumnDataIndex::None);
                assert_eq!(c4_index, &ColumnDataIndex::None);

                assert!(c4.column().is_const());
                assert!(c1.column().is_owned());



                let group_ids=c3.column().downcast_ref::<usize>()?;
                let number_of_groups=c4.column().downcast_ref::<usize>()?[0];

                let (c1_data, c1_bitmap)=c1.get_inner_mut();
                let c1_data=c1_data.downcast_vec::<T>()?;
                let c1_bitmap=c1_bitmap.downcast_vec()?;

                assert_eq!(c1_data.len(),0);
                c1_data.reserve(number_of_groups);
                c1_data.extend((0..number_of_groups).into_iter().map(|_| 0));
                c1_bitmap.extend((0..number_of_groups).into_iter().map(|_| false));

                let c2=ReadColumn::<T>::from_input(&input[0]);
                c2.zip_and_for_each(group_ids.iter(), |((value,bitmap),group_id)|
                 {
                     if *bitmap&&(*value>c1_data[*group_id]){
                        c1_data[*group_id]+=*value;
                    };
                });

                Ok(())
            }
        }
    )+)
}
/*
macro_rules! operation_impl_binary {
    ($( $tr:ty)+) => ($(
        paste!   {
            fn [<max_ $tr:lower>](c1: &mut ColumnWrapper, c1_index: &ColumnDataIndex, input:&[InputTypes])->Result<(),ErrorDesc>
            {
                type T=$tr;

                let (c2, c2_index) = match &input[0] {
                    InputTypes::Ref(c, i) => (*c, *i),
                    InputTypes::Owned(c, i) => (c, i),
                };



                let (c3, c3_index) = match &input[1] {
                    InputTypes::Ref(c, i) => (*c, *i),
                    InputTypes::Owned(c, i) => (c, i),
                };


                let (c4, c4_index) = match &input[2] {
                    InputTypes::Ref(c, i) => (*c, *i),
                    InputTypes::Owned(c, i) => (c, i),
                };

                assert_eq!(c1_index, &ColumnDataIndex::None);
                assert_eq!(c1.bitmap(), &ColumnDataF::None);
                assert_eq!(c3_index, &ColumnDataIndex::None);
                assert_eq!(c4_index, &ColumnDataIndex::None);

                assert!(c4.column().is_const());
                assert!(c1.column().is_owned());

                let (c2_data, c2_bitmap)=(c2.column(), c2.bitmap());
                let (_c2_datau8, c2_start_pos, _c2_len, _offset)=c2_data.downcast_binary_ref::<T>()?;


                let group_ids=c3.column().downcast_ref::<usize>()?;
                let number_of_groups=c4.column().downcast_ref::<usize>()?[0];

                let c1_data=c1.column_mut().downcast_vec::<u64>()?;
                assert_eq!(c1_data.len(),0);
                c1_data.reserve(number_of_groups);
                c1_data.extend((0..number_of_groups).into_iter().map(|_| 0));





                match (c2_index.is_some(), c2_bitmap.is_some()){
                    (true, true)=>{
                        let c2_index=c2_index.downcast_ref()?;
                        let c2_bitmap=c2_bitmap.downcast_ref()?;
                        assert_eq!(group_ids.len(),c2_index.len());
                        c2_index.iter().zip(group_ids.iter()).for_each(|(index, group_id)|
                            c1_data[*group_id]+=c2_bitmap[*index] as u64
                        );
                    }
                    (true, false)=>{
                        let c2_index=c2_index.downcast_ref()?;
                        assert_eq!(group_ids.len(),c2_index.len());
                        c2_index.iter().zip(group_ids.iter()).for_each(|(_index, group_id)|
                            c1_data[*group_id]+=1
                        );
                    }
                    (false, true)=>{
                        assert_eq!(group_ids.len(),c2_start_pos.len());
                        let c2_bitmap=c2_bitmap.downcast_ref()?;
                        c2_start_pos.iter().zip(c2_bitmap).zip(group_ids).for_each(|((_data, bitmap), group_id)|
                            c1_data[*group_id]+=*bitmap as u64
                        );
                    }
                    (false, false)=>{
                        assert_eq!(group_ids.len(),c2_start_pos.len());
                        c2_start_pos.iter().zip(group_ids).for_each(|(_data, group_id)|
                            c1_data[*group_id]+=1
                        );}

                };
                Ok(())
            }
        }
    )+)
}
*/
operation_impl_sized! {
    u64 u32
}
/*
operation_impl_binary! {
    String
}
*/
pub(crate) fn load_op_dict(dict: &mut OpDictionary) {
    operation_load! {dict;
        u64 u32 /*String*/
    };
}

/*
operation_impl! {
    (usize, usize) (usize, u16) (usize, u8)  (usize, bool)
            (u8, u8) (u8, bool)
            (u16, u16) (u16, u8) (u16, bool)
            (u32, u32) (u32, u16) (u32, u8) (u32, bool)
            (u64, u64) (u64, u32) (u64, u16) (u64, u8) (u64, bool)
            (u128, u128) (u128, u64) (u128, u32) (u128, u16) (u128, u8) (u128, bool)
            (isize, isize)  (isize, i16) (isize, i8) (isize, u8) (isize, bool)
            (i8, i8) (i8, bool)
            (i16, i16) (i16, i8) (i16, u8) (i16, bool)
            (i32, i32)  (i32, i16) (i32, u16)  (i32, i8) (i32, u8) (i32, bool)
            (i64, i64) (i64, i32) (i64, u32)  (i64, i16) (i64, u16)  (i64, i8) (i64, u8) (i64, bool)
            (i128, i128) (i128, i64) (i128, u64) (i128, i32) (i128, u32)  (i128, i16) (i128, u16)  (i128, i8) (i128, u8) (i128, bool)
            (f32, f32) (f32, i16) (f32, u16)  (f32, i8) (f32, u8)
            (f64, f64) (f64, f32) (f64, i32) (f64, u32) (f64, i16) (f64, u16) (f64, i8) (f64, u8)
}

pub(crate) fn load_op_dict(dict: &mut OpDictionary) {
    operation_load! {dict;
        (usize, usize) (usize, u16) (usize, u8)  (usize, bool)
            (u8, u8) (u8, bool)
            (u16, u16) (u16, u8) (u16, bool)
            (u32, u32) (u32, u16) (u32, u8) (u32, bool)
            (u64, u64) (u64, u32) (u64, u16) (u64, u8) (u64, bool)
            (u128, u128) (u128, u64) (u128, u32) (u128, u16) (u128, u8) (u128, bool)
            (isize, isize)  (isize, i16) (isize, i8) (isize, u8) (isize, bool)
            (i8, i8) (i8, bool)
            (i16, i16) (i16, i8) (i16, u8) (i16, bool)
            (i32, i32)  (i32, i16) (i32, u16)  (i32, i8) (i32, u8) (i32, bool)
            (i64, i64) (i64, i32) (i64, u32)  (i64, i16) (i64, u16)  (i64, i8) (i64, u8) (i64, bool)
            (i128, i128) (i128, i64) (i128, u64) (i128, i32) (i128, u32)  (i128, i16) (i128, u16)  (i128, i8) (i128, u8) (i128, bool)
            (f32, f32) (f32, i16) (f32, u16)  (f32, i8) (f32, u8)
            (f64, f64) (f64, f32) (f64, i32) (f64, u32) (f64, i16) (f64, u16) (f64, i8) (f64, u8)
    };
}
*/
