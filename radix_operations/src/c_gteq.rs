use crate::*;
use radix_column::*;

use paste::paste;

const OP: &str = ">=";

macro_rules! operation_load {
    ($dict:ident; $(($tl:ty, $tr:ty))+) => ($(
            {
                type T1=bool;
                type T2=$tl;
                type T3=$tr;
                let signature=sig![OP; T2, T3];
                let op=Operation{
                    f:  paste!{[<eq_ $tl:lower _ $tr:lower>]},
                    output_type_id: std::any::TypeId::of::<T1>(),
                    is_assign_op: false,
                    associated_assign_op: None,
                    associated_input_switch_op: Some("<".to_string()),
                };
            $dict.insert(signature, op);
            }
    )+)
}

macro_rules! operation_impl_copy {
    ($(($tl:ty, $tr:ty))+) => ($(
        paste!
        {
            fn [<eq_$tl:lower _ $tr:lower>](c1: &mut ColumnWrapper, _c1_index: &ColumnDataIndex, input:&[InputTypes])->Result<(),ErrorDesc>
            {

                type T1=bool;
                type T2=$tl;
                type T3=$tr;

                let (c2, _c2_index) = match &input[0] {
                    InputTypes::Ref(c, i) => (*c, *i),
                    InputTypes::Owned(c, i) => (c, i),
                };

                let (c3, _c3_index) = match &input[1] {
                    InputTypes::Ref(c, i) => (*c, *i),
                    InputTypes::Owned(c, i) => (c, i),
                };

                let bitmap_update_required=c2.bitmap().is_some()||c3.bitmap().is_some();

                set_3_sized_sized_sized_unroll::<T1,T2, T3,_,_,>(c1, &input, &bitmap_update_required, |c2_data, _c2_bool, c3_data,_c3_bool,| {
                    *c2_data>=T2::from(*c3_data)}, |c2_bitmap, c3_bitmap| *c2_bitmap&&*c3_bitmap)
            }
        }
    )+)
}

macro_rules! operation_impl_binary {
    ($(($tl:ty, $tr:ty))+) => ($(
        paste!   {
            fn [<eq_$tl:lower _ $tr:lower>](c1: &mut ColumnWrapper, _c1_index: &ColumnDataIndex, input:&[InputTypes])->Result<(),ErrorDesc>
            {
                type T1=bool;
                type T2=$tl;
                type T3=$tr;



                let (c2, _c2_index) = match &input[0] {
                    InputTypes::Ref(c, i) => (*c, *i),
                    InputTypes::Owned(c, i) => (c, i),
                };

                let (c3, _c3_index) = match &input[1] {
                    InputTypes::Ref(c, i) => (*c, *i),
                    InputTypes::Owned(c, i) => (c, i),
                };

                let bitmap_update_required=c2.bitmap().is_some()||c3.bitmap().is_some();


                set_3_sized_binary_binary_unroll::<T1,T2, T3,_,_,>(c1, &input, &bitmap_update_required, |c2_data, _c2_bool, c3_data,_c3_bool,| {
                    *c2_data>=*c3_data}, |c2_bitmap, c3_bitmap| *c2_bitmap&&*c3_bitmap)


            }
        }
    )+)
}

operation_impl_copy! {
    (u64, u64) (u32,u32)
}

operation_impl_binary! {
    (String, String)
}

pub(crate) fn load_op_dict(dict: &mut OpDictionary) {
    operation_load! {dict;
        (u64, u64) (u32,u32)  (String, String)
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
