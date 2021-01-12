use crate::{
    ftype::FType3, IndexedMutColumn, InputTypes, InsertColumn, ReadBinaryColumn, ReadColumn,
    UpdateColumn,
};
use radix_column::*;

////////////////////////////////////////////////////////////////////////////
//////////////////                                 /////////////////////////
//////////////////            1                    /////////////////////////
//////////////////    sized sized sized            /////////////////////////
////////////////////////////////////////////////////////////////////////////

fn f_1_sized_sized_sized<'a, 'i, T1, T2, T3, U2, U3, F1, F2>(
    c1: &'a mut IndexedMutColumn<T1>,
    c2: U2,
    c3: U3,
    f: FType3<'a, T1, T2, T3, F1, F2>,
) -> Result<(), ErrorDesc>
where
    'a: 'i,
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    T3: 'static + Send + Sync,
    U2: IntoIterator<Item = (&'i T2, &'i bool)>,
    U2::IntoIter: ExactSizeIterator,
    U2: Clone,
    U3: IntoIterator<Item = (&'i T3, &'i bool)>,
    U3::IntoIter: ExactSizeIterator,
    U3: Clone,
    F1: Fn(&T2, &bool, &T3, &bool) -> (bool, T1),
    F2: Fn(&mut T1, &mut bool, (&T2, &bool, &T3, &bool)),
{
    let iter = c2
        .into_iter()
        .zip(c3.into_iter())
        .map(|((d2, b2), (d3, b3))| (d2, b2, d3, b3));
    match (f, c1) {
        (FType3::Assign(f), IndexedMutColumn::Insert(tgt)) => {
            tgt.insert(iter, &|(a, b, c, d)| f(a, b, c, d))
        }
        (FType3::Assign(f), IndexedMutColumn::Update(tgt)) => {
            tgt.assign(iter, &|(a, b, c, d)| f(a, b, c, d))
        }
        (FType3::Update(f), IndexedMutColumn::Update(tgt)) => {
            tgt.update(iter, |a, b, c| f(a, b, c))
        }
        _ => panic!(),
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////

fn f_2_sized_sized_sized<'a, 'i, T1, T2, T3, U3, F1, F2>(
    c1: &'a mut IndexedMutColumn<T1>,
    c: &'a ReadColumn<'a, T2>,
    c3: U3,
    f: FType3<'a, T1, T2, T3, F1, F2>,
) -> Result<(), ErrorDesc>
where
    'a: 'i,
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    T3: 'static + Send + Sync,
    U3: IntoIterator<Item = (&'i T3, &'i bool)>,
    U3::IntoIter: ExactSizeIterator,
    U3: Clone,
    F1: Fn(&T2, &bool, &T3, &bool) -> (bool, T1),
    F2: Fn(&mut T1, &mut bool, (&T2, &bool, &T3, &bool)),
{
    match c {
        ReadColumn::BitmapIndex(col) => f_1_sized_sized_sized(c1, col.as_iter(), c3, f),
        ReadColumn::BitmapNoIndex(col) => f_1_sized_sized_sized(c1, col.as_iter(), c3, f),
        ReadColumn::NoBitmapIndex(col) => f_1_sized_sized_sized(c1, col.as_iter(), c3, f),
        ReadColumn::NoBitmapNoIndex(col) => f_1_sized_sized_sized(c1, col.as_iter(), c3, f),
        ReadColumn::BitmapIndexOption(col) => f_1_sized_sized_sized(c1, col.as_iter(), c3, f),
        ReadColumn::NoBitmapIndexOption(col) => f_1_sized_sized_sized(c1, col.as_iter(), c3, f),
        ReadColumn::Const(col) => f_1_sized_sized_sized(c1, col.as_iter(), c3, f),
    }
}

////////////////////////////////////////////////////////////////////////////

fn f_3_sized_sized_sized<'a, 'i, T1, T2, T3, F1, F2>(
    c1: &'a mut IndexedMutColumn<T1>,
    c2: &'a ReadColumn<'a, T2>,
    c: &'a ReadColumn<'a, T3>,
    f: FType3<'a, T1, T2, T3, F1, F2>,
) -> Result<(), ErrorDesc>
where
    'a: 'i,
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    T3: 'static + Send + Sync,
    F1: Fn(&T2, &bool, &T3, &bool) -> (bool, T1),
    F2: Fn(&mut T1, &mut bool, (&T2, &bool, &T3, &bool)),
{
    match c {
        ReadColumn::BitmapIndex(col) => f_2_sized_sized_sized(c1, c2, col.as_iter(), f),
        ReadColumn::BitmapNoIndex(col) => f_2_sized_sized_sized(c1, c2, col.as_iter(), f),
        ReadColumn::NoBitmapIndex(col) => f_2_sized_sized_sized(c1, c2, col.as_iter(), f),
        ReadColumn::NoBitmapNoIndex(col) => f_2_sized_sized_sized(c1, c2, col.as_iter(), f),
        ReadColumn::BitmapIndexOption(col) => f_2_sized_sized_sized(c1, c2, col.as_iter(), f),
        ReadColumn::NoBitmapIndexOption(col) => f_2_sized_sized_sized(c1, c2, col.as_iter(), f),
        ReadColumn::Const(col) => f_2_sized_sized_sized(c1, c2, col.as_iter(), f),
    }
}

////////////////////////////////////////////////////////////////////////////

pub fn assign_3_sized_sized_sized_unroll<'a, T1, T2, T3, F>(
    c1: &'a mut ColumnWrapper,
    input: &'a [InputTypes<'a>],
    f: F,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    T3: 'static + Send + Sync,
    F: Fn(&T2, &bool, &T3, &bool) -> (bool, T1),
{
    let mut c2_read_column = ReadColumn::<T2>::from_input(&input[0]);
    let mut c3_read_column = ReadColumn::<T3>::from_input(&input[1]);

    let c1_assign_column = UpdateColumn::<T1>::from_destination(c1, &ColumnDataIndex::None);
    let len = c1_assign_column.len();

    c2_read_column.update_len_if_const(len);
    c3_read_column.update_len_if_const(len);

    assert_eq!(len, c2_read_column.len());
    assert_eq!(len, c3_read_column.len());

    let mut c1_mut_column = IndexedMutColumn::Update(c1_assign_column);

    let dummy = |_: &mut T1, _: &mut bool, (_, _, _, _): (&T2, &bool, &T3, &bool)| {
        panic!("Dummy function called")
    };
    let f: FType3<T1, T2, T3, _, _> = FType3::new_assign(f, dummy);

    f_3_sized_sized_sized(&mut c1_mut_column, &c2_read_column, &c3_read_column, f)
}

////////////////////////////////////////////////////////////////////////////

pub fn insert_3_sized_sized_sized_unroll<'a, T1, T2, T3, F>(
    c1: &'a mut ColumnWrapper,
    input: &'a [InputTypes<'a>],
    bitmap_update_required: &bool,
    f: F,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    T3: 'static + Send + Sync,
    F: Fn(&T2, &bool, &T3, &bool) -> (bool, T1),
{
    let mut c2_read_column = ReadColumn::<T2>::from_input(&input[0]);
    let mut c3_read_column = ReadColumn::<T3>::from_input(&input[1]);

    let len = std::cmp::max(c2_read_column.len(), c3_read_column.len());

    c2_read_column.update_len_if_const(len);
    c3_read_column.update_len_if_const(len);

    assert_eq!(c2_read_column.len(), len);
    assert_eq!(c3_read_column.len(), len);

    let c1_insert_column = InsertColumn::<T1>::from_destination(c1, *bitmap_update_required, len);

    let mut c1_mut_column = IndexedMutColumn::Insert(c1_insert_column);

    let dummy = |_: &mut T1, _: &mut bool, (_, _, _, _): (&T2, &bool, &T3, &bool)| {
        panic!("Dummy function called")
    };
    let f: FType3<T1, T2, T3, _, _> = FType3::new_assign(f, dummy);

    f_3_sized_sized_sized(&mut c1_mut_column, &c2_read_column, &c3_read_column, f)
}

////////////////////////////////////////////////////////////////////////////

pub fn update_3_sized_sized_sized_unroll<'a, T1, T2, T3, F>(
    c1: &'a mut ColumnWrapper,
    c1_index: &ColumnDataIndex,
    input: &'a [InputTypes<'a>],
    f: F,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    T3: 'static + Send + Sync,
    F: Fn(&mut T1, &mut bool, (&T2, &bool, &T3, &bool)),
{
    let mut c2_read_column = ReadColumn::<T2>::from_input(&input[0]);
    let mut c3_read_column = ReadColumn::<T3>::from_input(&input[1]);

    let c1_update_column = UpdateColumn::from_destination(c1, c1_index);

    let len = c1_update_column.len();

    c2_read_column.update_len_if_const(len);
    c3_read_column.update_len_if_const(len);

    assert_eq!(len, c2_read_column.len());
    assert_eq!(len, c3_read_column.len());

    let mut c1_mut_column = IndexedMutColumn::Update(c1_update_column);

    let dummy =
        |_: &T2, _: &bool, _: &T3, _: &bool| -> (bool, T1) { panic!("dummy function called") };

    let f: FType3<T1, T2, T3, _, _> = FType3::new_update(dummy, f);

    f_3_sized_sized_sized(&mut c1_mut_column, &c2_read_column, &c3_read_column, f)
}

////////////////////////////////////////////////////////////////////////////
//////////////////                                 /////////////////////////
//////////////////            2                    /////////////////////////
//////////////////    sized binary binary          /////////////////////////
////////////////////////////////////////////////////////////////////////////

fn f_1_sized_binary_binary<'a, 'i, T1, T2, T3, U2, U3, F1, F2>(
    c1: &'a mut IndexedMutColumn<T1>,
    c2: U2,
    c3: U3,
    f: FType3<'a, T1, [u8], [u8], F1, F2>,
) -> Result<(), ErrorDesc>
where
    'a: 'i,
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    T3: 'static + Send + Sync,
    U2: IntoIterator<Item = (&'i [u8], &'i bool)>,
    U2::IntoIter: ExactSizeIterator,
    U2: Clone,
    U3: IntoIterator<Item = (&'i [u8], &'i bool)>,
    U3::IntoIter: ExactSizeIterator,
    U3: Clone,
    F1: Fn(&[u8], &bool, &[u8], &bool) -> (bool, T1),
    F2: Fn(&mut T1, &mut bool, (&[u8], &bool, &[u8], &bool)),
{
    let iter = c2
        .into_iter()
        .zip(c3.into_iter())
        .map(|((d2, b2), (d3, b3))| (d2, b2, d3, b3));
    match (f, c1) {
        (FType3::Assign(f), IndexedMutColumn::Insert(tgt)) => {
            tgt.insert(iter, &|(a, b, c, d)| f(a, b, c, d))
        }
        (FType3::Assign(f), IndexedMutColumn::Update(tgt)) => {
            tgt.assign(iter, &|(a, b, c, d)| f(a, b, c, d))
        }
        (FType3::Update(f), IndexedMutColumn::Update(tgt)) => {
            tgt.update(iter, |a, b, c| f(a, b, c))
        }
        _ => panic!(),
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////

fn f_2_sized_binary_binary<'a, 'i, T1, T2, T3, U3, F1, F2>(
    c1: &'a mut IndexedMutColumn<T1>,
    c: &'a ReadBinaryColumn<'a, T2>,
    c3: U3,
    f: FType3<'a, T1, [u8], [u8], F1, F2>,
) -> Result<(), ErrorDesc>
where
    'a: 'i,
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync + AsBytes,
    T3: 'static + Send + Sync,
    U3: IntoIterator<Item = (&'i [u8], &'i bool)>,
    U3::IntoIter: ExactSizeIterator,
    U3: Clone,
    F1: Fn(&[u8], &bool, &[u8], &bool) -> (bool, T1),
    F2: Fn(&mut T1, &mut bool, (&[u8], &bool, &[u8], &bool)),
{
    match c {
        ReadBinaryColumn::BitmapIndex(c) => {
            f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _>(c1, c.as_binary_iter(), c3, f)
        }
        ReadBinaryColumn::BitmapNoIndex(c) => {
            f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _>(c1, c.as_binary_iter(), c3, f)
        }
        ReadBinaryColumn::NoBitmapIndex(c) => {
            f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _>(c1, c.as_binary_iter(), c3, f)
        }
        ReadBinaryColumn::NoBitmapNoIndex(c) => {
            f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _>(c1, c.as_binary_iter(), c3, f)
        }
        ReadBinaryColumn::BitmapIndexOption(c) => {
            f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _>(c1, c.as_binary_iter(), c3, f)
        }
        ReadBinaryColumn::NoBitmapIndexOption(c) => {
            f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _>(c1, c.as_binary_iter(), c3, f)
        }
        ReadBinaryColumn::Const(c) => {
            f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _>(c1, c.as_binary_iter(), c3, f)
        }
        ReadBinaryColumn::BitmapIndexOrig(c) => {
            f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _>(c1, c.as_binary_iter(), c3, f)
        }
        ReadBinaryColumn::BitmapNoIndexOrig(c) => {
            f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _>(c1, c.as_binary_iter(), c3, f)
        }
        ReadBinaryColumn::NoBitmapIndexOrig(c) => {
            f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _>(c1, c.as_binary_iter(), c3, f)
        }
        ReadBinaryColumn::NoBitmapNoIndexOrig(c) => {
            f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _>(c1, c.as_binary_iter(), c3, f)
        }
        ReadBinaryColumn::BitmapIndexOptionOrig(c) => {
            f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _>(c1, c.as_binary_iter(), c3, f)
        }
        ReadBinaryColumn::NoBitmapIndexOptionOrig(c) => {
            f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _>(c1, c.as_binary_iter(), c3, f)
        }
        ReadBinaryColumn::ConstOrig(c) => {
            f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _>(c1, c.as_binary_iter(), c3, f)
        }
    }
}

////////////////////////////////////////////////////////////////////////////

fn f_3_sized_binary_binary<'a, 'i, T1, T2, T3, F1, F2>(
    c1: &'a mut IndexedMutColumn<T1>,
    c2: &'a ReadBinaryColumn<'a, T2>,
    c: &'a ReadBinaryColumn<'a, T3>,
    f: FType3<'a, T1, [u8], [u8], F1, F2>,
) -> Result<(), ErrorDesc>
where
    'a: 'i,
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync + AsBytes,
    T3: 'static + Send + Sync + AsBytes,
    F1: Fn(&[u8], &bool, &[u8], &bool) -> (bool, T1),
    F2: Fn(&mut T1, &mut bool, (&[u8], &bool, &[u8], &bool)),
{
    match c {
        ReadBinaryColumn::BitmapIndex(c) => {
            f_2_sized_binary_binary::<T1, T2, T3, _, _, _>(c1, c2, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::BitmapNoIndex(c) => {
            f_2_sized_binary_binary::<T1, T2, T3, _, _, _>(c1, c2, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::NoBitmapIndex(c) => {
            f_2_sized_binary_binary::<T1, T2, T3, _, _, _>(c1, c2, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::NoBitmapNoIndex(c) => {
            f_2_sized_binary_binary::<T1, T2, T3, _, _, _>(c1, c2, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::BitmapIndexOption(c) => {
            f_2_sized_binary_binary::<T1, T2, T3, _, _, _>(c1, c2, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::NoBitmapIndexOption(c) => {
            f_2_sized_binary_binary::<T1, T2, T3, _, _, _>(c1, c2, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::Const(c) => {
            f_2_sized_binary_binary::<T1, T2, T3, _, _, _>(c1, c2, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::BitmapIndexOrig(c) => {
            f_2_sized_binary_binary::<T1, T2, T3, _, _, _>(c1, c2, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::BitmapNoIndexOrig(c) => {
            f_2_sized_binary_binary::<T1, T2, T3, _, _, _>(c1, c2, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::NoBitmapIndexOrig(c) => {
            f_2_sized_binary_binary::<T1, T2, T3, _, _, _>(c1, c2, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::NoBitmapNoIndexOrig(c) => {
            f_2_sized_binary_binary::<T1, T2, T3, _, _, _>(c1, c2, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::BitmapIndexOptionOrig(c) => {
            f_2_sized_binary_binary::<T1, T2, T3, _, _, _>(c1, c2, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::NoBitmapIndexOptionOrig(c) => {
            f_2_sized_binary_binary::<T1, T2, T3, _, _, _>(c1, c2, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::ConstOrig(c) => {
            f_2_sized_binary_binary::<T1, T2, T3, _, _, _>(c1, c2, c.as_binary_iter(), f)
        }
    }
}

////////////////////////////////////////////////////////////////////////////

pub fn assign_3_sized_binary_binary_unroll<'a, T1, T2, T3, F>(
    c1: &'a mut ColumnWrapper,
    input: &'a [InputTypes<'a>],
    f: F,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync + AsBytes,
    T3: 'static + Send + Sync + AsBytes,
    F: Fn(&[u8], &bool, &[u8], &bool) -> (bool, T1),
{
    let mut c2_read_column = ReadBinaryColumn::<T2>::from_input(&input[0]);
    let mut c3_read_column = ReadBinaryColumn::<T3>::from_input(&input[1]);

    let c1_assign_column = UpdateColumn::<T1>::from_destination(c1, &ColumnDataIndex::None);
    let len = c1_assign_column.len();

    c2_read_column.update_len_if_const(len);
    c3_read_column.update_len_if_const(len);

    assert_eq!(len, c2_read_column.len());
    assert_eq!(len, c3_read_column.len());

    let mut c1_mut_column = IndexedMutColumn::Update(c1_assign_column);

    let dummy = |_: &mut T1, _: &mut bool, (_, _, _, _): (&[u8], &bool, &[u8], &bool)| {
        panic!("Dummy function called")
    };
    let f: FType3<T1, [u8], [u8], _, _> = FType3::new_assign(f, dummy);

    f_3_sized_binary_binary(&mut c1_mut_column, &c2_read_column, &c3_read_column, f)
}

////////////////////////////////////////////////////////////////////////////

pub fn insert_3_sized_binary_binary_unroll<'a, T1, T2, T3, F>(
    c1: &'a mut ColumnWrapper,
    input: &'a [InputTypes<'a>],
    bitmap_update_required: &bool,
    f: F,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync + AsBytes,
    T3: 'static + Send + Sync + AsBytes,
    F: Fn(&[u8], &bool, &[u8], &bool) -> (bool, T1),
{
    let mut c2_read_column = ReadBinaryColumn::<T2>::from_input(&input[0]);
    let mut c3_read_column = ReadBinaryColumn::<T3>::from_input(&input[1]);

    let len = std::cmp::max(c2_read_column.len(), c3_read_column.len());

    c2_read_column.update_len_if_const(len);
    c3_read_column.update_len_if_const(len);

    assert_eq!(c2_read_column.len(), len);
    assert_eq!(c3_read_column.len(), len);

    let c1_insert_column = InsertColumn::<T1>::from_destination(c1, *bitmap_update_required, len);

    let mut c1_mut_column = IndexedMutColumn::Insert(c1_insert_column);

    let dummy = |_: &mut T1, _: &mut bool, (_, _, _, _): (&[u8], &bool, &[u8], &bool)| {
        panic!("Dummy function called")
    };
    let f: FType3<T1, [u8], [u8], _, _> = FType3::new_assign(f, dummy);

    f_3_sized_binary_binary(&mut c1_mut_column, &c2_read_column, &c3_read_column, f)
}

////////////////////////////////////////////////////////////////////////////

pub fn update_3_sized_binary_binary_unroll<'a, T1, T2, T3, F>(
    c1: &'a mut ColumnWrapper,
    c1_index: &ColumnDataIndex,
    input: &'a [InputTypes<'a>],
    f: F,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync + AsBytes,
    T3: 'static + Send + Sync + AsBytes,
    F: Fn(&mut T1, &mut bool, (&[u8], &bool, &[u8], &bool)),
{
    let mut c2_read_column = ReadBinaryColumn::<T2>::from_input(&input[0]);
    let mut c3_read_column = ReadBinaryColumn::<T3>::from_input(&input[1]);

    let c1_update_column = UpdateColumn::from_destination(c1, c1_index);

    let len = c1_update_column.len();

    c2_read_column.update_len_if_const(len);
    c3_read_column.update_len_if_const(len);

    assert_eq!(len, c2_read_column.len());
    assert_eq!(len, c3_read_column.len());

    let mut c1_mut_column = IndexedMutColumn::Update(c1_update_column);

    let dummy =
        |_: &[u8], _: &bool, _: &[u8], _: &bool| -> (bool, T1) { panic!("dummy function called") };

    let f: FType3<T1, [u8], [u8], _, _> = FType3::new_update(dummy, f);

    f_3_sized_binary_binary(&mut c1_mut_column, &c2_read_column, &c3_read_column, f)
}

////////////////////////////////////////////////////////////////////////////
//////////////////                                 /////////////////////////
//////////////////            3                    /////////////////////////
//////////////////           END                   /////////////////////////
////////////////////////////////////////////////////////////////////////////
