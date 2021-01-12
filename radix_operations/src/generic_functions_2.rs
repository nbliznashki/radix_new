use crate::{
    FType2, IndexedMutColumn, InputTypes, InsertColumn, ReadBinaryColumn, ReadColumn, UpdateColumn,
};
use radix_column::*;

////////////////////////////////////////////////////////////////////////////
//////////////////                                 /////////////////////////
//////////////////            1                    /////////////////////////
//////////////////       sized sized               /////////////////////////
////////////////////////////////////////////////////////////////////////////

fn f_1_sized_sized<'a, 'i, T1, T2, U2, F1, F2>(
    c1: &'a mut IndexedMutColumn<T1>,
    c2: U2,
    f: FType2<'a, T1, T2, F1, F2>,
) -> Result<(), ErrorDesc>
where
    'a: 'i,
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    U2: IntoIterator<Item = (&'i T2, &'i bool)>,
    U2::IntoIter: ExactSizeIterator,
    U2: Clone,
    F1: Fn(&T2, &bool) -> (bool, T1),
    F2: Fn(&mut T1, &mut bool, (&T2, &bool)),
{
    match (f, c1) {
        (FType2::Assign(f), IndexedMutColumn::Insert(tgt)) => {
            tgt.insert(c2.into_iter(), &|(a, b)| f(a, b))
        }
        (FType2::Assign(f), IndexedMutColumn::Update(tgt)) => {
            tgt.assign(c2.into_iter(), &|(a, b)| f(a, b))
        }
        (FType2::Update(f), IndexedMutColumn::Update(tgt)) => {
            tgt.update(c2.into_iter(), |a, b, c| f(a, b, c))
        }
        _ => panic!(),
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////

fn f_2_sized_sized<'a, 'i, T1, T2, F1, F2>(
    c1: &'a mut IndexedMutColumn<T1>,
    c: &'a ReadColumn<'a, T2>,
    f: FType2<'a, T1, T2, F1, F2>,
) -> Result<(), ErrorDesc>
where
    'a: 'i,
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    F1: Fn(&T2, &bool) -> (bool, T1),
    F2: Fn(&mut T1, &mut bool, (&T2, &bool)),
{
    match c {
        ReadColumn::BitmapIndex(col) => f_1_sized_sized(c1, col.as_iter(), f),
        ReadColumn::BitmapNoIndex(col) => f_1_sized_sized(c1, col.as_iter(), f),
        ReadColumn::NoBitmapIndex(col) => f_1_sized_sized(c1, col.as_iter(), f),
        ReadColumn::NoBitmapNoIndex(col) => f_1_sized_sized(c1, col.as_iter(), f),
        ReadColumn::BitmapIndexOption(col) => f_1_sized_sized(c1, col.as_iter(), f),
        ReadColumn::NoBitmapIndexOption(col) => f_1_sized_sized(c1, col.as_iter(), f),
        ReadColumn::Const(col) => f_1_sized_sized(c1, col.as_iter(), f),
    }
}

////////////////////////////////////////////////////////////////////////////

pub fn assign_2_sized_sized_unroll<'a, 'b, T1, T2, F>(
    c1: &'a mut ColumnWrapper<'b>,
    input: &'a [InputTypes<'a>],
    f: F,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    F: Fn(&T2, &bool) -> (bool, T1),
{
    let (c2, c2_index) = match &input[0] {
        InputTypes::Ref(c, i) => (*c, *i),
        InputTypes::Owned(c, i) => (c, i),
    };

    let (c2_col, c2_bitmap) = c2.get_inner_ref();
    let mut c2_read_column: ReadColumn<T2> = ReadColumn::from((c2_col, c2_bitmap, c2_index, 1));

    let c1_assign_column = UpdateColumn::<T1>::from_destination(c1, &ColumnDataIndex::None);
    let len = c1_assign_column.len();
    c2_read_column.update_len_if_const(len);

    let mut c1_mut_column = IndexedMutColumn::Update(c1_assign_column);

    let dummy = |_: &mut T1, _: &mut bool, (_, _): (&T2, &bool)| panic!("Dummy function called");
    let f: FType2<T1, T2, _, _> = FType2::new_assign(f, dummy);

    f_2_sized_sized(&mut c1_mut_column, &c2_read_column, f)
}

////////////////////////////////////////////////////////////////////////////

pub fn insert_2_sized_sized_unroll<'a, 'b, T1, T2, F>(
    c1: &'a mut ColumnWrapper<'b>,
    input: &'a [InputTypes<'a>],
    bitmap_update_required: &bool,
    f: F,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    F: Fn(&T2, &bool) -> (bool, T1),
{
    let (c2, c2_index) = match &input[0] {
        InputTypes::Ref(c, i) => (*c, *i),
        InputTypes::Owned(c, i) => (c, i),
    };

    let (c2_col, c2_bitmap) = c2.get_inner_ref();
    let c2_read_column: ReadColumn<T2> = ReadColumn::from((c2_col, c2_bitmap, c2_index, 1));

    let len = c2_read_column.len();

    let c1_insert_column = InsertColumn::<T1>::from_destination(c1, *bitmap_update_required, len);
    let mut c1_mut_column = IndexedMutColumn::Insert(c1_insert_column);

    let dummy = |_: &mut T1, _: &mut bool, (_, _): (&T2, &bool)| {};
    let f: FType2<T1, T2, _, _> = FType2::new_assign(f, dummy);

    f_2_sized_sized(&mut c1_mut_column, &c2_read_column, f)
}
////////////////////////////////////////////////////////
pub fn update_2_sized_sized_unroll<'a, 'b, T1, T2, F>(
    c1: &'a mut ColumnWrapper<'b>,
    c1_index: &'a ColumnDataIndex,
    input: &'a [InputTypes<'a>],
    f: F,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    F: Fn(&mut T1, &mut bool, (&T2, &bool)),
{
    let mut c2_read_column = ReadColumn::<T2>::from_input(&input[0]);

    let c1_update_column = UpdateColumn::from_destination(c1, c1_index);
    c2_read_column.update_len_if_const(c1_update_column.len());

    assert_eq!(c2_read_column.len(), c1_update_column.len());

    let dummy = |_: &T2, _: &bool| -> (bool, T1) { panic!("dummy function called") };
    let f: FType2<T1, T2, _, _> = FType2::new_update(dummy, f);
    let mut c1 = IndexedMutColumn::Update(c1_update_column);
    f_2_sized_sized(&mut c1, &c2_read_column, f)
}

////////////////////////////////////////////////////////////////////////////
//////////////////                                 /////////////////////////
//////////////////            2                    /////////////////////////
//////////////////    sized binary binary          /////////////////////////
////////////////////////////////////////////////////////////////////////////

fn f_1_sized_binary<'a, 'i, T1, T2, U2, F1, F2>(
    c1: &'a mut IndexedMutColumn<T1>,
    c2: U2,
    f: FType2<'a, T1, [u8], F1, F2>,
) -> Result<(), ErrorDesc>
where
    'a: 'i,
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    U2: IntoIterator<Item = (&'i [u8], &'i bool)>,
    U2::IntoIter: ExactSizeIterator,
    U2: Clone,
    F1: Fn(&[u8], &bool) -> (bool, T1),
    F2: Fn(&mut T1, &mut bool, (&[u8], &bool)),
{
    match (f, c1) {
        (FType2::Assign(f), IndexedMutColumn::Insert(tgt)) => {
            tgt.insert(c2.into_iter(), &|(a, b)| f(a, b))
        }
        (FType2::Assign(f), IndexedMutColumn::Update(tgt)) => {
            tgt.assign(c2.into_iter(), &|(a, b)| f(a, b))
        }
        (FType2::Update(f), IndexedMutColumn::Update(tgt)) => {
            tgt.update(c2.into_iter(), |a, b, c| f(a, b, c))
        }
        _ => panic!(),
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////

fn f_2_sized_binary<'a, 'i, T1, T2, F1, F2>(
    c1: &'a mut IndexedMutColumn<T1>,
    c: &'a ReadBinaryColumn<'a, T2>,
    f: FType2<'a, T1, [u8], F1, F2>,
) -> Result<(), ErrorDesc>
where
    'a: 'i,
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync + AsBytes,
    F1: Fn(&[u8], &bool) -> (bool, T1),
    F2: Fn(&mut T1, &mut bool, (&[u8], &bool)),
{
    match c {
        ReadBinaryColumn::BitmapIndex(c) => {
            f_1_sized_binary::<T1, T2, _, _, _>(c1, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::BitmapNoIndex(c) => {
            f_1_sized_binary::<T1, T2, _, _, _>(c1, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::NoBitmapIndex(c) => {
            f_1_sized_binary::<T1, T2, _, _, _>(c1, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::NoBitmapNoIndex(c) => {
            f_1_sized_binary::<T1, T2, _, _, _>(c1, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::BitmapIndexOption(c) => {
            f_1_sized_binary::<T1, T2, _, _, _>(c1, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::NoBitmapIndexOption(c) => {
            f_1_sized_binary::<T1, T2, _, _, _>(c1, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::Const(c) => {
            f_1_sized_binary::<T1, T2, _, _, _>(c1, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::BitmapIndexOrig(c) => {
            f_1_sized_binary::<T1, T2, _, _, _>(c1, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::BitmapNoIndexOrig(c) => {
            f_1_sized_binary::<T1, T2, _, _, _>(c1, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::NoBitmapIndexOrig(c) => {
            f_1_sized_binary::<T1, T2, _, _, _>(c1, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::NoBitmapNoIndexOrig(c) => {
            f_1_sized_binary::<T1, T2, _, _, _>(c1, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::BitmapIndexOptionOrig(c) => {
            f_1_sized_binary::<T1, T2, _, _, _>(c1, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::NoBitmapIndexOptionOrig(c) => {
            f_1_sized_binary::<T1, T2, _, _, _>(c1, c.as_binary_iter(), f)
        }
        ReadBinaryColumn::ConstOrig(c) => {
            f_1_sized_binary::<T1, T2, _, _, _>(c1, c.as_binary_iter(), f)
        }
    }
}

////////////////////////////////////////////////////////////////////////////

pub fn assign_2_sized_binary_unroll<'a, T1, T2, F>(
    c1: &'a mut ColumnWrapper,
    input: &'a [InputTypes<'a>],
    f: F,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync + AsBytes,
    F: Fn(&[u8], &bool) -> (bool, T1),
{
    let mut c2_read_column = ReadBinaryColumn::<T2>::from_input(&input[0]);

    let c1_assign_column = UpdateColumn::<T1>::from_destination(c1, &ColumnDataIndex::None);
    let len = c1_assign_column.len();
    c2_read_column.update_len_if_const(len);
    assert_eq!(c2_read_column.len(), len);
    let mut c1_mut_column = IndexedMutColumn::Update(c1_assign_column);

    let dummy = |_: &mut T1, _: &mut bool, (_, _): (&[u8], &bool)| {};
    let f: FType2<T1, [u8], _, _> = FType2::new_assign(f, dummy);

    f_2_sized_binary::<T1, T2, _, _>(&mut c1_mut_column, &c2_read_column, f)
}
////////////////////////////////////////////////////////
pub fn update_2_sized_binary_unroll<'a, 'b, T1, T2, F>(
    c1: &'a mut ColumnWrapper<'b>,
    c1_index: &'a ColumnDataIndex,
    input: &'a [InputTypes<'a>],
    f: F,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync + AsBytes,
    F: Fn(&mut T1, &mut bool, (&[u8], &bool)),
{
    let mut c2_read_column = ReadBinaryColumn::<T2>::from_input(&input[0]);

    let c1_update_column = UpdateColumn::from_destination(c1, c1_index);
    c2_read_column.update_len_if_const(c1_update_column.len());

    assert_eq!(c2_read_column.len(), c1_update_column.len());

    let dummy = |_: &[u8], _: &bool| -> (bool, T1) { panic!("dummy function called") };
    let f: FType2<T1, [u8], _, _> = FType2::new_update(dummy, f);
    let mut c1 = IndexedMutColumn::Update(c1_update_column);
    f_2_sized_binary(&mut c1, &c2_read_column, f)
}
////////////////////////////////////////////////////////
pub fn insert_2_sized_binary_unroll<'a, T1, T2, F>(
    c1: &'a mut ColumnWrapper,
    input: &'a [InputTypes<'a>],
    bitmap_update_required: &bool,
    f: F,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync + AsBytes,
    F: Fn(&[u8], &bool) -> (bool, T1),
{
    let c2_read_column = ReadBinaryColumn::<T2>::from_input(&input[0]);
    let len = c2_read_column.len();

    let c1_insert_column = InsertColumn::<T1>::from_destination(c1, *bitmap_update_required, len);
    let mut c1_mut_column = IndexedMutColumn::Insert(c1_insert_column);

    let dummy = |_: &mut T1, _: &mut bool, (_, _): (&[u8], &bool)| {};
    let f: FType2<T1, [u8], _, _> = FType2::new_assign(f, dummy);

    f_2_sized_binary::<T1, T2, _, _>(&mut c1_mut_column, &c2_read_column, f)
}

////////////////////////////////////////////////////////////////////////////
//////////////////                                 /////////////////////////
//////////////////            3                    /////////////////////////
//////////////////           END                   /////////////////////////
////////////////////////////////////////////////////////////////////////////
