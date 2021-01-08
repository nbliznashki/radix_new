use crate::{
    IndexedMutColumn, InputTypes, InsertColumn, ReadBinaryColumn, ReadColumn, UpdateColumn,
};
use radix_column::*;
enum FType<'a, O1, O2, F1, F2>
where
    O1: ?Sized,
    O2: ?Sized,
    F1: Fn(&O2, &bool) -> (bool, O1),
    F2: Fn(&'a mut O1, &'a mut bool, (&O2, &bool)),
{
    Insert(F1),
    Update(F2),
    _Phantom((std::marker::PhantomData<&'a u8>, &'a O1, &'a O2)),
}

impl<'a, O1, O2, F1, F2> FType<'a, O1, O2, F1, F2>
where
    O1: ?Sized,
    O2: ?Sized,
    F1: Fn(&O2, &bool) -> (bool, O1),
    F2: Fn(&'a mut O1, &'a mut bool, (&O2, &bool)),
{
    fn new_insert(f: F1, _: F2) -> Self {
        Self::Insert(f)
    }
    fn new_update(_: F1, f: F2) -> Self {
        Self::Update(f)
    }
}

////////////////////////////////////////////////////////////////////////////
//////////////////                                 /////////////////////////
//////////////////            1                    /////////////////////////
//////////////////       sized sized               /////////////////////////
////////////////////////////////////////////////////////////////////////////

fn f_1_sized_sized<'a, 'i, T1, T2, U2, F1, F2>(
    c1: &'a mut IndexedMutColumn<'a, T1>,
    bitmap_update_required: &bool,
    c2: U2,
    f: FType<'a, T1, T2, F1, F2>,
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
        (FType::Insert(f), IndexedMutColumn::Insert(tgt)) => {
            tgt.insert(c2.into_iter(), c2.into_iter(), |(a, b)| f(a, b))
        }
        (FType::Update(f), IndexedMutColumn::Update(tgt)) => {
            tgt.apply(c2.into_iter(), |a, b, c| f(a, b, c))
        }
        _ => panic!(),
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////

fn f_2_sized_sized<'a, 'i, T1, T2, F1, F2>(
    c1: &'a mut IndexedMutColumn<'a, T1>,
    bitmap_update_required: &bool,
    c: &'a ReadColumn<'a, T2>,
    f: FType<'a, T1, T2, F1, F2>,
) -> Result<(), ErrorDesc>
where
    'a: 'i,
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    F1: Fn(&T2, &bool) -> (bool, T1),
    F2: Fn(&mut T1, &mut bool, (&T2, &bool)),
{
    match c {
        ReadColumn::BitmapIndex(col) => {
            f_1_sized_sized(c1, bitmap_update_required, col.as_iter(), f)
        }
        ReadColumn::BitmapNoIndex(col) => {
            f_1_sized_sized(c1, bitmap_update_required, col.as_iter(), f)
        }
        ReadColumn::NoBitmapIndex(col) => {
            f_1_sized_sized(c1, bitmap_update_required, col.as_iter(), f)
        }
        ReadColumn::NoBitmapNoIndex(col) => {
            f_1_sized_sized(c1, bitmap_update_required, col.as_iter(), f)
        }
        ReadColumn::Const(col) => f_1_sized_sized(c1, bitmap_update_required, col.as_iter(), f),
    }
}

////////////////////////////////////////////////////////////////////////////

pub fn insert_2_sized_sized_unroll<'a, T1, T2, FBool, F>(
    c1: &'a mut ColumnWrapper<'a>,
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
    let mut c2_read_column: ReadColumn<T2> = ReadColumn::from((c2_col, c2_bitmap, c2_index, 1));

    let len = c1.column().data_len::<T1>()?;

    let c1_insert_column = InsertColumn::<T1>::from_destination(c1, *bitmap_update_required, len);
    let c1_mut_column = IndexedMutColumn::Insert(c1_insert_column);

    let dummy = |_: &mut T1, _: &mut bool, (_, _): (&T2, &bool)| {};
    let f: FType<T1, T2, _, _> = FType::new_update(f, dummy);

    f_2_sized_sized(
        &mut c1_mut_column,
        bitmap_update_required,
        &c2_read_column,
        f,
    )
}

pub fn update_2_sized_sized_unroll<'a, T1, T2, F>(
    c1: &'a mut ColumnWrapper<'a>,
    c1_index: &'a ColumnDataIndex,
    input: &'a [InputTypes<'a>],
    bitmap_update_required: &bool,
    f: F,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    F: Fn(&mut T1, &mut bool, (&T2, &bool)),
{
    let mut c2_read_column = ReadColumn::<T2>::from_input(&input[0]);

    let len = c2_read_column.len();
    let c1 = UpdateColumn::from_destination(c1, c1_index);
    assert_eq!(len, c1.len());

    let dummy = |_: &T2, _: &bool| -> (bool, T1) { panic!("dummy function called") };
    let f: FType<T1, T2, _, _> = FType::new_update(dummy, f);
    let c1 = IndexedMutColumn::Update(c1);
    f_2_sized_sized(&mut c1, bitmap_update_required, &c2_read_column, f)
}

////////////////////////////////////////////////////////////////////////////
//////////////////                                 /////////////////////////
//////////////////            2                    /////////////////////////
//////////////////    sized binary binary          /////////////////////////
////////////////////////////////////////////////////////////////////////////

fn f_1_sized_binary<'a, 'i, T1, T2, U2, FBool, F1, F2>(
    c1: &'a mut ColumnWrapper,
    c1_index: &ColumnDataIndex,
    bitmap_update_required: &bool,
    c2: U2,
    f: FType<'a, T1, [u8], F1, F2>,
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
    let (c1, c1_bitmap) = c1.get_inner_mut();
    let c2_iter = c2.clone().into_iter();

    let is_set_operation = match f {
        FType::Set(_) => true,
        FType::Update(_) => false,
        FType::_Phantom(_) => unreachable!(),
    };

    if is_set_operation && c1.is_owned() && c1.data_len::<T1>()? == 0 {
        // We have to do an insert
        // Requires an Owned column
        // Bitmap should also be owned and empty
        // Index should be empty

        let c1_data: &mut Vec<T1> = c1.downcast_vec::<T1>()?;
        let c1_bitmap = c1_bitmap.downcast_vec()?;

        assert_eq!(c1_bitmap.len(), 0);
        assert_eq!(c1_index.len(), None);

        let (f_data, f_bitmap): (F1, FBool) = match f {
            FType::Set((f1, f2)) => (f1, f2),
            _ => Err("Cannnot update a non-slice")?,
        };
        match bitmap_update_required {
            true => {
                c1_data.extend(c2_iter.map(|(c2_value, c2_bitmap)| f_data(c2_value, c2_bitmap)));
                c1_bitmap.extend(
                    c2.into_iter()
                        .map(|(_c2_value, c2_bitmap)| f_bitmap(c2_bitmap)),
                );
            }
            false => {
                c1_data.extend(c2_iter.map(|(c2_value, c2_bitmap)| f_data(c2_value, c2_bitmap)))
            }
        }
    } else {
        // Mut Borrow column
        //- Should have correct storate reserved
        //- Supports Set and Update operations

        let c1_data = c1.downcast_mut::<T1>()?;
        //let c1_bitmap = c1_bitmap.downcast_mut()?;
        if *bitmap_update_required {
            assert_eq!(c1_bitmap.len(), Some(c1_data.len()));
        } else {
            assert!(!c1_bitmap.is_some());
        }

        if c1_index.is_some() {
            assert_eq!(c2_iter.len(), c1_index.len().unwrap());
        } else {
            assert_eq!(c1_data.len(), c2_iter.len());
        }

        match f {
            FType::Set((f_data, f_bitmap)) => match (c1_index.is_some(), bitmap_update_required) {
                (true, true) => {
                    let c1_bitmap = c1_bitmap.downcast_mut().unwrap();
                    c1_index.downcast_ref()?.iter().zip(c2).for_each(
                        |(i, (c2_value, c2_bitmap))| {
                            c1_data[*i] = f_data(c2_value, c2_bitmap);
                            c1_bitmap[*i] = f_bitmap(c2_bitmap);
                        },
                    )
                }
                (true, false) => c1_index.downcast_ref()?.iter().zip(c2).for_each(
                    |(i, (c2_value, c2_bitmap))| {
                        c1_data[*i] = f_data(c2_value, c2_bitmap);
                    },
                ),
                (false, true) => {
                    let c1_bitmap = c1_bitmap.downcast_mut().unwrap();
                    c1_data
                        .iter_mut()
                        .zip(c1_bitmap.iter_mut())
                        .zip(c2)
                        .for_each(|((c1_value, c1_bitmap), (c2_value, c2_bitmap))| {
                            *c1_value = f_data(c2_value, c2_bitmap);
                            *c1_bitmap = f_bitmap(c2_bitmap);
                        })
                }
                (false, false) => {
                    c1_data
                        .iter_mut()
                        .zip(c2)
                        .for_each(|(c1_value, (c2_value, c2_bitmap))| {
                            *c1_value = f_data(c2_value, c2_bitmap);
                        })
                }
            },

            FType::Update(f) => match (c1_index.is_some(), bitmap_update_required) {
                (true, true) => c1_index.downcast_ref()?.iter().zip(c2).for_each(
                    |(i, (c2_value, c2_bitmap))| {
                        let c1_bitmap = c1_bitmap.downcast_mut().unwrap();
                        f(
                            c1_data.get_mut(*i).unwrap(),
                            c1_bitmap.get_mut(*i).unwrap(),
                            c2_value,
                            c2_bitmap,
                        );
                    },
                ),
                (true, false) => c1_index.downcast_ref()?.iter().zip(c2).for_each(
                    |(i, (c2_value, c2_bitmap))| {
                        f(c1_data.get_mut(*i).unwrap(), &mut true, c2_value, c2_bitmap);
                    },
                ),
                (false, true) => c1_data
                    .iter_mut()
                    .zip(c1_bitmap.downcast_mut().unwrap().iter_mut())
                    .zip(c2)
                    .for_each(|((c1_value, c1_bitmap), (c2_value, c2_bitmap))| {
                        f(c1_value, c1_bitmap, c2_value, c2_bitmap);
                    }),
                (false, false) => {
                    c1_data
                        .iter_mut()
                        .zip(c2)
                        .for_each(|(c1_value, (c2_value, c2_bitmap))| {
                            f(c1_value, &mut true, c2_value, c2_bitmap);
                        })
                }
            },
            _ => Err("Function type should be either assign or update")?,
        }
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////

fn f_2_sized_binary<'a, 'i, T1, T2, FBool, F1, F2>(
    c1: &'a mut ColumnWrapper,
    c1_index: &ColumnDataIndex,
    bitmap_update_required: &bool,
    c: &'a ReadBinaryColumn<'a, T2>,
    f: FType<'a, T1, [u8], F1, F2>,
) -> Result<(), ErrorDesc>
where
    'a: 'i,
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync + AsBytes,
    F1: Fn(&[u8], &bool) -> (bool, T1),
    F2: Fn(&mut T1, &mut bool, (&[u8], &bool)),
{
    match c {
        ReadBinaryColumn::BitmapIndex(c) => f_1_sized_binary::<T1, T2, _, _, _, _>(
            c1,
            c1_index,
            bitmap_update_required,
            c.as_binary_iter(),
            f,
        ),
        ReadBinaryColumn::BitmapNoIndex(c) => f_1_sized_binary::<T1, T2, _, _, _, _>(
            c1,
            c1_index,
            bitmap_update_required,
            c.as_binary_iter(),
            f,
        ),
        ReadBinaryColumn::NoBitmapIndex(c) => f_1_sized_binary::<T1, T2, _, _, _, _>(
            c1,
            c1_index,
            bitmap_update_required,
            c.as_binary_iter(),
            f,
        ),
        ReadBinaryColumn::NoBitmapNoIndex(c) => f_1_sized_binary::<T1, T2, _, _, _, _>(
            c1,
            c1_index,
            bitmap_update_required,
            c.as_binary_iter(),
            f,
        ),
        ReadBinaryColumn::Const(c) => f_1_sized_binary::<T1, T2, _, _, _, _>(
            c1,
            c1_index,
            bitmap_update_required,
            c.as_binary_iter(),
            f,
        ),
        ReadBinaryColumn::BitmapIndexOrig(c) => f_1_sized_binary::<T1, T2, _, _, _, _>(
            c1,
            c1_index,
            bitmap_update_required,
            c.as_binary_iter(),
            f,
        ),
        ReadBinaryColumn::BitmapNoIndexOrig(c) => f_1_sized_binary::<T1, T2, _, _, _, _>(
            c1,
            c1_index,
            bitmap_update_required,
            c.as_binary_iter(),
            f,
        ),
        ReadBinaryColumn::NoBitmapIndexOrig(c) => f_1_sized_binary::<T1, T2, _, _, _, _>(
            c1,
            c1_index,
            bitmap_update_required,
            c.as_binary_iter(),
            f,
        ),
        ReadBinaryColumn::NoBitmapNoIndexOrig(c) => f_1_sized_binary::<T1, T2, _, _, _, _>(
            c1,
            c1_index,
            bitmap_update_required,
            c.as_binary_iter(),
            f,
        ),
        ReadBinaryColumn::ConstOrig(c) => f_1_sized_binary::<T1, T2, _, _, _, _>(
            c1,
            c1_index,
            bitmap_update_required,
            c.as_binary_iter(),
            f,
        ),
    }
}

////////////////////////////////////////////////////////////////////////////

pub fn set_2_sized_binary_unroll<'a, T1, T2, FBool, F>(
    c1: &'a mut ColumnWrapper,
    input: &'a [InputTypes<'a>],
    bitmap_update_required: &bool,
    f: F,
    f_bitmap: FBool,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync + AsBytes,

    FBool: Fn(&bool) -> bool,
    F: Fn(&[u8], &bool) -> T1,
{
    let mut c2_read_column = ReadBinaryColumn::<T2>::from_input(&input[0]);

    let len = c1.column().data_len::<T1>()?;

    let len = std::cmp::max(len, c2_read_column.len());
    c2_read_column.update_len_if_const(len);

    if *bitmap_update_required && !c1.bitmap().is_some() {
        if c1.column().is_owned() {
            c1.bitmap_set(ColumnDataF::new(Vec::with_capacity(len)))
        } else {
            Err("Bitmap update is required, but the column is a reference and has no bitmap")?
        }
    };

    if !*bitmap_update_required && c1.bitmap().is_some() {
        if c1.column().is_owned() {
            c1.bitmap_set(ColumnDataF::None)
        } else {
            Err("Bitmap should be None, but the column is a reference and has a bitmap")?
        }
    };

    let dummy = |_: &mut T1, _: &mut bool, _: &[u8], _: &bool| {};
    let f: FType<T1, [u8], _, _, _> = FType::new_set(f, dummy, f_bitmap);

    f_2_sized_binary::<T1, T2, _, _, _>(
        c1,
        &ColumnDataIndex::None,
        bitmap_update_required,
        &c2_read_column,
        f,
    )
}

////////////////////////////////////////////////////////////////////////////
//////////////////                                 /////////////////////////
//////////////////            3                    /////////////////////////
//////////////////           END                   /////////////////////////
////////////////////////////////////////////////////////////////////////////
