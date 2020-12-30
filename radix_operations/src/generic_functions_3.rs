use crate::InputTypes;
use radix_column::*;
enum FType<'a, O1, O2, O3, FBool, F1, F2>
where
    O1: ?Sized,
    O2: ?Sized,
    O3: ?Sized,
    FBool: Fn(&bool, &bool) -> bool,
    F1: Fn(&O2, &bool, &O3, &bool) -> O1,
    F2: Fn(&'a mut O1, &'a mut bool, &O2, &bool, &O3, &bool),
{
    Set((F1, FBool)),
    Update(F2),
    _Phantom((std::marker::PhantomData<&'a u8>, &'a O1, &'a O2, &'a O3)),
}

impl<'a, O1, O2, O3, FBool, F1, F2> FType<'a, O1, O2, O3, FBool, F1, F2>
where
    O1: ?Sized,
    O2: ?Sized,
    O3: ?Sized,
    FBool: Fn(&bool, &bool) -> bool,
    F1: Fn(&O2, &bool, &O3, &bool) -> O1,
    F2: Fn(&'a mut O1, &'a mut bool, &O2, &bool, &O3, &bool),
{
    fn new_set(f: F1, _: F2, f_bitmap: FBool) -> Self {
        Self::Set((f, f_bitmap))
    }
    fn new_update(_: F1, f: F2, _: FBool) -> Self {
        Self::Update(f)
    }
}

////////////////////////////////////////////////////////////////////////////
//////////////////                                 /////////////////////////
//////////////////            1                    /////////////////////////
//////////////////    sized sized sized            /////////////////////////
////////////////////////////////////////////////////////////////////////////

fn f_1_sized_sized_sized<'a, 'i, T1, T2, T3, U2, U3, FBool, F1, F2>(
    c1: &'a mut ColumnWrapper,
    c1_index: &ColumnDataF<usize>,
    bitmap_update_required: &bool,
    c2: U2,
    c3: U3,
    f: FType<'a, T1, T2, T3, FBool, F1, F2>,
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
    FBool: Fn(&bool, &bool) -> bool,
    F1: Fn(&T2, &bool, &T3, &bool) -> T1,
    F2: Fn(&mut T1, &mut bool, &T2, &bool, &T3, &bool),
{
    let (c1, c1_bitmap) = c1.get_inner_mut();
    let c2_iter = c2.clone().into_iter();
    let c3_iter = c3.clone().into_iter();

    assert_eq!(c2_iter.len(), c3_iter.len());

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

        assert_eq!(c1_index.len(), None);

        let (f_data, f_bitmap): (F1, FBool) = match f {
            FType::Set((f1, f2)) => (f1, f2),
            _ => Err("Cannnot update a non-slice")?,
        };
        match bitmap_update_required {
            true => {
                c1_data.extend(c2_iter.zip(c3_iter).map(
                    |((c2_value, c2_bitmap), (c3_value, c3_bitmap))| {
                        f_data(c2_value, c2_bitmap, c3_value, c3_bitmap)
                    },
                ));
                let c1_bitmap = c1_bitmap.downcast_vec()?;
                assert_eq!(c1_bitmap.len(), 0);
                c1_bitmap.extend(c2.into_iter().zip(c3.into_iter()).map(
                    |((_c2_value, c2_bitmap), (_c3_value, c3_bitmap))| {
                        f_bitmap(c2_bitmap, c3_bitmap)
                    },
                ));
            }
            false => c1_data.extend(c2_iter.zip(c3_iter).map(
                |((c2_value, c2_bitmap), (c3_value, c3_bitmap))| {
                    f_data(c2_value, c2_bitmap, c3_value, c3_bitmap)
                },
            )),
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
            assert!(!c1_bitmap.is_some(), 0);
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
                    c1_index.downcast_ref()?.iter().zip(c2).zip(c3).for_each(
                        |((i, (c2_value, c2_bitmap)), (c3_value, c3_bitmap))| {
                            c1_data[*i] = f_data(c2_value, c2_bitmap, c3_value, c3_bitmap);
                            c1_bitmap[*i] = f_bitmap(c2_bitmap, c3_bitmap);
                        },
                    )
                }
                (true, false) => c1_index.downcast_ref()?.iter().zip(c2).zip(c3).for_each(
                    |((i, (c2_value, c2_bitmap)), (c3_value, c3_bitmap))| {
                        c1_data[*i] = f_data(c2_value, c2_bitmap, c3_value, c3_bitmap);
                    },
                ),
                (false, true) => {
                    let c1_bitmap = c1_bitmap.downcast_mut().unwrap();
                    c1_data
                        .iter_mut()
                        .zip(c1_bitmap.iter_mut())
                        .zip(c2)
                        .zip(c3)
                        .for_each(
                            |(
                                ((c1_value, c1_bitmap), (c2_value, c2_bitmap)),
                                (c3_value, c3_bitmap),
                            )| {
                                *c1_value = f_data(c2_value, c2_bitmap, c3_value, c3_bitmap);
                                *c1_bitmap = f_bitmap(c2_bitmap, c3_bitmap);
                            },
                        )
                }
                (false, false) => c1_data.iter_mut().zip(c2).zip(c3).for_each(
                    |((c1_value, (c2_value, c2_bitmap)), (c3_value, c3_bitmap))| {
                        *c1_value = f_data(c2_value, c2_bitmap, c3_value, c3_bitmap);
                    },
                ),
            },

            FType::Update(f) => match (c1_index.is_some(), bitmap_update_required) {
                (true, true) => {
                    let c1_bitmap = c1_bitmap.downcast_mut().unwrap();
                    c1_index.downcast_ref()?.iter().zip(c2).zip(c3).for_each(
                        |((i, (c2_value, c2_bitmap)), (c3_value, c3_bitmap))| {
                            f(
                                c1_data.get_mut(*i).unwrap(),
                                c1_bitmap.get_mut(*i).unwrap(),
                                c2_value,
                                c2_bitmap,
                                c3_value,
                                c3_bitmap,
                            );
                        },
                    )
                }
                (true, false) => c1_index.downcast_ref()?.iter().zip(c2).zip(c3).for_each(
                    |((i, (c2_value, c2_bitmap)), (c3_value, c3_bitmap))| {
                        f(
                            c1_data.get_mut(*i).unwrap(),
                            &mut true,
                            c2_value,
                            c2_bitmap,
                            c3_value,
                            c3_bitmap,
                        );
                    },
                ),
                (false, true) => {
                    let c1_bitmap = c1_bitmap.downcast_mut().unwrap();
                    c1_data
                        .iter_mut()
                        .zip(c1_bitmap.iter_mut())
                        .zip(c2)
                        .zip(c3)
                        .for_each(
                            |(
                                ((c1_value, c1_bitmap), (c2_value, c2_bitmap)),
                                (c3_value, c3_bitmap),
                            )| {
                                f(
                                    c1_value, c1_bitmap, c2_value, c2_bitmap, c3_value, c3_bitmap,
                                );
                            },
                        )
                }
                (false, false) => c1_data.iter_mut().zip(c2).zip(c3).for_each(
                    |((c1_value, (c2_value, c2_bitmap)), (c3_value, c3_bitmap))| {
                        f(
                            c1_value, &mut true, c2_value, c2_bitmap, c3_value, c3_bitmap,
                        );
                    },
                ),
            },
            _ => Err("Function type should be either assign or update")?,
        }
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////

fn f_2_sized_sized_sized<'a, 'i, T1, T2, T3, U3, FBool, F1, F2>(
    c1: &'a mut ColumnWrapper,
    c1_index: &ColumnDataF<usize>,
    bitmap_update_required: &bool,
    c: &'a ColumnWrapper,
    c_index: &'a ColumnDataF<usize>,
    c3: U3,
    f: FType<'a, T1, T2, T3, FBool, F1, F2>,
    len: &usize,
) -> Result<(), ErrorDesc>
where
    'a: 'i,
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    T3: 'static + Send + Sync,
    U3: IntoIterator<Item = (&'i T3, &'i bool)>,
    U3::IntoIter: ExactSizeIterator,
    U3: Clone,
    FBool: Fn(&bool, &bool) -> bool,
    F1: Fn(&T2, &bool, &T3, &bool) -> T1,
    F2: Fn(&mut T1, &mut bool, &T2, &bool, &T3, &bool),
{
    let c_is_const = c.column().is_const();
    let (c, c_bitmap) = c.get_inner_ref();
    let c_data = c.downcast_ref::<T2>()?;

    let c_const_val = || {
        (0..*len)
            .into_iter()
            .map(|_| (&c_data[0], &c_bitmap.downcast_ref().unwrap()[0]))
    };
    let c_const_true = || (0..*len).into_iter().map(|_| (&c_data[0], &true));
    let c_index_bitmap = || {
        c_index
            .downcast_ref()
            .unwrap()
            .iter()
            .map(|i| (&c_data[*i], &c_bitmap.downcast_ref().unwrap()[*i]))
    };
    let c_index_nobitmap = || {
        c_index
            .downcast_ref()
            .unwrap()
            .iter()
            .map(|i| (&c_data[*i], &true))
    };
    let c_noindex_bitmap = || {
        c_data
            .iter()
            .zip(c_bitmap.downcast_ref().unwrap().iter())
            .map(|(v, b)| (v, b))
    };
    let c_noindex_nobitmap = || c_data.iter().map(|v| (v, &true));

    if c_is_const {
        match c_bitmap.is_some() {
            true => {
                f_1_sized_sized_sized(c1, c1_index, bitmap_update_required, c_const_val(), c3, f)
            }
            false => {
                f_1_sized_sized_sized(c1, c1_index, bitmap_update_required, c_const_true(), c3, f)
            }
        }
    } else {
        match (c_index.is_some(), c_bitmap.is_some()) {
            (true, true) => f_1_sized_sized_sized(
                c1,
                c1_index,
                bitmap_update_required,
                c_index_bitmap(),
                c3,
                f,
            ),
            (true, false) => f_1_sized_sized_sized(
                c1,
                c1_index,
                bitmap_update_required,
                c_index_nobitmap(),
                c3,
                f,
            ),
            (false, true) => f_1_sized_sized_sized(
                c1,
                c1_index,
                bitmap_update_required,
                c_noindex_bitmap(),
                c3,
                f,
            ),
            (false, false) => f_1_sized_sized_sized(
                c1,
                c1_index,
                bitmap_update_required,
                c_noindex_nobitmap(),
                c3,
                f,
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////

fn f_3_sized_sized_sized<'a, 'i, T1, T2, T3, FBool, F1, F2>(
    c1: &'a mut ColumnWrapper,
    c1_index: &ColumnDataF<usize>,
    bitmap_update_required: &bool,
    c2: &'a ColumnWrapper,
    c2_index: &'a ColumnDataF<usize>,
    c: &'a ColumnWrapper,
    c_index: &'a ColumnDataF<usize>,
    f: FType<'a, T1, T2, T3, FBool, F1, F2>,
    len: &usize,
) -> Result<(), ErrorDesc>
where
    'a: 'i,
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    T3: 'static + Send + Sync,
    FBool: Fn(&bool, &bool) -> bool,
    F1: Fn(&T2, &bool, &T3, &bool) -> T1,
    F2: Fn(&mut T1, &mut bool, &T2, &bool, &T3, &bool),
{
    let c_is_const = c.column().is_const();
    let (c, c_bitmap) = c.get_inner_ref();
    let c_data = c.downcast_ref::<T3>()?;

    let c_const_val = || {
        (0..*len)
            .into_iter()
            .map(|_| (&c_data[0], &c_bitmap.downcast_ref().unwrap()[0]))
    };
    let c_const_true = || (0..*len).into_iter().map(|_| (&c_data[0], &true));
    let c_index_bitmap = || {
        c_index
            .downcast_ref()
            .unwrap()
            .iter()
            .map(|i| (&c_data[*i], &c_bitmap.downcast_ref().unwrap()[*i]))
    };
    let c_index_nobitmap = || {
        c_index
            .downcast_ref()
            .unwrap()
            .iter()
            .map(|i| (&c_data[*i], &true))
    };
    let c_noindex_bitmap = || {
        c_data
            .iter()
            .zip(c_bitmap.downcast_ref().unwrap().iter())
            .map(|(v, b)| (v, b))
    };
    let c_noindex_nobitmap = || c_data.iter().map(|v| (v, &true));

    if c_is_const {
        match c_bitmap.is_some() {
            true => f_2_sized_sized_sized(
                c1,
                c1_index,
                bitmap_update_required,
                c2,
                c2_index,
                c_const_val(),
                f,
                len,
            ),
            false => f_2_sized_sized_sized(
                c1,
                c1_index,
                bitmap_update_required,
                c2,
                c2_index,
                c_const_true(),
                f,
                len,
            ),
        }
    } else {
        match (c_index.is_some(), c_bitmap.is_some()) {
            (true, true) => f_2_sized_sized_sized(
                c1,
                c1_index,
                bitmap_update_required,
                c2,
                c2_index,
                c_index_bitmap(),
                f,
                len,
            ),
            (true, false) => f_2_sized_sized_sized(
                c1,
                c1_index,
                bitmap_update_required,
                c2,
                c2_index,
                c_index_nobitmap(),
                f,
                len,
            ),
            (false, true) => f_2_sized_sized_sized(
                c1,
                c1_index,
                bitmap_update_required,
                c2,
                c2_index,
                c_noindex_bitmap(),
                f,
                len,
            ),
            (false, false) => f_2_sized_sized_sized(
                c1,
                c1_index,
                bitmap_update_required,
                c2,
                c2_index,
                c_noindex_nobitmap(),
                f,
                len,
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////

pub fn set_3_sized_sized_sized_unroll<'a, T1, T2, T3, FBool, F>(
    c1: &'a mut ColumnWrapper,
    input: &'a [InputTypes<'a>],
    bitmap_update_required: &bool,
    f: F,
    f_bitmap: FBool,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    T3: 'static + Send + Sync,
    FBool: Fn(&bool, &bool) -> bool,
    F: Fn(&T2, &bool, &T3, &bool) -> T1,
{
    let (c2, c2_index) = match &input[0] {
        InputTypes::Ref(c, i) => (*c, *i),
        InputTypes::Owned(c, i) => (c, i),
    };

    let (c3, c3_index) = match &input[1] {
        InputTypes::Ref(c, i) => (*c, *i),
        InputTypes::Owned(c, i) => (c, i),
    };

    let len = c1.column().data_len::<T1>()?;

    let c2_len = if c2_index.is_some() {
        c2_index.downcast_ref()?.len()
    } else {
        c2.column().data_len::<T2>()?
    };

    let c3_len = if c3_index.is_some() {
        c3_index.downcast_ref()?.len()
    } else {
        c3.column().data_len::<T2>()?
    };

    let len = std::cmp::max(len, c2_len);
    let len = std::cmp::max(len, c3_len);

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

    let dummy = |_: &mut T1, _: &mut bool, _: &T2, _: &bool, _: &T3, _: &bool| {};
    let f: FType<T1, T2, T3, _, _, _> = FType::new_set(f, dummy, f_bitmap);

    f_3_sized_sized_sized(
        c1,
        &ColumnDataF::None,
        bitmap_update_required,
        c2,
        c2_index,
        c3,
        c3_index,
        f,
        &len,
    )
}
////////////////////////////////////////////////////////////////////////////

pub fn update_3_sized_sized_sized_unroll<'a, T1, T2, T3, F>(
    c1: &'a mut ColumnWrapper,
    c1_index: &ColumnDataF<usize>,
    input: &'a [InputTypes<'a>],
    bitmap_update_required: &bool,
    f: F,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    T3: 'static + Send + Sync,
    F: Fn(&mut T1, &mut bool, &T2, &bool, &T3, &bool),
{
    let (c2, c2_index) = match &input[0] {
        InputTypes::Ref(c, i) => (*c, *i),
        InputTypes::Owned(c, i) => (c, i),
    };

    let (c3, c3_index) = match &input[1] {
        InputTypes::Ref(c, i) => (*c, *i),
        InputTypes::Owned(c, i) => (c, i),
    };

    let len = c1.column().data_len::<T1>()?;

    let c2_len = if c2_index.is_some() {
        c2_index.downcast_ref()?.len()
    } else {
        c2.column().data_len::<T2>()?
    };

    let c3_len = if c3_index.is_some() {
        c3_index.downcast_ref()?.len()
    } else {
        c3.column().data_len::<T3>()?
    };

    let len = std::cmp::max(len, c2_len);
    let len = std::cmp::max(len, c3_len);

    if *bitmap_update_required && !c1.bitmap().is_some() {
        if c1.column().is_owned() {
            c1.bitmap_set(ColumnDataF::new(Vec::with_capacity(len)))
        } else {
            Err("Bitmap update is required, but the column is a reference and has no bitmap")?
        }
    };

    let dummy = |_: &T2, _: &bool, _: &T3, _: &bool| -> T1 { panic!("dummy function called") };
    let dummy_bool = |_: &bool, _: &bool| -> bool { panic!("dummy function called") };

    let f: FType<T1, T2, T3, _, _, _> = FType::new_update(dummy, f, dummy_bool);

    f_3_sized_sized_sized(
        c1,
        c1_index,
        bitmap_update_required,
        c2,
        c2_index,
        c3,
        c3_index,
        f,
        &len,
    )
}

////////////////////////////////////////////////////////////////////////////
//////////////////                                 /////////////////////////
//////////////////            2                    /////////////////////////
//////////////////    sized binary binary          /////////////////////////
////////////////////////////////////////////////////////////////////////////

fn f_1_sized_binary_binary<'a, 'i, T1, T2, T3, U2, U3, FBool, F1, F2>(
    c1: &'a mut ColumnWrapper,
    c1_index: &ColumnDataF<usize>,
    bitmap_update_required: &bool,
    c2: U2,
    c3: U3,
    f: FType<'a, T1, [u8], [u8], FBool, F1, F2>,
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
    FBool: Fn(&bool, &bool) -> bool,
    F1: Fn(&[u8], &bool, &[u8], &bool) -> T1,
    F2: Fn(&mut T1, &mut bool, &[u8], &bool, &[u8], &bool),
{
    let (c1, c1_bitmap) = c1.get_inner_mut();
    let c2_iter = c2.clone().into_iter();
    let c3_iter = c3.clone().into_iter();

    assert_eq!(c2_iter.len(), c3_iter.len());

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
                c1_data.extend(c2_iter.zip(c3_iter).map(
                    |((c2_value, c2_bitmap), (c3_value, c3_bitmap))| {
                        f_data(c2_value, c2_bitmap, c3_value, c3_bitmap)
                    },
                ));
                c1_bitmap.extend(c2.into_iter().zip(c3.into_iter()).map(
                    |((_c2_value, c2_bitmap), (_c3_value, c3_bitmap))| {
                        f_bitmap(c2_bitmap, c3_bitmap)
                    },
                ));
            }
            false => c1_data.extend(c2_iter.zip(c3_iter).map(
                |((c2_value, c2_bitmap), (c3_value, c3_bitmap))| {
                    f_data(c2_value, c2_bitmap, c3_value, c3_bitmap)
                },
            )),
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
                    c1_index.downcast_ref()?.iter().zip(c2).zip(c3).for_each(
                        |((i, (c2_value, c2_bitmap)), (c3_value, c3_bitmap))| {
                            c1_data[*i] = f_data(c2_value, c2_bitmap, c3_value, c3_bitmap);
                            c1_bitmap[*i] = f_bitmap(c2_bitmap, c3_bitmap);
                        },
                    )
                }
                (true, false) => c1_index.downcast_ref()?.iter().zip(c2).zip(c3).for_each(
                    |((i, (c2_value, c2_bitmap)), (c3_value, c3_bitmap))| {
                        c1_data[*i] = f_data(c2_value, c2_bitmap, c3_value, c3_bitmap);
                    },
                ),
                (false, true) => {
                    let c1_bitmap = c1_bitmap.downcast_mut().unwrap();
                    c1_data
                        .iter_mut()
                        .zip(c1_bitmap.iter_mut())
                        .zip(c2)
                        .zip(c3)
                        .for_each(
                            |(
                                ((c1_value, c1_bitmap), (c2_value, c2_bitmap)),
                                (c3_value, c3_bitmap),
                            )| {
                                *c1_value = f_data(c2_value, c2_bitmap, c3_value, c3_bitmap);
                                *c1_bitmap = f_bitmap(c2_bitmap, c3_bitmap);
                            },
                        )
                }
                (false, false) => c1_data.iter_mut().zip(c2).zip(c3).for_each(
                    |((c1_value, (c2_value, c2_bitmap)), (c3_value, c3_bitmap))| {
                        *c1_value = f_data(c2_value, c2_bitmap, c3_value, c3_bitmap);
                    },
                ),
            },

            FType::Update(f) => match (c1_index.is_some(), bitmap_update_required) {
                (true, true) => {
                    let c1_bitmap = c1_bitmap.downcast_mut().unwrap();
                    c1_index.downcast_ref()?.iter().zip(c2).zip(c3).for_each(
                        |((i, (c2_value, c2_bitmap)), (c3_value, c3_bitmap))| {
                            f(
                                c1_data.get_mut(*i).unwrap(),
                                c1_bitmap.get_mut(*i).unwrap(),
                                c2_value,
                                c2_bitmap,
                                c3_value,
                                c3_bitmap,
                            );
                        },
                    )
                }
                (true, false) => c1_index.downcast_ref()?.iter().zip(c2).zip(c3).for_each(
                    |((i, (c2_value, c2_bitmap)), (c3_value, c3_bitmap))| {
                        f(
                            c1_data.get_mut(*i).unwrap(),
                            &mut true,
                            c2_value,
                            c2_bitmap,
                            c3_value,
                            c3_bitmap,
                        );
                    },
                ),
                (false, true) => {
                    let c1_bitmap = c1_bitmap.downcast_mut().unwrap();
                    c1_data
                        .iter_mut()
                        .zip(c1_bitmap.iter_mut())
                        .zip(c2)
                        .zip(c3)
                        .for_each(
                            |(
                                ((c1_value, c1_bitmap), (c2_value, c2_bitmap)),
                                (c3_value, c3_bitmap),
                            )| {
                                f(
                                    c1_value, c1_bitmap, c2_value, c2_bitmap, c3_value, c3_bitmap,
                                );
                            },
                        )
                }
                (false, false) => c1_data.iter_mut().zip(c2).zip(c3).for_each(
                    |((c1_value, (c2_value, c2_bitmap)), (c3_value, c3_bitmap))| {
                        f(
                            c1_value, &mut true, c2_value, c2_bitmap, c3_value, c3_bitmap,
                        );
                    },
                ),
            },
            _ => Err("Function type should be either assign or update")?,
        }
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////

fn f_2_sized_binary_binary<'a, 'i, T1, T2, T3, U3, FBool, F1, F2>(
    c1: &'a mut ColumnWrapper,
    c1_index: &ColumnDataF<usize>,
    bitmap_update_required: &bool,
    c: &'a ColumnWrapper,
    c_index: &'a ColumnDataF<usize>,
    c3: U3,
    f: FType<'a, T1, [u8], [u8], FBool, F1, F2>,
    len: &usize,
) -> Result<(), ErrorDesc>
where
    'a: 'i,
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    T3: 'static + Send + Sync,
    U3: IntoIterator<Item = (&'i [u8], &'i bool)>,
    U3::IntoIter: ExactSizeIterator,
    U3: Clone,
    FBool: Fn(&bool, &bool) -> bool,
    F1: Fn(&[u8], &bool, &[u8], &bool) -> T1,
    F2: Fn(&mut T1, &mut bool, &[u8], &bool, &[u8], &bool),
{
    let c_is_const = c.column().is_const();
    let (c, c_bitmap) = c.get_inner_ref();
    let (c_datau8, c_start_pos, c_len, c_offset) = c.downcast_binary_ref::<T2>()?;

    let c_const_val = || {
        (0..*len).into_iter().map(|_| {
            (
                &c_datau8[c_start_pos[0] - c_offset..c_start_pos[0] - c_offset + c_len[0]],
                &c_bitmap.downcast_ref().unwrap()[0],
            )
        })
    };
    let c_const_true = || {
        (0..*len).into_iter().map(|_| {
            (
                &c_datau8[c_start_pos[0] - c_offset..c_start_pos[0] - c_offset + c_len[0]],
                &true,
            )
        })
    };
    let c_index_bitmap = || {
        c_index.downcast_ref().unwrap().iter().map(|i| {
            (
                &c_datau8[c_start_pos[*i] - c_offset..c_start_pos[*i] - c_offset + c_len[*i]],
                &c_bitmap.downcast_ref().unwrap()[*i],
            )
        })
    };
    let c_index_nobitmap = || {
        c_index.downcast_ref().unwrap().iter().map(|i| {
            (
                &c_datau8[c_start_pos[*i] - c_offset..c_start_pos[*i] - c_offset + c_len[*i]],
                &true,
            )
        })
    };
    let c_noindex_bitmap = || {
        c_start_pos
            .iter()
            .zip(c_len.iter())
            .zip(c_bitmap.downcast_ref().unwrap().iter())
            .map(|((s, l), b)| (&c_datau8[s - c_offset..s - c_offset + l], b))
    };
    let c_noindex_nobitmap = || {
        c_start_pos
            .iter()
            .zip(c_len.iter())
            .map(|(s, l)| (&c_datau8[s - c_offset..s - c_offset + l], &true))
    };

    if c_is_const {
        match c_bitmap.is_some() {
            true => f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _, _>(
                c1,
                c1_index,
                bitmap_update_required,
                c_const_val(),
                c3,
                f,
            ),
            false => f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _, _>(
                c1,
                c1_index,
                bitmap_update_required,
                c_const_true(),
                c3,
                f,
            ),
        }
    } else {
        match (c_index.is_some(), c_bitmap.is_some()) {
            (true, true) => f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _, _>(
                c1,
                c1_index,
                bitmap_update_required,
                c_index_bitmap(),
                c3,
                f,
            ),
            (true, false) => f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _, _>(
                c1,
                c1_index,
                bitmap_update_required,
                c_index_nobitmap(),
                c3,
                f,
            ),
            (false, true) => f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _, _>(
                c1,
                c1_index,
                bitmap_update_required,
                c_noindex_bitmap(),
                c3,
                f,
            ),
            (false, false) => f_1_sized_binary_binary::<T1, T2, T3, _, _, _, _, _>(
                c1,
                c1_index,
                bitmap_update_required,
                c_noindex_nobitmap(),
                c3,
                f,
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////

fn f_3_sized_binary_binary<'a, 'i, T1, T2, T3, FBool, F1, F2>(
    c1: &'a mut ColumnWrapper,
    c1_index: &ColumnDataF<usize>,
    bitmap_update_required: &bool,
    c2: &'a ColumnWrapper,
    c2_index: &'a ColumnDataF<usize>,
    c: &'a ColumnWrapper,
    c_index: &'a ColumnDataF<usize>,
    f: FType<'a, T1, [u8], [u8], FBool, F1, F2>,
    len: &usize,
) -> Result<(), ErrorDesc>
where
    'a: 'i,
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    T3: 'static + Send + Sync,
    FBool: Fn(&bool, &bool) -> bool,
    F1: Fn(&[u8], &bool, &[u8], &bool) -> T1,
    F2: Fn(&mut T1, &mut bool, &[u8], &bool, &[u8], &bool),
{
    let c_is_const = c.column().is_const();
    let (c, c_bitmap) = c.get_inner_ref();
    let (c_datau8, c_start_pos, c_len, c_offset) = c.downcast_binary_ref::<T3>()?;

    let c_const_val = || {
        (0..*len).into_iter().map(|_| {
            (
                &c_datau8[c_start_pos[0] - c_offset..c_start_pos[0] - c_offset + c_len[0]],
                &c_bitmap.downcast_ref().unwrap()[0],
            )
        })
    };
    let c_const_true = || {
        (0..*len).into_iter().map(|_| {
            (
                &c_datau8[c_start_pos[0] - c_offset..c_start_pos[0] - c_offset + c_len[0]],
                &true,
            )
        })
    };
    let c_index_bitmap = || {
        c_index.downcast_ref().unwrap().iter().map(|i| {
            (
                &c_datau8[c_start_pos[*i] - c_offset..c_start_pos[*i] - c_offset + c_len[*i]],
                &c_bitmap.downcast_ref().unwrap()[*i],
            )
        })
    };
    let c_index_nobitmap = || {
        c_index.downcast_ref().unwrap().iter().map(|i| {
            (
                &c_datau8[c_start_pos[*i] - c_offset..c_start_pos[*i] - c_offset + c_len[*i]],
                &true,
            )
        })
    };
    let c_noindex_bitmap = || {
        c_start_pos
            .iter()
            .zip(c_len.iter())
            .zip(c_bitmap.downcast_ref().unwrap().iter())
            .map(|((s, l), b)| (&c_datau8[s - c_offset..s - c_offset + l], b))
    };
    let c_noindex_nobitmap = || {
        c_start_pos
            .iter()
            .zip(c_len.iter())
            .map(|(s, l)| (&c_datau8[s - c_offset..s - c_offset + l], &true))
    };

    if c_is_const {
        match c_bitmap.is_some() {
            true => f_2_sized_binary_binary::<T1, T2, T3, _, _, _, _>(
                c1,
                c1_index,
                bitmap_update_required,
                c2,
                c2_index,
                c_const_val(),
                f,
                len,
            ),
            false => f_2_sized_binary_binary::<T1, T2, T3, _, _, _, _>(
                c1,
                c1_index,
                bitmap_update_required,
                c2,
                c2_index,
                c_const_true(),
                f,
                len,
            ),
        }
    } else {
        match (c_index.is_some(), c_bitmap.is_some()) {
            (true, true) => f_2_sized_binary_binary::<T1, T2, T3, _, _, _, _>(
                c1,
                c1_index,
                bitmap_update_required,
                c2,
                c2_index,
                c_index_bitmap(),
                f,
                len,
            ),
            (true, false) => f_2_sized_binary_binary::<T1, T2, T3, _, _, _, _>(
                c1,
                c1_index,
                bitmap_update_required,
                c2,
                c2_index,
                c_index_nobitmap(),
                f,
                len,
            ),
            (false, true) => f_2_sized_binary_binary::<T1, T2, T3, _, _, _, _>(
                c1,
                c1_index,
                bitmap_update_required,
                c2,
                c2_index,
                c_noindex_bitmap(),
                f,
                len,
            ),
            (false, false) => f_2_sized_binary_binary::<T1, T2, T3, _, _, _, _>(
                c1,
                c1_index,
                bitmap_update_required,
                c2,
                c2_index,
                c_noindex_nobitmap(),
                f,
                len,
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////

pub fn set_3_sized_binary_binary_unroll<'a, T1, T2, T3, FBool, F>(
    c1: &'a mut ColumnWrapper,
    input: &'a [InputTypes<'a>],
    bitmap_update_required: &bool,
    f: F,
    f_bitmap: FBool,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    T3: 'static + Send + Sync,
    FBool: Fn(&bool, &bool) -> bool,
    F: Fn(&[u8], &bool, &[u8], &bool) -> T1,
{
    let (c2, c2_index) = match &input[0] {
        InputTypes::Ref(c, i) => (*c, *i),
        InputTypes::Owned(c, i) => (c, i),
    };

    let (c3, c3_index) = match &input[1] {
        InputTypes::Ref(c, i) => (*c, *i),
        InputTypes::Owned(c, i) => (c, i),
    };

    let len = c1.column().data_len::<T1>()?;

    let c2_len = if c2_index.is_some() {
        c2_index.downcast_ref()?.len()
    } else {
        c2.column().data_len::<T2>()?
    };

    let c3_len = if c3_index.is_some() {
        c3_index.downcast_ref()?.len()
    } else {
        c3.column().data_len::<T2>()?
    };

    let len = std::cmp::max(len, c2_len);
    let len = std::cmp::max(len, c3_len);

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
    let dummy = |_: &mut T1, _: &mut bool, _: &[u8], _: &bool, _: &[u8], _: &bool| {};
    let f: FType<T1, [u8], [u8], _, _, _> = FType::new_set(f, dummy, f_bitmap);

    f_3_sized_binary_binary::<T1, T2, T3, _, _, _>(
        c1,
        &ColumnDataF::None,
        bitmap_update_required,
        c2,
        c2_index,
        c3,
        c3_index,
        f,
        &len,
    )
}
///////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////

pub fn update_3_sized_binary_binary_unroll<'a, T1, T2, T3, F>(
    c1: &'a mut ColumnWrapper,
    c1_index: &ColumnDataF<usize>,
    input: &'a [InputTypes<'a>],
    bitmap_update_required: &bool,
    f: F,
) -> Result<(), ErrorDesc>
where
    T1: 'static + Send + Sync,
    T2: 'static + Send + Sync,
    T3: 'static + Send + Sync,
    F: Fn(&mut T1, &mut bool, &[u8], &bool, &[u8], &bool),
{
    let (c2, c2_index) = match &input[0] {
        InputTypes::Ref(c, i) => (*c, *i),
        InputTypes::Owned(c, i) => (c, i),
    };

    let (c3, c3_index) = match &input[1] {
        InputTypes::Ref(c, i) => (*c, *i),
        InputTypes::Owned(c, i) => (c, i),
    };

    let len = c1.column().data_len::<T1>()?;

    let c2_len = if c2_index.is_some() {
        c2_index.downcast_ref()?.len()
    } else {
        c2.column().data_len::<T2>()?
    };

    let c3_len = if c3_index.is_some() {
        c3_index.downcast_ref()?.len()
    } else {
        c3.column().data_len::<T3>()?
    };

    let len = std::cmp::max(len, c2_len);
    let len = std::cmp::max(len, c3_len);

    if *bitmap_update_required && !c1.bitmap().is_some() {
        if c1.column().is_owned() {
            c1.bitmap_set(ColumnDataF::new(Vec::with_capacity(len)))
        } else {
            Err("Bitmap update is required, but the column is a reference and has no bitmap")?
        }
    };

    let dummy = |_: &[u8], _: &bool, _: &[u8], _: &bool| -> T1 { panic!("dummy function called") };
    let dummy_bool = |_: &bool, _: &bool| -> bool { panic!("dummy function called") };

    let f: FType<T1, [u8], [u8], _, _, _> = FType::new_update(dummy, f, dummy_bool);

    f_3_sized_binary_binary::<T1, T2, T3, _, _, _>(
        c1,
        c1_index,
        bitmap_update_required,
        c2,
        c2_index,
        c3,
        c3_index,
        f,
        &len,
    )
}

////////////////////////////////////////////////////////////////////////////
//////////////////                                 /////////////////////////
//////////////////            3                    /////////////////////////
//////////////////           END                   /////////////////////////
////////////////////////////////////////////////////////////////////////////
