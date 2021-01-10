pub(crate) enum FType2<'a, O1, O2, F1, F2>
where
    O1: ?Sized,
    O2: ?Sized,
    F1: Fn(&O2, &bool) -> (bool, O1),
    F2: Fn(&'a mut O1, &'a mut bool, (&O2, &bool)),
{
    Assign(F1),
    Update(F2),
    _Phantom((std::marker::PhantomData<&'a u8>, &'a O1, &'a O2)),
}

impl<'a, O1, O2, F1, F2> FType2<'a, O1, O2, F1, F2>
where
    O1: ?Sized,
    O2: ?Sized,
    F1: Fn(&O2, &bool) -> (bool, O1),
    F2: Fn(&'a mut O1, &'a mut bool, (&O2, &bool)),
{
    pub(crate) fn new_assign(f: F1, _: F2) -> Self {
        Self::Assign(f)
    }
    pub(crate) fn new_update(_: F1, f: F2) -> Self {
        Self::Update(f)
    }
}

pub(crate) enum FType3<'a, O1, O2, O3, F1, F2>
where
    O1: ?Sized,
    O2: ?Sized,
    O3: ?Sized,
    F1: Fn(&O2, &bool, &O3, &bool) -> (bool, O1),
    F2: Fn(&'a mut O1, &'a mut bool, (&O2, &bool, &O3, &bool)),
{
    Assign(F1),
    Update(F2),
    _Phantom((std::marker::PhantomData<&'a u8>, &'a O1, &'a O2, &'a O3)),
}

impl<'a, O1, O2, O3, F1, F2> FType3<'a, O1, O2, O3, F1, F2>
where
    O1: ?Sized,
    O2: ?Sized,
    O3: ?Sized,
    F1: Fn(&O2, &bool, &O3, &bool) -> (bool, O1),
    F2: Fn(&'a mut O1, &'a mut bool, (&O2, &bool, &O3, &bool)),
{
    pub(crate) fn new_assign(f: F1, _: F2) -> Self {
        Self::Assign(f)
    }
    pub(crate) fn new_update(_: F1, f: F2) -> Self {
        Self::Update(f)
    }
}
