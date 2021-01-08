use crate::{InsertColumn, UpdateColumn};

pub enum IndexedMutColumn<'a, T> {
    Insert(InsertColumn<'a, T>),
    Update(UpdateColumn<'a, T>),
}
