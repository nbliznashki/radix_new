use std::{
    any::{Any, TypeId},
    collections::{HashMap, VecDeque},
    hash::Hash,
    hash::Hasher,
};

pub struct NullableValue<T> {
    pub value: T,
    pub bitmap: bool,
}

impl<T: PartialEq> PartialEq for NullableValue<T> {
    fn eq(&self, other: &Self) -> bool {
        (!self.bitmap && !other.bitmap) | (self.value == other.value)
    }
}
impl<T: Eq> Eq for NullableValue<T> {}

impl<T: Hash> Hash for NullableValue<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bitmap.hash(state);
        if self.bitmap {
            self.value.hash(state);
        }
    }
}

pub struct HashMapBuffer {
    stored: VecDeque<(Box<dyn Any>, TypeId)>,
}

impl<'a> HashMapBuffer {
    pub fn new() -> Self {
        Self {
            stored: VecDeque::new(),
        }
    }
    pub fn push<T: 'static>(
        &mut self,
        mut h: Box<HashMap<(usize, NullableValue<T>), usize, ahash::RandomState>>,
    ) {
        h.clear();
        self.stored
            .push_back((Box::new(h) as Box<dyn Any + 'static>, TypeId::of::<T>()));
    }
    pub fn pop<T: 'static>(
        &mut self,
    ) -> Box<HashMap<(usize, NullableValue<T>), usize, ahash::RandomState>> {
        let item_type_id = TypeId::of::<T>();
        let pos = self.stored.iter().position(|c| c.1 == item_type_id);
        match pos {
            Some(i) => self
                .stored
                .swap_remove_front(i)
                .unwrap()
                .0
                .downcast::<HashMap<(usize, NullableValue<T>), usize, ahash::RandomState>>()
                .unwrap(),
            None => Box::new(HashMap::<
                (usize, NullableValue<T>),
                usize,
                ahash::RandomState,
            >::with_capacity_and_hasher(
                100, ahash::RandomState::default()
            )),
        }
    }
}
