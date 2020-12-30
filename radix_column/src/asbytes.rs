use std::mem::MaybeUninit;

pub trait AsBytes {
    fn bytelen(&self) -> usize;
    //SAFETY: The type T should contain any references
    //and therefore it should be able to simply copy it as bytes
    unsafe fn copy(&self, data: &mut [MaybeUninit<u8>]);
    fn as_bytes(&self) -> &[u8];
    fn from_bytes(data: &[u8]) -> Self;
}

impl AsBytes for String {
    fn bytelen(&self) -> usize {
        self.as_bytes().len()
    }
    unsafe fn copy(&self, data: &mut [MaybeUninit<u8>]) {
        let len = data.len();
        assert_eq!(len, self.bytelen());
        std::intrinsics::copy_nonoverlapping(
            self.as_bytes().as_ptr(),
            data.as_mut_ptr() as *mut u8,
            len,
        );
    }
    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
    fn from_bytes(data: &[u8]) -> Self {
        String::from_utf8(data.to_vec()).unwrap()
    }
}
