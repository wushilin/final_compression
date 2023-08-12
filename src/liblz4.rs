use std::io::Write;

pub struct Lz4Wrapper {
    src: Option<lz4::Encoder<Box<dyn Write>>>
}

impl Lz4Wrapper {
    pub fn new(enc:lz4::Encoder<Box<dyn Write>>) -> Lz4Wrapper {
        Lz4Wrapper {
            src: Some(enc)
        }
    }
}
impl Write for Lz4Wrapper {
    fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        return self.src.as_mut().unwrap().write(data);
    }

    fn flush(&mut self) ->Result<(), std::io::Error>{
        return self.src.as_mut().unwrap().flush();
    }
}
impl Drop for Lz4Wrapper {
    fn drop(&mut self) {
        let src = self.src.take().unwrap();
        let mut w = src.finish();
        let _ = w.0.flush();
    }
}