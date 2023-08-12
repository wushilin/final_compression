use rust_lzo::{LZOContext, LZOError};
use std::io::{Write, ErrorKind};

pub struct LZOWrapperW {
    buffer: Vec<u8>,
    context: LZOContext,
    writer: Box<dyn Write>
}

impl LZOWrapperW {
    pub fn new(w:Box<dyn Write>) -> LZOWrapperW {
        LZOWrapperW { 
            buffer: Vec::with_capacity(8192), 
            context: LZOContext::new(), 
            writer: w 
        }
    }
}

impl Write for LZOWrapperW {
    fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        self.buffer.clear();
        let cr = self.context.compress(data, &mut self.buffer);
        match cr {
            LZOError::OK => {
                // OK
                let written = self.buffer.len();
                let to_write = &self.buffer[0..written];
                return self.writer.write(to_write);
                //return Ok(self.buffer.len());
            },
            LZOError::NOT_COMPRESSIBLE => {
                return self.writer.write(data);
            },
            LZOError::OUTPUT_OVERRUN => {
                self.buffer.resize(self.buffer.capacity() * 2, 0u8);
                return self.write(data);
            },
            _ => {
                return Err(std::io::Error::new(ErrorKind::InvalidData, "LZO Other error code"));
            }
        }
    }
    fn flush(&mut self) -> Result<(), std::io::Error> {
        return self.writer.flush();
    }
}

impl Drop for LZOWrapperW {
    fn drop(&mut self) {
        
    }
}