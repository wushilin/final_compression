pub mod liblz4;
pub mod liblzo;
use std::io::Write;
use std::io::Read;
use std::error::Error;
use std::collections::HashMap;
use core::str::FromStr;
use bzip2::write::BzEncoder;
use bzip2::read::BzDecoder;
use zstd::Encoder;
use urlencoding::decode;
use flate2::write::{GzEncoder, ZlibEncoder, DeflateEncoder};
use flate2::read::{GzDecoder, ZlibDecoder, DeflateDecoder};
use xz2::write::XzEncoder;
use xz2::read::XzDecoder;
/// final_compression consolidates almost all popular compression algorithms together
/// and provide a unified Read/Write interface to support compression and decompression
/// of stream data.
/// 
/// You can use this library to operate on the following Stream Compression types:
/// - Zstd
/// - Snappy
/// - Gzip
/// - Zlib
/// - Deflate
/// - Bzip2
/// - LZ4
/// - XZ

/// Represent the intended compression type
#[derive(Debug, Clone, Copy)]
pub enum CompressionType {
    /// No compression - pass through
    None,
    /// zstd compression type. 
    /// Supported parameter: level=u32 (1~22. 1-fastest, 22-highest, Default 3)
    /// Example of parameter: "level=3"
    Zstd,
    /// snappy compression type.
    /// Supported parameter: None
    /// Example of parameter: "". All parameters are ignored
    Snappy,
    /// gzip compression type.
    /// Supported parameter: level=u32 (1~9 1-fastest, 9-highest, default 3)
    /// Example of parameter: "level=3"
    Gzip,
    /// zlib compression type.
    /// Supported parameter: level=u32 (0~9 0-fastest, 9-highest, default 3)
    /// Example of parameter: "level=3"
    Zlib,
    /// deflate compression type.
    /// Supported parameter: level=u32 (0~9 0-fastest, 9-highest, default 3)
    /// Example of parameter: "level=3"
    Deflate,
    /// bz2 compression type.
    /// Supported parameter: level=u32 (1~9 1-fastest, 9-highest, default 3)
    /// Example of parameter: "level=3"
    Bzip2,
    /// lz4 compression type.
    /// Supported parameter: 
    ///     level=u32 (0~16 1-fastest, 16-highest, default 1)
    ///     block_mode=linked (linked|independent, default linked)
    /// Example of parameter: "level=1;block_mode=linked"
    LZ4,
    /// xz compression type.
    /// Supported parameter: level=u32 (0~9 0-fastest, 9-highest, default 6)
    /// Example of parameter: "level=3"
    XZ,
}

impl From<&str> for CompressionType {
    fn from(ctype: &str) -> Self {
        match ctype {
            "zstd" | "ZSTD" | "zst" | "ZST" => CompressionType::Zstd,
            "gzip" | "GZIP" | "gz" | "GZ" => CompressionType::Gzip,
            "lz4" | "LZ4" => CompressionType::LZ4,
            "snappy" | "SNAPPY" => CompressionType::Snappy,
            "xz" | "XZ" => CompressionType::XZ,
            "zlib" | "ZLIB" => CompressionType::Zlib,
            "bzip2" | "BZIP2" | "bz2" | "BZ2" => CompressionType::Bzip2,
            "deflate" | "DEFLATE" => CompressionType::Deflate,
            _ => {
                panic!("Unknown compression type")
            }
        }
    }
}
/// Represents parameter set for Compression
/// The `ParamSet` can be obtained from String and &str
/// ParamSet string expression is "key1=value1;key2=value2;key3=value3" format
/// Internally it is a HashMap<String,, String>
/// 
/// Typical paramset used "level=3" (set compression level). See each compression algorithm for supported parameters
/// 
/// You can use "" as ParamSet and it won't contain any actual parameter
pub struct ParamSet {
    map: HashMap<String, String>
}

impl ParamSet {
    /// Read parameter identified by `key` as `&str`. If not set, use the `default_value`.
    pub fn get_string<'a, 'b>(&'a self, key:&'b str, default_value:&'b str) ->&'b str 
        where 'a:'b
    {
        let result = self.map.get(key);
        if result.is_none() {
            return default_value;
        }
        return result.unwrap();
    }

    /// Read parameter identified by `key` as bool. If not set, use the `default_value`.
    /// 
    /// only `true` (case insensitive) is considered as true. Other values are `false`.
    pub fn get_bool(&self, key:&str, default_value: bool) -> bool {
        let str_value = self.get_string(key, "");
        if str_value == "" {
            return default_value;
        }
        return str_value.to_ascii_lowercase() == "TRUE" || str_value.to_ascii_lowercase() == "false";
    }

    /// Read parameter identified by `key` as T (where T:FromStr). If not set, use `default_value`.
    /// 
    /// Typical `FromStr` includes all numbers (i32, usize, etc), or `IpAddress`, or any other supported type
    /// that implements `FromStr` trait.
    pub fn get_parse<T:FromStr>(&self, key:&str, default_value: T) -> T {
        let str_value = self.get_string(key, "");
        if str_value == "" {
            return default_value;
        }
        let result = str_value.parse();
        if result.is_err() {
            return default_value;
        }
        let result =  result.ok().unwrap();
        return result;
    }

    fn url_decode(input:&str) -> String {
        let decoded = decode(input).expect("UTF-8");
        return decoded.to_string();
    }
}

impl From<&str> for ParamSet {
    fn from(what:&str) -> Self {
        return what.to_string().into();
    }
}

/// Load ParamSet from String
impl From<String> for ParamSet {
    /// `what` must be "key=value;key1=value1" format. Empty tokens (e.g. "key=value;;;") are skipped.
    /// If you need to specify values that may contain special characters (e.g. include`;` or `=`), you can use
    /// '%%:' to prefix your value, and the value should be url encoded. 
    /// For example format of `key=%%:%3B%3B%3B;key2=%%:%3B%3B%3B" => key = ";;;", key2 = ";;;"
    /// 
    /// What if your key should be "%%:123"? 
    /// 
    /// No worries, "%%:123" => "%%:%25%25%3A123"
    fn from(what: String) -> Self {
        let tokens = what.split(";").filter(|x| x.trim().len() > 0);
        let mut map = HashMap::<String, String>::new();
        for next in tokens {
            let equal_pos = next.find("=");
            if equal_pos.is_none() {
                continue;
            }
            let equal_pos = equal_pos.unwrap();

            let first = next[0..equal_pos].trim();
            let mut second = next[equal_pos + 1..].trim();
            let actual_value:String;
            if second.starts_with("%%:") {
                second = &second[3..];
                actual_value = ParamSet::url_decode(second);
            } else {
                actual_value = second.into();
            }

            map.insert(first.into(), actual_value);
        }

        return ParamSet{map};
    }
}

/// Create a compressing writer to wrap another writer.
/// 
/// The being wrapped writer should be a raw writer, and the wrapped writer is the compressing writer.
/// 
/// All data written to the wrapped writer are compressed and then written to the actual writer.
/// 
/// 
/// Example:
/// ```
/// use final_compression::{compressed_writer, CompressionType};
/// let out = std::fs::File::create("test.out.txt.doc.gz").unwrap();
/// let mut gz_out = crate::final_compression::compressed_writer(Box::new(out), CompressionType::Gzip, "level=3").unwrap();
/// gz_out.write("hello world".as_bytes()).unwrap();
/// drop(gz_out);
/// // Now out.txt.gz should be the compressed version of `hello world`.
/// // You can use `gunzip out.txt.gz` to verify the content.
/// ```
pub fn compressed_writer<T:Into<ParamSet>>(
    out:Box<dyn Write>, 
    compression_type:CompressionType, 
    option:T) -> Result<Box<dyn Write>, Box<dyn Error>> {
    let param_set:ParamSet = option.into();
    match compression_type {
        CompressionType::Zstd => {
            let level = param_set.get_parse("level", 3);
            let write = Encoder::new(out, 
                level)?;
            let autof = write.auto_finish();
            return Ok(Box::new(autof));

        },
        CompressionType::Snappy => {
            let result_w = snap::write::FrameEncoder::new(out);
            return Ok(Box::new(result_w));
        },
        CompressionType::Gzip => {
            let level = param_set.get_parse("level", 3);
            let encoder = GzEncoder::new(out, flate2::Compression::new(level));
            return Ok(Box::new(encoder));
        },
        CompressionType::Zlib => {
            let level = param_set.get_parse("level", 3);
            let encoder = ZlibEncoder::new(out, flate2::Compression::new(level));
            return Ok(Box::new(encoder));
        }, 
        CompressionType::Deflate => {
            let level = param_set.get_parse("level", 3);
            let encoder = DeflateEncoder::new(out, flate2::Compression::new(level));
            return Ok(Box::new(encoder));
        },
        CompressionType::Bzip2 => {
            let level = param_set.get_parse("level", 3);
            let encoder = BzEncoder::new(out, bzip2::Compression::new(level));
            return Ok(Box::new(encoder));
        },
        CompressionType::LZ4 => {
            let block_mode = param_set.get_string("block_mode", "linked");
            let level = param_set.get_parse("level", 1);
            let mut encoder = lz4::EncoderBuilder::new();
            encoder.auto_flush(true);
            match block_mode {
                "independent" => {
                    encoder.block_mode(lz4::BlockMode::Independent);
                },
                _ => {
                    encoder.block_mode(lz4::BlockMode::Linked);
                }
            }
            encoder.checksum(lz4::ContentChecksum::ChecksumEnabled);
            encoder.level(level);
            let lz4enc = encoder.build(out).unwrap();
            let lz4w = liblz4::Lz4Wrapper::new(lz4enc);
            return Ok(Box::new(lz4w));
        },
        CompressionType::XZ => {
            let level = param_set.get_parse("level", 6);
            let w = XzEncoder::new(out, level);
            return Ok(Box::new(w));
        },
        CompressionType::None => {
            return Ok(Box::new(out));
        }
    }
}


/// Create a decompress reader to wrap another reader.
/// 
/// The being wrapped reader should be a compressed datastream, and the wrapped reader is the decompressed stream.
/// 
/// All data read by the raw reader will be decompressed by the decompressor before application consumes it.
/// 
/// Example:
/// ```
/// use final_compression::{decompressed_reader, CompressionType};
/// let input = std::fs::File::open("test.out.txt.doc.gz").unwrap();
/// let mut gz_in = crate::final_compression::decompressed_reader(Box::new(input), CompressionType::Gzip).unwrap();
/// let mut data = String::new();
/// gz_in.read_to_string(&mut data);
/// drop(gz_in);
/// // Data should be "hello world" (we have written that file in the other test)
/// ```
pub fn decompressed_reader(src:Box<dyn Read>, compression_type:CompressionType)->Result<Box<dyn Read>, Box<dyn Error>> {
    match compression_type {
        CompressionType::Zstd => {
            let read = zstd::Decoder::new(src)?;
            return Ok(Box::new(read));
        },
        CompressionType::Snappy => {
            let result_r = snap::read::FrameDecoder::new(src);
            return Ok(Box::new(result_r));
        },
        CompressionType::Gzip => {
            let result_r = GzDecoder::new(src);
            return Ok(Box::new(result_r));
        },
        CompressionType::Zlib => {
            let result_r = ZlibDecoder::new(src);
            return Ok(Box::new(result_r));
        }, 
        CompressionType::Deflate => {
            let result_r = DeflateDecoder::new(src);
            return Ok(Box::new(result_r));
        },
        CompressionType::Bzip2 => {
            let result_r = BzDecoder::new(src);
            return Ok(Box::new(result_r));
        },
        CompressionType::LZ4 => {
            let decoder = lz4::Decoder::new(src)?;
            return Ok(Box::new(decoder));
        },
        CompressionType::XZ => {
            let result_r = XzDecoder::new(src);
            return Ok(Box::new(result_r));
        },
        CompressionType::None => {
            return Ok(Box::new(src));
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_compressed_writer_zstd() {
        let file_name = "test.out.txt.zstd";
        let test_data = "hello, world, hello, world, hello, world, hello, world";
        let ct = CompressionType::Zstd;
        let options = "level=0";
        test(file_name, ct, test_data, options);
        //////////////////////////////
    }

    pub fn test(file_name:&str, ct:CompressionType, test_data: &str, options:&str) {
        let out = std::fs::File::create(file_name).unwrap();
        let mut wrapper = compressed_writer(Box::new(out), ct, options).unwrap();
        let written = wrapper.write(test_data.as_bytes()).unwrap();
        drop(wrapper);

        let input = std::fs::File::open(file_name).unwrap();
        let mut wrapper = decompressed_reader(Box::new(input), ct).unwrap();
        let mut data = String::new();
        let rr = wrapper.read_to_string(&mut data).unwrap();
        assert_eq!(rr, test_data.as_bytes().len());
        assert_eq!(written, rr);
        assert_eq!(test_data, &data);
    }
    #[test]
    pub fn test_compressed_writer_snappy() {
        let file_name = "test.out.txt.snappy";
        let test_data = "hello, world, hello, world, hello, world, hello, world";
        let ct = CompressionType::Snappy;
        let options = "level=0";
        test(file_name, ct, test_data, options);
    }


    #[test]
    pub fn test_compressed_writer_gzip() {
        let file_name = "test.out.txt.gz";
        let test_data = "hello, world, hello, world, hello, world, hello, world";
        let ct = CompressionType::Gzip;
        let options = "level=0";
        test(file_name, ct, test_data, options);
    }

    #[test]
    pub fn test_compressed_writer_bz2() {
        let file_name = "test.out.txt.bz2";
        let test_data = "hello, world, hello, world, hello, world, hello, world";
        let ct = CompressionType::Bzip2;
        let options = "level=3";
        test(file_name, ct, test_data, options);
    }
   
    #[test]
    pub fn test_compressed_writer_lz4() {
        let file_name = "test.out.txt.lz4";
        let test_data = "hello, world, hello, world, hello, world, hello, world";
        let ct = CompressionType::LZ4;
        let options = "level=3";
        test(file_name, ct, test_data, options);
    }

    #[test]
    pub fn test_compressed_writer_xz() {
        let file_name = "test.out.txt.xz";
        let test_data = "hello, world, hello, world, hello, world, hello, world";
        let ct = CompressionType::XZ;
        let options = "level=3";
        test(file_name, ct, test_data, options);
    }
}
