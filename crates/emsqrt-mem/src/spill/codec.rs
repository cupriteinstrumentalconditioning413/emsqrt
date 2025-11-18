//! Compression facade for spill segments (feature-gated).
//!
//! Keep this tiny and synchronous. We only support `None`, `Zstd`, `Lz4`.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Codec {
    None = 0,
    Zstd = 1,
    Lz4 = 2,
}

impl Codec {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(Codec::None),
            1 => Ok(Codec::Zstd),
            2 => Ok(Codec::Lz4),
            _ => Err(Error::CodecUnsupported("unknown")),
        }
    }
}

pub fn compress(codec: Codec, input: &[u8]) -> Result<Vec<u8>> {
    match codec {
        Codec::None => Ok(input.to_vec()),
        Codec::Zstd => {
            #[cfg(feature = "zstd")]
            {
                let lvl = 3; // TODO: tune / make configurable
                let mut out = Vec::new();
                zstd::stream::copy_encode(input, &mut out, lvl)
                    .map_err(|e| Error::Codec(format!("zstd: {e}")))?;
                Ok(out)
            }
            #[cfg(not(feature = "zstd"))]
            {
                Err(Error::CodecUnsupported("zstd"))
            }
        }
        Codec::Lz4 => {
            #[cfg(feature = "lz4")]
            {
                Ok(lz4_flex::compress_prepend_size(input))
            }
            #[cfg(not(feature = "lz4"))]
            {
                Err(Error::CodecUnsupported("lz4"))
            }
        }
    }
}

pub fn decompress(codec: Codec, input: &[u8]) -> Result<Vec<u8>> {
    match codec {
        Codec::None => Ok(input.to_vec()),
        Codec::Zstd => {
            #[cfg(feature = "zstd")]
            {
                let mut out = Vec::new();
                zstd::stream::copy_decode(input, &mut out)
                    .map_err(|e| Error::Codec(format!("zstd: {e}")))?;
                Ok(out)
            }
            #[cfg(not(feature = "zstd"))]
            {
                Err(Error::CodecUnsupported("zstd"))
            }
        }
        Codec::Lz4 => {
            #[cfg(feature = "lz4")]
            {
                lz4_flex::decompress_size_prepended(input)
                    .map_err(|e| Error::Codec(format!("lz4: {e}")))
            }
            #[cfg(not(feature = "lz4"))]
            {
                Err(Error::CodecUnsupported("lz4"))
            }
        }
    }
}
