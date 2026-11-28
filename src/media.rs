//! Bounded 8 KiB media container/brand detection, RFC 8089 file URIs, and opt-in image scaling.

mod compress;
mod detect;
mod uri;

#[cfg(test)]
mod tests;

pub use compress::{CompressFailure, compress_image, encode_base64, is_compressible_image};
pub use detect::{DetectedMedia, MediaKind, SNIFF, detect_inline, detect_path, extension_for_mime};
pub use uri::file_uri;
