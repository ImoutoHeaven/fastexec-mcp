//! Bounded 8 KiB media container/brand detection, RFC 8089 file URIs, and opt-in image scaling.

use crate::lease::Lease;
use base64::Engine;
use image::metadata::Orientation;
use image::{DynamicImage, ImageDecoder, ImageFormat, ImageReader, codecs::jpeg::JpegEncoder};
use rmcp::model::{ContentBlock, ResourceContents};
use std::io::Cursor;
use std::path::Path;

/// Bytes inspected at the head of a file to classify media containers and brands.
pub const SNIFF: usize = 8 * 1024;

/// Longest side an image is scaled down to when `FASTEXEC_IMAGE_COMPRESS=true`.
const MAX_DISPLAY_DIMENSION: u32 = 2000;

/// Base64 payload cap when `FASTEXEC_IMAGE_COMPRESS=true` (4.5 MiB).
const MAX_COMPRESSED_BASE64: usize = 4_718_592;

/// Maximum single-axis pixel dimension allowed before decoding under `FASTEXEC_IMAGE_COMPRESS=true`.
pub const MAX_DECODE_DIMENSION: u32 = 32_768;

/// Maximum total pixel count (64M pixels) allowed before decoding under `FASTEXEC_IMAGE_COMPRESS=true`.
pub const MAX_DECODE_PIXELS: u64 = 64_000_000;

/// Maximum decoder buffer allocation allowed under `FASTEXEC_IMAGE_COMPRESS=true`.
const MAX_DECODE_ALLOC: u64 = 256 << 20;

const QUALITIES: [u8; 5] = [80, 85, 70, 55, 40];
const PNG_SIGNATURE: [u8; 8] = [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MediaKind {
    Document,
    Image,
    Audio,
    Video,
}

impl MediaKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Document => "document",
            Self::Image => "image",
            Self::Audio => "audio",
            Self::Video => "video",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DetectedMedia {
    pub kind: MediaKind,
    pub mime: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Hint<'a> {
    None,
    Ext(&'a str),
    Mime(&'a str),
}

/// Classifies `prefix` (bounded to 8 KiB) using `path`'s extension only as a controlled container hint.
pub fn detect_path(prefix: &[u8], path: Option<&Path>) -> Option<DetectedMedia> {
    let ext = path
        .and_then(Path::extension)
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase());
    let hint = match ext.as_deref() {
        Some(ext) if !ext.is_empty() => Hint::Ext(ext),
        _ => Hint::None,
    };
    detect_with_hint(prefix, hint)
}

/// Classifies inline pathless bytes, allowing `declared_mime` as a controlled container hint once
/// structural container evidence holds, and rejecting conflicting or unsubstantiated MIME claims.
pub fn detect_inline(prefix: &[u8], declared_mime: Option<&str>) -> Result<DetectedMedia, String> {
    let normalized = declared_mime
        .map(normalize_mime)
        .filter(|mime| !mime.is_empty());
    let hint = match normalized.as_deref() {
        Some(mime) => Hint::Mime(mime),
        None => Hint::None,
    };
    let Some(detected) = detect_with_hint(prefix, hint) else {
        return match normalized {
            Some(decl) => Err(format!(
                "Media bytes do not match supported container structure for declared MIME {decl}."
            )),
            None => Err("Media bytes do not match any supported media format.".into()),
        };
    };
    if let Some(decl) = normalized.as_deref()
        && !mime_compatible(decl, detected.mime)
    {
        return Err(format!(
            "Declared MIME {decl} conflicts with detected {} ({}).",
            detected.mime,
            detected.kind.as_str()
        ));
    }
    Ok(detected)
}

fn normalize_mime(raw: &str) -> String {
    raw.split(';')
        .next()
        .unwrap_or(raw)
        .trim()
        .to_ascii_lowercase()
}

fn mime_compatible(declared: &str, detected: &str) -> bool {
    if declared == detected {
        return true;
    }
    matches!(
        (declared, detected),
        ("image/jpg", "image/jpeg")
            | ("audio/wave" | "audio/x-wav", "audio/wav")
            | ("audio/x-flac", "audio/flac")
            | ("audio/x-aiff", "audio/aiff")
            | ("audio/x-m4a" | "audio/m4a", "audio/mp4")
            | ("video/x-msvideo", "video/avi")
    )
}

pub fn extension_for_mime(mime: &str) -> &'static str {
    match mime {
        "application/pdf" => "pdf",
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "image/bmp" => "bmp",
        "image/tiff" => "tiff",
        "image/heic" => "heic",
        "image/heif" => "heif",
        "audio/wav" => "wav",
        "audio/flac" => "flac",
        "audio/aiff" => "aiff",
        "audio/ogg" => "ogg",
        "audio/mpeg" => "mp3",
        "audio/aac" => "aac",
        "audio/mp4" => "m4a",
        "video/mp4" => "mp4",
        "video/quicktime" => "mov",
        "video/3gpp" => "3gp",
        "video/webm" => "webm",
        "video/avi" => "avi",
        _ => "bin",
    }
}

pub fn content_block(
    kind: MediaKind,
    mime: &str,
    encoded_base64: String,
    path: &Path,
) -> ContentBlock {
    match kind {
        MediaKind::Image => ContentBlock::image(encoded_base64, mime.to_owned()),
        MediaKind::Audio => ContentBlock::audio(encoded_base64, mime.to_owned()),
        MediaKind::Document | MediaKind::Video => ContentBlock::resource(
            ResourceContents::blob(encoded_base64, file_uri(path)).with_mime_type(mime.to_owned()),
        ),
    }
}

fn detect_with_hint(prefix: &[u8], hint: Hint<'_>) -> Option<DetectedMedia> {
    let buf = &prefix[..prefix.len().min(SNIFF)];
    let total_known = prefix.len() < SNIFF;
    let ftyp = parse_ftyp(buf, total_known);

    // 1. document
    if is_pdf(buf) {
        return Some(DetectedMedia {
            kind: MediaKind::Document,
            mime: "application/pdf",
        });
    }

    // 2. image
    if let Some(mime) = detect_image(buf, ftyp.as_ref()) {
        return Some(DetectedMedia {
            kind: MediaKind::Image,
            mime,
        });
    }

    // 3. audio
    if let Some(mime) = detect_audio(buf, ftyp.as_ref(), hint) {
        return Some(DetectedMedia {
            kind: MediaKind::Audio,
            mime,
        });
    }

    // 4. video
    if let Some(mime) = detect_video(buf, ftyp.as_ref(), hint) {
        return Some(DetectedMedia {
            kind: MediaKind::Video,
            mime,
        });
    }

    None
}

fn is_pdf(buf: &[u8]) -> bool {
    if buf.len() < 10 {
        return false;
    }
    let header_ok = (buf.starts_with(b"%PDF-1.") || buf.starts_with(b"%PDF-2."))
        && buf[7].is_ascii_digit()
        && matches!(buf[8], b'\r' | b'\n');
    if !header_ok {
        return false;
    }
    let mut after_header = 9;
    if buf[8] == b'\r' && buf.get(9) == Some(&b'\n') {
        after_header = 10;
    }
    let rest = &buf[after_header..];
    has_pdf_binary_comment(rest)
        || rest.windows(2).any(|w| w == b"<<")
        || has_pdf_indirect_obj(rest)
        || has_pdf_line_keyword(rest, b"xref")
        || has_pdf_line_keyword(rest, b"startxref")
}

fn has_pdf_binary_comment(rest: &[u8]) -> bool {
    let mut i = 0;
    while i < rest.len() {
        let at_line_start = i == 0 || matches!(rest[i - 1], b'\r' | b'\n');
        if at_line_start && rest[i] == b'%' {
            let mut j = i + 1;
            while j < rest.len() && !matches!(rest[j], b'\r' | b'\n') {
                if rest[j] >= 0x80 {
                    return true;
                }
                j += 1;
            }
            i = j;
        } else {
            i += 1;
        }
    }
    false
}

fn is_pdf_ws(b: u8) -> bool {
    matches!(b, 0x00 | 0x09 | 0x0A | 0x0C | 0x0D | 0x20)
}

fn is_pdf_delim_or_ws(b: u8) -> bool {
    is_pdf_ws(b)
        || matches!(
            b,
            b'(' | b')' | b'<' | b'>' | b'[' | b']' | b'{' | b'}' | b'/' | b'%'
        )
}

/// Matches `<pos_int><ws><non_neg_int><ws>obj<delim|ws>`, skipping `%` comments and rewinding to
/// the second integer when a pair of numbers is not followed by `obj`.
fn has_pdf_indirect_obj(rest: &[u8]) -> bool {
    let mut i = 0;
    while i < rest.len() {
        if rest[i] == b'%' {
            i += 1;
            while i < rest.len() && !matches!(rest[i], b'\r' | b'\n') {
                i += 1;
            }
            continue;
        }
        if (i == 0 || is_pdf_delim_or_ws(rest[i - 1])) && rest[i].is_ascii_digit() {
            let first_start = i;
            while i < rest.len() && rest[i].is_ascii_digit() {
                i += 1;
            }
            if !rest[first_start..i].iter().any(|&b| b != b'0') {
                continue;
            }
            let ws1 = i;
            while i < rest.len() && is_pdf_ws(rest[i]) {
                i += 1;
            }
            if i == ws1 || i >= rest.len() || !rest[i].is_ascii_digit() {
                continue;
            }
            let second_start = i;
            while i < rest.len() && rest[i].is_ascii_digit() {
                i += 1;
            }
            let ws2 = i;
            while i < rest.len() && is_pdf_ws(rest[i]) {
                i += 1;
            }
            if i > ws2
                && rest[i..].starts_with(b"obj")
                && rest.get(i + 3).is_none_or(|&next| is_pdf_delim_or_ws(next))
            {
                return true;
            }
            i = second_start;
        } else {
            i += 1;
        }
    }
    false
}

fn has_pdf_line_keyword(rest: &[u8], kw: &[u8]) -> bool {
    let mut i = 0;
    while i + kw.len() <= rest.len() {
        let line_start = i == 0 || matches!(rest[i - 1], b'\r' | b'\n');
        if line_start
            && &rest[i..i + kw.len()] == kw
            && rest
                .get(i + kw.len())
                .is_none_or(|&b| matches!(b, b'\r' | b'\n' | b' ' | b'\t'))
        {
            return true;
        }
        i += 1;
    }
    false
}

struct Ftyp<'a> {
    major: &'a [u8; 4],
    compatible: &'a [u8],
}

impl Ftyp<'_> {
    fn brands(&self) -> impl Iterator<Item = &[u8; 4]> {
        std::iter::once(self.major).chain(self.compatible.as_chunks::<4>().0.iter())
    }

    fn has_brand(&self, target: &[u8; 4]) -> bool {
        self.brands().any(|brand| brand == target)
    }
}

fn parse_ftyp(buf: &[u8], total_known: bool) -> Option<Ftyp<'_>> {
    if buf.len() < 16 || &buf[4..8] != b"ftyp" {
        return None;
    }
    let size32 = read_u32_be(buf, 0);
    let (header_len, box_size) = if size32 == 1 {
        if buf.len() < 24 {
            return None;
        }
        let large = read_u64_be(buf, 8);
        if large < 24 || !(large - 16).is_multiple_of(4) {
            return None;
        }
        (16usize, large)
    } else if size32 >= 16 {
        let size = u64::from(size32);
        if !(size - 8).is_multiple_of(4) {
            return None;
        }
        (8usize, size)
    } else {
        return None;
    };
    if total_known && (buf.len() as u64) < box_size {
        return None;
    }
    let major: &[u8; 4] = buf.get(header_len..header_len + 4)?.try_into().ok()?;
    if !major.iter().all(|b| (0x20..=0x7E).contains(b)) {
        return None;
    }
    let end = (box_size.min(buf.len() as u64) as usize).saturating_sub(header_len + 8);
    let compat_start = header_len + 8;
    let compat_len = end - (end % 4);
    let compatible = buf.get(compat_start..compat_start + compat_len)?;
    Some(Ftyp { major, compatible })
}

fn detect_image<'a>(buf: &[u8], ftyp: Option<&Ftyp<'a>>) -> Option<&'static str> {
    if is_png(buf) {
        return Some("image/png");
    }
    if is_jpeg(buf) {
        return Some("image/jpeg");
    }
    if is_gif(buf) {
        return Some("image/gif");
    }
    if is_webp(buf) {
        return Some("image/webp");
    }
    if is_bmp(buf) {
        return Some("image/bmp");
    }
    if is_tiff(buf) {
        return Some("image/tiff");
    }
    if let Some(ftyp) = ftyp {
        if ftyp.has_brand(b"heic") || ftyp.has_brand(b"heix") {
            return Some("image/heic");
        }
        if ftyp.has_brand(b"heif") {
            return Some("image/heif");
        }
    }
    None
}

fn is_png(buf: &[u8]) -> bool {
    buf.len() >= 16
        && buf.starts_with(&PNG_SIGNATURE)
        && read_u32_be(buf, 8) == 13
        && &buf[12..16] == b"IHDR"
}

pub(crate) fn is_animated_png(buf: &[u8]) -> bool {
    if !buf.starts_with(&PNG_SIGNATURE) {
        return false;
    }
    let mut offset = PNG_SIGNATURE.len();
    while offset + 8 <= buf.len() {
        let chunk_len = read_u32_be(buf, offset) as usize;
        let kind = &buf[offset + 4..offset + 8];
        if kind == b"acTL" {
            return true;
        }
        if kind == b"IDAT" {
            return false;
        }
        let Some(next) = offset
            .checked_add(8)
            .and_then(|n| n.checked_add(chunk_len))
            .and_then(|n| n.checked_add(4))
        else {
            return false;
        };
        if next <= offset || next > buf.len() {
            return false;
        }
        offset = next;
    }
    false
}

fn is_jpeg(buf: &[u8]) -> bool {
    if buf.len() < 4 || buf[0..3] != [0xFF, 0xD8, 0xFF] {
        return false;
    }
    let mut idx = 3;
    while idx < buf.len() && buf[idx] == 0xFF {
        idx += 1;
    }
    let Some(&marker) = buf.get(idx) else {
        return false;
    };
    matches!(marker, 0xC0..=0xCF | 0xDB..=0xFE)
}

pub(crate) fn is_jpeg_ls(buf: &[u8]) -> bool {
    if buf.len() < 4 || buf[0..2] != [0xFF, 0xD8] {
        return false;
    }
    let mut idx = 2;
    while idx + 1 < buf.len() {
        if buf[idx] != 0xFF {
            return false;
        }
        while idx < buf.len() && buf[idx] == 0xFF {
            idx += 1;
        }
        let Some(&marker) = buf.get(idx) else {
            return false;
        };
        idx += 1;
        if matches!(marker, 0xF7 | 0xF8) {
            return true;
        }
        if matches!(marker, 0xD9 | 0xDA) {
            return false;
        }
        if matches!(marker, 0x01 | 0xD0..=0xD8) {
            continue;
        }
        if idx + 2 > buf.len() {
            return false;
        }
        let seg_len = u16::from_be_bytes([buf[idx], buf[idx + 1]]) as usize;
        if seg_len < 2 {
            return false;
        }
        idx = match idx.checked_add(seg_len) {
            Some(next) => next,
            None => return false,
        };
    }
    false
}

/// 6-byte GIF87a/GIF89a signature + complete 7-byte Logical Screen Descriptor (13 bytes minimum).
fn is_gif(buf: &[u8]) -> bool {
    if buf.len() < 13 || (&buf[0..6] != b"GIF87a" && &buf[0..6] != b"GIF89a") {
        return false;
    }
    let width = u16::from_le_bytes([buf[6], buf[7]]);
    let height = u16::from_le_bytes([buf[8], buf[9]]);
    if width == 0 || height == 0 {
        return false;
    }
    let packed = buf[10];
    let gct_bytes = if (packed & 0x80) != 0 {
        3usize * (1usize << ((packed & 0x07) + 1))
    } else {
        0
    };
    let after_gct = 13 + gct_bytes;
    if let Some(&introducer) = buf.get(after_gct) {
        matches!(introducer, 0x21 | 0x2C | 0x3B)
    } else {
        true
    }
}

fn is_webp(buf: &[u8]) -> bool {
    buf.len() >= 16
        && &buf[0..4] == b"RIFF"
        && &buf[8..12] == b"WEBP"
        && matches!(&buf[12..16], b"VP8 " | b"VP8L" | b"VP8X")
}

fn is_bmp(buf: &[u8]) -> bool {
    if buf.len() < 26 || &buf[0..2] != b"BM" {
        return false;
    }
    let declared_file_size = read_u32_le(buf, 2);
    let pixel_data_offset = read_u32_le(buf, 10);
    let dib_header_size = read_u32_le(buf, 14);
    if declared_file_size != 0 && declared_file_size < 26 {
        return false;
    }
    if u64::from(pixel_data_offset) < 14 + u64::from(dib_header_size) {
        return false;
    }
    if declared_file_size != 0 && pixel_data_offset >= declared_file_size {
        return false;
    }
    let (planes, bpp) = match dib_header_size {
        12 => (read_u16_le(buf, 22), read_u16_le(buf, 24)),
        40 | 52 | 56 | 64 | 108 | 124 => {
            if buf.len() < 30 {
                return false;
            }
            (read_u16_le(buf, 26), read_u16_le(buf, 28))
        }
        _ => return false,
    };
    planes == 1 && matches!(bpp, 1 | 4 | 8 | 16 | 24 | 32)
}

fn is_tiff(buf: &[u8]) -> bool {
    if buf.len() < 8 {
        return false;
    }
    let (little, ifd_offset) = if &buf[0..4] == b"II*\0" {
        (true, read_u32_le(buf, 4))
    } else if &buf[0..4] == b"MM\0*" {
        (false, read_u32_be(buf, 4))
    } else {
        return false;
    };
    if ifd_offset < 8 {
        return false;
    }
    let off = ifd_offset as usize;
    if off + 2 <= buf.len() {
        let entries = if little {
            read_u16_le(buf, off)
        } else {
            read_u16_be(buf, off)
        };
        if entries == 0 {
            return false;
        }
    }
    true
}

fn detect_audio<'a>(buf: &[u8], ftyp: Option<&Ftyp<'a>>, hint: Hint<'_>) -> Option<&'static str> {
    if is_wav(buf) {
        return Some("audio/wav");
    }
    if is_flac(buf) {
        return Some("audio/flac");
    }
    if is_aiff(buf) {
        return Some("audio/aiff");
    }
    if is_ogg_audio(buf) {
        return Some("audio/ogg");
    }
    if is_mp3(buf) {
        return Some("audio/mpeg");
    }
    if is_adts_aac(buf) {
        return Some("audio/aac");
    }
    if let Some(ftyp) = ftyp
        && (ftyp.has_brand(b"M4A ")
            || matches!(
                hint,
                Hint::Ext("m4a") | Hint::Mime("audio/mp4" | "audio/x-m4a" | "audio/m4a")
            ))
    {
        return Some("audio/mp4");
    }
    None
}

fn is_wav(buf: &[u8]) -> bool {
    if buf.len() < 36 || &buf[0..4] != b"RIFF" || &buf[8..12] != b"WAVE" {
        return false;
    }
    let mut offset = 12usize;
    while offset + 8 <= buf.len() {
        let id = &buf[offset..offset + 4];
        let size = read_u32_le(buf, offset + 4) as usize;
        if id == b"fmt " {
            if size < 16 || offset + 24 > buf.len() {
                return false;
            }
            let channels = read_u16_le(buf, offset + 10);
            let sample_rate = read_u32_le(buf, offset + 12);
            return channels >= 1 && sample_rate >= 1;
        }
        let padded = size.checked_add(size & 1);
        let Some(next) = padded.and_then(|s| offset.checked_add(8 + s)) else {
            return false;
        };
        if next <= offset {
            return false;
        }
        offset = next;
    }
    false
}

fn is_flac(buf: &[u8]) -> bool {
    if buf.len() < 42 || &buf[0..4] != b"fLaC" {
        return false;
    }
    let block_type = buf[4] & 0x7F;
    let block_len = ((buf[5] as u32) << 16) | ((buf[6] as u32) << 8) | (buf[7] as u32);
    block_type == 0 && block_len == 34
}

fn is_aiff(buf: &[u8]) -> bool {
    if buf.len() < 38 || &buf[0..4] != b"FORM" || !matches!(&buf[8..12], b"AIFF" | b"AIFC") {
        return false;
    }
    let mut offset = 12usize;
    while offset + 8 <= buf.len() {
        let id = &buf[offset..offset + 4];
        let size = read_u32_be(buf, offset + 4) as usize;
        if id == b"COMM" {
            if size < 18 || offset + 26 > buf.len() {
                return false;
            }
            let channels = read_u16_be(buf, offset + 8);
            return channels >= 1;
        }
        let padded = size.checked_add(size & 1);
        let Some(next) = padded.and_then(|s| offset.checked_add(8 + s)) else {
            return false;
        };
        if next <= offset {
            return false;
        }
        offset = next;
    }
    false
}

fn is_ogg_audio(buf: &[u8]) -> bool {
    if buf.len() < 28 || &buf[0..4] != b"OggS" {
        return false;
    }
    let version = buf[4];
    let header_type = buf[5];
    if version != 0 || (header_type & 0x02) == 0 {
        return false;
    }
    let seg_count = buf[26] as usize;
    let payload_start = 27 + seg_count;
    if seg_count == 0 || payload_start > buf.len() {
        return false;
    }
    let mut pkt_len = 0usize;
    for &seg in &buf[27..payload_start] {
        pkt_len += seg as usize;
        if seg < 255 {
            break;
        }
    }
    let avail = (buf.len() - payload_start).min(pkt_len);
    let pkt = &buf[payload_start..payload_start + avail];
    pkt.starts_with(b"\x01vorbis")
        || pkt.starts_with(b"OpusHead")
        || pkt.starts_with(b"\x7FFLAC")
        || pkt.starts_with(b"Speex   ")
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct MpegFrame {
    ver: u8,
    layer: u8,
    sr_idx: u8,
    frame_len: usize,
}

fn parse_mpeg_frame(buf: &[u8]) -> Option<MpegFrame> {
    if buf.len() < 4 || buf[0] != 0xFF || (buf[1] & 0xE0) != 0xE0 {
        return None;
    }
    let ver = (buf[1] >> 3) & 0x03;
    let layer = (buf[1] >> 1) & 0x03;
    let br_idx = ((buf[2] >> 4) & 0x0F) as usize;
    let sr_idx = (buf[2] >> 2) & 0x03;
    let padding = ((buf[2] >> 1) & 0x01) as usize;
    let emphasis = buf[3] & 0x03;
    if ver == 0x01
        || layer == 0x00
        || br_idx == 0
        || br_idx == 15
        || sr_idx == 0x03
        || emphasis == 0x02
    {
        return None;
    }
    let sr = match ver {
        3 => [44_100usize, 48_000, 32_000][sr_idx as usize],
        2 => [22_050usize, 24_000, 16_000][sr_idx as usize],
        0 => [11_025usize, 12_000, 8_000][sr_idx as usize],
        _ => return None,
    };
    let kbps = match (ver, layer) {
        (3, 3) => [
            0, 32, 64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 384, 416, 448,
        ][br_idx],
        (3, 2) => [
            0, 32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384,
        ][br_idx],
        (3, 1) => [
            0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320,
        ][br_idx],
        (_, 3) => [
            0, 32, 48, 56, 64, 80, 96, 112, 128, 144, 160, 176, 192, 224, 256,
        ][br_idx],
        (_, 1 | 2) => [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160][br_idx],
        _ => return None,
    };
    let frame_len = match (ver, layer) {
        (_, 3) => (12 * kbps * 1000 / sr + padding) * 4,
        (_, 2) | (3, 1) => 144 * kbps * 1000 / sr + padding,
        (_, 1) => 72 * kbps * 1000 / sr + padding,
        _ => return None,
    };
    if frame_len < 4 {
        return None;
    }
    Some(MpegFrame {
        ver,
        layer,
        sr_idx,
        frame_len,
    })
}

fn is_mp3(buf: &[u8]) -> bool {
    if buf.len() >= 10
        && &buf[0..3] == b"ID3"
        && matches!(buf[3], 2..=4)
        && buf[4] != 0xFF
        && buf[6..10].iter().all(|b| (b & 0x80) == 0)
    {
        let tag_size = ((buf[6] as usize) << 21)
            | ((buf[7] as usize) << 14)
            | ((buf[8] as usize) << 7)
            | (buf[9] as usize);
        let footer = if (buf[5] & 0x10) != 0 { 10 } else { 0 };
        let Some(frame_offset) = 10usize
            .checked_add(tag_size)
            .and_then(|n| n.checked_add(footer))
        else {
            return false;
        };
        return buf.get(frame_offset..).and_then(parse_mpeg_frame).is_some();
    }
    let Some(first) = parse_mpeg_frame(buf) else {
        return false;
    };
    let Some(second) = buf.get(first.frame_len..).and_then(parse_mpeg_frame) else {
        return false;
    };
    first.ver == second.ver && first.layer == second.layer && first.sr_idx == second.sr_idx
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct AdtsFrame {
    id: u8,
    profile: u8,
    sf_idx: u8,
    ch_cfg: u8,
    frame_len: usize,
}

fn parse_adts_frame(buf: &[u8]) -> Option<AdtsFrame> {
    if buf.len() < 7 || buf[0] != 0xFF || (buf[1] & 0xF0) != 0xF0 {
        return None;
    }
    let id = (buf[1] >> 3) & 0x01;
    let layer = (buf[1] >> 1) & 0x03;
    if layer != 0 {
        return None;
    }
    let protection_absent = buf[1] & 0x01;
    let hdr_len = if protection_absent == 1 {
        7usize
    } else {
        9usize
    };
    if buf.len() < hdr_len {
        return None;
    }
    let profile = (buf[2] >> 6) & 0x03;
    let sf_idx = (buf[2] >> 2) & 0x0F;
    if sf_idx >= 13 {
        return None;
    }
    let ch_cfg = ((buf[2] & 0x01) << 2) | ((buf[3] >> 6) & 0x03);
    let frame_len = (((buf[3] & 0x03) as usize) << 11)
        | ((buf[4] as usize) << 3)
        | (((buf[5] >> 5) & 0x07) as usize);
    if frame_len < hdr_len {
        return None;
    }
    Some(AdtsFrame {
        id,
        profile,
        sf_idx,
        ch_cfg,
        frame_len,
    })
}

fn is_adts_aac(buf: &[u8]) -> bool {
    let Some(first) = parse_adts_frame(buf) else {
        return false;
    };
    let Some(second) = buf.get(first.frame_len..).and_then(parse_adts_frame) else {
        return false;
    };
    first.id == second.id
        && first.profile == second.profile
        && first.sf_idx == second.sf_idx
        && first.ch_cfg == second.ch_cfg
}

fn detect_video<'a>(buf: &[u8], ftyp: Option<&Ftyp<'a>>, hint: Hint<'_>) -> Option<&'static str> {
    if let Some(_ftyp) = ftyp {
        return match hint {
            Hint::Ext("mp4" | "m4v") | Hint::Mime("video/mp4") => Some("video/mp4"),
            Hint::Ext("mov" | "qt") | Hint::Mime("video/quicktime") => Some("video/quicktime"),
            Hint::Ext("3gp") | Hint::Mime("video/3gpp") => Some("video/3gpp"),
            _ => None,
        };
    }
    if matches!(hint, Hint::Ext("webm") | Hint::Mime("video/webm")) && is_ebml_webm(buf) {
        return Some("video/webm");
    }
    if is_avi(buf) {
        return Some("video/avi");
    }
    None
}

fn read_ebml_vint(buf: &[u8], keep_marker: bool) -> Option<(u64, usize)> {
    let first = *buf.first()?;
    if first == 0 {
        return None;
    }
    let width = first.leading_zeros() as usize + 1;
    if width > 8 || buf.len() < width {
        return None;
    }
    let mut val = if keep_marker {
        u64::from(first)
    } else {
        u64::from(first & (0xFFu8.checked_shr(width as u32).unwrap_or(0)))
    };
    for &b in &buf[1..width] {
        val = (val << 8) | u64::from(b);
    }
    Some((val, width))
}

fn is_ebml_webm(buf: &[u8]) -> bool {
    parse_ebml_webm(buf).unwrap_or(false)
}

fn parse_ebml_webm(buf: &[u8]) -> Option<bool> {
    if buf.len() < 8 || buf[0..4] != [0x1A, 0x45, 0xDF, 0xA3] {
        return Some(false);
    }
    let (hdr_size, vint_len) = read_ebml_vint(&buf[4..], false)?;
    let body_start = 4 + vint_len;
    let unknown_mask = (1u64 << (7 * vint_len)) - 1;
    let body_end = if hdr_size == unknown_mask {
        buf.len()
    } else {
        body_start
            .saturating_add(hdr_size.min(buf.len() as u64) as usize)
            .min(buf.len())
    };
    let body = buf.get(body_start..body_end)?;
    let mut pos = 0usize;
    while pos < body.len() {
        let (elem_id, id_len) = read_ebml_vint(&body[pos..], true)?;
        pos += id_len;
        let (elem_size, sz_len) = read_ebml_vint(body.get(pos..)?, false)?;
        pos += sz_len;
        let elem_len = usize::try_from(elem_size).ok()?;
        let data = body.get(pos..pos.checked_add(elem_len)?)?;
        if elem_id == 0x4282 {
            let trimmed = match data.iter().position(|&b| b == 0) {
                Some(nul) => &data[..nul],
                None => data,
            };
            return Some(trimmed == b"webm");
        }
        pos += elem_len;
    }
    Some(false)
}

fn is_avi(buf: &[u8]) -> bool {
    if buf.len() < 24 || &buf[0..4] != b"RIFF" || &buf[8..12] != b"AVI " {
        return false;
    }
    let mut offset = 12usize;
    while offset + 8 <= buf.len() {
        let id = &buf[offset..offset + 4];
        let size = read_u32_le(buf, offset + 4) as usize;
        if id == b"LIST" && offset + 12 <= buf.len() && &buf[offset + 8..offset + 12] == b"hdrl" {
            return true;
        }
        let padded = size.checked_add(size & 1);
        let Some(next) = padded.and_then(|s| offset.checked_add(8 + s)) else {
            return false;
        };
        if next <= offset {
            return false;
        }
        offset = next;
    }
    false
}

/// Builds an RFC 8089 `file://` URI from `path` via `Path::components()`.
pub fn file_uri(path: &Path) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        use std::path::Component;

        let mut out = String::from("file://");
        let mut wrote_segment = false;
        for component in path.components() {
            match component {
                Component::RootDir => {
                    if !wrote_segment {
                        out.push('/');
                    }
                }
                Component::CurDir => {}
                Component::ParentDir => {
                    if !out.ends_with('/') {
                        out.push('/');
                    }
                    out.push_str("..");
                    wrote_segment = true;
                }
                Component::Normal(seg) => {
                    if !out.ends_with('/') {
                        out.push('/');
                    }
                    percent_encode_bytes(&mut out, seg.as_bytes());
                    wrote_segment = true;
                }
                Component::Prefix(_) => {}
            }
        }
        if out == "file://" {
            out.push('/');
        }
        out
    }
    #[cfg(windows)]
    {
        use std::path::{Component, Prefix};

        let mut owned: Vec<Vec<u8>> = Vec::new();
        for component in path.components() {
            match component {
                Component::Prefix(prefix) => match prefix.kind() {
                    Prefix::Disk(_) | Prefix::VerbatimDisk(_) => {}
                    Prefix::UNC(server, share) | Prefix::VerbatimUNC(server, share) => {
                        owned.push(windows_os_bytes(server));
                        owned.push(windows_os_bytes(share));
                    }
                    Prefix::Verbatim(name) | Prefix::DeviceNS(name) => {
                        owned.push(windows_os_bytes(name));
                    }
                },
                Component::Normal(seg) => owned.push(windows_os_bytes(seg)),
                Component::RootDir | Component::CurDir | Component::ParentDir => {}
            }
        }
        let mut idx = 0usize;
        let mut parts = Vec::new();
        for component in path.components() {
            match component {
                Component::Prefix(prefix) => match prefix.kind() {
                    Prefix::Disk(drive) | Prefix::VerbatimDisk(drive) => {
                        parts.push(WinUriPart::Disk(drive));
                    }
                    Prefix::UNC(_, _) | Prefix::VerbatimUNC(_, _) => {
                        let server = &owned[idx];
                        let share = &owned[idx + 1];
                        idx += 2;
                        parts.push(WinUriPart::Unc { server, share });
                    }
                    Prefix::Verbatim(_) | Prefix::DeviceNS(_) => {
                        let name = &owned[idx];
                        idx += 1;
                        parts.push(WinUriPart::OtherPrefix(name));
                    }
                },
                Component::RootDir => parts.push(WinUriPart::RootDir),
                Component::CurDir => {}
                Component::ParentDir => parts.push(WinUriPart::ParentDir),
                Component::Normal(_) => {
                    let seg = &owned[idx];
                    idx += 1;
                    parts.push(WinUriPart::Normal(seg));
                }
            }
        }
        format_windows_uri(parts)
    }
}

#[cfg(windows)]
fn windows_os_bytes(text: &std::ffi::OsStr) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt;
    windows_utf16_bytes(text.encode_wide())
}

#[cfg(any(windows, test))]
fn windows_utf16_bytes(units: impl IntoIterator<Item = u16>) -> Vec<u8> {
    let mut out = Vec::new();
    for decoded in char::decode_utf16(units) {
        match decoded {
            Ok(ch) => {
                let mut buf = [0u8; 4];
                out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
            }
            Err(error) => {
                // WTF-8 preserves an unpaired native Windows code unit.
                // decode_utf16 already combined every valid surrogate pair.
                let unit = error.unpaired_surrogate();
                out.extend_from_slice(&[
                    0xE0 | (unit >> 12) as u8,
                    0x80 | ((unit >> 6) as u8 & 0x3F),
                    0x80 | (unit as u8 & 0x3F),
                ]);
            }
        }
    }
    out
}

#[cfg(any(windows, test))]
enum WinUriPart<'a> {
    Disk(u8),
    Unc { server: &'a [u8], share: &'a [u8] },
    OtherPrefix(&'a [u8]),
    RootDir,
    ParentDir,
    Normal(&'a [u8]),
}

#[cfg(any(windows, test))]
fn format_windows_uri<'a>(parts: impl IntoIterator<Item = WinUriPart<'a>>) -> String {
    let mut out = String::from("file://");
    for part in parts {
        match part {
            WinUriPart::Disk(drive) => {
                out.push('/');
                out.push((drive as char).to_ascii_uppercase());
                out.push(':');
            }
            WinUriPart::Unc { server, share } => {
                encode_authority_host(&mut out, server);
                out.push('/');
                percent_encode_bytes(&mut out, share);
            }
            WinUriPart::OtherPrefix(name) => {
                out.push('/');
                percent_encode_bytes(&mut out, name);
            }
            WinUriPart::RootDir => {
                if !out.ends_with('/') {
                    out.push('/');
                }
            }
            WinUriPart::ParentDir => {
                if !out.ends_with('/') {
                    out.push('/');
                }
                out.push_str("..");
            }
            WinUriPart::Normal(seg) => {
                if !out.ends_with('/') {
                    out.push('/');
                }
                percent_encode_bytes(&mut out, seg);
            }
        }
    }
    if out == "file://" {
        out.push('/');
    }
    out
}

#[cfg(any(windows, test))]
fn encode_authority_host(out: &mut String, host: &[u8]) {
    let inner = host
        .strip_prefix(b"[")
        .and_then(|s| s.strip_suffix(b"]"))
        .unwrap_or(host);
    if inner.contains(&b':') {
        out.push('[');
        for &b in inner {
            if is_unreserved(b) || b == b':' {
                out.push(b as char);
            } else {
                push_pct(out, b);
            }
        }
        out.push(']');
        return;
    }
    for &b in inner {
        if is_unreserved(b) {
            out.push(b as char);
        } else {
            push_pct(out, b);
        }
    }
}

fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

fn push_pct(out: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    out.push('%');
    out.push(HEX[(byte >> 4) as usize] as char);
    out.push(HEX[(byte & 0x0F) as usize] as char);
}

fn percent_encode_bytes(out: &mut String, bytes: &[u8]) {
    for &b in bytes {
        if is_unreserved(b) {
            out.push(b as char);
        } else {
            push_pct(out, b);
        }
    }
}

/// Whether `FASTEXEC_IMAGE_COMPRESS=true` applies to this detected image.
pub fn is_compressible_image(mime: &str, bytes: &[u8]) -> bool {
    match mime {
        "image/png" => !is_animated_png(bytes),
        "image/jpeg" => !is_jpeg_ls(bytes),
        "image/gif" | "image/webp" | "image/bmp" => true,
        _ => false,
    }
}

#[derive(Debug)]
pub struct CompressedImage {
    pub bytes: Vec<u8>,
    pub mime: String,
    pub original_bytes: u64,
    pub original_width: u32,
    pub original_height: u32,
    pub width: u32,
    pub height: u32,
    pub transformed: bool,
    pub hints: Vec<String>,
}

#[derive(Debug)]
pub enum CompressFailure {
    Overload,
    Rejected(String),
}

/// Applies Pi-compatible image scaling and format normalization under strict pre-decode bounds.
pub fn compress_image(
    bytes: Vec<u8>,
    mime: &str,
    lease: &mut Lease,
) -> Result<CompressedImage, CompressFailure> {
    let original_bytes = bytes.len() as u64;
    let base = normalize_mime(mime);
    let passthrough_mime = match base.as_str() {
        "image/png" => Some("image/png"),
        "image/jpeg" | "image/jpg" => Some("image/jpeg"),
        "image/gif" => Some("image/gif"),
        "image/webp" => Some("image/webp"),
        _ => None,
    };
    let filtered = if bytes.starts_with(&PNG_SIGNATURE) {
        png_without_unused_metadata(&bytes, bytes.capacity(), lease)?
    } else {
        None
    };
    let working = filtered.as_ref().unwrap_or(&bytes);
    let live_input = bytes
        .capacity()
        .checked_add(filtered.as_ref().map_or(0, Vec::capacity))
        .ok_or(CompressFailure::Overload)?;
    let info = inspect_and_check_bounds(working, live_input, lease)?;
    let (original_width, original_height) =
        oriented_size(info.width, info.height, info.orientation);
    let converted_from = passthrough_mime.is_none().then_some(base);
    let resized = if let Some(supported) = passthrough_mime
        && original_width <= MAX_DISPLAY_DIMENSION
        && original_height <= MAX_DISPLAY_DIMENSION
        && bytes.len() <= COMPRESSED_BYTES_LIMIT
    {
        drop(filtered);
        ResizedImage {
            bytes,
            mime: supported.to_owned(),
            original_width,
            original_height,
            width: original_width,
            height: original_height,
            was_resized: false,
        }
    } else {
        let bytes = match filtered {
            Some(filtered) => {
                drop(bytes);
                filtered
            }
            None => bytes,
        };
        lease.shrink_to(bytes.capacity());
        let mut image = decode_checked(&bytes, bytes.capacity(), &info, lease)?;
        drop(bytes);
        lease.shrink_to(image.as_bytes().len());
        reserve_compression(
            lease,
            orientation_peak(image.as_bytes().len(), info.orientation),
        )?;
        image.apply_orientation(info.orientation);
        resize_decoded(image, converted_from.is_some(), lease)?
    };
    // Decoder, filtered input, orientation, and resize buffers have gone before handoff.
    lease.shrink_to(resized.bytes.capacity());
    let mut hints = Vec::new();
    if let Some(from) = &converted_from
        && *from != resized.mime
    {
        hints.push(format!(
            "[Image converted from {from} to {}.]",
            resized.mime
        ));
    }
    if resized.was_resized {
        hints.push(format!(
            "[Image: original {ow}x{oh}, displayed at {w}x{h}. Multiply x coordinates by {ow}/{w} and y coordinates by {oh}/{h} to map to original image.]",
            ow = resized.original_width,
            oh = resized.original_height,
            w = resized.width,
            h = resized.height,
        ));
    }
    let transformed = converted_from
        .as_ref()
        .is_some_and(|from| *from != resized.mime)
        || resized.was_resized;
    Ok(CompressedImage {
        bytes: resized.bytes,
        mime: resized.mime,
        original_bytes,
        original_width: resized.original_width,
        original_height: resized.original_height,
        width: resized.width,
        height: resized.height,
        transformed,
        hints,
    })
}

const CODEC_WORKSPACE: usize = 1 << 20;
const ENCODER_WORKSPACE: usize = 16 << 20;
const COMPRESSED_BYTES_LIMIT: usize = (MAX_COMPRESSED_BASE64 / 4 - 1) * 3;

fn checked_sum(parts: &[usize]) -> Option<usize> {
    parts
        .iter()
        .try_fold(0usize, |sum, &part| sum.checked_add(part))
}

fn reserve_compression(lease: &mut Lease, target: Option<usize>) -> Result<(), CompressFailure> {
    let target = target.ok_or(CompressFailure::Overload)?;
    if target > lease.bytes() && !lease.try_resize(target) {
        return Err(CompressFailure::Overload);
    }
    Ok(())
}

fn check_image_dimensions(width: u32, height: u32) -> Result<(), CompressFailure> {
    if width == 0 || height == 0 || width > MAX_DECODE_DIMENSION || height > MAX_DECODE_DIMENSION {
        return Err(CompressFailure::Rejected(format!(
            "image dimensions {width}x{height} exceed the {MAX_DECODE_DIMENSION} px single-side decode limit."
        )));
    }
    let pixels = u64::from(width) * u64::from(height);
    if pixels > MAX_DECODE_PIXELS {
        return Err(CompressFailure::Rejected(format!(
            "image pixel count {pixels} ({width}x{height}) exceeds the {MAX_DECODE_PIXELS} pixel decode limit."
        )));
    }
    Ok(())
}

fn png_chunks(bytes: &[u8], mut visit: impl FnMut(&[u8], bool)) -> Result<(), CompressFailure> {
    let invalid =
        || CompressFailure::Rejected("PNG chunk extends beyond the available image bytes.".into());
    if !is_png(bytes) {
        return Err(invalid());
    }
    let mut at = PNG_SIGNATURE.len();
    while at < bytes.len() {
        if bytes.len() - at < 12 {
            return Err(invalid());
        }
        let len = read_u32_be(bytes, at) as usize;
        let end = at
            .checked_add(12)
            .and_then(|at| at.checked_add(len))
            .ok_or_else(invalid)?;
        if end > bytes.len() {
            return Err(invalid());
        }
        let kind = &bytes[at + 4..at + 8];
        let ignored = matches!(kind, b"iCCP" | b"tEXt" | b"zTXt" | b"iTXt");
        visit(&bytes[at..end], ignored);
        at = end;
        if kind == b"IEND" {
            break;
        }
    }
    Ok(())
}

fn png_without_unused_metadata(
    bytes: &[u8],
    raw_capacity: usize,
    lease: &mut Lease,
) -> Result<Option<Vec<u8>>, CompressFailure> {
    if bytes.len() < 33 || !is_png(bytes) {
        return Err(CompressFailure::Rejected(
            "PNG header is truncated or invalid.".into(),
        ));
    }
    check_image_dimensions(read_u32_be(bytes, 16), read_u32_be(bytes, 20))?;
    let mut kept = PNG_SIGNATURE.len();
    let mut omitted = false;
    png_chunks(bytes, |chunk, ignored| {
        if ignored {
            omitted = true;
        } else {
            kept += chunk.len();
        }
    })?;
    if !omitted {
        return Ok(None);
    }
    reserve_compression(lease, raw_capacity.checked_add(kept))?;
    let mut filtered = Vec::with_capacity(kept);
    filtered.extend_from_slice(&PNG_SIGNATURE);
    png_chunks(bytes, |chunk, ignored| {
        if !ignored {
            filtered.extend_from_slice(chunk);
        }
    })?;
    Ok(Some(filtered))
}

#[derive(Default)]
struct WebpWorkspace {
    padded_pixels: usize,
    lossless_bytes: usize,
}

impl WebpWorkspace {
    fn dimensions(&mut self, width: u32, height: u32) -> Result<(), CompressFailure> {
        check_image_dimensions(width, height)?;
        let pixels = (width as usize).div_ceil(16) * 16 * (height as usize).div_ceil(16) * 16;
        self.padded_pixels = self.padded_pixels.max(pixels);
        Ok(())
    }
}

fn inspect_webp_chunks(
    mut bytes: &[u8],
    in_frame: bool,
    workspace: &mut WebpWorkspace,
) -> Result<(), CompressFailure> {
    let invalid =
        || CompressFailure::Rejected("WebP chunk extends beyond the available image bytes.".into());
    let mut saw_frame = false;
    while !bytes.is_empty() {
        if bytes.len() < 8 {
            return Err(invalid());
        }
        let len = read_u32_le(bytes, 4) as usize;
        let end = 8usize.checked_add(len).ok_or_else(invalid)?;
        let padded_end = end.checked_add(len & 1).ok_or_else(invalid)?;
        if padded_end > bytes.len() {
            return Err(invalid());
        }
        let payload = &bytes[8..end];
        match &bytes[..4] {
            b"VP8 " => {
                if payload.len() < 10 {
                    return Err(invalid());
                }
                workspace.dimensions(
                    u32::from(read_u16_le(payload, 6) & 0x3fff),
                    u32::from(read_u16_le(payload, 8) & 0x3fff),
                )?;
            }
            b"VP8L" => {
                if payload.len() < 5 {
                    return Err(invalid());
                }
                let header = read_u32_le(payload, 1);
                workspace.dimensions((header & 0x3fff) + 1, ((header >> 14) & 0x3fff) + 1)?;
                workspace.lossless_bytes = workspace.lossless_bytes.saturating_add(payload.len());
            }
            b"ALPH" => {
                if in_frame && bytes.get(padded_end..padded_end.saturating_add(4)) != Some(b"VP8 ")
                {
                    return Err(invalid());
                }
                if payload.first().is_some_and(|flags| flags & 3 == 1) {
                    workspace.lossless_bytes =
                        workspace.lossless_bytes.saturating_add(payload.len());
                }
            }
            b"ANMF" if !in_frame && !saw_frame => {
                if payload.len() < 16 {
                    return Err(invalid());
                }
                let axis = |offset: usize| {
                    1 + u32::from_le_bytes([
                        payload[offset],
                        payload[offset + 1],
                        payload[offset + 2],
                        0,
                    ])
                };
                workspace.dimensions(axis(6), axis(9))?;
                inspect_webp_chunks(&payload[16..], true, workspace)?;
                saw_frame = true;
            }
            _ => {}
        }
        bytes = &bytes[padded_end..];
    }
    Ok(())
}

struct HeaderInfo {
    format: ImageFormat,
    orientation: Orientation,
    width: u32,
    height: u32,
    total_bytes: usize,
    decoder_len: usize,
    webp: WebpWorkspace,
}

fn header_peak(format: ImageFormat, live_input: usize, raw_len: usize) -> Option<usize> {
    let workspace = match format {
        // Raw chunk capacity can approach 2R; Info and orientation each clone eXIf.
        ImageFormat::Png => raw_len.checked_mul(4)?,
        // image copies JPEG input; zune retains APP data and orientation clones EXIF.
        ImageFormat::Jpeg => raw_len.checked_mul(5)?,
        ImageFormat::Gif => raw_len.checked_mul(2)?,
        ImageFormat::WebP => raw_len,
        _ => 0,
    };
    checked_sum(&[live_input, workspace, CODEC_WORKSPACE])
}

fn image_limits(total_bytes: usize) -> Result<image::Limits, CompressFailure> {
    let mut limits = image::Limits::default();
    limits.max_alloc = Some(MAX_DECODE_ALLOC);
    limits.reserve(total_bytes as u64).map_err(|_| {
        CompressFailure::Rejected("image exceeds image::Limits allocation bounds.".into())
    })?;
    Ok(limits)
}

fn inspect_and_check_bounds(
    bytes: &[u8],
    live_input: usize,
    lease: &mut Lease,
) -> Result<HeaderInfo, CompressFailure> {
    let mut reader = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|_| {
            CompressFailure::Rejected(
                "image could not be recognized for FASTEXEC_IMAGE_COMPRESS=true.".into(),
            )
        })?;
    let format = reader.format().ok_or_else(|| {
        CompressFailure::Rejected(
            "image could not be recognized for FASTEXEC_IMAGE_COMPRESS=true.".into(),
        )
    })?;
    let mut webp = WebpWorkspace::default();
    let mut decoder_len = bytes.len();
    if format == ImageFormat::WebP {
        // The decoder allocates EXIF by declared size, so check RIFF ranges first.
        if bytes.len() < 12 {
            return Err(CompressFailure::Rejected(
                "WebP header is truncated.".into(),
            ));
        }
        let end = (read_u32_le(bytes, 4) as usize)
            .checked_add(8)
            .filter(|&end| end >= 12 && end <= bytes.len())
            .ok_or_else(|| {
                CompressFailure::Rejected("WebP RIFF length exceeds available bytes.".into())
            })?;
        inspect_webp_chunks(&bytes[12..end], false, &mut webp)?;
        decoder_len = end;
        reader = ImageReader::with_format(Cursor::new(&bytes[..end]), format);
    }
    reserve_compression(lease, header_peak(format, live_input, bytes.len()))?;
    // PNG only reads max_alloc at construction.
    reader.limits(image_limits(0)?);
    let mut decoder = reader.into_decoder().map_err(|_| {
        CompressFailure::Rejected(
            "image header could not be decoded under FASTEXEC_IMAGE_COMPRESS=true.".into(),
        )
    })?;
    let orientation = decoder.orientation().unwrap_or(Orientation::NoTransforms);
    let (width, height) = decoder.dimensions();
    check_image_dimensions(width, height)?;
    let total_bytes = decoder.total_bytes();
    if total_bytes > MAX_DECODE_ALLOC {
        return Err(CompressFailure::Rejected(format!(
            "image decoded buffer ({total_bytes} bytes) exceeds the {} MiB decode limit.",
            MAX_DECODE_ALLOC >> 20
        )));
    }
    if format == ImageFormat::WebP {
        webp.dimensions(width, height)?;
    }
    Ok(HeaderInfo {
        format,
        orientation,
        width,
        height,
        total_bytes: total_bytes as usize,
        decoder_len,
        webp,
    })
}

fn decode_peak(info: &HeaderInfo, raw_capacity: usize, raw_len: usize) -> Option<usize> {
    match info.format {
        ImageFormat::Jpeg => {
            let padded_width = (info.width as usize).checked_add(31)? / 32 * 32;
            let padded_height = (info.height as usize).checked_add(31)? / 32 * 32;
            // zune progressive coefficients and padded component rows do not honor max_alloc.
            let coefficients = padded_width.checked_mul(padded_height)?.checked_mul(8)?;
            let rows = padded_width.checked_mul(3072)?;
            checked_sum(&[
                header_peak(info.format, raw_capacity, raw_len)?,
                info.total_bytes,
                coefficients,
                rows,
            ])
        }
        ImageFormat::Png => {
            // png limits logical bytes; its Vec capacity can double. The unfiltering window
            // is separate and holds several padded rows, plus scratch and inflate tables.
            let rows = (info.width as usize)
                .checked_mul(8)?
                .checked_add(1)?
                .checked_mul(16)?;
            checked_sum(&[
                raw_capacity,
                raw_len.checked_mul(3)?,
                info.total_bytes,
                (MAX_DECODE_ALLOC as usize)
                    .checked_sub(info.total_bytes)?
                    .checked_mul(2)?,
                rows,
                CODEC_WORKSPACE,
            ])
        }
        ImageFormat::Gif => checked_sum(&[
            raw_capacity,
            raw_len.checked_mul(2)?,
            MAX_DECODE_ALLOC as usize,
            CODEC_WORKSPACE,
        ]),
        ImageFormat::WebP => {
            // image-webp 0.2.4 leaves lossless Huffman allocations unlimited. Each group
            // consumes >=20 encoded bits and has five trees with <=3136 symbols total.
            // 256 KiB/group includes nodes, tables, metadata, and allocation growth.
            // The conservative bound may reject ordinary lossless/alpha images. It is
            // used only when decoding, never for byte passthrough.
            let groups = info
                .webp
                .lossless_bytes
                .checked_mul(8)?
                .checked_div(20)?
                .min(65536);
            checked_sum(&[
                raw_capacity,
                raw_len.checked_mul(4)?,
                info.webp.padded_pixels.checked_mul(16)?,
                groups.checked_mul(256 << 10)?,
                // The VP8 reader reserves a 24-bit partition before its read can fail.
                16 << 20,
                CODEC_WORKSPACE,
            ])
        }
        _ => checked_sum(&[raw_capacity, info.total_bytes, CODEC_WORKSPACE]),
    }
}

fn decode_checked(
    bytes: &[u8],
    raw_capacity: usize,
    info: &HeaderInfo,
    lease: &mut Lease,
) -> Result<DynamicImage, CompressFailure> {
    reserve_compression(lease, decode_peak(info, raw_capacity, bytes.len()))?;
    let mut limits = image_limits(info.total_bytes)?;
    limits.max_image_width = Some(MAX_DECODE_DIMENSION);
    limits.max_image_height = Some(MAX_DECODE_DIMENSION);
    // Rebuild after inspecting the header so PNG's internal allowance excludes output D.
    let mut reader = ImageReader::with_format(Cursor::new(&bytes[..info.decoder_len]), info.format);
    reader.limits(limits);
    let decoder = reader.into_decoder().map_err(|_| {
        CompressFailure::Rejected(
            "image could not be decoded under FASTEXEC_IMAGE_COMPRESS=true.".into(),
        )
    })?;
    DynamicImage::from_decoder(decoder).map_err(|_| {
        CompressFailure::Rejected(
            "image pixels could not be decoded under FASTEXEC_IMAGE_COMPRESS=true.".into(),
        )
    })
}

fn orientation_peak(decoded: usize, orientation: Orientation) -> Option<usize> {
    match orientation {
        Orientation::Rotate90
        | Orientation::Rotate270
        | Orientation::Rotate90FlipH
        | Orientation::Rotate270FlipH => decoded.checked_mul(2),
        _ => Some(decoded),
    }
}

fn resize_peak(image: &DynamicImage, width: u32, height: u32) -> Option<usize> {
    let decoded = image.as_bytes().len();
    if (image.width(), image.height()) == (width, height) {
        return Some(decoded);
    }
    let floating = (image.width() as usize)
        .checked_mul(height as usize)?
        .checked_mul(16)?;
    let candidate = (width as usize)
        .checked_mul(height as usize)?
        .checked_mul(image.color().bytes_per_pixel() as usize)?;
    let weights = (image.width().max(image.height()) as usize).checked_mul(8)?;
    checked_sum(&[decoded, floating, candidate, weights, CODEC_WORKSPACE])
}

struct ResizedImage {
    bytes: Vec<u8>,
    mime: String,
    original_width: u32,
    original_height: u32,
    width: u32,
    height: u32,
    was_resized: bool,
}

fn resize_decoded(
    image: DynamicImage,
    converted: bool,
    lease: &mut Lease,
) -> Result<ResizedImage, CompressFailure> {
    let (original_width, original_height) = (image.width(), image.height());
    let mut width = original_width;
    let mut height = original_height;
    if width > MAX_DISPLAY_DIMENSION {
        height = scale_axis(height, MAX_DISPLAY_DIMENSION, width);
        width = MAX_DISPLAY_DIMENSION;
    }
    if height > MAX_DISPLAY_DIMENSION {
        width = scale_axis(width, MAX_DISPLAY_DIMENSION, height);
        height = MAX_DISPLAY_DIMENSION;
    }
    loop {
        lease.shrink_to(image.as_bytes().len());
        reserve_compression(lease, resize_peak(&image, width, height))?;
        let resized = ((width, height) != (original_width, original_height))
            .then(|| image.resize_exact(width, height, image::imageops::FilterType::Lanczos3));
        let candidate = resized.as_ref().unwrap_or(&image);
        let live = image
            .as_bytes()
            .len()
            .checked_add(resized.as_ref().map_or(0, |image| image.as_bytes().len()))
            .ok_or(CompressFailure::Overload)?;
        lease.shrink_to(live);
        if let Some((encoded, out_mime)) = first_fitting_encoding(candidate, live, lease)? {
            return Ok(ResizedImage {
                bytes: encoded,
                mime: out_mime.to_owned(),
                original_width,
                original_height,
                width,
                height,
                was_resized: !converted
                    || (width, height) != (original_width, original_height)
                    || out_mime != "image/png",
            });
        }
        if width == 1 && height == 1 {
            break;
        }
        let next_width = if width == 1 {
            1
        } else {
            (width * 3 / 4).max(1)
        };
        let next_height = if height == 1 {
            1
        } else {
            (height * 3 / 4).max(1)
        };
        if next_width == width && next_height == height {
            break;
        }
        width = next_width;
        height = next_height;
    }
    Err(CompressFailure::Rejected(
        "image could not be resized below the compressed inline size limit.".into(),
    ))
}

fn scale_axis(side: u32, limit: u32, other: u32) -> u32 {
    let scaled = (f64::from(side) * f64::from(limit) / f64::from(other)).round();
    scaled.max(1.0).min(f64::from(u32::MAX)) as u32
}

fn png_encoding_peak(live: usize, candidate: usize) -> Option<usize> {
    // Internal compressed Vecs (including fallback), PNG16 endian copy, and row buffers.
    // Two bounded outputs cover shrinking the returned Vec to exact capacity.
    checked_sum(&[
        live,
        candidate.checked_mul(8)?,
        COMPRESSED_BYTES_LIMIT.checked_mul(2)?,
        ENCODER_WORKSPACE,
    ])
}

fn jpeg_encoding_peak(live: usize, width: u32, height: u32) -> Option<usize> {
    checked_sum(&[
        live,
        (width as usize)
            .checked_mul(height as usize)?
            .checked_mul(3)?,
        COMPRESSED_BYTES_LIMIT.checked_mul(2)?,
        CODEC_WORKSPACE,
    ])
}

fn encode_bounded(
    encode: impl FnOnce(&mut Cursor<&mut [u8]>) -> image::ImageResult<()>,
) -> Option<Vec<u8>> {
    let mut bytes = vec![0; COMPRESSED_BYTES_LIMIT];
    let mut writer = Cursor::new(bytes.as_mut_slice());
    encode(&mut writer).ok()?;
    let len = writer.position() as usize;
    bytes.truncate(len);
    Some(bytes.into_boxed_slice().into_vec())
}

fn first_fitting_encoding(
    image: &DynamicImage,
    live: usize,
    lease: &mut Lease,
) -> Result<Option<(Vec<u8>, &'static str)>, CompressFailure> {
    reserve_compression(lease, png_encoding_peak(live, image.as_bytes().len()))?;
    if let Some(bytes) = encode_bounded(|writer| image.write_to(writer, ImageFormat::Png)) {
        return Ok(Some((bytes, "image/png")));
    }
    // Failed candidate and PNG encoder internals are gone before making an RGB copy.
    lease.shrink_to(live);
    reserve_compression(
        lease,
        jpeg_encoding_peak(live, image.width(), image.height()),
    )?;
    let converted = image.as_rgb8().is_none().then(|| image.to_rgb8());
    let rgb = converted.as_ref().or_else(|| image.as_rgb8()).unwrap();
    for quality in QUALITIES {
        if let Some(bytes) = encode_bounded(|writer| {
            JpegEncoder::new_with_quality(writer, quality).encode_image(rgb)
        }) {
            return Ok(Some((bytes, "image/jpeg")));
        }
    }
    Ok(None)
}

fn oriented_size(width: u32, height: u32, orientation: Orientation) -> (u32, u32) {
    match orientation {
        Orientation::Rotate90
        | Orientation::Rotate270
        | Orientation::Rotate90FlipH
        | Orientation::Rotate270FlipH => (height, width),
        _ => (width, height),
    }
}

pub fn encode_base64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn read_u16_le(buf: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([buf[offset], buf[offset + 1]])
}

fn read_u16_be(buf: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes([buf[offset], buf[offset + 1]])
}

fn read_u32_le(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ])
}

fn read_u32_be(buf: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ])
}

fn read_u64_be(buf: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
        buf[offset + 4],
        buf[offset + 5],
        buf[offset + 6],
        buf[offset + 7],
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::BytePool;
    use image::{GenericImageView, Rgb, RgbImage, Rgba, RgbaImage};

    fn make_ftyp(major: &[u8; 4], compat: &[&[u8; 4]]) -> Vec<u8> {
        let size = (16 + compat.len() * 4) as u32;
        let mut out = Vec::with_capacity(size as usize);
        out.extend_from_slice(&size.to_be_bytes());
        out.extend_from_slice(b"ftyp");
        out.extend_from_slice(major);
        out.extend_from_slice(&0u32.to_be_bytes());
        for brand in compat {
            out.extend_from_slice(*brand);
        }
        out
    }

    fn make_ogg_bos(packet: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"OggS");
        out.push(0); // version 0
        out.push(0x02); // BOS flag
        out.extend_from_slice(&[0u8; 20]); // granule + serial + seq + crc
        assert!(packet.len() < 255);
        out.push(1);
        out.push(packet.len() as u8);
        out.extend_from_slice(packet);
        out
    }

    /// MPEG-1 Layer III 128 kbps 44100 Hz frame (417 bytes without padding).
    fn make_mp3_frame() -> Vec<u8> {
        let mut frame = vec![0u8; 417];
        frame[0] = 0xFF;
        frame[1] = 0xFB; // MPEG-1, Layer III, no CRC
        frame[2] = 0x90; // 128 kbps (idx 9), 44100 Hz (idx 0), padding 0
        frame[3] = 0x00;
        frame
    }

    /// MPEG-4 ADTS AAC LC 44100 Hz stereo frame of 16 bytes.
    fn make_adts_frame() -> Vec<u8> {
        let len = 16usize;
        let mut frame = vec![0u8; len];
        frame[0] = 0xFF;
        frame[1] = 0xF1; // MPEG-4, layer 0, no CRC
        frame[2] = (1 << 6) | (4 << 2); // AAC LC, 44.1kHz (idx 4)
        frame[3] = (2 << 6) | ((len >> 11) as u8 & 0x03);
        frame[4] = ((len >> 3) & 0xFF) as u8;
        frame[5] = (((len & 0x07) as u8) << 5) | 0x1F;
        frame[6] = 0xFC;
        frame
    }

    fn make_ebml(doctype: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&[0x42, 0x82]); // DocType Element ID
        body.push(0x80 | (doctype.len() as u8)); // 1-byte VINT size
        body.extend_from_slice(doctype);
        let mut out = vec![0x1A, 0x45, 0xDF, 0xA3];
        out.push(0x80 | (body.len() as u8));
        out.extend_from_slice(&body);
        out
    }

    fn encoded_img(width: u32, height: u32, format: ImageFormat) -> Vec<u8> {
        let img = DynamicImage::ImageRgb8(RgbImage::from_pixel(width, height, Rgb([12, 90, 210])));
        let mut bytes = Vec::new();
        img.write_to(&mut Cursor::new(&mut bytes), format).unwrap();
        bytes
    }

    #[test]
    fn document_pdf_requires_header_and_structural_evidence() {
        let real_pdf = b"%PDF-1.7\n%\xE2\xE3\xCF\xD3\n1 0 obj\n<< /Type /Catalog >>\nendobj\n";
        assert_eq!(
            detect_path(real_pdf, Some(Path::new("doc.txt"))),
            Some(DetectedMedia {
                kind: MediaKind::Document,
                mime: "application/pdf",
            })
        );
        let pdf2 = b"%PDF-2.0\r\n1 0 obj\r\n[1 2 3]\r\nendobj\r\n";
        assert_eq!(
            detect_path(pdf2, None),
            Some(DetectedMedia {
                kind: MediaKind::Document,
                mime: "application/pdf",
            })
        );
        let commented_pdf = b"%PDF-2.0\n% rev 2\n1 0 obj\nendobj\n";
        assert_eq!(
            detect_path(commented_pdf, None),
            Some(DetectedMedia {
                kind: MediaKind::Document,
                mime: "application/pdf",
            })
        );
        let backtrack_pdf = b"%PDF-1.7\nnote 99 1 0 obj\nendobj\n";
        assert_eq!(
            detect_path(backtrack_pdf, None),
            Some(DetectedMedia {
                kind: MediaKind::Document,
                mime: "application/pdf",
            })
        );
        // Single-line or prose lookalikes fall back to text.
        assert_eq!(
            detect_path(b"%PDF-1.7\n", Some(Path::new("fake.pdf"))),
            None
        );
        assert_eq!(
            detect_path(
                b"%PDF-1.7\nThis object explains PDF headers in plain prose.\n",
                Some(Path::new("notes.txt"))
            ),
            None
        );
    }

    #[test]
    fn image_formats_cover_gif_boundary_apng_jpeg_ls_tiff_and_heic_brands() {
        // PNG and APNG both detect as image/png; only static PNG is compressible.
        let png = encoded_img(4, 4, ImageFormat::Png);
        let mut apng = png[..33].to_vec();
        apng.extend_from_slice(&8u32.to_be_bytes());
        apng.extend_from_slice(b"acTL");
        apng.extend_from_slice(&[0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0]);
        apng.extend_from_slice(&png[33..]);
        assert_eq!(
            detect_path(&png, None),
            Some(DetectedMedia {
                kind: MediaKind::Image,
                mime: "image/png"
            })
        );
        assert_eq!(
            detect_path(&apng, None),
            Some(DetectedMedia {
                kind: MediaKind::Image,
                mime: "image/png"
            })
        );
        assert!(is_compressible_image("image/png", &png));
        assert!(!is_compressible_image("image/png", &apng));

        // JPEG and JPEG-LS both detect as image/jpeg; JPEG-LS skips compression.
        let jpeg = encoded_img(4, 4, ImageFormat::Jpeg);
        let jpeg_ls = [0xFF, 0xD8, 0xFF, 0xF7, 0x00, 0x0B];
        assert_eq!(detect_path(&jpeg, None).map(|d| d.mime), Some("image/jpeg"));
        assert_eq!(
            detect_path(&jpeg_ls, None).map(|d| d.mime),
            Some("image/jpeg")
        );
        assert!(is_compressible_image("image/jpeg", &jpeg));
        assert!(!is_compressible_image("image/jpeg", &jpeg_ls));
        assert_eq!(detect_path(&[0xFF, 0xD8, 0xFF, 0x00], None), None);

        // GIF 12-byte truncated LSD is rejected; 13-byte complete LSD passes; prose lookalike fails.
        let gif13 = b"GIF89a\x01\x00\x01\x00\x00\x00\x00";
        assert_eq!(gif13.len(), 13);
        assert_eq!(detect_path(&gif13[..12], None), None);
        assert_eq!(detect_path(gif13, None).map(|d| d.mime), Some("image/gif"));
        assert_eq!(
            detect_path(b"GIF89a is an image format described in 1989.\n", None),
            None
        );

        // WebP & BMP
        let mut webp = b"RIFF\x20\x00\x00\x00WEBPVP8 ".to_vec();
        webp.extend_from_slice(&[0u8; 16]);
        assert_eq!(detect_path(&webp, None).map(|d| d.mime), Some("image/webp"));
        let bmp = encoded_img(8, 8, ImageFormat::Bmp);
        assert_eq!(detect_path(&bmp, None).map(|d| d.mime), Some("image/bmp"));

        // TIFF little/big endian with IFD >= 8 vs invalid IFD < 8
        assert_eq!(
            detect_path(b"II*\0\x08\0\0\0\x01\0", None).map(|d| d.mime),
            Some("image/tiff")
        );
        assert_eq!(
            detect_path(b"MM\0*\0\0\0\x08\0\x01", None).map(|d| d.mime),
            Some("image/tiff")
        );
        assert_eq!(detect_path(b"II*\0\x04\0\0\0", None), None);
        assert_eq!(detect_path(&b"II*\0\x08\0\0\0"[..7], None), None);

        // ISO BMFF HEIC/HEIF vs standalone mif1
        let heic = make_ftyp(b"mif1", &[b"mif1", b"heic"]);
        let heix = make_ftyp(b"heix", &[]);
        let heif = make_ftyp(b"mif1", &[b"heif"]);
        let mif1_only = make_ftyp(b"mif1", &[b"miaf"]);
        assert_eq!(detect_path(&heic, None).map(|d| d.mime), Some("image/heic"));
        assert_eq!(detect_path(&heix, None).map(|d| d.mime), Some("image/heic"));
        assert_eq!(detect_path(&heif, None).map(|d| d.mime), Some("image/heif"));
        assert_eq!(
            detect_path(&mif1_only, Some(Path::new("pic.heic"))),
            None,
            "generic mif1 alone must not claim image/heic"
        );
    }

    #[test]
    fn audio_formats_distinguish_ogg_theora_isolated_id3_adts_and_m4a() {
        // WAV with fmt chunk vs missing fmt chunk
        let mut wav = b"RIFF\x24\x00\x00\x00WAVEfmt \x10\x00\x00\x00".to_vec();
        wav.extend_from_slice(&1u16.to_le_bytes()); // PCM
        wav.extend_from_slice(&2u16.to_le_bytes()); // 2 channels
        wav.extend_from_slice(&44_100u32.to_le_bytes());
        wav.extend_from_slice(&[0u8; 8]);
        assert_eq!(detect_path(&wav, None).map(|d| d.mime), Some("audio/wav"));
        assert_eq!(
            detect_path(b"RIFF\x10\x00\x00\x00WAVEdata\x04\0\0\0abcd", None),
            None
        );

        // FLAC with STREAMINFO (type 0, len 34) vs wrong block
        let mut flac = vec![b'f', b'L', b'a', b'C', 0x80, 0x00, 0x00, 34];
        flac.extend_from_slice(&[0u8; 34]);
        assert_eq!(detect_path(&flac, None).map(|d| d.mime), Some("audio/flac"));
        let mut bad_flac = flac.clone();
        bad_flac[7] = 33;
        assert_eq!(detect_path(&bad_flac, None), None);

        // AIFF with COMM chunk
        let mut aiff = b"FORM\x00\x00\x00\x1EAIFFCOMM\x00\x00\x00\x12".to_vec();
        aiff.extend_from_slice(&2u16.to_be_bytes());
        aiff.extend_from_slice(&[0u8; 16]);
        assert_eq!(detect_path(&aiff, None).map(|d| d.mime), Some("audio/aiff"));

        // Ogg Vorbis / Opus / FLAC / Speex pass; Ogg Theora is excluded
        for pkt in [
            &b"\x01vorbis\x00\x00\x00\x00"[..],
            &b"OpusHead\x01\x02"[..],
            &b"\x7FFLAC\x01\x00\x00\x01fLaC"[..],
            &b"Speex   1.2"[..],
        ] {
            let ogg = make_ogg_bos(pkt);
            assert_eq!(detect_path(&ogg, None).map(|d| d.mime), Some("audio/ogg"));
        }
        let theora = make_ogg_bos(b"\x80theora\x03\x02\x01");
        assert_eq!(detect_path(&theora, Some(Path::new("clip.ogg"))), None);

        // MP3: isolated ID3 fails; ID3 + valid frame passes; 2 consecutive frames pass; 1 bare frame fails
        let mut isolated_id3 = vec![b'I', b'D', b'3', 4, 0, 0, 0, 0, 0, 4];
        isolated_id3.extend_from_slice(b"TEXT");
        assert_eq!(
            detect_path(&isolated_id3, Some(Path::new("song.mp3"))),
            None
        );

        let mut id3_mp3 = isolated_id3;
        id3_mp3.extend_from_slice(&make_mp3_frame());
        assert_eq!(
            detect_path(&id3_mp3, None).map(|d| d.mime),
            Some("audio/mpeg")
        );

        let one_mp3 = make_mp3_frame();
        assert_eq!(detect_path(&one_mp3, None), None);
        let mut two_mp3 = one_mp3.clone();
        two_mp3.extend_from_slice(&one_mp3);
        assert_eq!(
            detect_path(&two_mp3, None).map(|d| d.mime),
            Some("audio/mpeg")
        );

        // ADTS AAC: 1 frame fails, 2 frames pass
        let one_aac = make_adts_frame();
        assert_eq!(detect_path(&one_aac, None), None);
        let mut two_aac = one_aac.clone();
        two_aac.extend_from_slice(&one_aac);
        assert_eq!(
            detect_path(&two_aac, None).map(|d| d.mime),
            Some("audio/aac")
        );

        // M4A brand or .m4a hint -> audio/mp4; audio-only .mp4 with isom brand -> video/mp4
        let m4a_brand = make_ftyp(b"M4A ", &[b"isom"]);
        assert_eq!(
            detect_path(&m4a_brand, None).map(|d| d.mime),
            Some("audio/mp4")
        );
        let isom = make_ftyp(b"isom", &[b"mp42"]);
        assert_eq!(
            detect_path(&isom, Some(Path::new("track.m4a"))).map(|d| d.mime),
            Some("audio/mp4")
        );
        assert_eq!(
            detect_path(&isom, Some(Path::new("audio_only.mp4"))),
            Some(DetectedMedia {
                kind: MediaKind::Video,
                mime: "video/mp4"
            })
        );
    }

    #[test]
    fn video_formats_and_inline_mime_disambiguation_and_conflict_rejection() {
        let isom = make_ftyp(b"isom", &[b"iso2", b"mp41"]);
        assert_eq!(detect_path(&isom, None), None);
        assert_eq!(detect_path(&isom, Some(Path::new("data.bin"))), None);
        assert_eq!(detect_path(&isom, Some(Path::new("notes.txt"))), None);
        assert_eq!(
            detect_path(&isom, Some(Path::new("clip.mp4"))).map(|d| d.mime),
            Some("video/mp4")
        );
        assert_eq!(
            detect_path(&isom, Some(Path::new("clip.mov"))).map(|d| d.mime),
            Some("video/quicktime")
        );
        assert_eq!(
            detect_path(&isom, Some(Path::new("clip.3gp"))).map(|d| d.mime),
            Some("video/3gpp")
        );

        let webm = make_ebml(b"webm");
        let mkv = make_ebml(b"matroska");
        assert_eq!(detect_path(&webm, None), None);
        assert_eq!(
            detect_path(&webm, Some(Path::new("clip.webm"))).map(|d| d.mime),
            Some("video/webm")
        );
        assert_eq!(detect_path(&mkv, Some(Path::new("clip.webm"))), None);

        let avi = b"RIFF\x20\x00\x00\x00AVI LIST\x0C\x00\x00\x00hdrlavih";
        assert_eq!(detect_path(avi, None).map(|d| d.mime), Some("video/avi"));

        // R4-4 inline MIME hint disambiguates generic MP4/WebM/M4A, and rejects mismatches.
        assert_eq!(
            detect_inline(&isom, Some("video/mp4")).unwrap(),
            DetectedMedia {
                kind: MediaKind::Video,
                mime: "video/mp4"
            }
        );
        assert_eq!(
            detect_inline(&isom, Some("audio/mp4")).unwrap(),
            DetectedMedia {
                kind: MediaKind::Audio,
                mime: "audio/mp4"
            }
        );
        assert_eq!(
            detect_inline(&webm, Some("video/webm; codecs=vp9")).unwrap(),
            DetectedMedia {
                kind: MediaKind::Video,
                mime: "video/webm"
            }
        );
        let png = encoded_img(4, 4, ImageFormat::Png);
        assert!(detect_inline(&png, Some("video/mp4")).is_err());
        assert!(detect_inline(b"plain text", Some("video/mp4")).is_err());
    }

    #[test]
    fn windows_uri_preserves_unicode_pairs_and_native_unpaired_units() {
        assert_eq!(
            windows_utf16_bytes("A😀é".encode_utf16()),
            "A😀é".as_bytes()
        );
        let units = [0x0061, 0xD83D, 0xDE00, 0xD800, 0x0062, 0xDC00];
        let bytes = windows_utf16_bytes(units);
        assert_eq!(
            bytes,
            [
                b'a', 0xF0, 0x9F, 0x98, 0x80, 0xED, 0xA0, 0x80, b'b', 0xED, 0xB0, 0x80,
            ]
        );
        assert_eq!(
            format_windows_uri([
                WinUriPart::Disk(b'C'),
                WinUriPart::RootDir,
                WinUriPart::Normal(&bytes),
            ]),
            "file:///C:/a%F0%9F%98%80%ED%A0%80b%ED%B0%80"
        );
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStringExt;
            let native = std::ffi::OsString::from_wide(&units);
            assert_eq!(windows_os_bytes(&native), bytes);
        }
    }

    #[test]
    fn rfc8089_file_uri_encodes_unix_bytes_and_windows_drive_and_unc_forms() {
        fn parse_win_parts(raw: &str) -> Vec<WinUriPart<'_>> {
            let mut parts = Vec::new();
            let rest = if raw.len() >= 8
                && raw.as_bytes()[..8].eq_ignore_ascii_case(b"\\\\?\\UNC\\")
            {
                let mut iter = raw[8..].split(['\\', '/']).filter(|s| !s.is_empty());
                if let (Some(server), Some(share)) = (iter.next(), iter.next()) {
                    parts.push(WinUriPart::Unc {
                        server: server.as_bytes(),
                        share: share.as_bytes(),
                    });
                    parts.push(WinUriPart::RootDir);
                }
                for seg in iter {
                    parts.push(if seg == ".." {
                        WinUriPart::ParentDir
                    } else {
                        WinUriPart::Normal(seg.as_bytes())
                    });
                }
                return parts;
            } else if let Some(dev) = raw.strip_prefix("\\\\.\\") {
                let mut iter = dev.split(['\\', '/']).filter(|s| !s.is_empty());
                if let Some(ns) = iter.next() {
                    parts.push(WinUriPart::OtherPrefix(ns.as_bytes()));
                    parts.push(WinUriPart::RootDir);
                }
                for seg in iter {
                    parts.push(if seg == ".." {
                        WinUriPart::ParentDir
                    } else {
                        WinUriPart::Normal(seg.as_bytes())
                    });
                }
                return parts;
            } else if let Some(unc) = raw.strip_prefix("\\\\").filter(|s| !s.starts_with("?\\")) {
                let mut iter = unc.split(['\\', '/']).filter(|s| !s.is_empty());
                if let (Some(server), Some(share)) = (iter.next(), iter.next()) {
                    parts.push(WinUriPart::Unc {
                        server: server.as_bytes(),
                        share: share.as_bytes(),
                    });
                    parts.push(WinUriPart::RootDir);
                }
                for seg in iter {
                    parts.push(if seg == ".." {
                        WinUriPart::ParentDir
                    } else {
                        WinUriPart::Normal(seg.as_bytes())
                    });
                }
                return parts;
            } else {
                raw.strip_prefix("\\\\?\\").unwrap_or(raw)
            };
            let b = rest.as_bytes();
            if b.len() >= 2 && b[0].is_ascii_alphabetic() && b[1] == b':' {
                parts.push(WinUriPart::Disk(b[0]));
                parts.push(WinUriPart::RootDir);
                for seg in rest[2..].split(['\\', '/']).filter(|s| !s.is_empty()) {
                    parts.push(if seg == ".." {
                        WinUriPart::ParentDir
                    } else {
                        WinUriPart::Normal(seg.as_bytes())
                    });
                }
            }
            parts
        }

        let cases = [
            (
                r"C:\Users\alice\my report #1?.pdf",
                "file:///C:/Users/alice/my%20report%20%231%3F.pdf",
            ),
            (
                r"\\?\d:\clips\100% done.mp4",
                "file:///D:/clips/100%25%20done.mp4",
            ),
            (
                r"\\fileserver\team share\doc a.pdf",
                "file://fileserver/team%20share/doc%20a.pdf",
            ),
            (
                r"\\?\UNC\fileserver\share\v.webm",
                "file://fileserver/share/v.webm",
            ),
            (
                r"\\fe80::1%eth0\share\doc.pdf",
                "file://[fe80::1%25eth0]/share/doc.pdf",
            ),
            (
                r"\\[fe80::1%eth0]\share\doc.pdf",
                "file://[fe80::1%25eth0]/share/doc.pdf",
            ),
            (
                r"C:\Users\alice\..\doc.pdf",
                "file:///C:/Users/alice/../doc.pdf",
            ),
            (r"\\.\Volume1\doc.pdf", "file:///Volume1/doc.pdf"),
        ];
        for (input, expected) in cases {
            assert_eq!(
                format_windows_uri(parse_win_parts(input)),
                expected,
                "{input}"
            );
            #[cfg(windows)]
            assert_eq!(file_uri(Path::new(input)), expected, "{input}");
        }

        #[cfg(unix)]
        {
            use std::ffi::OsString;
            use std::os::unix::ffi::OsStringExt;
            use std::path::PathBuf;

            assert_eq!(
                file_uri(Path::new("/tmp/space name/a#b?c%d.pdf")),
                "file:///tmp/space%20name/a%23b%3Fc%25d.pdf"
            );
            let non_utf8 = PathBuf::from(OsString::from_vec(vec![
                b'/', b't', b'm', b'p', b'/', b'f', 0xFF, 0x80, b'.', b'm', b'p', b'4',
            ]));
            assert_eq!(file_uri(&non_utf8), "file:///tmp/f%FF%80.mp4");
        }
    }

    fn png_header_only(width: u32, height: u32) -> Vec<u8> {
        fn crc32(bytes: &[u8]) -> u32 {
            let mut crc = 0xFFFF_FFFFu32;
            for &b in bytes {
                crc ^= u32::from(b);
                for _ in 0..8 {
                    let mask = 0u32.wrapping_sub(crc & 1);
                    crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
                }
            }
            !crc
        }
        let mut chunk = Vec::with_capacity(17);
        chunk.extend_from_slice(b"IHDR");
        chunk.extend_from_slice(&width.to_be_bytes());
        chunk.extend_from_slice(&height.to_be_bytes());
        chunk.extend_from_slice(&[8, 2, 0, 0, 0]);
        let mut out = PNG_SIGNATURE.to_vec();
        out.extend_from_slice(&13u32.to_be_bytes());
        out.extend_from_slice(&chunk);
        out.extend_from_slice(&crc32(&chunk).to_be_bytes());
        // Minimal empty IDAT + IEND so header readers see chunk framing after IHDR.
        out.extend_from_slice(&0u32.to_be_bytes());
        out.extend_from_slice(b"IDAT");
        out.extend_from_slice(&crc32(b"IDAT").to_be_bytes());
        out.extend_from_slice(&0u32.to_be_bytes());
        out.extend_from_slice(b"IEND");
        out.extend_from_slice(&crc32(b"IEND").to_be_bytes());
        out
    }

    #[test]
    fn compression_exemptions_scan_past_the_classification_prefix() {
        let mut png = PNG_SIGNATURE.to_vec();
        png.extend_from_slice(&9000u32.to_be_bytes());
        png.extend_from_slice(b"tEXt");
        png.resize(png.len() + 9000 + 4, 0);
        png.extend_from_slice(&8u32.to_be_bytes());
        png.extend_from_slice(b"acTL");
        png.extend_from_slice(&[0; 12]);
        assert!(!is_compressible_image("image/png", &png));

        let mut jpeg_ls = vec![0xff, 0xd8, 0xff, 0xe1];
        jpeg_ls.extend_from_slice(&9002u16.to_be_bytes());
        jpeg_ls.resize(jpeg_ls.len() + 9000, 0);
        jpeg_ls.extend_from_slice(&[0xff, 0xf7, 0, 2]);
        assert!(!is_compressible_image("image/jpeg", &jpeg_ls));
    }

    #[test]
    fn compression_png_truncated_ihdr_is_rejected_at_every_prefix() {
        let raw = encoded_img(8, 6, ImageFormat::Png);
        for len in 0..33 {
            let bytes = raw[..len].to_vec();
            let pool = BytePool::new(8 << 20);
            let mut lease = pool.try_acquire(bytes.capacity()).unwrap();
            assert!(
                matches!(
                    compress_image(bytes, "image/png", &mut lease),
                    Err(CompressFailure::Rejected(_))
                ),
                "truncated IHDR length {len} must be rejected"
            );
        }
    }

    #[test]
    fn compression_png_header_covers_retained_chunk_and_two_exif_copies() {
        // The retained chunk can approach 2R; Info and orientation each retain another R.
        let raw_len = (2 << 20) + 1;
        let required = raw_len + 4 * raw_len + CODEC_WORKSPACE;
        let peak = header_peak(ImageFormat::Png, raw_len, raw_len).unwrap();
        assert_eq!(peak, required);
        for limit in [required - 1, required, required + 1] {
            let pool = BytePool::new(limit);
            let mut lease = pool.try_acquire(raw_len).unwrap();
            assert_eq!(
                reserve_compression(&mut lease, Some(peak)).is_ok(),
                limit >= required
            );
        }
    }

    #[test]
    fn compression_webp_ignores_exif_after_the_declared_riff_end() {
        let simple = base64::engine::general_purpose::STANDARD.decode(
            "UklGRqQAAABXRUJQVlA4IJgAAADQDQCdASq4CwMAPm02mUmkIyKhICgAgA2JaW7hdrEe3AfgAAAT2Ae+2TkPfbJyHvtk5D32ych77ZOQ99snIe+2TkPfbJyHvtk5D32ych77ZOQ99snIe+2TkPfbJyHvtk5D32ych77ZOQ99snIe+2TkPfbJyHvSAAD+/2yx///1aHy0Plofuff//zBK/IN2EzwoAAAAAAAAAA=="
        ).unwrap();
        let mut raw = b"RIFF".to_vec();
        raw.extend_from_slice(&((simple.len() + 18 - 8) as u32).to_le_bytes());
        raw.extend_from_slice(b"WEBPVP8X");
        raw.extend_from_slice(&10u32.to_le_bytes());
        raw.extend_from_slice(&[0, 0, 0, 0, 0xb7, 0x0b, 0, 2, 0, 0]);
        raw.extend_from_slice(&simple[12..]);
        // A complete, ordinary EXIF orientation record appended after the image container.
        let exif = b"MM\x00*\x00\x00\x00\x08\x00\x01\x01\x12\x00\x03\x00\x00\x00\x01\x00\x06\x00\x00\x00\x00\x00\x00";
        raw.extend_from_slice(b"EXIF");
        raw.extend_from_slice(&(exif.len() as u32).to_le_bytes());
        raw.extend_from_slice(exif);
        let pool = BytePool::new(8 << 20);
        let mut lease = pool.try_acquire(raw.capacity()).unwrap();
        let info = inspect_and_check_bounds(&raw, raw.capacity(), &mut lease).unwrap();
        assert_eq!(info.orientation, Orientation::NoTransforms);
    }

    #[test]
    fn compression_stage_bounds_cover_rotation_resize_and_png16() {
        let mut image = DynamicImage::ImageRgba16(image::ImageBuffer::from_pixel(
            8,
            6,
            Rgba([1200, 4000, 65000, 32768]),
        ));
        let decoded = image.as_bytes().len();
        let rotation = orientation_peak(decoded, Orientation::Rotate90).unwrap();
        assert_eq!(rotation, 2 * decoded);
        let resize = resize_peak(&image, 4, 3).unwrap();
        assert!(resize >= decoded + 16 * 8 * 3 + 8 * 4 * 3);
        assert_eq!(resize_peak(&image, 8, 6), Some(decoded));
        for peak in [rotation, resize] {
            for limit in [peak - 1, peak, peak + 1] {
                let pool = BytePool::new(limit);
                let mut lease = pool.try_acquire(decoded).unwrap();
                assert_eq!(
                    reserve_compression(&mut lease, Some(peak)).is_ok(),
                    limit >= peak
                );
                drop(lease);
                assert_eq!(pool.used(), 0);
            }
        }
        image.apply_orientation(Orientation::Rotate90);
        assert_eq!((image.width(), image.height()), (6, 8));
        let peak = png_encoding_peak(decoded, decoded).unwrap();
        for limit in [peak - 1, peak, peak + 1] {
            let pool = BytePool::new(limit);
            let mut lease = pool.try_acquire(decoded).unwrap();
            let encoded = first_fitting_encoding(&image, decoded, &mut lease);
            if limit < peak {
                assert!(matches!(encoded, Err(CompressFailure::Overload)));
            } else {
                let (bytes, mime) = encoded.unwrap().unwrap();
                assert_eq!(mime, "image/png");
                assert_eq!(bytes.capacity(), bytes.len());
                let roundtrip = image::load_from_memory(&bytes).unwrap();
                assert_eq!(roundtrip.as_rgba16().unwrap(), image.as_rgba16().unwrap());
            }
            drop(lease);
            assert_eq!(pool.used(), 0);
        }
    }

    #[test]
    fn compression_filters_unused_png_metadata_but_keeps_passthrough_and_orientation() {
        // An ordinary 8x6 PNG with text/ICC metadata and EXIF orientation 6.
        let raw = base64::engine::general_purpose::STANDARD.decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAgAAAAGCAIAAABxZ0isAAAAJGlDQ1BJQ0MgUHJvZmlsZQAAeJzLTM/LL0pNUSgoyk/LzEkFAC/1Bfq/lh2jAAAAG3RFWHRwbGFpbgBrZXB0IG9ubHkgaW4gb3JpZ2luYWzYpyg3AAAAJnpUWHRjb21wcmVzc2VkAAB4nMsvKMnMz0vMUShJrSjJH+WQxQEAUneEVDBeJOEAAAAiaVRYdHV0ZjgAAQAAAHicyy8oyczPS8xRyE0tSUxJLEkEAD4MBsgfjJsjAAAAGmVYSWZNTQAqAAAACAABARIAAwAAAAEABgAAAAAAANZnS2kAAAAUSURBVHicY+SJusSADTBhFaWTBAC3tAFE08AREQAAAABJRU5ErkJggg=="
        ).unwrap();
        let pool = BytePool::new(8 << 20);
        let mut lease = pool.try_acquire(raw.capacity()).unwrap();
        let filtered = png_without_unused_metadata(&raw, raw.capacity(), &mut lease)
            .unwrap()
            .unwrap();
        png_chunks(&filtered, |_, ignored| assert!(!ignored)).unwrap();
        assert!(filtered.windows(4).any(|window| window == b"eXIf"));
        let info =
            inspect_and_check_bounds(&filtered, raw.capacity() + filtered.capacity(), &mut lease)
                .unwrap();
        assert_eq!(
            oriented_size(info.width, info.height, info.orientation),
            (6, 8)
        );
        assert_eq!(
            image::load_from_memory(&filtered)
                .unwrap()
                .to_rgb8()
                .get_pixel(0, 0),
            &Rgb([12, 90, 210])
        );
        drop(filtered);
        let mut exif = Vec::new();
        png_chunks(&raw, |chunk, _| {
            if &chunk[4..8] == b"eXIf" {
                exif.extend_from_slice(chunk);
            }
        })
        .unwrap();
        let wide = encoded_img(3000, 3, ImageFormat::Png);
        let mut rotated = wide[..33].to_vec();
        rotated.extend_from_slice(&exif);
        rotated.extend_from_slice(&wide[33..]);
        let rotation_pool = BytePool::new(1 << 30);
        let mut rotation_lease = rotation_pool.try_acquire(rotated.capacity()).unwrap();
        let out = compress_image(rotated, "image/png", &mut rotation_lease).unwrap();
        assert_eq!((out.original_width, out.original_height), (3, 3000));
        assert_eq!((out.width, out.height), (2, 2000));
        assert_eq!(
            image::load_from_memory(&out.bytes).unwrap().dimensions(),
            (2, 2000)
        );
        drop(out);
        drop(rotation_lease);
        assert_eq!(rotation_pool.used(), 0);
        let out = compress_image(raw.clone(), "image/png", &mut lease).unwrap();
        assert_eq!(out.bytes, raw);
        assert!(!out.transformed);
        assert_eq!((out.width, out.height), (6, 8));
        drop(out);
        drop(lease);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn compression_webp_real_images_and_decode_admission_boundaries() {
        // Generated from the same solid-color 3000x3 image as the lossless fixture.
        let lossy = base64::engine::general_purpose::STANDARD.decode(
            "UklGRqQAAABXRUJQVlA4IJgAAADQDQCdASq4CwMAPm02mUmkIyKhICgAgA2JaW7hdrEe3AfgAAAT2Ae+2TkPfbJyHvtk5D32ych77ZOQ99snIe+2TkPfbJyHvtk5D32ych77ZOQ99snIe+2TkPfbJyHvtk5D32ych77ZOQ99snIe+2TkPfbJyHvSAAD+/2yx///1aHy0Plofuff//zBK/IN2EzwoAAAAAAAAAA=="
        ).unwrap();
        let lossless = encoded_img(3000, 3, ImageFormat::WebP);
        for raw in [lossy, lossless] {
            let pool = BytePool::new(1 << 30);
            let mut lease = pool.try_acquire(raw.capacity()).unwrap();
            let info = inspect_and_check_bounds(&raw, raw.capacity(), &mut lease).unwrap();
            let peak = decode_peak(&info, raw.capacity(), raw.len()).unwrap();
            drop(lease);
            for limit in [peak - 1, peak, peak + 1] {
                let pool = BytePool::new(limit);
                let mut lease = pool.try_acquire(raw.capacity()).unwrap();
                let decoded = decode_checked(&raw, raw.capacity(), &info, &mut lease);
                if limit < peak {
                    assert!(matches!(decoded, Err(CompressFailure::Overload)));
                } else {
                    let decoded = decoded.unwrap();
                    assert_eq!((decoded.width(), decoded.height()), (3000, 3));
                }
                drop(lease);
                assert_eq!(pool.used(), 0);
            }
            let mut lease = pool.try_acquire(raw.capacity()).unwrap();
            let compressed = compress_image(raw, "image/webp", &mut lease).unwrap();
            assert_eq!((compressed.width, compressed.height), (2000, 2));
            assert!(compressed.transformed);
            assert_eq!(lease.bytes(), compressed.bytes.capacity());
            drop(compressed);
            drop(lease);
            assert_eq!(pool.used(), 0);
        }
    }

    #[test]
    fn compression_rejects_unleased_resize_workspace() {
        let raw = encoded_img(3000, 1000, ImageFormat::Png);
        let pool = BytePool::new(32 << 20);
        let mut lease = pool.try_acquire(raw.capacity()).unwrap();
        assert!(matches!(
            compress_image(raw, "image/png", &mut lease),
            Err(CompressFailure::Overload)
        ));
        drop(lease);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn compress_enforces_pre_decode_bounds_and_scales_large_images() {
        let pool = BytePool::new(1 << 30);
        let mut lease = pool.try_acquire(1024).unwrap();

        // Crafted PNG IHDR with single side > MAX_DECODE_DIMENSION (40000 x 1) fails before decoding.
        let wide_ihdr = png_header_only(40_000, 1);
        match compress_image(wide_ihdr, "image/png", &mut lease) {
            Err(CompressFailure::Rejected(msg)) => assert!(msg.contains("32768"), "{msg}"),
            other => panic!("expected dimension rejection, got {other:?}"),
        }

        // Crafted PNG IHDR with each side <= 32768 but total pixels > 64M (10000 x 10000 = 100M).
        let many_pixels = png_header_only(10_000, 10_000);
        match compress_image(many_pixels, "image/png", &mut lease) {
            Err(CompressFailure::Rejected(msg)) => assert!(msg.contains("64000000"), "{msg}"),
            other => panic!("expected pixel count rejection, got {other:?}"),
        }

        // Decode allocation exceeding tiny BytePool returns Overload instead of allocating unchecked.
        let tiny_pool = BytePool::new(1024);
        let mut tiny_lease = tiny_pool.try_acquire(512).unwrap();
        let big_png = encoded_img(2400, 100, ImageFormat::Png);
        assert!(matches!(
            compress_image(big_png.clone(), "image/png", &mut tiny_lease),
            Err(CompressFailure::Overload)
        ));

        // Normal scaling under sufficient pool shrinks 3000x1000 to 2000x667 and records metadata.
        let wide_png = encoded_img(3000, 1000, ImageFormat::Png);
        let orig_len = wide_png.len() as u64;
        let out = compress_image(wide_png, "image/png", &mut lease).expect("scales cleanly");
        assert!(out.transformed);
        assert_eq!(out.original_bytes, orig_len);
        assert_eq!((out.original_width, out.original_height), (3000, 1000));
        assert_eq!((out.width, out.height), (2000, 667));
        assert_eq!(out.hints.len(), 1);
        assert!(out.hints[0].contains("original 3000x1000, displayed at 2000x667"));
        let decoded = image::load_from_memory(&out.bytes).unwrap();
        assert_eq!(decoded.dimensions(), (2000, 667));

        // Unresized small PNG is not marked transformed; BMP converts to PNG and IS marked transformed.
        let small_png = encoded_img(8, 6, ImageFormat::Png);
        let passthrough = compress_image(small_png.clone(), "image/png", &mut lease).unwrap();
        assert!(!passthrough.transformed);
        assert_eq!(passthrough.bytes, small_png);

        let small_bmp = encoded_img(8, 6, ImageFormat::Bmp);
        let converted = compress_image(small_bmp, "image/bmp", &mut lease).unwrap();
        assert!(converted.transformed);
        assert_eq!(converted.mime, "image/png");
        assert!(converted.hints[0].contains("converted from image/bmp to image/png"));

        // Noisy 2000x2000 RGBA falls back to JPEG under 4.5 MiB base64.
        let mut state = 0x2b992ddfa232_u64;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as u8
        };
        let mut noisy = RgbaImage::new(2000, 2000);
        for pixel in noisy.pixels_mut() {
            *pixel = Rgba([next(), next(), next(), next()]);
        }
        let mut noisy_png = Vec::new();
        DynamicImage::ImageRgba8(noisy)
            .write_to(&mut Cursor::new(&mut noisy_png), ImageFormat::Png)
            .unwrap();
        let compressed = compress_image(noisy_png, "image/png", &mut lease).unwrap();
        assert!(compressed.transformed);
        assert_eq!(compressed.mime, "image/jpeg");
        assert!(compressed.bytes.len().div_ceil(3) * 4 < MAX_COMPRESSED_BASE64);
    }
}
