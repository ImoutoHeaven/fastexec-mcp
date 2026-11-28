use std::path::Path;

/// Bytes inspected at the head of a file to classify media containers and brands.
pub const SNIFF: usize = 8 * 1024;

pub(super) const PNG_SIGNATURE: [u8; 8] = [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];

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

pub(super) fn normalize_mime(raw: &str) -> String {
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

pub(super) fn is_png(buf: &[u8]) -> bool {
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

pub(super) fn read_u16_le(buf: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([buf[offset], buf[offset + 1]])
}

fn read_u16_be(buf: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes([buf[offset], buf[offset + 1]])
}

pub(super) fn read_u32_le(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ])
}

pub(super) fn read_u32_be(buf: &[u8], offset: usize) -> u32 {
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
