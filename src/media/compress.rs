use super::detect::{
    PNG_SIGNATURE, is_animated_png, is_jpeg_ls, is_png, normalize_mime, read_u16_le, read_u32_be,
    read_u32_le,
};
use crate::lease::Lease;
use base64::Engine;
use image::metadata::Orientation;
use image::{DynamicImage, ImageDecoder, ImageFormat, ImageReader, codecs::jpeg::JpegEncoder};
use std::io::Cursor;

/// Longest side an image is scaled down to when `FASTEXEC_IMAGE_COMPRESS=true`.
const MAX_DISPLAY_DIMENSION: u32 = 2000;

/// Base64 payload cap when `FASTEXEC_IMAGE_COMPRESS=true` (4.5 MiB).
pub(super) const MAX_COMPRESSED_BASE64: usize = 4_718_592;

/// Maximum single-axis pixel dimension allowed before decoding under `FASTEXEC_IMAGE_COMPRESS=true`.
pub const MAX_DECODE_DIMENSION: u32 = 32_768;

/// Maximum total pixel count (64M pixels) allowed before decoding under `FASTEXEC_IMAGE_COMPRESS=true`.
pub const MAX_DECODE_PIXELS: u64 = 64_000_000;

/// Maximum decoder buffer allocation allowed under `FASTEXEC_IMAGE_COMPRESS=true`.
const MAX_DECODE_ALLOC: u64 = 256 << 20;

const QUALITIES: [u8; 5] = [80, 85, 70, 55, 40];

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
    #[cfg(test)]
    pub original_bytes: u64,
    #[cfg(test)]
    pub original_width: u32,
    #[cfg(test)]
    pub original_height: u32,
    #[cfg(test)]
    pub width: u32,
    #[cfg(test)]
    pub height: u32,
    #[cfg(test)]
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
    #[cfg(test)]
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
    #[cfg(test)]
    let transformed = converted_from
        .as_ref()
        .is_some_and(|from| *from != resized.mime)
        || resized.was_resized;
    Ok(CompressedImage {
        bytes: resized.bytes,
        mime: resized.mime,
        #[cfg(test)]
        original_bytes,
        #[cfg(test)]
        original_width: resized.original_width,
        #[cfg(test)]
        original_height: resized.original_height,
        #[cfg(test)]
        width: resized.width,
        #[cfg(test)]
        height: resized.height,
        #[cfg(test)]
        transformed,
        hints,
    })
}

pub(super) const CODEC_WORKSPACE: usize = 1 << 20;
const ENCODER_WORKSPACE: usize = 16 << 20;
const COMPRESSED_BYTES_LIMIT: usize = (MAX_COMPRESSED_BASE64 / 4 - 1) * 3;

fn checked_sum(parts: &[usize]) -> Option<usize> {
    parts
        .iter()
        .try_fold(0usize, |sum, &part| sum.checked_add(part))
}

pub(super) fn reserve_compression(
    lease: &mut Lease,
    target: Option<usize>,
) -> Result<(), CompressFailure> {
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

pub(super) fn png_chunks(
    bytes: &[u8],
    mut visit: impl FnMut(&[u8], bool),
) -> Result<(), CompressFailure> {
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

pub(super) fn png_without_unused_metadata(
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

pub(super) struct HeaderInfo {
    format: ImageFormat,
    pub(super) orientation: Orientation,
    pub(super) width: u32,
    pub(super) height: u32,
    total_bytes: usize,
    decoder_len: usize,
    webp: WebpWorkspace,
}

pub(super) fn header_peak(format: ImageFormat, live_input: usize, raw_len: usize) -> Option<usize> {
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

pub(super) fn inspect_and_check_bounds(
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

pub(super) fn decode_peak(info: &HeaderInfo, raw_capacity: usize, raw_len: usize) -> Option<usize> {
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

pub(super) fn decode_checked(
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

pub(super) fn orientation_peak(decoded: usize, orientation: Orientation) -> Option<usize> {
    match orientation {
        Orientation::Rotate90
        | Orientation::Rotate270
        | Orientation::Rotate90FlipH
        | Orientation::Rotate270FlipH => decoded.checked_mul(2),
        _ => Some(decoded),
    }
}

pub(super) fn resize_peak(image: &DynamicImage, width: u32, height: u32) -> Option<usize> {
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

pub(super) fn png_encoding_peak(live: usize, candidate: usize) -> Option<usize> {
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

pub(super) fn first_fitting_encoding(
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

pub(super) fn oriented_size(width: u32, height: u32, orientation: Orientation) -> (u32, u32) {
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
