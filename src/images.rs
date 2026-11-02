//! Inline images: which files are images, and how a file becomes the bytes a model may see.

use base64::Engine;
use image::metadata::Orientation;
use image::{DynamicImage, ImageDecoder, ImageFormat, ImageReader, codecs::jpeg::JpegEncoder};
use std::io::{Cursor, Read, Seek, SeekFrom};

/// The longest side a displayed image may have.
const MAX_DIMENSION: u32 = 2000;

/// The base64 payload an inline image may have: 4.5 MiB, with room under a 5 MiB API limit.
const MAX_BASE64: usize = 4718592;

/// The bytes Pi sniffs a file with before it decides it is an image.
const SNIFF: usize = 4100;

/// The JPEG qualities tried after the first one, in the order Pi tries them.
const QUALITIES: [u8; 5] = [80, 85, 70, 55, 40];

/// The message for an image that could not be made small enough to send.
const RESIZE_FAILURE: &str =
    "[Image omitted: could not be resized below the inline image size limit.]";

/// The message for an image that could not be read as one.
const CONVERT_FAILURE: &str =
    "[Image omitted: could not be converted to a supported inline image format.]";

/// The most encoded bytes a read loads: the ceiling the decoder itself allocates under.
fn ceiling() -> u64 {
    image::Limits::default().max_alloc.unwrap_or(512 << 20)
}

/// Reads an image file whole, within [`ceiling`]: a file past it is not loaded at all, and one
/// that grows after the check is cut at the ceiling and so counts as past it.
pub fn read_encoded(reader: &mut (impl Read + Seek), len: u64) -> std::io::Result<Option<Vec<u8>>> {
    let ceiling = ceiling();
    if len > ceiling {
        return Ok(None);
    }
    reader.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    reader.take(ceiling + 1).read_to_end(&mut bytes)?;
    Ok((bytes.len() as u64 <= ceiling).then_some(bytes))
}

/// The text-only result for an image past the encoded ceiling, which is never loaded.
pub fn oversized(mime: &str) -> Processed {
    failed(
        mime,
        &format!(
            "[Image omitted: the file is larger than {} MiB.]",
            ceiling() >> 20
        ),
    )
}

/// What a file that is an image becomes: the text that says so, and the image itself.
pub struct Processed {
    /// The text block: `Read image file [<mime>]` and each hint on its own line.
    pub text: String,
    /// The image block, base64 data and MIME type, when the image could be processed.
    pub image: Option<(String, String)>,
}

/// The MIME type of an image whose first bytes are `prefix`, when the file is one a model reads.
pub fn detect(prefix: &[u8]) -> Option<&'static str> {
    let buffer = &prefix[..prefix.len().min(SNIFF)];
    if starts_with(buffer, &[0xff, 0xd8, 0xff]) {
        return match buffer.get(3) {
            Some(0xf7) => None,
            _ => Some("image/jpeg"),
        };
    }
    if starts_with(buffer, &PNG_SIGNATURE) {
        return match is_png(buffer) && !is_animated_png(buffer) {
            true => Some("image/png"),
            false => None,
        };
    }
    if starts_with_ascii(buffer, 0, "GIF87a") || starts_with_ascii(buffer, 0, "GIF89a") {
        return Some("image/gif");
    }
    if starts_with_ascii(buffer, 0, "RIFF") && starts_with_ascii(buffer, 8, "WEBP") {
        return Some("image/webp");
    }
    if starts_with_ascii(buffer, 0, "BM") && is_bmp(buffer) {
        return Some("image/bmp");
    }
    None
}

/// An image's bytes as they will be sent, or the text saying why they are not.
pub fn process(bytes: Vec<u8>, mime: &str) -> Processed {
    let Some(normalized) = normalize(bytes, mime) else {
        return failed(mime, CONVERT_FAILURE);
    };
    let Some(resized) = resize(&normalized.bytes, &normalized.mime) else {
        return failed(mime, RESIZE_FAILURE);
    };
    let mut hints = Vec::new();
    if let Some(from) = &normalized.converted_from
        && *from != resized.mime
    {
        hints.push(format!(
            "[Image converted from {from} to {}.]",
            resized.mime
        ));
    }
    if let Some(note) = dimension_note(&resized) {
        hints.push(note);
    }
    let mut text = format!("Read image file [{}]", resized.mime);
    for hint in hints {
        text.push('\n');
        text.push_str(&hint);
    }
    Processed {
        text,
        image: Some((resized.data, resized.mime)),
    }
}

/// The text-only result for an image that stays unshown.
fn failed(mime: &str, message: &str) -> Processed {
    Processed {
        text: format!("Read image file [{mime}]\n{message}"),
        image: None,
    }
}

/// An image in a format a model reads, with the format it came from when it had to be converted.
struct Normalized {
    bytes: Vec<u8>,
    mime: String,
    converted_from: Option<String>,
}

/// The bytes as one of the four formats a model reads, converting anything else to PNG.
fn normalize(bytes: Vec<u8>, mime: &str) -> Option<Normalized> {
    let base = base_mime(mime);
    let supported = match base.as_str() {
        "image/png" => Some("image/png"),
        "image/jpeg" | "image/jpg" => Some("image/jpeg"),
        "image/gif" => Some("image/gif"),
        "image/webp" => Some("image/webp"),
        _ => None,
    };
    if let Some(supported) = supported {
        return Some(Normalized {
            bytes,
            mime: supported.to_owned(),
            converted_from: None,
        });
    }
    let image = decode(&bytes)?;
    let mut png = Vec::new();
    image
        .write_to(&mut Cursor::new(&mut png), ImageFormat::Png)
        .ok()?;
    Some(Normalized {
        bytes: png,
        mime: "image/png".to_owned(),
        converted_from: Some(base.to_owned()),
    })
}

/// A MIME type without its parameters, lowercased.
fn base_mime(mime: &str) -> String {
    mime.split(';').next().unwrap_or(mime).trim().to_lowercase()
}

/// The bytes an image is sent as: unchanged when they already fit, else re-encoded smaller.
struct Resized {
    data: String,
    mime: String,
    original_width: u32,
    original_height: u32,
    width: u32,
    height: u32,
    was_resized: bool,
}

/// The image as it will be sent, or `None` when no size fits the limit.
fn resize(bytes: &[u8], mime: &str) -> Option<Resized> {
    let reader = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .ok()?;
    let mut decoder = reader.into_decoder().ok()?;
    let orientation = decoder.orientation().unwrap_or(Orientation::NoTransforms);
    let (width, height) = decoder.dimensions();
    let (original_width, original_height) = oriented_size(width, height, orientation);
    let input_base64 = bytes.len().div_ceil(3) * 4;
    if original_width <= MAX_DIMENSION
        && original_height <= MAX_DIMENSION
        && input_base64 < MAX_BASE64
    {
        return Some(Resized {
            data: base64(bytes),
            mime: mime.to_owned(),
            original_width,
            original_height,
            width: original_width,
            height: original_height,
            was_resized: false,
        });
    }
    let mut image = into_image(decoder)?;
    image.apply_orientation(orientation);
    let mut width = original_width;
    let mut height = original_height;
    if width > MAX_DIMENSION {
        height = scale(height, MAX_DIMENSION, width);
        width = MAX_DIMENSION;
    }
    if height > MAX_DIMENSION {
        width = scale(width, MAX_DIMENSION, height);
        height = MAX_DIMENSION;
    }
    loop {
        let candidate = image.resize_exact(width, height, image::imageops::FilterType::Lanczos3);
        for (bytes, mime) in encodings(&candidate) {
            let data = base64(&bytes);
            if data.len() < MAX_BASE64 {
                return Some(Resized {
                    data,
                    mime,
                    original_width,
                    original_height,
                    width,
                    height,
                    was_resized: true,
                });
            }
        }
        if width == 1 && height == 1 {
            return None;
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
            return None;
        }
        width = next_width;
        height = next_height;
    }
}

/// One scaled side, rounded the way Pi rounds it and never below one pixel: a very thin image
/// keeps a visible side instead of failing, which Pi's own rounding would not.
fn scale(side: u32, limit: u32, other: u32) -> u32 {
    let scaled = (f64::from(side) * f64::from(limit) / f64::from(other)).round();
    scaled.max(1.0).min(f64::from(u32::MAX)) as u32
}

/// The PNG and JPEG candidates for one size, in the order Pi tries them.
fn encodings(image: &DynamicImage) -> Vec<(Vec<u8>, String)> {
    let mut candidates = Vec::with_capacity(QUALITIES.len() + 1);
    let mut png = Vec::new();
    if image
        .write_to(&mut Cursor::new(&mut png), ImageFormat::Png)
        .is_ok()
    {
        candidates.push((png, "image/png".to_owned()));
    }
    let rgb = image.to_rgb8();
    for quality in QUALITIES {
        let mut jpeg = Vec::new();
        if JpegEncoder::new_with_quality(&mut Cursor::new(&mut jpeg), quality)
            .encode_image(&rgb)
            .is_ok()
        {
            candidates.push((jpeg, "image/jpeg".to_owned()));
        }
    }
    candidates
}

/// The note that maps a displayed image back to the file's own pixels.
fn dimension_note(image: &Resized) -> Option<String> {
    if !image.was_resized {
        return None;
    }
    Some(format!(
        "[Image: original {}x{}, displayed at {}x{}. Multiply coordinates by {} to map to original image.]",
        image.original_width,
        image.original_height,
        image.width,
        image.height,
        format_scale(image.original_width, image.width)
    ))
}

/// A ratio at two decimals the way JS `toFixed(2)` rounds it: an exact hundredth tie goes up,
/// so 2250/2000 prints 1.13 while 5350/2000 prints 2.67.
fn format_scale(original_width: u32, displayed_width: u32) -> String {
    let scale = f64::from(original_width) / f64::from(displayed_width);
    // Representable ties here are odd eighths, and multiplying by eight is exact for them.
    let eighths = scale * 8.0;
    if eighths.fract() == 0.0 && eighths as u64 % 2 == 1 {
        let cents = (scale * 100.0).round() as u64;
        format!("{}.{:02}", cents / 100, cents % 100)
    } else {
        format!("{scale:.2}")
    }
}

/// Every pixel of a decoder, with the default allocation limit kept.
fn into_image(mut decoder: impl ImageDecoder) -> Option<DynamicImage> {
    let mut limits = image::Limits::default();
    limits.reserve(decoder.total_bytes()).ok()?;
    decoder.set_limits(limits).ok()?;
    DynamicImage::from_decoder(decoder).ok()
}

/// The dimensions an image shows at once its orientation is applied.
fn oriented_size(width: u32, height: u32, orientation: Orientation) -> (u32, u32) {
    match orientation {
        Orientation::Rotate90
        | Orientation::Rotate270
        | Orientation::Rotate90FlipH
        | Orientation::Rotate270FlipH => (height, width),
        _ => (width, height),
    }
}

/// Every pixel of an image, upright, or `None` when it cannot be read.
fn decode(bytes: &[u8]) -> Option<DynamicImage> {
    let reader = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .ok()?;
    let mut decoder = reader.into_decoder().ok()?;
    let orientation = decoder.orientation().unwrap_or(Orientation::NoTransforms);
    let mut image = into_image(decoder)?;
    image.apply_orientation(orientation);
    Some(image)
}

fn base64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

const PNG_SIGNATURE: [u8; 8] = [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];

fn starts_with(buffer: &[u8], bytes: &[u8]) -> bool {
    buffer.len() >= bytes.len()
        && bytes
            .iter()
            .enumerate()
            .all(|(at, byte)| buffer[at] == *byte)
}

fn starts_with_ascii(buffer: &[u8], offset: usize, text: &str) -> bool {
    let text = text.as_bytes();
    buffer.len() >= offset + text.len()
        && text
            .iter()
            .enumerate()
            .all(|(at, byte)| buffer[offset + at] == *byte)
}

/// Whether the PNG signature is followed by a well-formed IHDR, as Pi requires.
fn is_png(buffer: &[u8]) -> bool {
    buffer.len() >= 16
        && read_uint32_be(buffer, PNG_SIGNATURE.len()) == 13
        && starts_with_ascii(buffer, 12, "IHDR")
}

/// Whether an animated PNG shows a `acTL` chunk before its first `IDAT`.
fn is_animated_png(buffer: &[u8]) -> bool {
    let mut offset = PNG_SIGNATURE.len();
    while offset + 8 <= buffer.len() {
        let chunk_length = read_uint32_be(buffer, offset) as usize;
        let chunk_type = offset + 4;
        if starts_with_ascii(buffer, chunk_type, "acTL") {
            return true;
        }
        if starts_with_ascii(buffer, chunk_type, "IDAT") {
            return false;
        }
        let next = match offset
            .checked_add(8)
            .and_then(|at| at.checked_add(chunk_length))
            .and_then(|at| at.checked_add(4))
        {
            Some(next) => next,
            None => return false,
        };
        if next <= offset || next > buffer.len() {
            return false;
        }
        offset = next;
    }
    false
}

/// Whether a `BM` file has the header Pi accepts as a bitmap.
fn is_bmp(buffer: &[u8]) -> bool {
    if buffer.len() < 26 {
        return false;
    }
    let declared_file_size = read_uint32_le(buffer, 2);
    let pixel_data_offset = read_uint32_le(buffer, 10);
    let dib_header_size = read_uint32_le(buffer, 14);
    if declared_file_size != 0 && declared_file_size < 26 {
        return false;
    }
    if u64::from(pixel_data_offset) < 14 + u64::from(dib_header_size) {
        return false;
    }
    if declared_file_size != 0 && pixel_data_offset >= declared_file_size {
        return false;
    }
    let (color_planes, bits_per_pixel) = match dib_header_size {
        12 => (read_uint16_le(buffer, 22), read_uint16_le(buffer, 24)),
        40..=124 => {
            if buffer.len() < 30 {
                return false;
            }
            (read_uint16_le(buffer, 26), read_uint16_le(buffer, 28))
        }
        _ => return false,
    };
    color_planes == 1 && matches!(bits_per_pixel, 1 | 4 | 8 | 16 | 24 | 32)
}

fn read_uint16_le(buffer: &[u8], offset: usize) -> u32 {
    let at = |index: usize| u32::from(buffer.get(index).copied().unwrap_or(0));
    at(offset) + (at(offset + 1) << 8)
}

fn read_uint32_be(buffer: &[u8], offset: usize) -> u32 {
    let at = |index: usize| u32::from(buffer.get(index).copied().unwrap_or(0));
    at(offset) * 0x100_0000 + (at(offset + 1) << 16) + (at(offset + 2) << 8) + at(offset + 3)
}

fn read_uint32_le(buffer: &[u8], offset: usize) -> u32 {
    let at = |index: usize| u32::from(buffer.get(index).copied().unwrap_or(0));
    at(offset) + (at(offset + 1) << 8) + (at(offset + 2) << 16) + at(offset + 3) * 0x100_0000
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{GenericImageView, Rgb, RgbImage, Rgba, RgbaImage};

    fn encoded(image: &DynamicImage, format: ImageFormat) -> Vec<u8> {
        let mut bytes = Vec::new();
        image
            .write_to(&mut Cursor::new(&mut bytes), format)
            .expect("the fixture encodes");
        bytes
    }

    fn solid(width: u32, height: u32) -> DynamicImage {
        DynamicImage::ImageRgb8(RgbImage::from_pixel(width, height, Rgb([10, 120, 200])))
    }

    /// Pixels no encoder can compress, so the encoded size is what the test decides by.
    fn noisy(width: u32, height: u32, alpha: bool) -> DynamicImage {
        let mut state = 0x2b992ddfa232_u64;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as u8
        };
        if alpha {
            let mut image = RgbaImage::new(width, height);
            for pixel in image.pixels_mut() {
                *pixel = Rgba([next(), next(), next(), next()]);
            }
            DynamicImage::ImageRgba8(image)
        } else {
            let mut image = RgbImage::new(width, height);
            for pixel in image.pixels_mut() {
                *pixel = Rgb([next(), next(), next()]);
            }
            DynamicImage::ImageRgb8(image)
        }
    }

    #[test]
    fn detect_reads_the_five_formats() {
        assert_eq!(
            detect(&encoded(&solid(4, 4), ImageFormat::Png)),
            Some("image/png")
        );
        assert_eq!(
            detect(&encoded(&solid(4, 4), ImageFormat::Jpeg)),
            Some("image/jpeg")
        );
        assert_eq!(
            detect(&encoded(&solid(4, 4), ImageFormat::Gif)),
            Some("image/gif")
        );
        assert_eq!(
            detect(&encoded(&solid(4, 4), ImageFormat::Bmp)),
            Some("image/bmp")
        );
        let mut webp = b"RIFF\x20\x00\x00\x00WEBPVP8 ".to_vec();
        webp.extend_from_slice(&[0u8; 16]);
        assert_eq!(detect(&webp), Some("image/webp"));
    }

    #[test]
    fn detect_refuses_a_jpeg_ls_header_and_an_animated_png() {
        assert_eq!(detect(&[0xff, 0xd8, 0xff, 0xf7, 0, 0]), None);
        // A real PNG with an `acTL` chunk inserted before its image data.
        let png = encoded(&solid(4, 4), ImageFormat::Png);
        let mut animated = png[..33].to_vec();
        animated.extend_from_slice(&[0, 0, 0, 8]);
        animated.extend_from_slice(b"acTL");
        animated.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 1]);
        animated.extend_from_slice(&[0, 0, 0, 0]);
        animated.extend_from_slice(&png[33..]);
        assert_eq!(detect(&png), Some("image/png"));
        assert_eq!(detect(&animated), None);
    }

    #[test]
    fn an_image_past_the_ceiling_is_not_read_at_all() {
        /// A file that fails the test if anything reads or seeks it.
        struct Refuses;

        impl Read for Refuses {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                panic!("a file past the ceiling was read");
            }
        }

        impl Seek for Refuses {
            fn seek(&mut self, _: SeekFrom) -> std::io::Result<u64> {
                panic!("a file past the ceiling was sought");
            }
        }

        let bytes = read_encoded(&mut Refuses, ceiling() + 1).unwrap();
        assert!(bytes.is_none(), "a file past the ceiling is not loaded");
    }

    #[test]
    fn process_passes_a_small_png_through() {
        let png = encoded(&solid(4, 4), ImageFormat::Png);
        let processed = process(png.clone(), "image/png");
        assert_eq!(processed.text, "Read image file [image/png]");
        let (data, mime) = processed.image.expect("the image is sent");
        assert_eq!(mime, "image/png");
        assert_eq!(data, base64(&png));
    }

    #[test]
    fn process_scales_a_wide_image_to_the_box() {
        let png = encoded(&solid(3000, 1000), ImageFormat::Png);
        let processed = process(png, "image/png");
        assert_eq!(
            processed.text,
            "Read image file [image/png]\n[Image: original 3000x1000, displayed at 2000x667. Multiply coordinates by 1.50 to map to original image.]"
        );
        let (data, mime) = processed.image.expect("the image is sent");
        assert_eq!(mime, "image/png");
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(data)
            .expect("the data is base64");
        let image = decode(&bytes).expect("the result decodes");
        assert_eq!(image.dimensions(), (2000, 667));
    }

    #[test]
    fn process_sends_a_large_image_as_jpeg_under_the_limit() {
        let png = encoded(&noisy(2000, 2000, false), ImageFormat::Png);
        assert!(png.len().div_ceil(3) * 4 >= MAX_BASE64);
        let processed = process(png, "image/png");
        let (data, mime) = processed.image.expect("the image is sent");
        assert_eq!(mime, "image/jpeg");
        assert!(data.len() < MAX_BASE64);
        assert_eq!(
            processed.text,
            "Read image file [image/jpeg]\n[Image: original 2000x2000, displayed at 2000x2000. Multiply coordinates by 1.00 to map to original image.]"
        );
    }

    #[test]
    fn process_drops_alpha_when_it_must_become_jpeg() {
        let png = encoded(&noisy(2000, 2000, true), ImageFormat::Png);
        let processed = process(png, "image/png");
        let (data, mime) = processed.image.expect("the image is sent");
        assert_eq!(mime, "image/jpeg");
        assert!(data.len() < MAX_BASE64);
    }

    #[test]
    fn process_converts_a_bitmap_to_png() {
        let bmp = encoded(&solid(40, 20), ImageFormat::Bmp);
        let processed = process(bmp, "image/bmp");
        assert_eq!(
            processed.text,
            "Read image file [image/png]\n[Image converted from image/bmp to image/png.]"
        );
        let (_, mime) = processed.image.expect("the image is sent");
        assert_eq!(mime, "image/png");
    }

    #[test]
    fn process_reports_an_undecodable_image() {
        let mut bytes = PNG_SIGNATURE.to_vec();
        bytes.extend_from_slice(&13u32.to_be_bytes());
        bytes.extend_from_slice(b"IHDR");
        bytes.extend_from_slice(&[0, 0, 0, 4, 0, 0, 0, 4, 8, 6, 0, 0, 0]);
        bytes.extend_from_slice(&[0, 0, 0, 0]);
        assert_eq!(detect(&bytes), Some("image/png"));
        let processed = process(bytes, "image/png");
        assert_eq!(
            processed.text,
            "Read image file [image/png]\n[Image omitted: could not be resized below the inline image size limit.]"
        );
        assert!(processed.image.is_none());
    }

    #[test]
    fn process_rounds_the_scale_note_like_javascript() {
        // An exact hundredth tie goes up: 2250/2000 is 1.125.
        let wide = encoded(&solid(2250, 2), ImageFormat::Png);
        assert_eq!(
            process(wide, "image/png").text,
            "Read image file [image/png]\n[Image: original 2250x2, displayed at 2000x2. Multiply coordinates by 1.13 to map to original image.]"
        );
        // A tie the binary value misses by a hair stays down: 5350/2000 is 2.675.
        let taller = encoded(&solid(5350, 2), ImageFormat::Png);
        assert_eq!(
            process(taller, "image/png").text,
            "Read image file [image/png]\n[Image: original 5350x2, displayed at 2000x1. Multiply coordinates by 2.67 to map to original image.]"
        );
        // A very thin image keeps a visible side instead of failing.
        let thin = encoded(&solid(5000, 1), ImageFormat::Png);
        assert_eq!(
            process(thin, "image/png").text,
            "Read image file [image/png]\n[Image: original 5000x1, displayed at 2000x1. Multiply coordinates by 2.50 to map to original image.]"
        );
    }

    #[test]
    fn detect_refuses_a_bitmap_header_that_would_overflow() {
        // Pi compares the pixel offset with 14 + the DIB size as numbers, so a DIB size near
        // u32::MAX is simply too large to be a bitmap; it must not wrap.
        for dib in [0xffff_fff1u32, 0xffff_fff2, 0xffff_fff3, 0xffff_ffff] {
            let mut header = b"BM".to_vec();
            header.extend_from_slice(&0u32.to_le_bytes());
            header.extend_from_slice(&[0; 4]);
            header.extend_from_slice(&0u32.to_le_bytes());
            header.extend_from_slice(&dib.to_le_bytes());
            header.resize(30, 0);
            assert_eq!(detect(&header), None, "DIB {dib:#x}");
        }
    }

    #[test]
    fn detect_sees_only_the_first_four_thousand_one_hundred_bytes() {
        let png = encoded(&solid(4, 4), ImageFormat::Png);
        let mut late = png[..33].to_vec();
        late.extend_from_slice(&4100u32.to_be_bytes());
        late.extend_from_slice(b"tEXt");
        late.extend_from_slice(&[0u8; 4100]);
        late.extend_from_slice(&[0, 0, 0, 0]);
        late.extend_from_slice(&[0, 0, 0, 8]);
        late.extend_from_slice(b"acTL");
        late.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 1]);
        late.extend_from_slice(&[0, 0, 0, 0]);
        late.extend_from_slice(&png[33..]);
        assert!(late.len() > 4200);
        // The `acTL` chunk sits past the 4100-byte prefix Pi reads, so the file is still a PNG.
        assert_eq!(detect(&late), Some("image/png"));
    }

    #[test]
    fn process_turns_the_pixels_the_way_the_exif_orientation_says() {
        use image::ImageEncoder;
        // A red left half and a blue right half, two pixels tall, tagged orientation 6.
        let mut pixels = RgbImage::new(3000, 2);
        for y in 0..2 {
            for x in 0..3000 {
                let color = if x < 1500 {
                    Rgb([255, 0, 0])
                } else {
                    Rgb([0, 0, 255])
                };
                pixels.put_pixel(x, y, color);
            }
        }
        let exif: &[u8] = b"II*\0\x08\0\0\0\x01\0\x12\x01\x03\0\x01\0\0\0\x06\0\0\0\0\0\0\0";
        let mut jpeg = Vec::new();
        let mut sink = Cursor::new(&mut jpeg);
        let mut encoder = JpegEncoder::new_with_quality(&mut sink, 95);
        encoder
            .set_exif_metadata(exif.to_vec())
            .expect("the encoder takes the tag");
        encoder.encode_image(&pixels).expect("the fixture encodes");
        let processed = process(jpeg, "image/jpeg");
        assert!(
            processed
                .text
                .contains("original 2x3000, displayed at 1x2000"),
            "{}",
            processed.text
        );
        let (data, mime) = processed.image.expect("the image is sent");
        assert_eq!(mime, "image/png");
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(data)
            .expect("the data is base64");
        let image = decode(&bytes).expect("the result decodes");
        assert_eq!(image.dimensions(), (1, 2000));
        let pixels = image.to_rgb8();
        let top = pixels.get_pixel(0, 0);
        let bottom = pixels.get_pixel(0, 1999);
        assert!(top[0] > 150 && top[2] < 100, "top {top:?}");
        assert!(bottom[2] > 150 && bottom[0] < 100, "bottom {bottom:?}");
    }
}
