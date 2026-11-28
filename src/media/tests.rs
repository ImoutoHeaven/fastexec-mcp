use super::compress::{
    CODEC_WORKSPACE, MAX_COMPRESSED_BASE64, decode_checked, decode_peak, first_fitting_encoding,
    header_peak, inspect_and_check_bounds, orientation_peak, oriented_size, png_chunks,
    png_encoding_peak, png_without_unused_metadata, reserve_compression, resize_peak,
};
use super::detect::PNG_SIGNATURE;
#[cfg(windows)]
use super::uri::windows_os_bytes;
use super::uri::{WinUriPart, format_windows_uri, windows_utf16_bytes};
use super::*;
use crate::lease::BytePool;
use base64::Engine;
use image::metadata::Orientation;
use image::{DynamicImage, GenericImageView, ImageFormat, Rgb, RgbImage, Rgba, RgbaImage};
use std::io::Cursor;
use std::path::Path;

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
        let rest = if raw.len() >= 8 && raw.as_bytes()[..8].eq_ignore_ascii_case(b"\\\\?\\UNC\\") {
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
