use image::{DynamicImage, ImageFormat};
use serde::Deserialize;
use std::env;
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::process_utils::new_hidden_command;
use crate::tools_path::{resolve_newbee_ocr_binary_path, resolve_newbee_ocr_models_path};

#[derive(Clone, Copy, Debug)]
pub struct OcrRect {
    pub x: u32,
    pub y: u32,
    pub width: u32,
    pub height: u32,
}

#[derive(Clone, Debug)]
pub struct OcrTextBox {
    pub text: String,
    pub confidence: f32,
    pub rect: OcrRect,
}

#[derive(Deserialize)]
struct OcrOutput {
    results: Vec<OcrOutputItem>,
}

#[derive(Deserialize)]
struct OcrOutputItem {
    text: String,
    confidence: f32,
    bbox: OcrOutputBox,
}

#[derive(Deserialize)]
struct OcrOutputBox {
    x: u32,
    y: u32,
    width: u32,
    height: u32,
}

fn dynamic_image_to_png_bytes(image: &DynamicImage) -> Result<Vec<u8>, String> {
    let mut buffer = Cursor::new(Vec::new());
    DynamicImage::ImageLuma8(image.to_luma8())
        .write_to(&mut buffer, ImageFormat::Png)
        .map_err(|err| format!("Failed to encode OCR image: {err}"))?;
    Ok(buffer.into_inner())
}

fn build_temp_path(extension: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();

    env::temp_dir().join(format!(
        "pmtaro-newbee-ocr-{}-{nanos}.{extension}",
        std::process::id()
    ))
}

pub fn run_ocr(image: &DynamicImage) -> Result<Vec<OcrTextBox>, String> {
    let png_bytes = dynamic_image_to_png_bytes(image)?;
    let binary_path = resolve_newbee_ocr_binary_path()
        .map_err(|err| format!("Failed to resolve nbocr path: {err}"))?;
    let models_path = resolve_newbee_ocr_models_path()
        .map_err(|err| format!("Failed to resolve nbocr models path: {err}"))?;

    let input_path = build_temp_path("png");
    let output_path = build_temp_path("json");

    fs::write(&input_path, png_bytes).map_err(|err| {
        format!(
            "Failed to write temporary OCR image '{}': {err}",
            input_path.to_string_lossy()
        )
    })?;

    let output = new_hidden_command(&binary_path)
        .arg("r")
        .arg(&input_path)
        .arg("-m")
        .arg(&models_path)
        .arg("-f")
        .arg("json")
        .arg("-o")
        .arg(&output_path)
        .arg("--precision")
        .arg("fast")
        .output()
        .map_err(|err| {
            format!(
                "Failed to execute nbocr '{}': {err}",
                binary_path.to_string_lossy()
            )
        });

    let cleanup_result = (|| {
        let output = output?;

        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!(
                "nbocr failed (code: {:?})\nstdout: {}\nstderr: {}",
                output.status.code(),
                stdout,
                stderr
            ));
        }

        let json_content = fs::read_to_string(&output_path).map_err(|err| {
            format!(
                "Failed to read OCR output '{}': {err}",
                output_path.to_string_lossy()
            )
        })?;

        let ocr_output: OcrOutput = serde_json::from_str(&json_content)
            .map_err(|err| format!("Failed to parse OCR output JSON: {err}"))?;

        Ok(ocr_output
            .results
            .into_iter()
            .map(|item| OcrTextBox {
                text: item.text,
                confidence: item.confidence,
                rect: OcrRect {
                    x: item.bbox.x,
                    y: item.bbox.y,
                    width: item.bbox.width,
                    height: item.bbox.height,
                },
            })
            .collect())
    })();

    let _ = fs::remove_file(&input_path);
    let _ = fs::remove_file(&output_path);

    cleanup_result
}