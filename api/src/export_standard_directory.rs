use napi_derive::napi;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::dicom_deidentification::dicom_deidentification::deidentify_2d_dicom;
use crate::dicom_deidentification::dicom_deidentification::deidentify_2d_dicom_with_ocr;

use crate::tools_path::{
    resolve_dcm2niix_path, resolve_dcmtk_bin_path, resolve_dcmtk_dictionary_path,
    resolve_ffmpeg_path,
};

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct ParsedDirectoryJson {
    level: u8,
    #[serde(rename = "PatientName")]
    patient_name: String,
    #[serde(rename = "StudyDescription")]
    study_description: String,
    #[serde(rename = "SeriesDescription")]
    series_description: String,
    #[serde(rename = "SeriesNumber")]
    series_number: i64,
    #[serde(rename = "fileName")]
    file_name: String,
    #[serde(rename = "filePath")]
    file_path: String,
    studies: HashMap<String, StudyNode>,
    #[serde(rename = "studiesInOrder")]
    studies_in_order: Vec<KeyRef>,
    series: HashMap<String, SeriesNode>,
    #[serde(rename = "seriesInOrder")]
    series_in_order: Vec<KeyRef>,
    instances: HashMap<String, InstanceNode>,
    #[serde(rename = "instancesInOrder")]
    instances_in_order: Vec<KeyRef>,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(default)]
struct KeyRef {
    key: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct StudyNode {
    #[serde(rename = "StudyDescription")]
    study_description: String,
    series: HashMap<String, SeriesNode>,
    #[serde(rename = "seriesInOrder")]
    series_in_order: Vec<KeyRef>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct SeriesNode {
    #[serde(rename = "SeriesDescription")]
    series_description: String,
    #[serde(rename = "SeriesNumber")]
    series_number: i64,
    instances: HashMap<String, InstanceNode>,
    #[serde(rename = "instancesInOrder")]
    instances_in_order: Vec<KeyRef>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct InstanceNode {
    #[serde(rename = "fileName")]
    file_name: String,
    #[serde(rename = "filePath")]
    file_path: String,
}

#[repr(u32)]
enum DeidentifyExportType {
    Copy = 0,
    TagDeidentify = 100,
    TagOcrDeidentify = 101,
    Nifti = 200,
    Jpeg = 300,
    Mp4 = 400,
}

#[napi]
/// Exports a parsed standard directory JSON into a patient/study/series folder structure.
///
/// Export types:
/// - 0: Copy original DICOM files
/// - 100: De-identify with tags only and export DICOM files
/// - 101: De-identify with tags + OCR and export DICOM files
/// - 200: Convert each series to NIfTI via dcmdjpeg + dcm2niix
/// - 300: Convert each instance to JPEG via dcm2img
/// - 400: Convert each series to MP4 via dcm2img (frames) + ffmpeg
pub fn export_parsed_standard_directory(
    json_utf8_content: String,
    export_root_dir: String,
    export_type: u32,
) -> napi::Result<String> {
    // Validate export root directory.
    let root_path = Path::new(&export_root_dir);
    if !root_path.exists() {
        return Err(napi::Error::from_reason(format!(
            "Export root directory does not exist: {}",
            export_root_dir
        )));
    }
    if !root_path.is_dir() {
        return Err(napi::Error::from_reason(format!(
            "Export root path is not a directory: {}",
            export_root_dir
        )));
    }

    let parsed_input: ParsedDirectoryJson =
        serde_json::from_str(&json_utf8_content).map_err(|e| {
            napi::Error::from_reason(format!("Failed to parse input JSON string: {}", e))
        })?;
    let (input_level, parsed) = normalize_parsed_directory(parsed_input)?;

    let output_root_dir = if input_level == 1 {
        create_unique_subdir(
            root_path,
            &non_empty_or_default(&parsed.patient_name, "Unknown Patient"),
        )?
    } else {
        root_path.to_path_buf()
    };

    // Traverse studies in stable order.
    for study_ref in ordered_keys(&parsed.studies, &parsed.studies_in_order) {
        let study = parsed.studies.get(&study_ref.key).ok_or_else(|| {
            napi::Error::from_reason(format!("Study key not found in studies: {}", study_ref.key))
        })?;

        let study_output_dir = if input_level <= 2 {
            create_unique_subdir(
                &output_root_dir,
                &non_empty_or_default(&study.study_description, "Unknown Study"),
            )?
        } else {
            output_root_dir.clone()
        };

        // Traverse series in stable order.
        for series_ref in ordered_keys(&study.series, &study.series_in_order) {
            let series = study.series.get(&series_ref.key).ok_or_else(|| {
                napi::Error::from_reason(format!(
                    "Series key not found in series: {}",
                    series_ref.key
                ))
            })?;

            let series_dir_name = format!(
                "{} #{}",
                non_empty_or_default(&series.series_description, "Unknown Series"),
                series.series_number
            );

            let series_output_dir = if input_level <= 3 {
                create_unique_subdir(&study_output_dir, &series_dir_name)?
            } else {
                study_output_dir.clone()
            };

            // nifti
            // convert whole series to NIfTI and continue.
            if export_type == DeidentifyExportType::Nifti as u32 {
                let instance_count = if !series.instances_in_order.is_empty() {
                    series.instances_in_order.len()
                } else {
                    series.instances.len()
                };

                if instance_count <= 1 {
                    return Err(napi::Error::from_reason(format!(
                        "NIfTI export skipped for series '{}' (#{}): only {} DICOM file(s).",
                        non_empty_or_default(&series.series_description, "Unknown Series"),
                        series.series_number,
                        instance_count
                    )));
                }

                export_series_to_nifti(series, &series_output_dir)?;
                continue;
            }

            // mp4
            // convert whole series to MP4 and continue.
            if export_type == DeidentifyExportType::Mp4 as u32 {
                export_series_to_mp4(
                    series,
                    &series_output_dir
                )?;
                continue;
            }

            // copy, de-identify, or jpeg exports
            // Types 0/1/3: process instance-by-instance.
            for instance_ref in ordered_keys(&series.instances, &series.instances_in_order) {
                let instance = series.instances.get(&instance_ref.key).ok_or_else(|| {
                    napi::Error::from_reason(format!(
                        "Instance key not found in instances: {}",
                        instance_ref.key
                    ))
                })?;

                let src = Path::new(&instance.file_path);
                if !src.exists() {
                    return Err(napi::Error::from_reason(format!(
                        "Source file does not exist: {}",
                        instance.file_path
                    )));
                }

                let dst = series_output_dir.join(&instance.file_name);

                // copy
                if export_type == DeidentifyExportType::Copy as u32 {
                    fs::copy(src, &dst).map_err(|e| {
                        napi::Error::from_reason(format!(
                            "Failed to copy file from '{}' to '{}': {}",
                            instance.file_path,
                            dst.to_string_lossy(),
                            e
                        ))
                    })?;
                    continue;
                }

                // tag de-identify
                if export_type == DeidentifyExportType::TagDeidentify as u32 {
                    deidentify_2d_dicom(
                        instance.file_path.clone(),
                        dst.to_string_lossy().to_string(),
                    )
                    .map_err(napi::Error::from_reason)?;
                    continue;
                }

                // tag + OCR de-identify
                if export_type == DeidentifyExportType::TagOcrDeidentify as u32 {
                    deidentify_2d_dicom_with_ocr(
                        instance.file_path.clone(),
                        dst.to_string_lossy().to_string(),
                    )
                    .map_err(napi::Error::from_reason)?;
                    continue;
                }

                // jpeg
                if export_type == DeidentifyExportType::Jpeg as u32 {
                    let base_name = Path::new(&instance.file_name)
                        .file_stem()
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_else(|| non_empty_or_default(&instance.file_name, "instance"));
                    let jpeg_file_name = format!("{}.jpg", base_name);
                    let jpeg_dst = series_output_dir.join(jpeg_file_name);
                    run_dcm2img_jpeg(src, &jpeg_dst)?;
                    continue;
                }

                return Err(napi::Error::from_reason(format!(
                    "Invalid export type: {}. ",
                    export_type
                )));
            }
        }
    }

    Ok(output_root_dir.to_string_lossy().to_string())
}

/// Normalizes parsed directory input to ensure consistent structure and valid level (1-4).
fn normalize_parsed_directory(
    input: ParsedDirectoryJson,
) -> napi::Result<(u8, ParsedDirectoryJson)> {
    let level = if input.level == 0 { 1 } else { input.level };

    if !(1..=4).contains(&level) {
        return Err(napi::Error::from_reason(format!(
            "Invalid input level: {}. Use 1 (patient), 2 (study), 3 (series), or 4 (instance).",
            level
        )));
    }

    if level == 1 {
        return Ok((level, input));
    }

    if level == 2 {
        let study_key = synthetic_key("study", &input.study_description);
        let study = StudyNode {
            study_description: input.study_description.clone(),
            series: input.series,
            series_in_order: input.series_in_order,
        };

        let mut studies = HashMap::new();
        studies.insert(study_key.clone(), study);

        return Ok((
            level,
            ParsedDirectoryJson {
                level,
                patient_name: String::new(),
                study_description: String::new(),
                series_description: String::new(),
                series_number: 0,
                file_name: String::new(),
                file_path: String::new(),
                studies,
                studies_in_order: vec![KeyRef { key: study_key }],
                series: HashMap::new(),
                series_in_order: Vec::new(),
                instances: HashMap::new(),
                instances_in_order: Vec::new(),
            },
        ));
    }

    if level == 3 {
        let series_key = synthetic_key("series", &input.series_description);
        let series = SeriesNode {
            series_description: input.series_description.clone(),
            series_number: input.series_number,
            instances: input.instances,
            instances_in_order: input.instances_in_order,
        };

        let mut series_map = HashMap::new();
        series_map.insert(series_key.clone(), series);

        let study_key = synthetic_key("study", &input.study_description);
        let study = StudyNode {
            study_description: input.study_description,
            series: series_map,
            series_in_order: vec![KeyRef { key: series_key }],
        };

        let mut studies = HashMap::new();
        studies.insert(study_key.clone(), study);

        return Ok((
            level,
            ParsedDirectoryJson {
                level,
                patient_name: String::new(),
                study_description: String::new(),
                series_description: String::new(),
                series_number: 0,
                file_name: String::new(),
                file_path: String::new(),
                studies,
                studies_in_order: vec![KeyRef { key: study_key }],
                series: HashMap::new(),
                series_in_order: Vec::new(),
                instances: HashMap::new(),
                instances_in_order: Vec::new(),
            },
        ));
    }

    let instance_key = synthetic_key("instance", &input.file_name);
    let instance = InstanceNode {
        file_name: input.file_name,
        file_path: input.file_path,
    };

    let mut instances = HashMap::new();
    instances.insert(instance_key.clone(), instance);

    let series_key = synthetic_key("series", &input.series_description);
    let series = SeriesNode {
        series_description: input.series_description,
        series_number: input.series_number,
        instances,
        instances_in_order: vec![KeyRef { key: instance_key }],
    };

    let mut series_map = HashMap::new();
    series_map.insert(series_key.clone(), series);

    let study_key = synthetic_key("study", &input.study_description);
    let study = StudyNode {
        study_description: input.study_description,
        series: series_map,
        series_in_order: vec![KeyRef { key: series_key }],
    };

    let mut studies = HashMap::new();
    studies.insert(study_key.clone(), study);

    Ok((
        level,
        ParsedDirectoryJson {
            level,
            patient_name: String::new(),
            study_description: String::new(),
            series_description: String::new(),
            series_number: 0,
            file_name: String::new(),
            file_path: String::new(),
            studies,
            studies_in_order: vec![KeyRef { key: study_key }],
            series: HashMap::new(),
            series_in_order: Vec::new(),
            instances: HashMap::new(),
            instances_in_order: Vec::new(),
        },
    ))
}

fn synthetic_key(prefix: &str, name: &str) -> String {
    format!("{}:{}", prefix, non_empty_or_default(name, "default"))
}

fn ordered_keys<T>(items: &HashMap<String, T>, in_order: &[KeyRef]) -> Vec<KeyRef> {
    if !in_order.is_empty() {
        return in_order.to_vec();
    }

    let mut keys: Vec<String> = items.keys().cloned().collect();
    keys.sort();
    keys.into_iter().map(|key| KeyRef { key }).collect()
}

/// Converts one series to NIfTI by first normalizing DICOM files with dcmdjpeg,
/// then invoking dcm2niix on a temporary input directory.
fn export_series_to_nifti(series: &SeriesNode, series_dir: &Path) -> napi::Result<()> {
    let temp_input_dir = create_temp_series_input_dir()?;

    let convert_result = (|| -> napi::Result<()> {
        for (index, instance_ref) in series.instances_in_order.iter().enumerate() {
            let instance = series.instances.get(&instance_ref.key).ok_or_else(|| {
                napi::Error::from_reason(format!(
                    "Instance key not found in instances: {}",
                    instance_ref.key
                ))
            })?;

            let src = Path::new(&instance.file_path);
            if !src.exists() {
                return Err(napi::Error::from_reason(format!(
                    "Source file does not exist: {}",
                    instance.file_path
                )));
            }

            let temp_name = format!(
                "{:08}_{}",
                index,
                non_empty_or_default(&instance.file_name, "instance.dcm")
            );
            let temp_dst = temp_input_dir.join(temp_name);
            run_dcmdjpeg(src, &temp_dst)?;
        }

        run_dcm2niix(&temp_input_dir, series_dir)
    })();

    if temp_input_dir.exists() {
        let _ = fs::remove_dir_all(&temp_input_dir);
    }

    convert_result
}

/// Creates a unique temporary directory used as dcm2niix input for one series.
fn create_temp_series_input_dir() -> napi::Result<PathBuf> {
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);

    let dir = env::temp_dir().join(format!(
        "pmtaro-dcm2niix-{}-{}",
        std::process::id(),
        now_nanos
    ));

    fs::create_dir_all(&dir).map_err(|e| {
        napi::Error::from_reason(format!(
            "Failed to create temporary directory for NIfTI export '{}': {}",
            dir.to_string_lossy(),
            e
        ))
    })?;

    Ok(dir)
}

/// Creates a unique temporary directory for JPEG frame files used by MP4 export.
fn create_temp_series_frames_dir() -> napi::Result<PathBuf> {
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);

    let dir = env::temp_dir().join(format!(
        "pmtaro-mp4-frames-{}-{}",
        std::process::id(),
        now_nanos
    ));

    fs::create_dir_all(&dir).map_err(|e| {
        napi::Error::from_reason(format!(
            "Failed to create temporary directory for MP4 frames '{}': {}",
            dir.to_string_lossy(),
            e
        ))
    })?;

    Ok(dir)
}

/// Converts one series into a single MP4 file by generating ordered JPEG frames
/// with dcm2img and then encoding them with ffmpeg.
fn export_series_to_mp4(
    series: &SeriesNode,
    series_dir: &Path,
) -> napi::Result<()> {
    let temp_frames_dir = create_temp_series_frames_dir()?;

    let convert_result = (|| -> napi::Result<()> {
        for (index, instance_ref) in series.instances_in_order.iter().enumerate() {
            let instance = series.instances.get(&instance_ref.key).ok_or_else(|| {
                napi::Error::from_reason(format!(
                    "Instance key not found in instances: {}",
                    instance_ref.key
                ))
            })?;

            let src = Path::new(&instance.file_path);
            if !src.exists() {
                return Err(napi::Error::from_reason(format!(
                    "Source file does not exist: {}",
                    instance.file_path
                )));
            }

            let frame_dst = temp_frames_dir.join(format!("{:08}.jpg", index));
            run_dcm2img_jpeg(src, &frame_dst)?;
        }

        let mp4_output = series_dir.join("series.mp4");
        run_ffmpeg_jpeg_to_mp4(&temp_frames_dir, &mp4_output)
    })();

    if temp_frames_dir.exists() {
        let _ = fs::remove_dir_all(&temp_frames_dir);
    }

    convert_result
}

/// Runs dcm2niix to convert a DICOM directory into NIfTI outputs.
fn run_dcm2niix(input_dir: &Path, output_dir: &Path) -> napi::Result<()> {
    let binary_path = resolve_dcm2niix_path()?;

    let output = Command::new(&binary_path)
        .arg("-o")
        .arg(output_dir)
        .arg("-z")
        .arg("y")
        .arg(input_dir)
        .output()
        .map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to execute dcm2niix '{}': {}",
                binary_path.to_string_lossy(),
                e
            ))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(napi::Error::from_reason(format!(
            "dcm2niix failed (code: {:?})\nstdout: {}\nstderr: {}",
            output.status.code(),
            stdout,
            stderr
        )));
    }

    Ok(())
}

/// Runs dcmdjpeg to decompress/normalize a DICOM file.
fn run_dcmdjpeg(

    input_path: &Path,
    output_path: &Path,
) -> napi::Result<()> {
    let binary_path = resolve_dcmtk_bin_path("dcmdjpeg")?;
    let dictionary_path = resolve_dcmtk_dictionary_path()?;

    let output = Command::new(&binary_path)
        .env("DCMDICTPATH", &dictionary_path)
        .arg(input_path)
        .arg(output_path)
        .output()
        .map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to execute dcmdjpeg '{}': {}",
                binary_path.to_string_lossy(),
                e
            ))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(napi::Error::from_reason(format!(
      "dcmdjpeg failed (code: {:?}) for input '{}' and output '{}' using DCMDICTPATH='{}'\nstdout: {}\nstderr: {}",
      output.status.code(),
      input_path.to_string_lossy(),
      output_path.to_string_lossy(),
      dictionary_path.to_string_lossy(),
      stdout,
      stderr
    )));
    }

    Ok(())
}

/// Runs dcm2img to export one input DICOM as a JPEG image.
fn run_dcm2img_jpeg(
    input_path: &Path,
    output_path: &Path,
) -> napi::Result<()> {
    let binary_path = resolve_dcmtk_bin_path("dcm2img")?;
    let dictionary_path = resolve_dcmtk_dictionary_path()?;

    // try +Wi 1
    let try_with_voi_window = Command::new(&binary_path)
        .env("DCMDICTPATH", &dictionary_path)
        .arg("+Wi")
        .arg("1")
        .arg("+oj")
        .arg(input_path)
        .arg(output_path)
        .output()
        .map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to execute dcm2img '{}': {}",
                binary_path.to_string_lossy(),
                e
            ))
        })?;

    if try_with_voi_window.status.success() {
        return Ok(());
    }

    // try +Wm
    let try_with_min_max = Command::new(&binary_path)
        .env("DCMDICTPATH", &dictionary_path)
        .arg("+Wm")
        .arg("+oj")
        .arg(input_path)
        .arg(output_path)
        .output()
        .map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to execute dcm2img '{}': {}",
                binary_path.to_string_lossy(),
                e
            ))
        })?;

    if !try_with_min_max.status.success() {
        let stderr_voi = String::from_utf8_lossy(&try_with_voi_window.stderr);
        let stdout_voi = String::from_utf8_lossy(&try_with_voi_window.stdout);
        let stderr_minmax = String::from_utf8_lossy(&try_with_min_max.stderr);
        let stdout_minmax = String::from_utf8_lossy(&try_with_min_max.stdout);

        return Err(napi::Error::from_reason(format!(
      "dcm2img failed for input '{}' and output '{}' using DCMDICTPATH='{}'.\nAttempt 1 (+Wi 1) code: {:?}\nstdout: {}\nstderr: {}\nAttempt 2 (+Wm) code: {:?}\nstdout: {}\nstderr: {}",
      input_path.to_string_lossy(),
      output_path.to_string_lossy(),
      dictionary_path.to_string_lossy(),
      try_with_voi_window.status.code(),
      stdout_voi,
      stderr_voi,
      try_with_min_max.status.code(),
      stdout_minmax,
      stderr_minmax
    )));
    }

    Ok(())
}

/// Runs ffmpeg to encode sequential JPEG frames into an H.264 MP4 file.
///
/// The pad filter ensures width/height are even so libx264 can encode safely.
fn run_ffmpeg_jpeg_to_mp4(
    input_frames_dir: &Path,
    output_mp4_path: &Path,
) -> napi::Result<()> {
    let input_pattern = input_frames_dir.join("%08d.jpg");

    let ffmpeg_path = resolve_ffmpeg_path()?;
    let output = Command::new(&ffmpeg_path)
        .arg("-y")
        .arg("-framerate")
        .arg("10")
        .arg("-i")
        .arg(&input_pattern)
        .arg("-vf")
        .arg("pad=width=ceil(iw/2)*2:height=ceil(ih/2)*2:x=0:y=0:color=black")
        .arg("-c:v")
        .arg("libx264")
        .arg("-pix_fmt")
        .arg("yuv420p")
        .arg(output_mp4_path)
        .output()
        .map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to execute ffmpeg '{}': {}",
                ffmpeg_path.to_string_lossy(),
                e
            ))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(napi::Error::from_reason(format!(
            "ffmpeg failed (code: {:?}) for input '{}' and output '{}'
stdout: {}
stderr: {}",
            output.status.code(),
            input_pattern.to_string_lossy(),
            output_mp4_path.to_string_lossy(),
            stdout,
            stderr
        )));
    }

    Ok(())
}

/// Creates a child directory under `parent`, appending numeric suffixes when needed
/// to avoid name collisions.
fn create_unique_subdir(parent: &Path, base_name: &str) -> napi::Result<PathBuf> {
    let trimmed = non_empty_or_default(base_name, "Untitled");

    let mut candidate = parent.join(&trimmed);
    let mut suffix = 1;

    while candidate.exists() {
        candidate = parent.join(format!("{} {}", trimmed, suffix));
        suffix += 1;
    }

    fs::create_dir_all(&candidate).map_err(|e| {
        napi::Error::from_reason(format!(
            "Failed to create directory '{}': {}",
            candidate.to_string_lossy(),
            e
        ))
    })?;

    Ok(candidate)
}

/// Returns a trimmed string, or the fallback value when input is empty.
fn non_empty_or_default(value: &str, default_value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        default_value.to_string()
    } else {
        trimmed.to_string()
    }
}
