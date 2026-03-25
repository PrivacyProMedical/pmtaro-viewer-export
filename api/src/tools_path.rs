use std::env;
use std::path::PathBuf;

pub fn resolve_runtime_base_dir() -> napi::Result<PathBuf> {
    let base_dir = if let Some(module_dir) = crate::get_native_module_dir() {
        module_dir.clone()
    } else {
        env::current_dir().map_err(|e| {
            napi::Error::from_reason(format!("Failed to get current working directory: {}", e))
        })?
    }
    .ancestors()
    .nth(3) // @pmt/export/api
    .map(|path| path.to_path_buf())
    .unwrap_or_else(|| {
        crate::get_native_module_dir()
            .cloned()
            .unwrap_or_else(|| env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
    });

    Ok(base_dir)
}

pub fn resolve_newbee_ocr_binary_path() -> napi::Result<PathBuf> {
    let base_dir = resolve_runtime_base_dir()?;

    #[cfg(target_os = "macos")]
    let relative_binary = PathBuf::from("@pmt")
        .join("newbee-ocr-cli")
        .join("mac-arm64")
        .join("nbocr");

    #[cfg(target_os = "windows")]
    let relative_binary = PathBuf::from("@pmt")
        .join("newbee-ocr-cli")
        .join("win-x64")
        .join("nbocr.exe");

    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        return Err(napi::Error::from_reason(
            "export is only supported on macOS and Windows.".to_string(),
        ));
    }

    let candidate = base_dir.join(&relative_binary);
    if candidate.exists() {
        return Ok(candidate);
    }

    Err(napi::Error::from_reason(format!(
        "nbocr binary not found. {}/{}",
        base_dir.to_string_lossy(),
        relative_binary.to_string_lossy()
    )))
}

pub fn resolve_newbee_ocr_models_path() -> napi::Result<PathBuf> {
    let base_dir = resolve_runtime_base_dir()?;
    let relative_path = PathBuf::from("@pmt").join("newbee-ocr-cli").join("models");
    let candidate = base_dir.join(&relative_path);

    if candidate.exists() {
        return Ok(candidate);
    }

    Err(napi::Error::from_reason(format!(
        "nbocr models directory not found. {}/{}",
        base_dir.to_string_lossy(),
        relative_path.to_string_lossy()
    )))
}

pub fn resolve_dcm2niix_path() -> napi::Result<PathBuf> {
    let base_dir = resolve_runtime_base_dir()?;

    #[cfg(target_os = "macos")]
    let relative_binary = PathBuf::from("@pmt")
        .join("dcm2niix")
        .join("mac-arm64")
        .join("dcm2niix");

    #[cfg(target_os = "windows")]
    let relative_binary = PathBuf::from("@pmt")
        .join("dcm2niix")
        .join("win-x64")
        .join("dcm2niix.exe");

    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        return Err(napi::Error::from_reason(
            "export is only supported on macOS and Windows.".to_string(),
        ));
    }

    let candidate = base_dir.join(&relative_binary);
    if candidate.exists() {
        return Ok(candidate);
    }

    Err(napi::Error::from_reason(format!(
        "dcm2niix binary not found. {}/{}",
        base_dir.to_string_lossy(),
        relative_binary.to_string_lossy()
    )))
}

pub fn resolve_dcmtk_bin_path(bin_name: &str) -> napi::Result<PathBuf> {
    let base_dir = resolve_runtime_base_dir()?;

    #[cfg(target_os = "macos")]
    let relative_binary = PathBuf::from("@pmt")
        .join("dcmtk")
        .join("mac-arm64")
        .join("bin")
        .join(bin_name);

    #[cfg(target_os = "windows")]
    let relative_binary = PathBuf::from("@pmt")
        .join("dcmtk")
        .join("win-x64")
        .join("bin")
        .join(format!("{}.exe", bin_name));

    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        return Err(napi::Error::from_reason(
            "export is only supported on macOS and Windows.".to_string(),
        ));
    }

    let candidate = base_dir.join(&relative_binary);
    if candidate.exists() {
        return Ok(candidate);
    }

    Err(napi::Error::from_reason(format!(
        "DCMTK binary '{}' not found at '{}' (base='{}').",
        bin_name,
        candidate.to_string_lossy(),
        base_dir.to_string_lossy()
    )))
}

pub fn resolve_dcmtk_dictionary_path() -> napi::Result<PathBuf> {
    let base_dir = resolve_runtime_base_dir()?;

    #[cfg(target_os = "macos")]
    let relative_binary = PathBuf::from("@pmt")
        .join("dcmtk")
        .join("mac-arm64")
        .join("share")
        .join("dcmtk-3.7.0")
        .join("dicom.dic");

    #[cfg(target_os = "windows")]
    let relative_binary = PathBuf::from("@pmt")
        .join("dcmtk")
        .join("win-x64")
        .join("share")
        .join("dcmtk-3.7.0")
        .join("dicom.dic");

    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        return Err(napi::Error::from_reason(
            "export is only supported on macOS and Windows.".to_string(),
        ));
    }

    let candidate = base_dir.join(&relative_binary);
    if candidate.exists() {
        return Ok(candidate);
    }

    Err(napi::Error::from_reason(format!(
        "DCMTK dictionary file 'dicom.dic' not found. {}/{}",
        base_dir.to_string_lossy(),
        relative_binary.to_string_lossy()
    )))
}

pub fn resolve_ffmpeg_path() -> napi::Result<PathBuf> {
    let base_dir = resolve_runtime_base_dir()?;

    #[cfg(target_os = "macos")]
    let relative_binary = PathBuf::from("@pmt")
        .join("ffmpeg")
        .join("mac-arm64")
        .join("ffmpeg");

    #[cfg(target_os = "windows")]
    let relative_binary = PathBuf::from("@pmt")
        .join("ffmpeg")
        .join("win-x64")
        .join("bin")
        .join("ffmpeg.exe");

    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        return Err(napi::Error::from_reason(
            "export is only supported on macOS and Windows.".to_string(),
        ));
    }

    let candidate = base_dir.join(&relative_binary);
    if candidate.exists() {
        return Ok(candidate);
    }

    Err(napi::Error::from_reason(format!(
        "ffmpeg binary not found. {}/{}",
        base_dir.to_string_lossy(),
        relative_binary.to_string_lossy()
    )))
}
