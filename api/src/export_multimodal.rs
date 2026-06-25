use napi_derive::napi;
use rusqlite::Connection;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Multimodal data export
///
/// Reads multimodal data from the `labeling_details` table in cache.db,
/// and creates corresponding directory hierarchy based on the hierarchical structure
/// in the `data` field (project_folder|||PatientName|||StudyUID|||SeriesUID|||SOPUID),
/// then writes description.md, meta.json, and attachment files into the export directory.
///
/// project_root_dir:  Project root directory (used to locate .pmtaro/cache.db)
/// export_root_dir:   Actual output directory path returned by the main export flow
///                    (may already include a numeric suffix, e.g. "PatientName 1").
///                    Based on this, only study/series subdirectories need to be added.
/// selection_json:    Selection JSON passed from the main export flow, used for UID→description name mapping.
///
/// Returns a JSON string: { success, exportedCount, skippedCount, errors }
#[napi]
pub fn export_multimodal_data(
    project_root_dir: String,
    export_root_dir: String,
    selection_json: String,
) -> napi::Result<String> {

    log::info!("export_multimodal_data called with project_root_dir='{}', export_root_dir='{}'", 
        project_root_dir, export_root_dir);

    let root_path = Path::new(&export_root_dir);
    if !root_path.exists() || !root_path.is_dir() {
        return Err(napi::Error::from_reason(format!(
            "Export root directory does not exist: {}",
            export_root_dir
        )));
    }

    // Parse selection JSON, build UID→description name lookup map
    let selection: Value =
        serde_json::from_str(&selection_json).map_err(|e| {
            napi::Error::from_reason(format!("Failed to parse selection JSON: {}", e))
        })?;

    let name_mapper = NameMapper::from_selection(&selection);

    // Open database
    let db_path = Path::new(&project_root_dir)
        .join(".pmtaro")
        .join("cache.db");
    if !db_path.exists() {
        return Err(napi::Error::from_reason(format!(
            "Cache database not found: {}",
            db_path.to_string_lossy()
        )));
    }

    let conn = Connection::open(&db_path).map_err(|e| {
        napi::Error::from_reason(format!("Failed to open database: {}", e))
    })?;
    log::info!("Opened cache database successfully: {}", db_path.to_string_lossy());

    // Query all records starting with the project path
    // Use rowid to handle name conflicts
    let sql = "SELECT rowid, data, label, description, meta, files \
               FROM labeling_details \
               WHERE data LIKE ?1 || '%' \
               ORDER BY data";
    let mut stmt = conn.prepare(sql).map_err(|e| {
        napi::Error::from_reason(format!("SQL prepare failed: {}", e))
    })?;

    let rows: Vec<(i64, String, String, Option<String>, Option<String>, Option<String>)> =
        stmt.query_map([&project_root_dir], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })
        .map_err(|e| napi::Error::from_reason(format!("Database query failed: {}", e)))?
        .filter_map(|r| r.ok())
        .collect();

    log::info!("Queried {} raw rows from labeling_details.", rows.len());

    if rows.is_empty() {
        log::info!("No multimodal records found, returning empty result.");
        return Ok(r#"{"success":true,"exportedCount":0,"skippedCount":0,"errors":[]}"#.to_string());
    }

    log::info!("Found {} multimodal records to process.", rows.len());

    // Group by data (same data may have multiple labels)
    let mut groups: HashMap<String, Vec<(i64, Option<String>, Option<String>, Option<String>)>> =
        HashMap::new();
    for (rowid, data, _label, desc, meta, files) in rows {
        groups
            .entry(data)
            .or_default()
            .push((rowid, desc, meta, files));
    }

    log::info!("Grouped into {} unique data groups for processing.", groups.len());

    let mut exported = 0u32;
    let mut skipped = 0u32;
    let mut errors: Vec<String> = Vec::new();

    for (idx, (data_key, details)) in groups.iter().enumerate() {
        log::info!("[{}/{}] Processing data group: '{}' ({} records)", idx + 1, groups.len(), data_key, details.len());
        match process_data_group(
            root_path,
            &name_mapper,
            &project_root_dir,
            data_key,
            details,
        ) {
            Ok(count) => {
                exported += count;
                log::info!("[{}/{}] Successfully processed group '{}': {} files exported", idx + 1, groups.len(), data_key, count);
            }
            Err(e) => {
                let msg = format!("Failed to process '{}': {}", data_key, e);
                log::error!("[{}/{}] {}", idx + 1, groups.len(), msg);
                errors.push(msg);
                skipped += details.len() as u32;
            }
        }
    }

    let result = MultimodalResult {
        success: errors.is_empty(),
        exported_count: exported,
        skipped_count: skipped,
        errors,
    };

    log::info!("Export completed: success={}, exported={}, skipped={}, total_errors={}",
        result.success, result.exported_count, result.skipped_count, result.errors.len());
    if !result.errors.is_empty() {
        for err in &result.errors {
            log::warn!("Export error detail: {}", err);
        }
    }

    serde_json::to_string(&result)
        .map_err(|e| napi::Error::from_reason(format!("Failed to serialize result: {}", e)))
}

// ---------------------------------------------------------------------------
// Data structures and helper types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct MultimodalResult {
    success: bool,
    exported_count: u32,
    skipped_count: u32,
    errors: Vec<String>,
}

/// Extract UID→description name mapping from selection JSON
struct NameMapper {
    /// StudyInstanceUID → StudyDescription
    study_descriptions: HashMap<String, String>,
    /// SeriesInstanceUID → (SeriesDescription, SeriesNumber)
    series_info: HashMap<String, (String, i64)>,
    /// SOPInstanceUID → fileName
    instance_file_names: HashMap<String, String>,
}

impl NameMapper {
    fn from_selection(sel: &Value) -> Self {
        let mut study_descriptions = HashMap::new();
        let mut series_info = HashMap::new();
        let mut instance_file_names = HashMap::new();

        // Iterate studies
        if let Some(studies) = sel.get("studies").and_then(|v| v.as_object()) {
            for (study_uid, study_val) in studies {
                let study_desc = study_val
                    .get("StudyDescription")
                    .and_then(|v| v.as_str())
                    .unwrap_or(study_uid)
                    .to_string();
                study_descriptions.insert(study_uid.clone(), study_desc);

                // Iterate series
                if let Some(series_map) = study_val.get("series").and_then(|v| v.as_object()) {
                    for (series_uid, series_val) in series_map {
                        let s_desc = series_val
                            .get("SeriesDescription")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Unknown Series")
                            .to_string();
                        let s_num = series_val
                            .get("SeriesNumber")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0);
                        series_info.insert(series_uid.clone(), (s_desc, s_num));

                        // Iterate instances
                        if let Some(inst_map) =
                            series_val.get("instances").and_then(|v| v.as_object())
                        {
                            for (sop_uid, inst_val) in inst_map {
                                let fname = inst_val
                                    .get("fileName")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or(sop_uid)
                                    .to_string();
                                instance_file_names.insert(sop_uid.clone(), fname);
                            }
                        }
                    }
                }
            }
        }

        // Top-level may also directly contain series/instances (when study level is selected)
        if let Some(series_map) = sel.get("series").and_then(|v| v.as_object()) {
            for (series_uid, series_val) in series_map {
                let s_desc = series_val
                    .get("SeriesDescription")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown Series")
                    .to_string();
                let s_num = series_val
                    .get("SeriesNumber")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                series_info.entry(series_uid.clone()).or_insert((s_desc, s_num));

                if let Some(inst_map) = series_val.get("instances").and_then(|v| v.as_object()) {
                    for (sop_uid, inst_val) in inst_map {
                        let fname = inst_val
                            .get("fileName")
                            .and_then(|v| v.as_str())
                            .unwrap_or(sop_uid)
                            .to_string();
                        instance_file_names.entry(sop_uid.clone()).or_insert(fname);
                    }
                }
            }
        }

        // Top-level may also directly contain instances (when series level is selected)
        if let Some(inst_map) = sel.get("instances").and_then(|v| v.as_object()) {
            for (sop_uid, inst_val) in inst_map {
                let fname = inst_val
                    .get("fileName")
                    .and_then(|v| v.as_str())
                    .unwrap_or(sop_uid)
                    .to_string();
                instance_file_names.entry(sop_uid.clone()).or_insert(fname);
            }
        }

        log::info!(
            "NameMapper built from selection: {} studies, {} series, {} instances",
            study_descriptions.len(),
            series_info.len(),
            instance_file_names.len(),
        );

        Self {
            study_descriptions,
            series_info,
            instance_file_names,
        }
    }

    fn get_study_description(&self, uid: &str) -> String {
        self.study_descriptions
            .get(uid)
            .cloned()
            .unwrap_or_else(|| uid.to_string())
    }

    fn get_series_dir_name(&self, uid: &str) -> String {
        match self.series_info.get(uid) {
            Some((desc, num)) => format!("{} #{}", desc, num),
            None => uid.to_string(),
        }
    }

    fn get_instance_file_name(&self, uid: &str) -> String {
        self.instance_file_names
            .get(uid)
            .cloned()
            .unwrap_or_else(|| uid.to_string())
    }
}

// ---------------------------------------------------------------------------
// Core processing logic
// ---------------------------------------------------------------------------

/// Index positions after splitting the data field
const SEG_PROJECT: usize = 0;
const SEG_PATIENT: usize = 1;
const SEG_STUDY: usize = 2;
const SEG_SERIES: usize = 3;
const SEG_INSTANCE: usize = 4;

/// Process a group (all label records under the same data value)
fn process_data_group(
    export_root: &Path,
    mapper: &NameMapper,
    project_root: &str,
    data_key: &str,
    details: &[(i64, Option<String>, Option<String>, Option<String>)],
) -> napi::Result<u32> {

    log::info!("Processing data group: '{}', {} records", data_key, details.len());

    let parts: Vec<&str> = data_key.split("|||").collect();
    if parts.len() < 2 || parts[SEG_PROJECT].is_empty() {
        return Err(napi::Error::from_reason(format!(
            "Invalid data field format, at least 2 segments required (project|||PatientName): {}",
            data_key
        )));
    }

    let patient_name = parts[SEG_PATIENT].trim();
    if patient_name.is_empty() {
        return Err(napi::Error::from_reason(format!(
            "PatientName is empty: {}",
            data_key
        )));
    }

    // Determine hierarchy depth: 2=patient, 3=study, 4=series, 5=instance
    let level = parts.len();

    // export_root is already the actual patient-level directory created by the main export
    // (may contain a numeric suffix like "PatientName 1"),
    // so use it as the base and only add lower-level (study/series) subdirectories
    let mut output_dir = export_root.to_path_buf();

    if level > 2 {
        let study_uid = parts[SEG_STUDY];
        let study_desc = mapper.get_study_description(study_uid);
        output_dir = output_dir.join(create_safe_dir_name(&study_desc));
    }

    if level > 3 {
        let series_uid = parts[SEG_SERIES];
        let series_dir_name = mapper.get_series_dir_name(series_uid);
        output_dir = output_dir.join(create_safe_dir_name(&series_dir_name));
    }

    log::info!("Output directory for data group '{}': {}", data_key, output_dir.to_string_lossy());

    // Create directory
    fs::create_dir_all(&output_dir).map_err(|e| {
        napi::Error::from_reason(format!(
            "Failed to create directory '{}': {}",
            output_dir.to_string_lossy(),
            e
        ))
    })?;

    log::info!("Output directory ready: {}", output_dir.to_string_lossy());

    let mut count = 0u32;
    for (rowid, description, meta, files_json) in details {
        // Determine base file name (required at Instance level)
        let base_name = if level > 4 {
            let sop_uid = parts[SEG_INSTANCE];
            mapper.get_instance_file_name(sop_uid)
        } else {
            String::new()
        };

        let rowid = *rowid;

        // Whether there are multiple records with the same data (same level labeled multiple times)
        let row_suffix = if details.len() > 1 {
            Some(rowid)
        } else {
            None
        };

        // Write description.md
        if let Some(desc) = description {
            let desc_content = desc.trim();
            if !desc_content.is_empty() {
                let desc_file_name = if level > 4 {
                    // Instance level: {baseName}-description.md
                    format!("{}-description.md", &base_name)
                } else {
                    "description.md".to_string()
                };
                let desc_file_name = apply_row_suffix(&desc_file_name, row_suffix);
                let desc_path = output_dir.join(&desc_file_name);

                // If file already exists (multiple records at same level but different labels), use row_suffix
                let desc_path = if desc_path.exists() && row_suffix.is_none() {
                    let new_name = format!(
                        "{}-description_row{}.md",
                        if level > 4 { &base_name } else { "description" },
                        rowid
                    );
                    log::warn!("Description file already exists, using alternative name: {}", new_name);
                    output_dir.join(&new_name)
                } else {
                    desc_path
                };

                log::info!("Writing description for rowid {} -> '{}'", rowid, desc_path.to_string_lossy());
                fs::write(&desc_path, desc_content).map_err(|e| {
                    napi::Error::from_reason(format!(
                        "Failed to write description file '{}': {}",
                        desc_path.to_string_lossy(),
                        e
                    ))
                })?;
                count += 1;
            }
        }

        // Write meta.json
        if let Some(meta_str) = meta {
            let meta_content = meta_str.trim();
            if !meta_content.is_empty() {
                let meta_file_name = if level > 4 {
                    format!("{}-meta.json", &base_name)
                } else {
                    "meta.json".to_string()
                };
                let meta_file_name = apply_row_suffix(&meta_file_name, row_suffix);
                let meta_path = output_dir.join(&meta_file_name);

                let meta_path = if meta_path.exists() && row_suffix.is_none() {
                    let new_name = format!(
                        "{}-meta_row{}.json",
                        if level > 4 { &base_name } else { "meta" },
                        rowid
                    );
                    log::warn!("Meta file already exists, using alternative name: {}", new_name);
                    output_dir.join(&new_name)
                } else {
                    meta_path
                };

                // Try to format as pretty JSON
                let formatted = serde_json::from_str::<Value>(meta_content)
                    .map(|v| serde_json::to_string_pretty(&v).unwrap_or_else(|_| meta_content.to_string()))
                    .unwrap_or_else(|_| meta_content.to_string());

                log::info!("Writing meta for rowid {} -> '{}' ({} bytes)", rowid, meta_path.to_string_lossy(), formatted.len());
                fs::write(&meta_path, &formatted).map_err(|e| {
                    napi::Error::from_reason(format!(
                        "Failed to write meta file '{}': {}",
                        meta_path.to_string_lossy(),
                        e
                    ))
                })?;
                count += 1;
            }
        }

        // Process attachment files
        if let Some(files_str) = files_json {
            let files_content = files_str.trim();
            if !files_content.is_empty() {
                log::info!("Processing attachments for rowid {} ({} chars)", rowid, files_content.len());
                if let Err(e) = process_attachments(
                    &output_dir,
                    project_root,
                    &base_name,
                    row_suffix,
                    rowid,
                    files_content,
                ) {
                    let msg = format!(
                        "Failed to process attachment (rowid={}): {}",
                        rowid, e
                    );
                    log::warn!("{}", msg);
                }
            }
        }
    }

    Ok(count)
}

/// Process attachment copying
fn process_attachments(
    output_dir: &Path,
    project_root: &str,
    base_name: &str,
    row_suffix: Option<i64>,
    rowid: i64,
    files_json: &str,
) -> napi::Result<()> {
    // The files field may be in two formats:
    // 1. Object: {"/abs/path": {"name": "file.txt", ...}}
    // 2. Array: [{"path": "relative/path", "name": "file.txt"}]
    let project_root_path = Path::new(project_root);

    // Try parsing as object first
    if let Ok(obj) = serde_json::from_str::<HashMap<String, Value>>(files_json) {
        log::info!("Parsed files field as object format ({} entries)", obj.len());
        for (src_path_str, info) in &obj {
            let src_path = Path::new(src_path_str);
            let file_name = info
                .get("name")
                .and_then(|v| v.as_str())
                .and_then(|s| if s.is_empty() { None } else { Some(s) })
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    src_path
                        .file_name()
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_default()
                });

            log::info!("Attachment object entry: src='{}', name='{}'", src_path_str, file_name);
            copy_attachment(output_dir, src_path, &file_name, base_name, row_suffix, rowid)?;
        }
        log::info!("Finished processing {} attachment(s) (object format).", obj.len());
        return Ok(());
    }

    // Then try parsing as array
    if let Ok(arr) = serde_json::from_str::<Vec<HashMap<String, Value>>>(files_json) {
        log::info!("Parsed files field as array format ({} entries)", arr.len());
        for item in &arr {
            let rel_path = item
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if rel_path.is_empty() {
                log::warn!("Skipping array entry with empty path: {:?}", item);
                continue;
            }
            let src_path = project_root_path.join(rel_path);
            let file_name = item
                .get("name")
                .and_then(|v| v.as_str())
                .and_then(|s| if s.is_empty() { None } else { Some(s) })
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    Path::new(rel_path)
                        .file_name()
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_default()
                });

            log::info!("Attachment array entry: path='{}', name='{}'", rel_path, file_name);
            copy_attachment(output_dir, &src_path, &file_name, base_name, row_suffix, rowid)?;
        }
        log::info!("Finished processing {} attachment(s) (array format).", arr.len());
        return Ok(());
    }

    // If both formats fail, log warning
    log::warn!("Unable to parse files field format (expected object or array): {}", files_json);
    Ok(())
}

/// Copy a single attachment file
fn copy_attachment(
    output_dir: &Path,
    src_path: &Path,
    file_name: &str,
    base_name: &str,
    row_suffix: Option<i64>,
    _rowid: i64,
) -> napi::Result<()> {
    // Check if source file exists
    let resolved_src = if src_path.exists() {
        src_path.to_path_buf()
    } else {
        log::warn!("Source file does not exist, skipping: {}", src_path.to_string_lossy());
        return Ok(());
    };

    // Determine destination file name
    let dest_file_name = if !base_name.is_empty() {
        // Instance level: {baseName}-{originalFileName}
        format!("{}-{}", base_name, file_name)
    } else {
        file_name.to_string()
    };

    let dest_file_name = apply_row_suffix(&dest_file_name, row_suffix);
    let dest_path = output_dir.join(&dest_file_name);

    // Copy file (overwrite if exists)
    let metadata = fs::metadata(&resolved_src).ok();
    let file_size = metadata.as_ref().map(|m| m.len()).unwrap_or(0);
    fs::copy(&resolved_src, &dest_path).map_err(|e| {
        napi::Error::from_reason(format!(
            "Failed to copy attachment '{}' -> '{}': {}",
            resolved_src.to_string_lossy(),
            dest_path.to_string_lossy(),
            e
        ))
    })?;
    log::info!("Copied attachment '{}' ({}) -> '{}' ({} bytes)",
        resolved_src.to_string_lossy(),
        if src_path.is_absolute() { "absolute" } else { "relative" },
        dest_path.to_string_lossy(),
        file_size);

    Ok(())
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Append a rowid suffix to the filename when there are multiple records
fn apply_row_suffix(file_name: &str, row_suffix: Option<i64>) -> String {
    match row_suffix {
        Some(rid) => {
            // Insert _row{id} before the extension
            let stem = Path::new(file_name)
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| file_name.to_string());
            let ext = Path::new(file_name)
                .extension()
                .map(|s| format!(".{}", s.to_string_lossy()))
                .unwrap_or_default();
            format!("{}_row{}{}", stem, rid, ext)
        }
        None => file_name.to_string(),
    }
}

/// Convert a name to a safe directory name (replace characters not supported by the filesystem)
fn create_safe_dir_name(name: &str) -> String {
    let invalid_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*', '\0'];
    let mut safe: String = name
        .chars()
        .map(|c| {
            if invalid_chars.contains(&c) {
                '_'
            } else {
                c
            }
        })
        .collect();

    // Truncate overly long directory names
    if safe.len() > 200 {
        safe.truncate(200);
    }

    let trimmed = safe.trim().to_string();
    if trimmed.is_empty() {
        "Untitled".to_string()
    } else {
        trimmed
    }
}
