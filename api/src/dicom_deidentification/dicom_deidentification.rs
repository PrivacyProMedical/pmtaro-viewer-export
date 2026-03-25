use csv::ReaderBuilder;
use dicom::core::ops::{ApplyOp, AttributeAction, AttributeOp};
use dicom::core::value::PrimitiveValue;
use dicom::core::{Tag, VR};
use dicom::object::{open_file, DefaultDicomObject, InMemDicomObject};
use dicom_pixeldata::{ConvertOptions, ModalityLutOption, PixelDecoder, VoiLutOption};
use image::DynamicImage;
// use napi_derive::napi;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::Path;

use crate::dicom_deidentification::ocr::{run_ocr, OcrRect};

// De-identification rule table (embedded CSV)
const DEID_TABLE: &str = include_str!("dicom_deidentification_table.csv");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
// De-identification action type
enum DeidAction {
    Remove,
    Zero,
    Dummy,
    Uid,
}

#[derive(Clone, Debug)]
// DICOM tag matching pattern
enum TagPattern {
    Exact(Tag),
    Masked {
        group_mask: u16,
        group_value: u16,
        element_mask: u16,
        element_value: u16,
        require_group_odd: bool,
    },
}

// TagPattern matching logic
impl TagPattern {
    fn matches(&self, tag: Tag) -> bool {
        match self {
            TagPattern::Exact(exact) => *exact == tag,
            TagPattern::Masked {
                group_mask,
                group_value,
                element_mask,
                element_value,
                require_group_odd,
            } => {
                let Tag(group, element) = tag;
                let group_match = (group & group_mask) == *group_value;
                let element_match = (element & element_mask) == *element_value;
                let odd_ok = if *require_group_odd {
                    group % 2 == 1
                } else {
                    true
                };
                group_match && element_match && odd_ok
            }
        }
    }
}

#[derive(Clone, Debug)]
// Rule: tag pattern + action
struct Rule {
    pattern: TagPattern,
    action: DeidAction,
}

// UID mapper (stable anonymization)
struct UidMapper {
    map: HashMap<String, String>,
    used: HashSet<String>,
}

#[derive(Clone, Debug)]
// De-identification record (original/new value/action)
struct DeidRecord {
    tag: Tag,
    original_value: String,
    action: DeidAction,
    new_value_raw: Option<PrimitiveValue>,
}

// Preprocessed form of sensitive text
struct SensitiveValue {
    lower: String,
    norm: String,
}

// UID mapping implementation
impl UidMapper {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            used: HashSet::new(),
        }
    }

    fn map_uid(&mut self, original: &str) -> String {
        if let Some(mapped) = self.map.get(original) {
            return mapped.clone();
        }

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        original.hash(&mut hasher);
        let mut hash = hasher.finish() as u128;
        let mut candidate = format!("2.25.{hash}");
        while self.used.contains(&candidate) {
            hash = hash.wrapping_add(1);
            candidate = format!("2.25.{hash}");
        }

        self.used.insert(candidate.clone());
        self.map.insert(original.to_string(), candidate.clone());
        candidate
    }
}

// Parse de-identification action from the Basic Profile field
fn action_from_basic_profile(value: &str) -> Option<DeidAction> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.contains('U') {
        return Some(DeidAction::Uid);
    }
    if trimmed.contains('X') {
        return Some(DeidAction::Remove);
    }
    if trimmed.contains('Z') {
        return Some(DeidAction::Zero);
    }
    if trimmed.contains('D') {
        return Some(DeidAction::Dummy);
    }
    None
}

// Parse a tag pattern string
fn parse_tag_pattern(raw: &str) -> Option<TagPattern> {
    let tag_text = raw.trim();
    let start = tag_text.find('(')?;
    let end = tag_text[start..].find(')')? + start;
    let inner = tag_text[start + 1..end].trim();
    let mut parts = inner.split(',');
    let group_part = parts.next()?.trim();
    let element_part = parts.next()?.trim();
    if group_part.len() != 4 || element_part.len() != 4 {
        return None;
    }

    let (group_mask, group_value) = parse_masked_hex(group_part)?;
    let (element_mask, element_value) = parse_masked_hex(element_part)?;
    let require_group_odd = tag_text.to_ascii_lowercase().contains("odd");

    if group_mask == 0xFFFF && element_mask == 0xFFFF && !require_group_odd {
        return Some(TagPattern::Exact(Tag(group_value, element_value)));
    }

    Some(TagPattern::Masked {
        group_mask,
        group_value,
        element_mask,
        element_value,
        require_group_odd,
    })
}

// Parse wildcard-enabled hexadecimal mask
fn parse_masked_hex(raw: &str) -> Option<(u16, u16)> {
    let mut mask: u16 = 0;
    let mut value: u16 = 0;
    for (idx, ch) in raw.chars().enumerate() {
        let shift = 12 - (idx * 4) as u16;
        if let Some(nibble) = ch.to_digit(16) {
            mask |= 0xF << shift;
            value |= (nibble as u16) << shift;
        } else if matches!(ch, 'x' | 'X' | 'g' | 'G' | 'e' | 'E') {
            // wildcard nibble
        } else {
            return None;
        }
    }
    Some((mask, value))
}

// Load de-identification rules from CSV
fn load_rules() -> Vec<Rule> {
    let mut rules = Vec::new();
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(DEID_TABLE.as_bytes());

    let headers = match reader.headers() {
        Ok(headers) => headers.clone(),
        Err(_) => return rules,
    };

    let tag_idx = headers.iter().position(|h| h == "Tag");
    let basic_idx = headers.iter().position(|h| h == "Basic Prof.");
    let (tag_idx, basic_idx) = match (tag_idx, basic_idx) {
        (Some(t), Some(b)) => (t, b),
        _ => return rules,
    };

    for record in reader.records() {
        let record = match record {
            Ok(rec) => rec,
            Err(_) => continue,
        };
        let tag_field = record.get(tag_idx).unwrap_or("");
        let basic_field = record.get(basic_idx).unwrap_or("");
        let action = match action_from_basic_profile(basic_field) {
            Some(action) => action,
            None => continue,
        };
        let pattern = match parse_tag_pattern(tag_field) {
            Some(pattern) => pattern,
            None => continue,
        };
        rules.push(Rule { pattern, action });
    }

    rules
}

// Map a UID or UID list to anonymized UID values
fn map_uid_value(value: &str, uid_map: &mut UidMapper) -> String {
    let mut mapped = Vec::new();
    for uid in value.split('\\') {
        let trimmed = uid.trim();
        if trimmed.is_empty() {
            continue;
        }
        mapped.push(uid_map.map_uid(trimmed));
    }
    mapped.join("\\")
}

// Generate a placeholder value for a given VR
fn dummy_value_for_vr(
    vr: VR,
    original_text: Option<&str>,
    uid_map: &mut UidMapper,
) -> PrimitiveValue {
    match vr {
        VR::AE | VR::CS | VR::LO | VR::LT | VR::PN | VR::SH | VR::ST | VR::UC | VR::UR | VR::UT => {
            PrimitiveValue::from("ANON")
        }
        VR::DA => PrimitiveValue::from("19000101"),
        VR::TM => PrimitiveValue::from("000000"),
        VR::DT => PrimitiveValue::from("19000101000000"),
        VR::AS => PrimitiveValue::from("000Y"),
        VR::DS | VR::IS => PrimitiveValue::from("0"),
        VR::UI => {
            let mapped = match original_text
                .map(|text| text.trim())
                .filter(|t| !t.is_empty())
            {
                Some(text) => map_uid_value(text, uid_map),
                None => uid_map.map_uid("__generated__"),
            };
            PrimitiveValue::from(mapped)
        }
        VR::US => PrimitiveValue::from(0_u16),
        VR::SS => PrimitiveValue::from(0_i16),
        VR::UL => PrimitiveValue::from(0_u32),
        VR::SL => PrimitiveValue::from(0_i32),
        VR::UV => PrimitiveValue::from(0_u64),
        VR::SV => PrimitiveValue::from(0_i64),
        VR::FL => PrimitiveValue::from(0.0_f32),
        VR::FD => PrimitiveValue::from(0.0_f64),
        _ => PrimitiveValue::Empty,
    }
}

// Find action for a tag according to the rule list
fn action_for_tag(tag: Tag, rules: &[Rule]) -> Option<DeidAction> {
    for rule in rules {
        if rule.pattern.matches(tag) {
            return Some(rule.action);
        }
    }
    None
}

// Convert PrimitiveValue to string
fn primitive_to_text(value: &PrimitiveValue) -> String {
    value.to_str().into_owned()
}

// Compute the new value after de-identification
fn compute_new_value(
    vr: VR,
    original_text: &str,
    action: DeidAction,
    uid_map: &mut UidMapper,
) -> (Option<PrimitiveValue>, String) {
    match action {
        DeidAction::Remove => (None, String::new()),
        DeidAction::Zero => (Some(PrimitiveValue::Empty), String::new()),
        DeidAction::Dummy => {
            let new_value = dummy_value_for_vr(vr, Some(original_text), uid_map);
            let new_text = primitive_to_text(&new_value);
            (Some(new_value), new_text)
        }
        DeidAction::Uid => {
            let trimmed = original_text.trim();
            if trimmed.is_empty() {
                (None, String::new())
            } else {
                let mapped = map_uid_value(trimmed, uid_map);
                (Some(PrimitiveValue::from(mapped.clone())), mapped)
            }
        }
    }
}

// Build a list of de-identification records
fn build_deid_list(
    obj: &mut InMemDicomObject,
    rules: &[Rule],
    uid_map: &mut UidMapper,
) -> Vec<DeidRecord> {
    let mut records = Vec::new();
    collect_deid_records(obj, rules, uid_map, &mut records);
    records
}

// Recursively collect de-identification records from object
fn collect_deid_records(
    obj: &mut InMemDicomObject,
    rules: &[Rule],
    uid_map: &mut UidMapper,
    records: &mut Vec<DeidRecord>,
) {
    let tags: Vec<Tag> = obj.tags().collect();
    for tag in tags {
        let action = match action_for_tag(tag, rules) {
            Some(action) => action,
            None => continue,
        };
        let elem = match obj.get(tag) {
            Some(elem) => elem,
            None => continue,
        };
        let original_value = elem
            .value()
            .to_str()
            .map(|value| value.to_string())
            .unwrap_or_default();
        let (new_value_raw, _new_value) =
            compute_new_value(elem.vr(), &original_value, action, uid_map);
        records.push(DeidRecord {
            tag,
            original_value,
            action,
            new_value_raw,
        });
    }

    let sequence_tags: Vec<Tag> = obj
        .iter()
        .filter(|elem| elem.vr() == VR::SQ)
        .map(|elem| elem.header().tag)
        .collect();
    for tag in sequence_tags {
        let _ = obj.update_value(tag, |value| {
            if let Some(items) = value.items_mut() {
                for item in items.iter_mut() {
                    collect_deid_records(item, rules, uid_map, records);
                }
            }
        });
    }
}

// Check whether a record matches the current element
fn record_matches_element(record: &DeidRecord, tag: Tag, current_value: &str) -> bool {
    if record.tag != tag {
        return false;
    }
    if record.original_value.is_empty() {
        return true;
    }
    record.original_value == current_value
}

// Apply one de-identification record to object
fn apply_deid_record(obj: &mut InMemDicomObject, record: &DeidRecord) {
    match record.action {
        DeidAction::Remove => {
            obj.remove_element(record.tag);
        }
        DeidAction::Zero => {
            let _ = obj.apply(AttributeOp::new(record.tag, AttributeAction::Empty));
        }
        DeidAction::Dummy | DeidAction::Uid => {
            if let Some(new_value) = record.new_value_raw.clone() {
                let _ = obj.apply(AttributeOp::new(
                    record.tag,
                    AttributeAction::Set(new_value),
                ));
            }
        }
    }
}

// Apply de-identification records to object in batch (including sequences)
fn apply_deid_list_to_object(obj: &mut InMemDicomObject, records: &mut Vec<DeidRecord>) {
    let tags: Vec<Tag> = obj.tags().collect();
    for tag in tags {
        let elem = match obj.get(tag) {
            Some(elem) => elem,
            None => continue,
        };
        let current_value = elem
            .value()
            .to_str()
            .map(|value| value.to_string())
            .unwrap_or_default();
        let index = records
            .iter()
            .position(|record| record_matches_element(record, tag, &current_value));
        if let Some(idx) = index {
            let record = records.remove(idx);
            apply_deid_record(obj, &record);
        }
    }

    let sequence_tags: Vec<Tag> = obj
        .iter()
        .filter(|elem| elem.vr() == VR::SQ)
        .map(|elem| elem.header().tag)
        .collect();
    for tag in sequence_tags {
        let _ = obj.update_value(tag, |value| {
            if let Some(items) = value.items_mut() {
                for item in items.iter_mut() {
                    apply_deid_list_to_object(item, records);
                }
            }
        });
    }
}

// Determine whether a string should be tracked as OCR-sensitive text
fn should_track_sensitive_value(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.len() < 3 {
        return false;
    }
    let mut alnum = 0;
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() {
            alnum += 1;
        }
    }
    alnum >= 3
}

// Normalize string: keep only alphanumerics and lowercase them
fn normalize_compact(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            output.push(ch.to_ascii_lowercase());
        }
    }
    output
}

// Extract OCR-sensitive values from de-identification records
fn collect_sensitive_values(records: &[DeidRecord]) -> Vec<SensitiveValue> {
    let mut values = HashSet::new();
    for record in records {
        for part in record.original_value.split('\\') {
            let value = part.trim();
            if should_track_sensitive_value(value) {
                values.insert(value.to_string());
            }
        }
    }
    values
        .into_iter()
        .map(|value| SensitiveValue {
            lower: value.to_lowercase(),
            norm: normalize_compact(&value),
        })
        .collect()
}

// Levenshtein distance
fn levenshtein_distance(a: &str, b: &str) -> usize {
    if a == b {
        return 0;
    }
    if a.is_empty() {
        return b.len();
    }
    if b.is_empty() {
        return a.len();
    }

    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut curr = vec![0; b.len() + 1];

    for (i, ca) in a.as_bytes().iter().enumerate() {
        curr[0] = i + 1;
        for (j, cb) in b.as_bytes().iter().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            let insert = curr[j] + 1;
            let delete = prev[j + 1] + 1;
            let replace = prev[j] + cost;
            curr[j + 1] = insert.min(delete).min(replace);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[b.len()]
}

// Similarity ratio (1 - normalized edit distance)
fn similarity_ratio(a: &str, b: &str) -> f32 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let max_len = a.len().max(b.len()) as f32;
    let distance = levenshtein_distance(a, b) as f32;
    1.0 - (distance / max_len)
}

// Fuzzy matching window delta
const FUZZY_WINDOW_DELTA: usize = 2;
// Fuzzy matching threshold
const FUZZY_THRESHOLD: f32 = 0.8;
// Minimum normalized length used in fuzzy matching
const MIN_NORMALIZED_LEN: usize = 6;

// Perform sliding-window fuzzy matching in text
fn fuzzy_contains(text_norm: &str, value_norm: &str) -> bool {
    if text_norm.is_empty() || value_norm.is_empty() {
        return false;
    }
    let target_len = value_norm.len();
    let min_len = target_len.saturating_sub(FUZZY_WINDOW_DELTA);
    let max_len = target_len + FUZZY_WINDOW_DELTA;

    for window_len in min_len..=max_len {
        if window_len == 0 || window_len > text_norm.len() {
            continue;
        }
        for start in 0..=text_norm.len() - window_len {
            let candidate = &text_norm[start..start + window_len];
            if similarity_ratio(candidate, value_norm) >= FUZZY_THRESHOLD {
                return true;
            }
        }
    }

    false
}

// Determine whether OCR text contains sensitive content
fn text_contains_sensitive(text: &str, sensitive_values: &[SensitiveValue]) -> bool {
    let text_lower = text.to_lowercase();
    if sensitive_values
        .iter()
        .any(|value| !value.lower.is_empty() && text_lower.contains(&value.lower))
    {
        return true;
    }

    let text_norm = normalize_compact(text);
    if text_norm.is_empty() {
        return false;
    }

    for value in sensitive_values {
        if value.norm.is_empty() {
            continue;
        }
        if value.norm.len() < MIN_NORMALIZED_LEN {
            continue;
        }
        if text_norm.contains(&value.norm) || fuzzy_contains(&text_norm, &value.norm) {
            return true;
        }
    }

    false
}

// Clamp rectangle to image bounds
fn clamp_rect(rect: OcrRect, width: u32, height: u32) -> Option<OcrRect> {
    if width == 0 || height == 0 {
        return None;
    }
    let x = rect.x.min(width);
    let y = rect.y.min(height);
    let max_width = width.saturating_sub(x);
    let max_height = height.saturating_sub(y);
    let w = rect.width.min(max_width);
    let h = rect.height.min(max_height);
    if w == 0 || h == 0 {
        None
    } else {
        Some(OcrRect {
            x,
            y,
            width: w,
            height: h,
        })
    }
}

// Fill rectangle for 8-bit grayscale image
fn fill_rect_luma(image: &mut image::GrayImage, rect: OcrRect) {
    let width = image.width();
    let height = image.height();
    let rect = match clamp_rect(rect, width, height) {
        Some(rect) => rect,
        None => return,
    };
    for y in rect.y..rect.y + rect.height {
        for x in rect.x..rect.x + rect.width {
            image.put_pixel(x, y, image::Luma([255]));
        }
    }
}

// Fill rectangle for 8-bit RGB image
fn fill_rect_rgb(image: &mut image::RgbImage, rect: OcrRect) {
    let width = image.width();
    let height = image.height();
    let rect = match clamp_rect(rect, width, height) {
        Some(rect) => rect,
        None => return,
    };
    for y in rect.y..rect.y + rect.height {
        for x in rect.x..rect.x + rect.width {
            image.put_pixel(x, y, image::Rgb([255, 255, 255]));
        }
    }
}

// Fill rectangle for 16-bit grayscale image
fn fill_rect_luma16(image: &mut image::ImageBuffer<image::Luma<u16>, Vec<u16>>, rect: OcrRect) {
    let width = image.width();
    let height = image.height();
    let rect = match clamp_rect(rect, width, height) {
        Some(rect) => rect,
        None => return,
    };
    for y in rect.y..rect.y + rect.height {
        for x in rect.x..rect.x + rect.width {
            image.put_pixel(x, y, image::Luma([u16::MAX]));
        }
    }
}

// Fill rectangle for 16-bit RGB image
fn fill_rect_rgb16(image: &mut image::ImageBuffer<image::Rgb<u16>, Vec<u16>>, rect: OcrRect) {
    let width = image.width();
    let height = image.height();
    let rect = match clamp_rect(rect, width, height) {
        Some(rect) => rect,
        None => return,
    };
    for y in rect.y..rect.y + rect.height {
        for x in rect.x..rect.x + rect.width {
            image.put_pixel(x, y, image::Rgb([u16::MAX, u16::MAX, u16::MAX]));
        }
    }
}

// Redact image regions according to OCR rectangles
fn apply_redactions(image: &mut DynamicImage, rects: &[OcrRect]) {
    match image {
        DynamicImage::ImageLuma8(img) => {
            for rect in rects {
                fill_rect_luma(img, *rect);
            }
        }
        DynamicImage::ImageRgb8(img) => {
            for rect in rects {
                fill_rect_rgb(img, *rect);
            }
        }
        DynamicImage::ImageLuma16(img) => {
            for rect in rects {
                fill_rect_luma16(img, *rect);
            }
        }
        DynamicImage::ImageRgb16(img) => {
            for rect in rects {
                fill_rect_rgb16(img, *rect);
            }
        }
        _ => {
            let mut rgb = image.to_rgb8();
            for rect in rects {
                fill_rect_rgb(&mut rgb, *rect);
            }
            *image = DynamicImage::ImageRgb8(rgb);
        }
    }
}

// Convert u16 vector to little-endian bytes
fn u16_vec_to_le_bytes(values: Vec<u16>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(values.len() * 2);
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

// Convert image frame to raw pixel bytes
fn image_to_frame_bytes(image: DynamicImage, samples_per_pixel: u16) -> Vec<u8> {
    match (samples_per_pixel, image) {
        (3, DynamicImage::ImageRgb8(img)) => img.into_raw(),
        (1, DynamicImage::ImageLuma8(img)) => img.into_raw(),
        (3, DynamicImage::ImageRgb16(img)) => u16_vec_to_le_bytes(img.into_raw()),
        (1, DynamicImage::ImageLuma16(img)) => u16_vec_to_le_bytes(img.into_raw()),
        (3, other) => other.to_rgb8().into_raw(),
        (_, other) => other.to_luma8().into_raw(),
    }
}

// Write back Pixel Data and related DICOM attributes
fn update_pixel_data(
    obj: &mut DefaultDicomObject,
    mut pixel_bytes: Vec<u8>,
    width: u32,
    height: u32,
    samples_per_pixel: u16,
    number_of_frames: u32,
    bits_allocated: u16,
    bits_stored: u16,
    high_bit: u16,
) {
    let photometric = if samples_per_pixel == 3 {
        "RGB"
    } else {
        "MONOCHROME2"
    };

    if pixel_bytes.len() % 2 == 1 {
        pixel_bytes.push(0);
    }

    let _ = obj.apply(AttributeOp::new(
        Tag(0x0028, 0x0010),
        AttributeAction::Set(PrimitiveValue::from(height as u16)),
    ));
    let _ = obj.apply(AttributeOp::new(
        Tag(0x0028, 0x0011),
        AttributeAction::Set(PrimitiveValue::from(width as u16)),
    ));
    let _ = obj.apply(AttributeOp::new(
        Tag(0x0028, 0x0002),
        AttributeAction::Set(PrimitiveValue::from(samples_per_pixel)),
    ));
    let _ = obj.apply(AttributeOp::new(
        Tag(0x0028, 0x0004),
        AttributeAction::SetStr(photometric.into()),
    ));
    if samples_per_pixel > 1 {
        let _ = obj.apply(AttributeOp::new(
            Tag(0x0028, 0x0006),
            AttributeAction::Set(PrimitiveValue::from(0_u16)),
        ));
    }
    let _ = obj.apply(AttributeOp::new(
        Tag(0x0028, 0x0100),
        AttributeAction::Set(PrimitiveValue::from(bits_allocated)),
    ));
    let _ = obj.apply(AttributeOp::new(
        Tag(0x0028, 0x0101),
        AttributeAction::Set(PrimitiveValue::from(bits_stored)),
    ));
    let _ = obj.apply(AttributeOp::new(
        Tag(0x0028, 0x0102),
        AttributeAction::Set(PrimitiveValue::from(high_bit)),
    ));
    let _ = obj.apply(AttributeOp::new(
        Tag(0x0028, 0x0103),
        AttributeAction::Set(PrimitiveValue::from(0_u16)),
    ));
    if number_of_frames > 1 {
        let _ = obj.apply(AttributeOp::new(
            Tag(0x0028, 0x0008),
            AttributeAction::SetStr(number_of_frames.to_string().into()),
        ));
    }

    let _ = obj.apply(AttributeOp::new(
        Tag(0x7FE0, 0x0010),
        AttributeAction::Set(PrimitiveValue::from(pixel_bytes)),
    ));

    let meta = obj.meta_mut();
    meta.transfer_syntax = "1.2.840.10008.1.2.1".to_string();
    meta.update_information_group_length();
}


// Run OCR detection and redact pixel data
fn ocr_and_redact_pixel_data(
    obj: &mut DefaultDicomObject,
    records: &[DeidRecord],
) -> Result<(), String> {
    let sensitive_values = collect_sensitive_values(records);
    if sensitive_values.is_empty() {
        return Ok(());
    }

    let pixel_data = obj
        .decode_pixel_data()
        .map_err(|err| format!("Failed to decode pixel data: {err}"))?;
    let options = ConvertOptions::new()
        .with_modality_lut(ModalityLutOption::None)
        .with_voi_lut(VoiLutOption::Identity);

    let mut output_bytes = Vec::new();
    let mut first_frame_info = None;
    let mut redacted = false;

    for frame_index in 0..pixel_data.number_of_frames() {
        let mut image = pixel_data
            .to_dynamic_image_with_options(frame_index, &options)
            .map_err(|err| format!("Failed to convert frame {frame_index}: {err}"))?;

        let (samples_per_pixel, bits_allocated, bits_stored, high_bit) = match &image {
            DynamicImage::ImageRgb16(_) => (3_u16, 16_u16, 16_u16, 15_u16),
            DynamicImage::ImageLuma16(_) => (1_u16, 16_u16, 16_u16, 15_u16),
            DynamicImage::ImageRgb8(_) => (3_u16, 8_u16, 8_u16, 7_u16),
            _ => (1_u16, 8_u16, 8_u16, 7_u16),
        };

        let ocr_image = DynamicImage::ImageLuma8(image.to_luma8());
        let rects: Vec<OcrRect> = run_ocr(&ocr_image)?
            .into_iter()
            .filter_map(|item| {
                let trimmed = item.text.trim();
                if trimmed.is_empty() {
                    return None;
                }

                println!(
                    "OCR Box: text='{}' confidence={} x={}, y={}, w={}, h={}",
                    trimmed,
                    item.confidence,
                    item.rect.x,
                    item.rect.y,
                    item.rect.width,
                    item.rect.height
                );

                if text_contains_sensitive(trimmed, &sensitive_values) {
                    println!(" -> Marked for redaction");
                    Some(item.rect)
                } else {
                    None
                }
            })
            .collect();
        if !rects.is_empty() {
            redacted = true;
            apply_redactions(&mut image, &rects);
        }

        let width = image.width();
        let height = image.height();
        let frame_bytes = image_to_frame_bytes(image, samples_per_pixel);
        output_bytes.extend_from_slice(&frame_bytes);

        if first_frame_info.is_none() {
            first_frame_info = Some((
                width,
                height,
                samples_per_pixel,
                bits_allocated,
                bits_stored,
                high_bit,
            ));
        }
    }

    if redacted {
        if let Some((width, height, samples_per_pixel, bits_allocated, bits_stored, high_bit)) =
            first_frame_info
        {
            update_pixel_data(
                obj,
                output_bytes,
                width,
                height,
                samples_per_pixel,
                pixel_data.number_of_frames(),
                bits_allocated,
                bits_stored,
                high_bit,
            );
        }
    }

    Ok(())
}

// Update UID fields in DICOM meta information
fn update_meta_uids(obj: &mut DefaultDicomObject, uid_map: &mut UidMapper) {
    let meta = obj.meta_mut();
    let old_sop_instance = meta.media_storage_sop_instance_uid().trim();
    if !old_sop_instance.is_empty() {
        let new_sop_instance = uid_map.map_uid(old_sop_instance);
        meta.media_storage_sop_instance_uid = new_sop_instance;
        meta.update_information_group_length();
    }
}

// tag De-identify a 2D DICOM file
// #[napi]
pub fn deidentify_2d_dicom(src_dcm_path: String, dst_dcm_path: String) -> Result<(), String> {
    let path = Path::new(&src_dcm_path);
    let mut obj = match open_file(path) {
        Ok(obj) => obj,
        Err(err) => {
            return Err(format!(
                "Failed to open DICOM file '{}': {}",
                src_dcm_path, err
            ));
        }
    };

    let rules = load_rules();
    let mut uid_map = UidMapper::new();
    let deid_list = build_deid_list(&mut *obj, &rules, &mut uid_map);
    let mut deid_list_for_apply = deid_list.clone();
    apply_deid_list_to_object(&mut *obj, &mut deid_list_for_apply);
    update_meta_uids(&mut obj, &mut uid_map);

    if let Err(err) = obj.write_to_file(&dst_dcm_path) {
        return Err(format!(
            "Failed to write DICOM file '{}': {}",
            dst_dcm_path, err
        ));
    }

    Ok(())
}

// tag + OCR De-identify a 2D DICOM file
// #[napi]
pub fn deidentify_2d_dicom_with_ocr(
    src_dcm_path: String,
    dst_dcm_path: String,
) -> Result<(), String> {
    let path = Path::new(&src_dcm_path);
    let mut obj = match open_file(path) {
        Ok(obj) => obj,
        Err(err) => {
            return Err(format!(
                "Failed to open DICOM file '{}': {}",
                src_dcm_path, err
            ));
        }
    };

    let rules = load_rules();
    let mut uid_map = UidMapper::new();
    let deid_list = build_deid_list(&mut *obj, &rules, &mut uid_map);
    let mut deid_list_for_apply = deid_list.clone();
    apply_deid_list_to_object(&mut *obj, &mut deid_list_for_apply);
    ocr_and_redact_pixel_data(&mut obj, &deid_list).map_err(|err| {
        format!(
            "Failed to OCR/redact DICOM file '{}': {}",
            src_dcm_path, err
        )
    })?;
    update_meta_uids(&mut obj, &mut uid_map);

    if let Err(err) = obj.write_to_file(&dst_dcm_path) {
        return Err(format!(
            "Failed to write DICOM file '{}': {}",
            dst_dcm_path, err
        ));
    }

    Ok(())
}
