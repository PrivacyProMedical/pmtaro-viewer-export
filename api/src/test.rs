
// cargo test -- --nocapture

#[test]
fn deidentify_with_ocr_propagates_ocr() {
    let src = "/Users/dxw/Downloads/dicom/超声/IM-0013-0005-0001.dcm";
    let dst = "/Users/dxw/Downloads/dicom/超声/IM-0013-0005-0001_deidentified_ocr.dcm";

    let result = crate::deidentify_2d_dicom_with_ocr(
        src.to_string(),
        dst.to_string(),
    );

    println!("De-identification result: {:?}", result);
    assert!(result.is_ok(), "De-identification with OCR failed: {:?}", result.err());
}
