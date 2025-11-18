use emsqrt_core::config::EngineConfig;
use emsqrt_io::storage::build_storage_from_config;
use std::fs;

fn temp_spill_dir(name: &str) -> String {
    let mut dir = std::env::temp_dir();
    dir.push(format!("emsqrt-storage-tests-{name}"));
    let _ = fs::remove_dir_all(&dir);
    dir.to_string_lossy().to_string()
}

#[test]
fn test_file_storage_builder_write_read() {
    let dir = temp_spill_dir("fs");
    let mut cfg = EngineConfig::default();
    cfg.spill_dir = dir.clone();

    let storage_cfg = cfg.storage_config();
    let storage = build_storage_from_config(&storage_cfg).expect("fs storage");

    let path = format!("{}/segment.seg", dir);
    let bytes = b"hello world";
    storage.write(&path, bytes).expect("write");
    let roundtrip = storage.read_range(&path, 0, bytes.len()).expect("read");
    assert_eq!(roundtrip, bytes);
}

#[test]
fn test_invalid_scheme_errors() {
    let mut cfg = EngineConfig::default();
    cfg.spill_uri = Some("ftp://example.com/spill".into());
    let storage_cfg = cfg.storage_config();
    let err = build_storage_from_config(&storage_cfg)
        .err()
        .expect("should fail");
    assert!(err.to_string().contains("unsupported spill scheme"));
}

#[cfg(not(feature = "s3"))]
#[test]
fn test_s3_without_feature_fails() {
    let mut cfg = EngineConfig::default();
    cfg.spill_uri = Some("s3://dummy/test".into());
    cfg.spill_aws_region = Some("us-east-1".into());
    let storage_cfg = cfg.storage_config();
    let err = build_storage_from_config(&storage_cfg)
        .err()
        .expect("feature missing");
    assert!(err
        .to_string()
        .contains("EM-√ was built without the `s3` feature"));
}

#[cfg(all(feature = "s3"))]
#[test]
fn test_s3_builder_initializes_with_dummy_credentials() {
    let mut cfg = EngineConfig::default();
    cfg.spill_uri = Some("s3://dummy-bucket/tests".into());
    cfg.spill_aws_region = Some("us-east-1".into());
    cfg.spill_aws_access_key_id = Some("ACCESSKEY123".into());
    cfg.spill_aws_secret_access_key = Some("SECRETKEY456".into());
    let storage_cfg = cfg.storage_config();
    build_storage_from_config(&storage_cfg).expect("s3 storage builds");
}
