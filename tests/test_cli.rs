use std::{
    fs::File,
    io::{Read, Write},
};

use assert_cmd::Command;
use httpmock::{Method::HEAD, prelude::*};
use predicates::prelude::*;
use tempfile::TempDir;

#[test]
fn test_help() {
    let mut cmd = Command::cargo_bin("http-replay-bench").unwrap();
    cmd.arg("--help");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("http-replay-bench"));
}

// Dummy file length for the mock server to return.
static TOTAL_LENGTH: usize = 16384;

#[test]
fn test_mock_http() -> std::io::Result<()> {
    let server = MockServer::start();
    let mock_header = server.mock(|when, then| {
        when.method(HEAD).path_prefix("/data/");
        then.status(reqwest::StatusCode::OK)
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", TOTAL_LENGTH.to_string());
    });
    let mock_body = server.mock(|when, then| {
        when.method(GET)
            .path_prefix("/data/")
            .header_prefix("Range", "bytes=");
        then.respond_with(|req| {
            let range_parts = req
                .headers()
                .get("Range")
                .unwrap()
                .to_str()
                .unwrap()
                .strip_prefix("bytes=")
                .unwrap()
                .split(',')
                .map(|s| {
                    s.trim()
                        .split('-')
                        .map(|s| s.parse().unwrap())
                        .collect::<Vec<usize>>()
                })
                .collect::<Vec<_>>();
            if range_parts.len() != 1 {
                // TODO: Support multiple ranges in a single request.
                return HttpMockResponse::builder()
                    .status(reqwest::StatusCode::INTERNAL_SERVER_ERROR.into())
                    .body(vec![])
                    .build();
            }
            let start: usize = range_parts[0][0];
            let end: usize = range_parts[0][1];
            if start >= TOTAL_LENGTH || end >= TOTAL_LENGTH || start > end {
                return HttpMockResponse::builder()
                    .status(reqwest::StatusCode::RANGE_NOT_SATISFIABLE.into())
                    .body(vec![])
                    .build();
            }
            let content_length = end - start + 1;
            let body = vec![0u8; content_length];
            HttpMockResponse::builder()
                .status(206)
                .header("Content-Type", "application/octet-stream")
                .header(
                    "Content-Range",
                    format!("bytes {}-{}/{}", start, end, TOTAL_LENGTH),
                )
                .header("Content-Length", content_length.to_string())
                .body(body)
                .build()
        });
    });

    let tmpdir = TempDir::new()?;
    let paths_file = tmpdir.path().join("test_paths.txt");
    {
        let mut file = File::create(paths_file.clone())?;
        file.write("file1\nfile2\nfile3\n".as_bytes())?;
        file.sync_all()?;
    }
    let output_file = tmpdir.path().join("output.json");

    let mut cmd = Command::cargo_bin("http-replay-bench").unwrap();
    cmd.arg("--endpoint")
        .arg(server.url("/data/"))
        .arg("--path-file")
        .arg(paths_file.as_os_str())
        .arg("--num-workers")
        .arg("2")
        .arg("--worker-concurrency")
        .arg("2")
        .arg("--output-file")
        .arg(output_file.as_os_str());
    cmd.arg("pattern")
        .arg("--num-requests")
        .arg("5")
        .arg("--request-size")
        .arg("1024");
    cmd.assert().success();

    mock_header.assert_calls(3);
    mock_body.assert_calls(3 * 5); // 3 files * 5 requests each

    let mut output_file = File::open(output_file)?;
    let mut output_content = String::new();
    output_file.read_to_string(&mut output_content)?;
    eprintln!("Output content: {}", output_content);
    let json: serde_json::Value = serde_json::from_str(&output_content).expect("valid json");
    json.as_object()
        .expect("is json object")
        .get("client_stats")
        .expect("client_stats exists")
        .as_array()
        .expect("client_stats is array")
        .iter()
        .for_each(|stat| {
            let stat = stat.as_object().expect("stat is object");
            let requests = stat
                .get("requests")
                .expect("requests exists")
                .as_u64()
                .expect("requests is u64");
            assert_eq!(requests, 5, "requests should be 5");
            let total_bytes = stat
                .get("total_bytes")
                .expect("total_bytes exists")
                .as_u64()
                .expect("total_bytes is u64");
            assert_eq!(total_bytes, 5 * 1024, "total_bytes should be 5 * 1024");
        });

    Ok(())
}
