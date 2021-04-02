use assert_cmd::assert::OutputAssertExt;
use assert_cmd::Command;

#[test]
fn test_network() {
    Command::new("nc")
        .args(&["localhost", "9092"])
        .unwrap()
        .assert()
        .append_context("main", "kafka not running at localhost:9092")
        .success();
}
