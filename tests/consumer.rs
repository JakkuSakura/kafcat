use assert_cmd::Command;

#[test]
fn consumer_short() {
    let mut cmd = Command::cargo_bin("kafcat").unwrap();
    cmd.args(&["-C", "-t", "test", "-e"]).assert().success();
}
