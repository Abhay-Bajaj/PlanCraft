use std::process::Command;

// Run the compiled binary directly
fn run_bin(args: &[&str]) -> (String, String, i32) {
    let exe = env!("CARGO_BIN_EXE_mini_query_engine");

    let output = Command::new(exe)
        .args(args)
        .output()
        .expect("failed to execute mini_query_engine binary");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let code = output.status.code().unwrap_or(-1);

    (stdout, stderr, code)
}

fn run_all(args: &[&str]) -> String {
    let (out, err, code) = run_bin(args);
    assert_eq!(code, 0, "process failed.\nSTDOUT:\n{out}\nSTDERR:\n{err}");
    format!("{out}{err}")
}

// Test 1
#[test]
fn parsing_and_explain_work() {
    let all = run_all(&["--explain", "queries/q1_sum_by_user.json"]);

    assert!(!all.trim().is_empty(), "Explain output was empty.\n{all}");

    let has_plan_node = all.contains("Scan")
        || all.contains("Project")
        || all.contains("Filter")
        || all.contains("Aggregate")
        || all.contains("Limit");

    assert!(
        has_plan_node,
        "Explain output did not contain a plan node:\n{all}"
    );
}

// Test 2
#[test]
fn group_by_sum_works() {
    let out = run_all(&["queries/q2_group_sum.json"]);

    assert!(out.contains("u1"));
    assert!(out.contains("130.0"));

    assert!(out.contains("u2"));
    assert!(out.contains("95.0"));

    assert!(out.contains("u3"));
    assert!(out.contains("55.0"));

    assert!(out.contains("u4"));
    assert!(out.contains("200.0"));
}

// Test 3
#[test]
fn count_star_works() {
    let out = run_all(&["queries/q3_sum_and_count.json"]);

    assert!(out.contains("count(*)"));
    assert!(out.contains("u1"));
    assert!(out.contains("u3"));
}

// Test 4
#[test]
fn filtered_grouped_query_works() {
    let out = run_all(&["queries/q4_filtered_grouped.json"]);

    assert!(out.contains("u1"));
    assert!(out.contains("u4"));
    assert!(!out.contains("u2"));
    assert!(!out.contains("u3"));
}
