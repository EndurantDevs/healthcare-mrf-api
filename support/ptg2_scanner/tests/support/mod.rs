use std::io::{self, ErrorKind, Write};
use std::process::{Child, Output};

pub(crate) fn write_stdin_and_wait(mut child: Child, input: &[u8]) -> Output {
    let input_result = child
        .stdin
        .take()
        .expect("child stdin must be piped")
        .write_all(input);
    let output = child.wait_with_output().expect("child process must finish");
    finish_stdin_write(input_result, output)
}

fn finish_stdin_write(input_result: io::Result<()>, output: Output) -> Output {
    match input_result {
        Ok(()) => output,
        Err(error) if error.kind() == ErrorKind::BrokenPipe && !output.status.success() => output,
        Err(error) => panic!("failed to write child stdin: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::catch_unwind;
    use std::process::Command;

    fn scanner_output(arguments: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
            .args(arguments)
            .output()
            .expect("scanner process must finish")
    }

    #[test]
    fn stdin_write_result_only_allows_broken_pipe_from_failed_child() {
        let completed = finish_stdin_write(Ok(()), scanner_output(&["--canon-version"]));
        assert!(completed.status.success());

        let completed = finish_stdin_write(
            Err(io::Error::from(ErrorKind::BrokenPipe)),
            scanner_output(&["--unsupported-test-mode"]),
        );
        assert!(!completed.status.success());

        let successful_broken_pipe = catch_unwind(|| {
            finish_stdin_write(
                Err(io::Error::from(ErrorKind::BrokenPipe)),
                scanner_output(&["--canon-version"]),
            )
        });
        assert!(successful_broken_pipe.is_err());

        let failed_other_error = catch_unwind(|| {
            finish_stdin_write(
                Err(io::Error::from(ErrorKind::PermissionDenied)),
                scanner_output(&["--unsupported-test-mode"]),
            )
        });
        assert!(failed_other_error.is_err());
    }
}
