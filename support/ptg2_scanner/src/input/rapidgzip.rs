//! Optional external rapidgzip reader used by the full scanner pass.

use super::{is_gzip, open_reader, strict_utf8_reader};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdout, Command, Stdio};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

include!("rapidgzip/config_and_state.rs");
include!("rapidgzip/process.rs");
include!("rapidgzip/tests.rs");
