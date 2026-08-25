//! Progress reporting for long-running scanner passes.

use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
include!("progress/runtime.rs");
include!("progress/tests.rs");
