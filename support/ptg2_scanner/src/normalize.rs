use serde_json::Value;
use std::cmp::Ordering;
use std::fmt::Display;
use std::io::{self, Read};
use struson::reader::{JsonReader, JsonStreamReader, ValueType};

include!("normalize/identifiers.rs");
include!("normalize/values.rs");
include!("normalize/tests.rs");
