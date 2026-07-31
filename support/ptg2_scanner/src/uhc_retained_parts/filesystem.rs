// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    byte_count: u64,
    mode: u32,
    link_count: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

impl FileIdentity {
    fn from_metadata(metadata: &Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            byte_count: metadata.len(),
            mode: metadata.mode(),
            link_count: metadata.nlink(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        }
    }

    #[cfg(target_os = "linux")]
    fn from_file(file: &File) -> io::Result<Self> {
        let mut metadata = unsafe { std::mem::zeroed::<libc::statx>() };
        let result = unsafe {
            libc::statx(
                file.as_raw_fd(),
                c"".as_ptr(),
                libc::AT_EMPTY_PATH | libc::AT_STATX_FORCE_SYNC,
                libc::STATX_BASIC_STATS,
                &mut metadata,
            )
        };
        Self::from_statx_result(result, &metadata)
    }

    #[cfg(target_os = "linux")]
    fn from_statx_result(result: libc::c_int, metadata: &libc::statx) -> io::Result<Self> {
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        if metadata.stx_mask & libc::STATX_BASIC_STATS != libc::STATX_BASIC_STATS {
            return Err(invalid_data(
                "UHC retained forced file identity is incomplete",
            ));
        }
        Ok(Self {
            device: libc::makedev(metadata.stx_dev_major, metadata.stx_dev_minor),
            inode: metadata.stx_ino,
            byte_count: metadata.stx_size,
            mode: u32::from(metadata.stx_mode),
            link_count: u64::from(metadata.stx_nlink),
            modified_seconds: metadata.stx_mtime.tv_sec,
            modified_nanoseconds: i64::from(metadata.stx_mtime.tv_nsec),
            changed_seconds: metadata.stx_ctime.tv_sec,
            changed_nanoseconds: i64::from(metadata.stx_ctime.tv_nsec),
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn from_file(file: &File) -> io::Result<Self> {
        Ok(Self::from_metadata(&file.metadata()?))
    }
}

fn require_stable_regular_file(
    file: &File,
    identity: FileIdentity,
    expected_byte_count: u64,
    label: &str,
) -> io::Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(invalid_data(format!(
            "UHC retained {label} is not a regular file"
        )));
    }
    let observed = FileIdentity::from_file(file)?;
    if observed != identity || observed.byte_count != expected_byte_count {
        return Err(invalid_data(format!(
            "UHC retained {label} changed while it was being verified"
        )));
    }
    Ok(())
}

fn open_regular_nofollow(path: &Path, label: &str) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid_data(format!(
            "UHC retained {label} is not a regular file"
        )));
    }
    Ok(file)
}

fn c_string(value: &str, label: &str) -> io::Result<CString> {
    match CString::new(value) {
        Ok(value) => Ok(value),
        Err(_) => Err(invalid_input(format!("{label} contains a NUL byte"))),
    }
}

struct RootDirectory {
    supplied_path: PathBuf,
    path: PathBuf,
    directory: File,
    identity: FileIdentity,
}

impl RootDirectory {
    fn open(path: &Path) -> io::Result<Arc<Self>> {
        let supplied_path = if path.is_absolute() {
            path.to_owned()
        } else {
            env::current_dir()?.join(path)
        };
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(&supplied_path)?;
        let metadata = directory.metadata()?;
        if !metadata.is_dir() {
            return Err(invalid_input("UHC retained output root is not a directory"));
        }
        let canonical_path = supplied_path.canonicalize()?;
        let identity = FileIdentity::from_file(&directory)?;
        let root = Arc::new(Self {
            supplied_path,
            path: canonical_path,
            directory,
            identity,
        });
        root.verify_path_identity()?;
        Ok(root)
    }

    fn descriptor(&self) -> RawFd {
        self.directory.as_raw_fd()
    }

    fn verify_path_identity(&self) -> io::Result<()> {
        let descriptor_metadata = self.directory.metadata()?;
        let descriptor_identity = FileIdentity::from_file(&self.directory)?;
        if !descriptor_metadata.is_dir() || !same_inode(descriptor_identity, self.identity) {
            return Err(invalid_data(
                "UHC retained output root identity changed during admission",
            ));
        }
        for candidate in [&self.supplied_path, &self.path] {
            let metadata = fs::symlink_metadata(candidate)?;
            let observed = FileIdentity::from_metadata(&metadata);
            if metadata.file_type().is_symlink()
                || !metadata.is_dir()
                || !same_inode(observed, self.identity)
            {
                return Err(invalid_data(
                    "UHC retained output root path changed during admission",
                ));
            }
        }
        Ok(())
    }

    fn output_path(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }

    fn open_existing_regular(&self, name: &str) -> io::Result<Option<File>> {
        let encoded_name = c_string(name, "UHC retained file name")?;
        for attempt in 0..PUBLICATION_LINK_RETRIES {
            let descriptor = unsafe {
                libc::openat(
                    self.descriptor(),
                    encoded_name.as_ptr(),
                    libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                )
            };
            if descriptor < 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::ENOENT) {
                    return Ok(None);
                }
                return Err(error);
            }
            let file = unsafe { File::from_raw_fd(descriptor) };
            let metadata = file.metadata()?;
            if !metadata.is_file() {
                return Err(invalid_data(format!(
                    "UHC retained existing {name} is not a regular file"
                )));
            }
            let identity = FileIdentity::from_file(&file)?;
            if identity.link_count == 1 && identity.mode & 0o022 == 0 {
                return Ok(Some(file));
            }
            if identity.link_count == 2
                && identity.mode & 0o022 == 0
                && attempt + 1 < PUBLICATION_LINK_RETRIES
            {
                drop(file);
                thread::sleep(PUBLICATION_LINK_RETRY_DELAY);
                continue;
            }
            return Err(invalid_data(format!(
                "UHC retained existing {name} must have one link and no group/other write bits"
            )));
        }
        unreachable!("publication-link retry loop always returns")
    }

    fn create_temporary(self: &Arc<Self>, label: &str) -> io::Result<RootTemporaryFile> {
        for _attempt in 0..64 {
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let name = format!(
                ".uhc-retain-{label}-{}-{timestamp}-{sequence}.partial",
                std::process::id()
            );
            let encoded_name = c_string(&name, "UHC retained temporary file name")?;
            let descriptor = unsafe {
                libc::openat(
                    self.descriptor(),
                    encoded_name.as_ptr(),
                    libc::O_RDWR
                        | libc::O_CREAT
                        | libc::O_EXCL
                        | libc::O_CLOEXEC
                        | libc::O_NOFOLLOW,
                    0o600,
                )
            };
            if descriptor >= 0 {
                return Ok(RootTemporaryFile {
                    root: Arc::clone(self),
                    name,
                    file: Some(unsafe { File::from_raw_fd(descriptor) }),
                    cleanup_required: true,
                    #[cfg(test)]
                    pre_unlink_probe: None,
                });
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EEXIST) {
                return Err(error);
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "unable to allocate a unique UHC retained temporary file",
        ))
    }

    fn unlink(&self, name: &str) -> io::Result<()> {
        let encoded_name = c_string(name, "UHC retained temporary file name")?;
        let result = unsafe { libc::unlinkat(self.descriptor(), encoded_name.as_ptr(), 0) };
        if result == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ENOENT) {
            return Ok(());
        }
        Err(error)
    }

    fn sync(&self) -> io::Result<()> {
        self.directory.sync_all()
    }
}

struct RootTemporaryFile {
    root: Arc<RootDirectory>,
    name: String,
    file: Option<File>,
    cleanup_required: bool,
    #[cfg(test)]
    pre_unlink_probe: Option<Box<dyn Fn()>>,
}

impl RootTemporaryFile {
    fn file(&self) -> &File {
        self.file
            .as_ref()
            .expect("retained temporary file descriptor must be open")
    }

    fn file_mut(&mut self) -> &mut File {
        self.file
            .as_mut()
            .expect("retained temporary file descriptor must be open")
    }

    fn close_and_unlink(&mut self) -> io::Result<()> {
        drop(self.file.take());
        #[cfg(test)]
        if let Some(probe) = self.pre_unlink_probe.take() {
            probe();
        }
        self.root.unlink(&self.name)?;
        self.cleanup_required = false;
        Ok(())
    }

    fn publish_noclobber(mut self, final_name: &str) -> io::Result<bool> {
        self.root.verify_path_identity()?;
        let temporary_name = c_string(&self.name, "UHC retained temporary file name")?;
        let encoded_final = c_string(final_name, "UHC retained final file name")?;
        let linked = unsafe {
            libc::linkat(
                self.root.descriptor(),
                temporary_name.as_ptr(),
                self.root.descriptor(),
                encoded_final.as_ptr(),
                0,
            )
        };
        if linked != 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EEXIST) {
                self.close_and_unlink()?;
                return Ok(false);
            }
            return Err(error);
        }
        self.close_and_unlink()?;
        Ok(true)
    }
}

impl Drop for RootTemporaryFile {
    fn drop(&mut self) {
        if self.cleanup_required {
            let _ = self.close_and_unlink();
        }
    }
}
