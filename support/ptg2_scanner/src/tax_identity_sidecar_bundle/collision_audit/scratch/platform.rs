use super::*;
use crate::tax_identity_sidecar_bundle::digests::encode_hex;
use std::fs::OpenOptions;

impl PrivateScratch {
    #[cfg(unix)]
    pub(super) fn create_inner(root: &Path) -> io::Result<Self> {
        use std::os::fd::{AsRawFd, FromRawFd};
        use std::os::unix::fs::OpenOptionsExt;

        let root_directory_result = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(root);
        let root_directory = match root_directory_result {
            Ok(directory) => directory,
            Err(_) => return Err(invalid_data(SCRATCH_UNAVAILABLE)),
        };
        let root_metadata = match root_directory.metadata() {
            Ok(metadata) => metadata,
            Err(_) => return Err(invalid_data(SCRATCH_UNAVAILABLE)),
        };
        if !root_metadata.is_dir() {
            return Err(invalid_data(SCRATCH_UNAVAILABLE));
        }
        for _ in 0..MAX_NAME_ATTEMPTS {
            let name = secure_random_directory_name()?;
            let encoded = c_string(&name)?;
            let created =
                unsafe { libc::mkdirat(root_directory.as_raw_fd(), encoded.as_ptr(), 0o700) };
            if created != 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::AlreadyExists {
                    continue;
                }
                return Err(invalid_data(SCRATCH_UNAVAILABLE));
            }
            let descriptor = unsafe {
                libc::openat(
                    root_directory.as_raw_fd(),
                    encoded.as_ptr(),
                    libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
                )
            };
            if descriptor < 0 {
                unsafe {
                    libc::unlinkat(
                        root_directory.as_raw_fd(),
                        encoded.as_ptr(),
                        libc::AT_REMOVEDIR,
                    );
                }
                return Err(invalid_data(SCRATCH_UNAVAILABLE));
            }
            let directory = unsafe { File::from_raw_fd(descriptor) };
            let mut cleanup = PendingDirectoryCleanup::new(&root_directory, &name)?;
            if unsafe { libc::fchmod(directory.as_raw_fd(), 0o700) } != 0
                || !private_directory_metadata(&directory)?
            {
                return Err(invalid_data(SCRATCH_UNAVAILABLE));
            }
            cleanup.disarm();
            drop(cleanup);
            return Ok(Self {
                root_directory,
                directory,
                directory_name: name,
                tracked_names: RefCell::new(HashSet::new()),
            });
        }
        Err(invalid_data(SCRATCH_UNAVAILABLE))
    }

    #[cfg(not(unix))]
    pub(super) fn create_inner(root: &Path) -> io::Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix("ptg2-tax-collision-")
            .tempdir_in(root)
            .map_err(|_| invalid_data(SCRATCH_UNAVAILABLE))?;
        Ok(Self {
            directory,
            tracked_names: RefCell::new(HashSet::new()),
        })
    }

    #[cfg(unix)]
    pub(super) fn create_new_file(&self, name: &str) -> io::Result<File> {
        use std::os::fd::{AsRawFd, FromRawFd};

        validate_run_name(name)?;
        let encoded = c_string(name)?;
        let descriptor = unsafe {
            libc::openat(
                self.directory.as_raw_fd(),
                encoded.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
        };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }

    #[cfg(not(unix))]
    pub(super) fn create_new_file(&self, name: &str) -> io::Result<File> {
        validate_run_name(name)?;
        OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(self.directory.path().join(name))
    }

    #[cfg(unix)]
    pub(super) fn secure_created_file(&self, file: &File) -> io::Result<()> {
        use std::os::fd::AsRawFd;

        if unsafe { libc::fchmod(file.as_raw_fd(), 0o600) } != 0 {
            return Err(invalid_data(SCRATCH_UNAVAILABLE));
        }
        let metadata = match file.metadata() {
            Ok(metadata) => metadata,
            Err(_) => return Err(invalid_data(SCRATCH_UNAVAILABLE)),
        };
        if !metadata.is_file() {
            return Err(invalid_data(SCRATCH_UNAVAILABLE));
        }
        Ok(())
    }

    #[cfg(not(unix))]
    pub(super) fn secure_created_file(&self, file: &File) -> io::Result<()> {
        if !file
            .metadata()
            .map_err(|_| invalid_data(SCRATCH_UNAVAILABLE))?
            .is_file()
        {
            return Err(invalid_data(SCRATCH_UNAVAILABLE));
        }
        Ok(())
    }

    #[cfg(unix)]
    pub(super) fn open_existing_file(&self, name: &str) -> io::Result<File> {
        use std::os::fd::{AsRawFd, FromRawFd};

        validate_run_name(name)?;
        let descriptor = unsafe {
            libc::openat(
                self.directory.as_raw_fd(),
                c_string(name)?.as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            )
        };
        if descriptor < 0 {
            return Err(invalid_data(SCRATCH_UNAVAILABLE));
        }
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }

    #[cfg(not(unix))]
    pub(super) fn open_existing_file(&self, name: &str) -> io::Result<File> {
        validate_run_name(name)?;
        OpenOptions::new()
            .read(true)
            .open(self.directory.path().join(name))
            .map_err(|_| invalid_data(SCRATCH_UNAVAILABLE))
    }

    #[cfg(unix)]
    pub(super) fn unlink(&self, name: &str) -> io::Result<()> {
        use std::os::fd::AsRawFd;

        validate_run_name(name)?;
        let result =
            unsafe { libc::unlinkat(self.directory.as_raw_fd(), c_string(name)?.as_ptr(), 0) };
        if result == 0 || io::Error::last_os_error().kind() == io::ErrorKind::NotFound {
            Ok(())
        } else {
            Err(invalid_data(SCRATCH_UNAVAILABLE))
        }
    }

    #[cfg(not(unix))]
    pub(super) fn unlink(&self, name: &str) -> io::Result<()> {
        validate_run_name(name)?;
        match std::fs::remove_file(self.directory.path().join(name)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(invalid_data(SCRATCH_UNAVAILABLE)),
        }
    }

    #[cfg(unix)]
    pub(super) fn available_bytes(&self) -> io::Result<u64> {
        use std::mem::MaybeUninit;
        use std::os::fd::AsRawFd;

        let mut statistics = MaybeUninit::<libc::statvfs>::uninit();
        if unsafe { libc::fstatvfs(self.directory.as_raw_fd(), statistics.as_mut_ptr()) } != 0 {
            return Err(invalid_data(SCRATCH_UNAVAILABLE));
        }
        let statistics = unsafe { statistics.assume_init() };
        let available = u128::from(statistics.f_bavail)
            .checked_mul(u128::from(statistics.f_frsize))
            .ok_or(invalid_data(SCRATCH_CAPACITY_INSUFFICIENT))?;
        match u64::try_from(available) {
            Ok(available) => Ok(available),
            Err(_) => Err(invalid_data(SCRATCH_CAPACITY_INSUFFICIENT)),
        }
    }

    #[cfg(not(unix))]
    pub(super) fn available_bytes(&self) -> io::Result<u64> {
        Err(invalid_data(SCRATCH_UNAVAILABLE))
    }
}

#[cfg(unix)]
struct PendingDirectoryCleanup<'a> {
    root_directory: &'a File,
    encoded_name: std::ffi::CString,
    armed: bool,
}

#[cfg(unix)]
impl<'a> PendingDirectoryCleanup<'a> {
    fn new(root_directory: &'a File, name: &str) -> io::Result<Self> {
        Ok(Self {
            root_directory,
            encoded_name: c_string(name)?,
            armed: true,
        })
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(unix)]
impl Drop for PendingDirectoryCleanup<'_> {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;

        if self.armed {
            unsafe {
                libc::unlinkat(
                    self.root_directory.as_raw_fd(),
                    self.encoded_name.as_ptr(),
                    libc::AT_REMOVEDIR,
                );
            }
        }
    }
}

#[cfg(unix)]
impl Drop for PrivateScratch {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;

        for name in self.tracked_names.get_mut().drain() {
            if let Ok(encoded) = c_string(&name) {
                unsafe {
                    libc::unlinkat(self.directory.as_raw_fd(), encoded.as_ptr(), 0);
                }
            }
        }
        if let Ok(encoded) = c_string(&self.directory_name) {
            unsafe {
                libc::unlinkat(
                    self.root_directory.as_raw_fd(),
                    encoded.as_ptr(),
                    libc::AT_REMOVEDIR,
                );
            }
        }
    }
}

#[cfg(unix)]
fn secure_random_directory_name() -> io::Result<String> {
    use std::os::unix::fs::{FileTypeExt, OpenOptionsExt};

    let random_result = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open("/dev/urandom");
    let mut random = match random_result {
        Ok(file) => file,
        Err(_) => return Err(invalid_data(SCRATCH_UNAVAILABLE)),
    };
    let metadata = match random.metadata() {
        Ok(metadata) => metadata,
        Err(_) => return Err(invalid_data(SCRATCH_UNAVAILABLE)),
    };
    if !metadata.file_type().is_char_device() {
        return Err(invalid_data(SCRATCH_UNAVAILABLE));
    }
    let mut entropy = [0u8; 16];
    if random.read_exact(&mut entropy).is_err() {
        return Err(invalid_data(SCRATCH_UNAVAILABLE));
    }
    let mut name = String::with_capacity(52);
    name.push_str(".ptg2-tax-collision-");
    name.push_str(&encode_hex(&entropy));
    Ok(name)
}

#[cfg(unix)]
fn private_directory_metadata(directory: &File) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt;

    let metadata = match directory.metadata() {
        Ok(metadata) => metadata,
        Err(_) => return Err(invalid_data(SCRATCH_UNAVAILABLE)),
    };
    Ok(metadata.is_dir()
        && metadata.mode() & 0o777 == 0o700
        && metadata.uid() == unsafe { libc::geteuid() })
}

#[cfg(unix)]
fn c_string(value: &str) -> io::Result<std::ffi::CString> {
    match std::ffi::CString::new(value) {
        Ok(encoded) => Ok(encoded),
        Err(_) => Err(invalid_data(SCRATCH_UNAVAILABLE)),
    }
}

#[cfg(all(test, unix))]
mod platform_guard_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn private_directory_metadata_and_name_encoding_fail_closed() {
        let temporary = tempfile::tempdir().unwrap();
        let directory_path = temporary.path().join("candidate");
        std::fs::create_dir(&directory_path).unwrap();
        std::fs::set_permissions(&directory_path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let directory = File::open(&directory_path).unwrap();
        assert!(!private_directory_metadata(&directory).unwrap());

        std::fs::set_permissions(&directory_path, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(private_directory_metadata(&directory).unwrap());
        assert_eq!(
            c_string("invalid\0name").unwrap_err().to_string(),
            SCRATCH_UNAVAILABLE
        );
        let random_name = secure_random_directory_name().unwrap();
        assert!(random_name.starts_with(".ptg2-tax-collision-"));
        validate_run_name(&random_name).unwrap();
    }

    #[test]
    fn scratch_root_must_exist_and_be_a_directory() {
        let temporary = tempfile::tempdir().unwrap();
        let missing = temporary.path().join("missing");
        assert_eq!(
            PrivateScratch::create_inner(&missing)
                .err()
                .unwrap()
                .to_string(),
            SCRATCH_UNAVAILABLE
        );
        let file_path = temporary.path().join("regular-file");
        std::fs::write(&file_path, b"synthetic").unwrap();
        assert_eq!(
            PrivateScratch::create_inner(&file_path)
                .err()
                .unwrap()
                .to_string(),
            SCRATCH_UNAVAILABLE
        );
    }
}
