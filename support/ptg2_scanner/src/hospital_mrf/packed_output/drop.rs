impl Drop for PackedOutputBuilder {
    fn drop(&mut self) {
        drop(self.selector_spool.take());
        let _ = fs::remove_file(&self.selector_spool_path);
        if self.selector_sorted_owned {
            let _ = fs::remove_file(&self.selector_sorted_path);
        }
        let _ = fs::remove_dir_all(&self.selector_sort_directory);
    }
}
