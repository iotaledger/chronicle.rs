pub struct Trytes<'a>(&'a str);

impl<'a> Trytes<'a> {
    pub fn new(trytes: &'a str) -> Self {
        Trytes(trytes)
    }
    pub fn trytes(&self) -> &str {
        &self.0
    }
    pub fn payload(&self) -> &str {
        &self.0[..2187]
    }
    pub fn address(&self) -> &str {
        &self.0[2187..2268]
    }
    pub fn value(&self) -> &str {
        &self.0[2268..2295]
    }
    pub fn obsolete_tag(&self) -> &str {
        &self.0[2295..2322]
    }
    pub fn timestamp(&self) -> &str {
        &self.0[2322..2331]
    }
    pub fn current_index(&self) -> &str {
        &self.0[2331..2340]
    }
    pub fn last_index(&self) -> &str {
        &self.0[2340..2349]
    }
    pub fn bundle(&self) -> &str {
        &self.0[2349..2430]
    }
    pub fn trunk(&self) -> &str {
        &self.0[2430..2511]
    }
    pub fn branch(&self) -> &str {
        &self.0[2511..2592]
    }
    pub fn tag(&self) -> &str {
        &self.0[2592..2619]
    }
    pub fn atch_timestamp(&self) -> &str {
        &self.0[2619..2628]
    }
    pub fn atch_timestamp_lower(&self) -> &str {
        &self.0[2628..2637]
    }
    pub fn atch_timestamp_upper(&self) -> &str {
        &self.0[2637..2646]
    }
    pub fn nonce(&self) -> &str {
        &self.0[2646..2673]
    }
}
