//! Process-tree members and the memory reserved while collecting their names.

/// A process of a task's tree other than its root bash.
#[derive(Clone, Debug)]
pub struct Member {
    pub pid: u32,
    pub name: String,
}

/// Per-query names and conversion workspace. The callback receives a live-byte
/// high-water requirement, not an accumulated count of repeated native scans.
pub(crate) struct MemberBudget<'a> {
    reserve: Option<&'a mut dyn FnMut(usize) -> bool>,
    retained: usize,
    refused: bool,
}

impl<'a> MemberBudget<'a> {
    pub(crate) fn new(reserve: &'a mut dyn FnMut(usize) -> bool) -> Self {
        Self {
            reserve: Some(reserve),
            retained: 0,
            refused: false,
        }
    }

    pub(crate) fn unlimited() -> Self {
        Self {
            reserve: None,
            retained: 0,
            refused: false,
        }
    }

    pub(crate) fn temporary(&mut self, bytes: usize) -> bool {
        if self.refused {
            return false;
        }
        let wanted = self.retained.saturating_add(bytes);
        if self
            .reserve
            .as_mut()
            .is_some_and(|reserve| !reserve(wanted))
        {
            self.refused = true;
            return false;
        }
        true
    }

    pub(crate) fn retain(&mut self, bytes: usize) {
        self.retained = self.retained.saturating_add(bytes);
    }

    #[cfg(target_os = "freebsd")]
    pub(crate) fn reset(&mut self) {
        self.retained = 0;
    }

    pub(crate) fn refused(&self) -> bool {
        self.refused
    }
}
