//! A plain language log of what Irminsul is doing, shown in the activity
//! panel.
//!
//! Entries are mirrored to the tracing log so they also end up in bug reports.

use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard, OnceLock};

use chrono::{DateTime, Local};

const MAX_ENTRIES: usize = 300;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Level {
    Info,
    Success,
    Warning,
    Error,
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub time: DateTime<Local>,
    pub level: Level,
    pub message: String,
    /// What the user can do about it.
    pub hint: Option<String>,
    /// How many times in a row this entry was logged.
    pub repeats: u32,
}

#[derive(Default)]
struct ActivityLog {
    entries: VecDeque<Entry>,
}

impl ActivityLog {
    fn push(&mut self, time: DateTime<Local>, level: Level, message: String, hint: Option<String>) {
        if let Some(last) = self.entries.back_mut()
            && last.level == level
            && last.message == message
            && last.hint == hint
        {
            last.repeats += 1;
            last.time = time;
            return;
        }

        if self.entries.len() == MAX_ENTRIES {
            self.entries.pop_front();
        }
        self.entries.push_back(Entry {
            time,
            level,
            message,
            hint,
            repeats: 1,
        });
    }
}

static LOG: Mutex<ActivityLog> = Mutex::new(ActivityLog {
    entries: VecDeque::new(),
});
static REPAINT_CTX: OnceLock<egui::Context> = OnceLock::new();

fn lock() -> MutexGuard<'static, ActivityLog> {
    LOG.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Lets new entries wake up the UI, which otherwise only repaints on input.
pub fn set_repaint_context(ctx: egui::Context) {
    let _ = REPAINT_CTX.set(ctx);
}

pub fn log(level: Level, message: impl Into<String>, hint: Option<&str>) {
    let message = message.into();
    let hint = hint.map(str::to_owned);

    let hint_suffix = hint
        .as_deref()
        .map(|hint| format!(" ({hint})"))
        .unwrap_or_default();
    match level {
        Level::Info | Level::Success => {
            tracing::info!(target: "irminsul::activity", "{message}{hint_suffix}")
        }
        Level::Warning => tracing::warn!(target: "irminsul::activity", "{message}{hint_suffix}"),
        Level::Error => tracing::error!(target: "irminsul::activity", "{message}{hint_suffix}"),
    }

    lock().push(Local::now(), level, message, hint);

    if let Some(ctx) = REPAINT_CTX.get() {
        ctx.request_repaint();
    }
}

pub fn info(message: impl Into<String>) {
    log(Level::Info, message, None);
}

pub fn success(message: impl Into<String>) {
    log(Level::Success, message, None);
}

pub fn warning(message: impl Into<String>) {
    log(Level::Warning, message, None);
}

pub fn error(message: impl Into<String>) {
    log(Level::Error, message, None);
}

pub fn entries() -> Vec<Entry> {
    lock().entries.iter().cloned().collect()
}

pub fn clear() {
    lock().entries.clear();
}

/// Formats a count with thousands separators, e.g. `2,271`.
pub fn format_count(count: usize) -> String {
    let digits = count.to_string();
    let mut formatted = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            formatted.push(',');
        }
        formatted.push(digit);
    }
    formatted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_entries_are_collapsed() {
        let mut log = ActivityLog::default();
        let now = Local::now();
        log.push(now, Level::Error, "failed".into(), None);
        log.push(now, Level::Error, "failed".into(), None);
        log.push(now, Level::Info, "failed".into(), None);
        log.push(now, Level::Info, "failed".into(), Some("hint".into()));

        let repeats: Vec<u32> = log.entries.iter().map(|entry| entry.repeats).collect();
        assert_eq!(repeats, [2, 1, 1]);
    }

    #[test]
    fn oldest_entries_are_dropped() {
        let mut log = ActivityLog::default();
        let now = Local::now();
        for i in 0..MAX_ENTRIES + 5 {
            log.push(now, Level::Info, i.to_string(), None);
        }

        assert_eq!(log.entries.len(), MAX_ENTRIES);
        assert_eq!(log.entries.front().unwrap().message, "5");
    }

    #[test]
    fn counts_have_thousands_separators() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1000), "1,000");
        assert_eq!(format_count(2271), "2,271");
        assert_eq!(format_count(1234567), "1,234,567");
    }
}
