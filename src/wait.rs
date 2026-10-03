//! The wait of `start` and `poll`: it ends when the task ends, `waitMs` elapses, or a
//! `returnWhen` condition holds.

use crate::output::{Cleaner, Matcher};
use crate::tasks::Task;
use encoding_rs::Encoding;
use rmcp::RoleServer;
use rmcp::model::ProgressNotificationParam;
use rmcp::service::RequestContext;
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};

const PROGRESS_EVERY: Duration = Duration::from_secs(20);
const MAX_TEXT_BYTES: usize = 4096;
const QUIET_MS: std::ops::RangeInclusive<u64> = 10..=60_000;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReturnWhen {
    /// Unseen output, cleaned as results show it, contains the text on one line; text a carriage return later rewrites counts.
    output_contains: Option<Text>,
    /// After output to see arrives, none follows for this many ms, 10-60000. With input or keys, only output after them counts, a PTY's echo of them included.
    #[schemars(range(min = 10, max = 60000))]
    output_quiet_for_ms: Option<u64>,
    /// PTY only: a row of the rendered screen contains the text.
    screen_contains: Option<Text>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Text {
    /// Literal text without line breaks, 1-4096 bytes.
    text: String,
    /// Default true; false ignores the case of ASCII letters only.
    case_sensitive: Option<bool>,
}

impl Text {
    fn found_in(&self, row: &str) -> bool {
        if self.case_sensitive.unwrap_or(true) {
            row.contains(&self.text)
        } else {
            row.to_ascii_lowercase()
                .contains(&self.text.to_ascii_lowercase())
        }
    }
}

impl ReturnWhen {
    pub fn validate(&self) -> Result<(), String> {
        if self.output_contains.is_none()
            && self.output_quiet_for_ms.is_none()
            && self.screen_contains.is_none()
        {
            return Err(
                "returnWhen needs outputContains, outputQuietForMs, or screenContains.".into(),
            );
        }
        for (name, text) in [
            ("outputContains", &self.output_contains),
            ("screenContains", &self.screen_contains),
        ] {
            let Some(text) = text.as_ref().map(|text| &text.text) else {
                continue;
            };
            if text.is_empty() || text.len() > MAX_TEXT_BYTES || text.contains(['\n', '\r']) {
                return Err(format!(
                    "{name}.text must be 1-{MAX_TEXT_BYTES} bytes without line breaks."
                ));
            }
        }
        if let Some(quiet) = self
            .output_quiet_for_ms
            .filter(|quiet| !QUIET_MS.contains(quiet))
        {
            return Err(format!(
                "outputQuietForMs {quiet} is out of range 10-60000."
            ));
        }
        Ok(())
    }

    pub fn watches_screen(&self) -> bool {
        self.screen_contains.is_some()
    }
}

/// Why a wait ended, in the precedence order of conditions that hold together.
#[derive(Clone, Copy)]
pub enum WaitEnd {
    TaskEnded,
    OutputContains,
    ScreenContains,
    OutputQuiet,
    MaxWait,
}

impl WaitEnd {
    pub fn name(self) -> &'static str {
        match self {
            WaitEnd::TaskEnded => "task_ended",
            WaitEnd::OutputContains => "output_contains",
            WaitEnd::ScreenContains => "screen_contains",
            WaitEnd::OutputQuiet => "output_quiet",
            WaitEnd::MaxWait => "max_wait",
        }
    }

    /// The status line note for a wait a condition ended, or that reached `waitMs` with
    /// `returnWhen` conditions (`conditions`) none of which held.
    pub fn note(self, conditions: bool) -> &'static str {
        match self {
            WaitEnd::OutputContains => " · wait: output matched",
            WaitEnd::ScreenContains => " · wait: screen matched",
            WaitEnd::OutputQuiet => " · wait: output quiet",
            WaitEnd::MaxWait if conditions => " · wait: waitMs elapsed",
            WaitEnd::TaskEnded | WaitEnd::MaxWait => "",
        }
    }
}

/// The output a call may observe, taken before it writes input.
pub struct Mark {
    cursor: u64,
    cleaner: Cleaner,
    unseen: bool,
    received: u64,
    after_input: bool,
}

/// Marks where the call's observable output starts. `input` says the call writes input or
/// keys, so only output after them arms the quiet condition.
pub fn mark(task: &Task, input: bool) -> Mark {
    let (cursor, cleaner, unseen, received) = task.unseen();
    Mark {
        cursor,
        cleaner,
        unseen,
        received,
        after_input: input,
    }
}

/// Waits until the task ends, `wait_ms` elapses, a `when` condition holds, or the call is
/// cancelled, sending progress. `encoding` decodes output for `outputContains`.
pub async fn wait(
    task: &Arc<Task>,
    wait_ms: u64,
    when: Option<&ReturnWhen>,
    mark: Mark,
    encoding: &'static Encoding,
    context: &RequestContext<RoleServer>,
) -> Result<WaitEnd, String> {
    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
    let token = context.meta.get_progress_token();
    let mut ticks =
        tokio::time::interval_at(tokio::time::Instant::now() + PROGRESS_EVERY, PROGRESS_EVERY);
    let mut done = task.done();
    let mut activity = task.activity();
    let mut matcher = when
        .and_then(|when| when.output_contains.as_ref())
        .map(|text| {
            Matcher::new(
                &text.text,
                text.case_sensitive.unwrap_or(true),
                encoding,
                mark.cleaner,
            )
        });
    let mut scanned = mark.cursor;
    let screen = when.and_then(|when| when.screen_contains.as_ref());
    let quiet = when
        .and_then(|when| when.output_quiet_for_ms)
        .map(Duration::from_millis);
    // Set once the deadline passes: the conditions get one last look at what arrived by then.
    let mut expired = false;
    loop {
        if *done.borrow_and_update() {
            return Ok(WaitEnd::TaskEnded);
        }
        let mut quiet_until = None;
        let met = 'check: {
            // Conditions are observed in reverse precedence order: each later observation sees
            // at least the output an earlier one saw, so the condition of higher precedence
            // wins when the same output satisfies several.
            //
            // The quiet condition arms once there is output to see; a synchronized update in
            // progress is not quiet, since its frame has not shown yet.
            let mut quiet_met = false;
            if let Some(quiet) = quiet {
                // The time is taken first: output arriving after it cannot have broken the quiet.
                let now = Instant::now();
                let (received, last, sync_pending) = task.received();
                let armed = received > mark.received || (mark.unseen && !mark.after_input);
                if armed
                    && let Some(last) = last
                    && !sync_pending
                {
                    let at = last + quiet;
                    quiet_met = at <= now;
                    quiet_until = Some(tokio::time::Instant::from_std(at));
                }
            }
            // Capture stores a chunk before it releases the terminal, so the output scan below
            // reads at least the output this screen shows.
            let screen_met = match screen {
                Some(text) => task.screen()?.rows.iter().any(|row| text.found_in(row)),
                None => false,
            };
            if let Some(mut searching) = matcher.take() {
                let (reader, mut from) = (Arc::clone(task), scanned);
                let (searching, from, found) = tokio::task::spawn_blocking(move || {
                    let found = reader.scan(&mut searching, &mut from);
                    (searching, from, found)
                })
                .await
                .map_err(|error| format!("Internal failure while reading output: {error}."))?;
                if found.map_err(|error| format!("Cannot read the task log: {error}."))? {
                    break 'check Some(WaitEnd::OutputContains);
                }
                (matcher, scanned) = (Some(searching), from);
            }
            if screen_met {
                break 'check Some(WaitEnd::ScreenContains);
            }
            quiet_met.then_some(WaitEnd::OutputQuiet)
        };
        // A task that ended while the conditions were checked takes precedence.
        if let Some(met) = met.or(expired.then_some(WaitEnd::MaxWait)) {
            return Ok(if *done.borrow() {
                WaitEnd::TaskEnded
            } else {
                met
            });
        }
        tokio::select! {
            biased;
            _ = context.ct.cancelled() => return Err("Cancelled; the task keeps running.".into()),
            _ = done.changed() => {}
            _ = tokio::time::sleep_until(deadline) => expired = true,
            _ = activity.changed() => {}
            _ = tokio::time::sleep_until(quiet_until.unwrap_or(deadline)), if quiet_until.is_some() => {}
            _ = ticks.tick(), if token.is_some() => {
                let seconds = started.elapsed().as_secs_f64();
                let message = format!("{} running, waited {seconds:.0}s of {}s", task.id, wait_ms / 1000);
                let progress = ProgressNotificationParam::new(token.clone().expect("guarded"), seconds).with_message(message);
                let _ = context.peer.notify_progress(progress).await;
            }
        }
    }
}
