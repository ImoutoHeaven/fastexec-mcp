//! The wait of `start` and `poll`: it ends when the task ends, `waitMs` elapses, or a
//! `returnWhen` condition holds. For a task that holds processes past its root's exit, the
//! task ends when they do, once a result has shown the root's exit.

use crate::output::{Cleaner, Matcher};
use crate::tasks::Task;
use encoding_rs::Encoding;
use rmcp::RoleServer;
use rmcp::model::ProgressNotificationParam;
use rmcp::service::RequestContext;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

const PROGRESS_EVERY: Duration = Duration::from_secs(20);
const MAX_TEXT_BYTES: usize = 4096;
const MAX_TEXTS: usize = 16;
const QUIET_MS: std::ops::RangeInclusive<u64> = 10..=60_000;
/// Characters of a matched text the status line quotes; structured content holds all of it.
const NOTE_CHARS: usize = 60;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReturnWhen {
    /// Unseen output, cleaned as results show it, has a line containing one of the texts; text a carriage return later rewrites counts. For line-oriented programs: a TUI's redraws repeat old text.
    output_contains: Option<Vec<String>>,
    /// PTY only: a row of the screen contains one of the texts now.
    screen_contains: Option<Vec<String>>,
    /// PTY only: the screen shows a row containing one of the texts that it did not show before this call's input, such as a TUI's new completion line. Old rows that only scrolled do not count.
    screen_appears: Option<Vec<String>>,
    /// PTY only: one of the texts, on the screen before this call's input or during the wait, no longer is, such as a TUI's busy indicator.
    screen_gone: Option<Vec<String>>,
    /// No output for this many ms, 10-60000, counted from the last output or the start of the wait, whichever is later; the wait starts after input and keys.
    #[schemars(range(min = 10, max = 60000))]
    quiet_ms: Option<u64>,
}

impl ReturnWhen {
    fn texts(&self) -> [(&'static str, Option<&Vec<String>>); 4] {
        [
            ("outputContains", self.output_contains.as_ref()),
            ("screenContains", self.screen_contains.as_ref()),
            ("screenAppears", self.screen_appears.as_ref()),
            ("screenGone", self.screen_gone.as_ref()),
        ]
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.texts().iter().all(|(_, texts)| texts.is_none()) && self.quiet_ms.is_none() {
            return Err("returnWhen needs outputContains, screenContains, screenAppears, screenGone, or quietMs.".into());
        }
        for (name, texts) in self.texts() {
            let Some(texts) = texts else { continue };
            if texts.is_empty() || texts.len() > MAX_TEXTS {
                return Err(format!("{name} takes 1-{MAX_TEXTS} texts."));
            }
            if texts.iter().any(|text| {
                text.is_empty() || text.len() > MAX_TEXT_BYTES || text.contains(['\n', '\r'])
            }) {
                return Err(format!(
                    "Each {name} text must be 1-{MAX_TEXT_BYTES} bytes without line breaks."
                ));
            }
        }
        if let Some(quiet) = self.quiet_ms.filter(|quiet| !QUIET_MS.contains(quiet)) {
            return Err(format!("quietMs {quiet} is out of range 10-60000."));
        }
        Ok(())
    }

    /// The first condition set that reads the screen.
    pub fn screen_condition(&self) -> Option<&'static str> {
        self.texts()[1..]
            .iter()
            .find_map(|(name, texts)| texts.is_some().then_some(*name))
    }
}

/// Why a wait ended, in the precedence order of conditions that hold together, with the text
/// that matched.
pub enum WaitEnd {
    TaskEnded,
    /// The processes a task held past its root's exit ended.
    TreeEnded,
    OutputContains(String),
    ScreenContains(String),
    ScreenAppears(String),
    ScreenGone(String),
    Quiet,
    MaxWait,
}

impl WaitEnd {
    fn parts(&self) -> (&'static str, Option<&str>) {
        match self {
            WaitEnd::TaskEnded => ("task_ended", None),
            WaitEnd::TreeEnded => ("tree_ended", None),
            WaitEnd::OutputContains(text) => ("output_contains", Some(text)),
            WaitEnd::ScreenContains(text) => ("screen_contains", Some(text)),
            WaitEnd::ScreenAppears(text) => ("screen_appears", Some(text)),
            WaitEnd::ScreenGone(text) => ("screen_gone", Some(text)),
            WaitEnd::Quiet => ("quiet", None),
            WaitEnd::MaxWait => ("max_wait", None),
        }
    }

    /// `waitEndedBy`: the condition, and the text that matched.
    pub fn json(&self) -> Value {
        match self.parts() {
            (condition, Some(text)) => json!({"condition": condition, "text": text}),
            (condition, None) => json!({"condition": condition}),
        }
    }

    /// The status line note for a wait a condition ended, or that reached `waitMs` with
    /// `returnWhen` conditions (`conditions`) none of which held.
    pub fn note(&self, conditions: bool) -> String {
        let quote = |text: &str| {
            let mut short: String = text.chars().take(NOTE_CHARS).collect();
            if short.len() < text.len() {
                short.push('…');
            }
            format!("{short:?}")
        };
        match self {
            WaitEnd::OutputContains(text) => format!(" · wait: output matched {}", quote(text)),
            WaitEnd::ScreenContains(text) => format!(" · wait: screen matched {}", quote(text)),
            WaitEnd::ScreenAppears(text) => {
                format!(" · wait: screen text appeared {}", quote(text))
            }
            WaitEnd::ScreenGone(text) => format!(" · wait: screen text gone {}", quote(text)),
            WaitEnd::Quiet => " · wait: output quiet".into(),
            WaitEnd::MaxWait if conditions => " · wait: waitMs elapsed".into(),
            WaitEnd::TaskEnded | WaitEnd::TreeEnded | WaitEnd::MaxWait => String::new(),
        }
    }
}

/// What a call may observe, taken before it writes input: where its unseen output starts and,
/// for `screenAppears` and `screenGone`, the screen rows before the input.
pub struct Mark {
    cursor: u64,
    cleaner: Cleaner,
    rows: Vec<String>,
    /// A result had shown the root's exit before this call, so the exit is not news to it.
    root_reported: bool,
}

pub fn mark(task: &Task, when: Option<&ReturnWhen>) -> Result<Mark, String> {
    let (cursor, cleaner) = task.unseen();
    let compares =
        when.is_some_and(|when| when.screen_appears.is_some() || when.screen_gone.is_some());
    let rows = if compares {
        task.screen()?.rows
    } else {
        Vec::new()
    };
    Ok(Mark {
        cursor,
        cleaner,
        rows,
        root_reported: task.is_reported(),
    })
}

/// Whether the end of the task's root or tree ends this wait, and how. The root's exit ends a
/// wait that has not seen it, unless the call waits for conditions, the root exited 0, and the
/// task held processes past the exit that can still meet them. Past that, a task that holds
/// processes counts as running until they end.
fn ended(task: &Task, root_reported: bool, conditions: bool) -> Option<WaitEnd> {
    let (code, held_past_exit) = task.root_end()?;
    let waits_past_exit = conditions && code == 0 && held_past_exit;
    if !root_reported && !waits_past_exit {
        return Some(WaitEnd::TaskEnded);
    }
    let holds = task.keeps_processes();
    match (holds, held_past_exit) {
        (true, _) => None,
        (false, true) => Some(WaitEnd::TreeEnded),
        (false, false) => Some(WaitEnd::TaskEnded),
    }
}

/// Ends a wait the end of the task or its tree ended. After the tree, the output those
/// processes held open reaches EOF shortly: the wait gives it up to the drain cap, within its
/// deadline, so the result holds their last output.
async fn finish(task: &Task, end: WaitEnd, deadline: tokio::time::Instant) -> WaitEnd {
    if matches!(end, WaitEnd::TreeEnded) {
        let until = deadline.min(tokio::time::Instant::now() + crate::tasks::DRAIN_CAP);
        let mut activity = task.activity();
        while !task.output_closed() {
            tokio::select! {
                _ = activity.changed() => {}
                _ = tokio::time::sleep_until(until) => break,
            }
        }
    }
    end
}

/// The screen conditions of a wait, with what they compare against.
struct ScreenWatch<'a> {
    contains: &'a [String],
    appears: &'a [String],
    gone: &'a [String],
    /// How often each row shows on the screen before the input.
    before: HashMap<String, usize>,
    /// Per `gone` text: the screen has shown it.
    seen: Vec<bool>,
}

impl<'a> ScreenWatch<'a> {
    fn new(when: &'a ReturnWhen, before: Vec<String>) -> Option<Self> {
        when.screen_condition()?;
        let texts = |texts: &'a Option<Vec<String>>| texts.as_deref().unwrap_or_default();
        let gone = texts(&when.screen_gone);
        let seen = gone
            .iter()
            .map(|text| before.iter().any(|row| row.contains(text.as_str())))
            .collect();
        let mut counts = HashMap::new();
        for row in before {
            *counts.entry(row).or_insert(0) += 1;
        }
        Some(Self {
            contains: texts(&when.screen_contains),
            appears: texts(&when.screen_appears),
            gone,
            before: counts,
            seen,
        })
    }

    /// The first screen condition `rows` satisfy, in precedence order.
    fn check(&mut self, rows: &[String]) -> Option<WaitEnd> {
        let shows = |text: &str| rows.iter().any(|row| row.contains(text));
        // Every check updates `seen`, so a text that shows only while another condition holds
        // still counts as shown.
        let mut gone = None;
        for (text, seen) in self.gone.iter().zip(&mut self.seen) {
            let shown = shows(text);
            *seen |= shown;
            if *seen && !shown && gone.is_none() {
                gone = Some(text);
            }
        }
        if let Some(text) = self.contains.iter().find(|text| shows(text)) {
            return Some(WaitEnd::ScreenContains(text.clone()));
        }
        // A row is new when the screen shows it more often than before the input, so rows
        // that only moved by scrolling do not count.
        for text in self.appears {
            let mut counts: HashMap<&str, usize> = HashMap::new();
            for row in rows.iter().filter(|row| row.contains(text.as_str())) {
                let count = counts.entry(row).or_insert(0);
                *count += 1;
                if *count > self.before.get(row.as_str()).copied().unwrap_or(0) {
                    return Some(WaitEnd::ScreenAppears(text.clone()));
                }
            }
        }
        gone.map(|text| WaitEnd::ScreenGone(text.clone()))
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
    // Quiet that begins past the deadline does not hold, however late the last check runs.
    let last_quiet = started + Duration::from_millis(wait_ms);
    let token = context.meta.get_progress_token();
    let mut ticks =
        tokio::time::interval_at(tokio::time::Instant::now() + PROGRESS_EVERY, PROGRESS_EVERY);
    let mut done = task.done();
    let mut tree_done = task.tree_done();
    let mut activity = task.activity();
    let (root_reported, conditions) = (mark.root_reported, when.is_some());
    let output_texts = when
        .and_then(|when| when.output_contains.as_deref())
        .unwrap_or_default();
    let mut matcher =
        (!output_texts.is_empty()).then(|| Matcher::new(output_texts, encoding, mark.cleaner));
    let mut scanned = mark.cursor;
    let mut screen = when.and_then(|when| ScreenWatch::new(when, mark.rows));
    let quiet = when
        .and_then(|when| when.quiet_ms)
        .map(Duration::from_millis);
    // Set once the deadline passes: the conditions get one last look at what arrived by then.
    let mut expired = false;
    loop {
        done.borrow_and_update();
        tree_done.borrow_and_update();
        if let Some(end) = ended(task, root_reported, conditions) {
            return Ok(finish(task, end, deadline).await);
        }
        let mut quiet_until = None;
        let met = 'check: {
            // Conditions are observed in reverse precedence order: each later observation sees
            // at least the output an earlier one saw, so the condition of higher precedence
            // wins when the same output satisfies several.
            //
            // A synchronized update in progress is not quiet, since its frame has not shown yet.
            let mut quiet_met = false;
            if let Some(quiet) = quiet {
                // The time is taken first: output arriving after it cannot have broken the quiet.
                let now = Instant::now();
                let (last, sync_pending) = task.last_output();
                if !sync_pending {
                    let at = last.map_or(started, |last| last.max(started)) + quiet;
                    quiet_met = at <= now.min(last_quiet);
                    quiet_until = Some(tokio::time::Instant::from_std(at));
                }
            }
            // Capture stores a chunk before it releases the terminal, so the output scan below
            // reads at least the output this screen shows.
            let screen_met = match screen.as_mut() {
                Some(screen) => screen.check(&task.screen()?.rows),
                None => None,
            };
            if let Some(mut searching) = matcher.take() {
                let (reader, mut from) = (Arc::clone(task), scanned);
                let (searching, from, found) = tokio::task::spawn_blocking(move || {
                    let found = reader.scan(&mut searching, &mut from);
                    (searching, from, found)
                })
                .await
                .map_err(|error| format!("Internal failure while reading output: {error}."))?;
                let found = found.map_err(|error| format!("Cannot read the task log: {error}."))?;
                if let Some(index) = found {
                    break 'check Some(WaitEnd::OutputContains(output_texts[index].clone()));
                }
                (matcher, scanned) = (Some(searching), from);
            }
            screen_met.or(quiet_met.then_some(WaitEnd::Quiet))
        };
        // A task that ended while the conditions were checked takes precedence.
        if let Some(met) = met.or(expired.then_some(WaitEnd::MaxWait)) {
            return Ok(match ended(task, root_reported, conditions) {
                Some(end) => finish(task, end, deadline).await,
                None => met,
            });
        }
        tokio::select! {
            biased;
            _ = context.ct.cancelled() => return Err("Cancelled; the task keeps running.".into()),
            _ = done.changed() => {}
            _ = tree_done.changed() => {}
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
