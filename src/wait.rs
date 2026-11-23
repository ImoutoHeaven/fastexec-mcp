//! The wait of `start` and `poll`: it ends when the task ends, `waitMs` elapses, or a
//! `returnWhen` condition holds. For a task that holds processes past its root's exit, the
//! task ends when they do, once a result has shown the root's exit.

use crate::lease::{BytePool, Lease};
use crate::session::{ReadState, Reader};
use crate::tasks::Task;
use crate::term::output::{Cleaner, Matcher};
use encoding_rs::Encoding;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

const PROGRESS_EVERY: Duration = Duration::from_secs(20);
const CANCELLED: &str = "Cancelled; the task keeps running.";
/// The latest progress message: elapsed seconds and its text.
pub type Progress = (f64, String);
const MAX_TEXT_BYTES: usize = 4096;
const MAX_TEXTS: usize = 16;
const QUIET_MS: std::ops::RangeInclusive<u64> = 10..=60_000;
/// Characters of a matched text the status line quotes; structured content holds all of it.
const NOTE_CHARS: usize = 60;

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReturnWhen {
    /// Match a substring within one unseen line, including text before carriage-return rewrites. Bash lines are cleaned. Codemode lines are unchanged.
    output_contains: Option<Vec<String>>,
    /// PTY only: a row of the screen contains one of the texts now.
    screen_contains: Option<Vec<String>>,
    /// PTY only: the screen shows a row containing one of the texts that it did not show before this call's input, such as a TUI's new completion line. Old rows that only scrolled do not count.
    screen_appears: Option<Vec<String>>,
    /// PTY only: one of the texts, on the screen before this call's input or during the wait, no longer is, such as a TUI's busy indicator.
    screen_gone: Option<Vec<String>>,
    /// 10-60000 ms of silence, counted from wait start (after input and keys) or the latest output, whichever is later.
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
                let why = if name == "outputContains" {
                    " (outputContains matches within one output line; to avoid matching an echoed command, print a marker the command line does not contain, such as one built with printf '%s_%s\\n' DONE 42, instead of adding a line break)"
                } else {
                    ""
                };
                return Err(format!(
                    "Each {name} text must be 1-{MAX_TEXT_BYTES} bytes without line breaks{why}."
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
#[derive(Debug)]
pub enum WaitEnd {
    TaskEnded,
    /// The processes a task held past its root's exit ended.
    TreeEnded,
    OutputContains(String, Lease),
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
            WaitEnd::OutputContains(text, lease) => {
                debug_assert_eq!(lease.bytes(), text.capacity());
                ("output_contains", Some(text))
            }
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
            WaitEnd::OutputContains(text, _) => format!(" · wait: output matched {}", quote(text)),
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
    memory: Arc<BytePool>,
    _lease: Option<Lease>,
    matcher_lease: Option<Lease>,
}

pub fn mark(
    task: &Task,
    reader: &Reader,
    state: &ReadState,
    when: Option<&ReturnWhen>,
    memory: &Arc<BytePool>,
) -> Result<Mark, String> {
    let matcher_lease = when
        .and_then(|when| when.output_contains.as_deref())
        .filter(|texts| !texts.is_empty())
        .map(|texts| Matcher::reserve(texts, memory))
        .transpose()
        .map_err(|message| {
            format!(
                "{message} The task may have run (taskId: {}, logPath: {}). Do not retry start or resend input; poll taskId without input once in-flight transfers complete.",
                reader.handle(&task.id),
                crate::files::display_path(&task.log_path)
            )
        })?;
    let (cursor, cleaner) = (state.cursor, state.cleaner.clone());
    let compares =
        when.is_some_and(|when| when.screen_appears.is_some() || when.screen_gone.is_some());
    let (rows, lease) = if compares {
        let screen = task.screen(memory)?;
        (screen.rows, Some(screen._lease))
    } else {
        (Vec::new(), None)
    };
    Ok(Mark {
        cursor,
        cleaner,
        rows,
        root_reported: reader.is_reported(),
        memory: Arc::clone(memory),
        _lease: lease,
        matcher_lease,
    })
}

/// Whether the end of the task's root or tree ends this wait, and how. The root's exit ends a
/// wait that has not seen it, unless the call waits for conditions, the root exited 0, and the
/// task held processes past the exit that can still meet them. Past that, a task that holds
/// processes counts as running until they end.
fn ended(task: &Task, root_reported: bool, conditions: bool) -> Option<WaitEnd> {
    let (code, held_past_exit) = task.root_end()?;
    let waits_past_exit = conditions && code == Some(0) && held_past_exit;
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
/// deadline when it has one, so the result holds their last output.
async fn finish(
    task: &Task,
    end: WaitEnd,
    deadline: Option<tokio::time::Instant>,
    cancel: &CancellationToken,
) -> Result<WaitEnd, String> {
    if matches!(end, WaitEnd::TreeEnded) {
        let drain_until = tokio::time::Instant::now() + crate::tasks::DRAIN_CAP;
        let until = deadline.map_or(drain_until, |d| d.min(drain_until));
        let mut activity = task.activity();
        while !task.output_closed() {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(CANCELLED.into()),
                _ = tokio::time::sleep_until(until) => break,
                _ = activity.changed() => {}
            }
        }
    }
    Ok(end)
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
    _lease: Option<Lease>,
}

impl<'a> ScreenWatch<'a> {
    fn new(when: &'a ReturnWhen, before: Vec<String>, lease: Option<Lease>) -> Option<Self> {
        if when.screen_condition().is_none() {
            drop(before);
            drop(lease);
            return None;
        }
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
            _lease: lease,
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

/// Waits until the task ends, `wait_duration` elapses (when `Some`), a `when` condition holds,
/// or the call is cancelled, sending progress. `encoding` decodes output for `outputContains`.
pub async fn wait(
    task: &Arc<Task>,
    wait_duration: Option<Duration>,
    when: Option<&ReturnWhen>,
    mark: Mark,
    encoding: &'static Encoding,
    cancel: &CancellationToken,
    progress: Option<&watch::Sender<Option<Progress>>>,
) -> Result<WaitEnd, String> {
    let started = Instant::now();
    let deadline = wait_duration.and_then(|d| tokio::time::Instant::now().checked_add(d));
    // Quiet that begins past the deadline does not hold, however late the last check runs.
    let last_quiet = wait_duration.and_then(|d| started.checked_add(d));
    // The caller's forwarder sends the latest message; the loop only replaces it.
    let mut ticks =
        tokio::time::interval_at(tokio::time::Instant::now() + PROGRESS_EVERY, PROGRESS_EVERY);
    let mut done = task.done();
    let mut tree_done = task.tree_done();
    let mut activity = task.activity();
    let (root_reported, conditions) = (mark.root_reported, when.is_some());
    let output_texts = when
        .and_then(|when| when.output_contains.as_deref())
        .unwrap_or_default();
    let mut matcher = mark.matcher_lease.map(|lease| {
        Matcher::new(
            output_texts,
            encoding,
            (!task.is_script()).then_some(mark.cleaner),
            lease,
        )
    });
    let mut scanned = mark.cursor;
    let mut screen = when.and_then(|when| ScreenWatch::new(when, mark.rows, mark._lease));
    let quiet = when
        .and_then(|when| when.quiet_ms)
        .map(Duration::from_millis);
    // Set once the deadline passes: the conditions get one last look at what arrived by then.
    let mut expired = false;
    loop {
        done.borrow_and_update();
        tree_done.borrow_and_update();
        if let Some(end) = ended(task, root_reported, conditions) {
            return finish(task, end, deadline, cancel).await;
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
                    let bound = last_quiet.map_or(now, |lq| now.min(lq));
                    quiet_met = at <= bound;
                    quiet_until = Some(tokio::time::Instant::from_std(at));
                }
            }
            // Capture stores a chunk before it releases the terminal, so the output scan below
            // reads at least the output this screen shows.
            let screen_met = match screen.as_mut() {
                Some(screen) => screen.check(&task.screen(&mark.memory)?.rows),
                None => None,
            };
            if let Some(mut searching) = matcher.take() {
                let (reader, mut from) = (Arc::clone(task), scanned);
                let scan_cancel = cancel.clone();
                let mut scan = tokio::task::spawn_blocking(move || {
                    let found = reader.scan(&mut searching, &mut from, &scan_cancel);
                    (searching, from, found)
                });
                let (searching, from, found) = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        scan.abort();
                        return Err(CANCELLED.into());
                    }
                    result = &mut scan => result,
                }
                .map_err(|error| format!("Internal failure while reading output: {error}."))?;
                let found = found.map_err(|error| format!("Cannot read the task log: {error}."))?;
                if let Some(index) = found {
                    let (text, lease) = searching.into_match(index);
                    break 'check Some(WaitEnd::OutputContains(text, lease));
                }
                (matcher, scanned) = (Some(searching), from);
            }
            screen_met.or(quiet_met.then_some(WaitEnd::Quiet))
        };
        // A task that ended while the conditions were checked takes precedence.
        if let Some(met) = met.or(expired.then_some(WaitEnd::MaxWait)) {
            return match ended(task, root_reported, conditions) {
                Some(end) => finish(task, end, deadline, cancel).await,
                None => Ok(met),
            };
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(CANCELLED.into()),
            _ = done.changed() => {}
            _ = tree_done.changed() => {}
            _ = async { tokio::time::sleep_until(deadline.unwrap()).await }, if deadline.is_some() => expired = true,
            _ = activity.changed() => {}
            _ = async { tokio::time::sleep_until(quiet_until.unwrap()).await }, if quiet_until.is_some() => {}
            _ = ticks.tick(), if progress.is_some() => {
                let seconds = started.elapsed().as_secs_f64();
                let message = match wait_duration {
                    Some(d) => format!("{} running, waited {seconds:.0}s of {}s", task.id, d.as_secs()),
                    None => format!("{} running, waited {seconds:.0}s", task.id),
                };
                if let Some(progress) = progress {
                    progress.send_replace(Some((seconds, message)));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::Service;
    use crate::session::{LaunchContext, ReadState, Session};
    use crate::tasks::{Limits, ScriptArgs, Tasks};

    #[tokio::test]
    async fn mark_admits_the_matcher_peak_without_advancing_the_reader() {
        let tasks = Tasks::new(Limits::OFF).unwrap();
        let launch = Arc::new(LaunchContext::current().unwrap());
        let session = Session::new(
            "match-budget".into(),
            "match-budget".into(),
            Arc::clone(&launch),
        );
        let (task, _, reader) = tasks
            .start_script(
                ScriptArgs {
                    code: "await sleep(60000);".into(),
                    launch: Arc::clone(&launch),
                    kill_after_ms: None,
                    transfer: false,
                    call_id: None,
                    service: Arc::new(Service::new(Arc::clone(&tasks), false)),
                    input_owner: Default::default(),
                },
                &session,
                "r001".into(),
            )
            .unwrap();
        let state = ReadState::default();
        let when: ReturnWhen = serde_json::from_value(serde_json::json!({
            "outputContains": ["accepted"]
        }))
        .unwrap();
        let admitted = mark(&task, &reader, &state, Some(&when), launch.memory()).unwrap();
        let needed = admitted.matcher_lease.as_ref().unwrap().bytes();
        drop(admitted);
        for limit in [needed - 1, needed, needed + 1] {
            let memory = BytePool::new(limit);
            let result = mark(&task, &reader, &state, Some(&when), &memory);
            assert_eq!(result.is_ok(), limit >= needed);
            assert_eq!(state.cursor, 0);
            if let Err(message) = &result {
                assert!(message.starts_with("Server active memory budget ("));
                assert!(message.contains(&reader.handle(&task.id)));
                assert!(message.contains(&crate::files::display_path(&task.log_path)));
                assert!(message.contains("Do not retry start or resend input"));
            } else {
                assert_eq!(memory.used(), needed);
            }
            drop(result);
            assert_eq!(
                memory.used(),
                0,
                "a mark dropped before wait releases admission"
            );
        }
        task.kill().unwrap();
        tasks.shutdown();
    }

    #[test]
    fn matched_text_keeps_its_owner_and_wire_shape() {
        let needles = ["ready 世界".to_owned()];
        let memory = BytePool::new(2 << 20);
        let mut matcher = Matcher::new(
            &needles,
            encoding_rs::UTF_8,
            None,
            Matcher::reserve(&needles, &memory).unwrap(),
        );
        assert_eq!(matcher.push("ready 世界".as_bytes()), Some(0));
        let (text, lease) = matcher.into_match(0);
        let retained = text.capacity();
        let ended = WaitEnd::OutputContains(text, lease);
        assert_eq!(memory.used(), retained);
        assert_eq!(
            ended.json(),
            serde_json::json!({
                "condition": "output_contains",
                "text": "ready 世界"
            })
        );
        assert_eq!(ended.note(true), " · wait: output matched \"ready 世界\"");
        drop(ended);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn screen_watch_keeps_the_snapshot_lease_with_its_rows() {
        let memory = BytePool::new(64);
        let lease = memory.try_acquire(64).unwrap();
        let when = serde_json::from_value(serde_json::json!({
            "screenAppears": ["ready"]
        }))
        .unwrap();
        let mut watch =
            ScreenWatch::new(&when, vec!["ready".into(), "ready".into()], Some(lease)).unwrap();
        assert_eq!(memory.used(), 64);
        assert!(watch.check(&["ready".into()]).is_none());
        assert!(matches!(
            watch.check(&["ready".into(), "ready".into(), "ready".into()]),
            Some(WaitEnd::ScreenAppears(text)) if text == "ready"
        ));
        assert_eq!(memory.used(), 64);
        drop(watch);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn cancellation_interrupts_a_queued_log_scan_and_an_open_output_drain() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let tasks = Tasks::new(Limits::OFF).unwrap();
            let launch = Arc::new(LaunchContext::current().unwrap());
            let session = Session::new("s".into(), "s".into(), Arc::clone(&launch));
            let (task, _, reader) = tasks
                .start_script(
                    ScriptArgs {
                        code: "await sleep(60000);".into(),
                        launch,
                        kill_after_ms: None,
                        transfer: false,
                        call_id: None,
                        service: Arc::new(Service::new(Arc::clone(&tasks), false)),
                        input_owner: Default::default(),
                    },
                    &session,
                    "r001".into(),
                )
                .unwrap();

            // Keep the only blocking worker occupied until the wait has returned.
            let (release, held) = std::sync::mpsc::channel::<()>();
            let (started, busy) = tokio::sync::oneshot::channel();
            let worker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                let _ = held.recv();
            });
            busy.await.unwrap();
            let when = serde_json::from_value(serde_json::json!({
                "outputContains": ["never printed"]
            }))
            .unwrap();
            let mark = mark(
                &task,
                &reader,
                &ReadState::default(),
                Some(&when),
                crate::lease::process_pool(),
            )
            .unwrap();
            let cancel = CancellationToken::new();
            let mut scan = std::pin::pin!(wait(
                &task,
                None,
                Some(&when),
                mark,
                encoding_rs::UTF_8,
                &cancel,
                None
            ));
            let mut drain = std::pin::pin!(finish(&task, WaitEnd::TreeEnded, None, &cancel));
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(scan.as_mut().poll(&mut cx).is_pending());
            assert!(drain.as_mut().poll(&mut cx).is_pending());
            cancel.cancel();
            for result in [scan.as_mut().poll(&mut cx), drain.as_mut().poll(&mut cx)] {
                assert!(
                    matches!(result, std::task::Poll::Ready(Err(message)) if message == CANCELLED)
                );
            }
            drop(release);
            worker.await.unwrap();
            task.kill().unwrap();
            tasks.shutdown();
        });
    }

    #[tokio::test]
    async fn unbounded_and_overflow_waits_resolve_on_task_exit_or_cancellation() {
        let tasks = Tasks::new(Limits::OFF).unwrap();
        let service = Arc::new(Service::new(Arc::clone(&tasks), false));
        let launch = Arc::new(LaunchContext::current().unwrap());
        let session = Session::new("s".into(), "s".into(), Arc::clone(&launch));

        let (task, _entry, reader) = tasks
            .start_script(
                ScriptArgs {
                    code: "text('done');".into(),
                    launch: Arc::clone(&launch),
                    kill_after_ms: None,
                    transfer: false,
                    call_id: None,
                    service: Arc::clone(&service),
                    input_owner: Default::default(),
                },
                &session,
                "r001".into(),
            )
            .unwrap();
        let m = mark(
            &task,
            &reader,
            &ReadState::default(),
            None,
            crate::lease::process_pool(),
        )
        .unwrap();
        let cancel = CancellationToken::new();
        let end = wait(&task, None, None, m, encoding_rs::UTF_8, &cancel, None)
            .await
            .unwrap();
        assert!(matches!(end, WaitEnd::TaskEnded));

        let (task2, _entry2, reader2) = tasks
            .start_script(
                ScriptArgs {
                    code: "while (true) {}".into(),
                    launch,
                    kill_after_ms: None,
                    transfer: false,
                    call_id: None,
                    service,
                    input_owner: Default::default(),
                },
                &session,
                "r002".into(),
            )
            .unwrap();
        let m2 = mark(
            &task2,
            &reader2,
            &ReadState::default(),
            None,
            crate::lease::process_pool(),
        )
        .unwrap();
        let cancel2 = CancellationToken::new();
        let trigger = cancel2.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            trigger.cancel();
        });
        let err = wait(
            &task2,
            Some(Duration::MAX),
            None,
            m2,
            encoding_rs::UTF_8,
            &cancel2,
            None,
        )
        .await
        .unwrap_err();
        assert!(err.contains("Cancelled"), "{err}");
        let _ = task2.kill();
        tasks.shutdown();
    }
}
