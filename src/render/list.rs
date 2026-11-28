//! Agent task-list rendering (`list` action) and tiered row budget fitting.

use super::budget::{
    ipc_wire_len, measure_outbound_len, members, result_payload_capacity, success, warning_text,
};
use super::types::AgentDelivery;
use crate::lease::Lease;
use crate::platform::process::clean_member_name;
#[cfg(test)]
use crate::service::Service;
use crate::service::core::{ServiceError, TaskMeta, resize_metadata_workspace, task_warnings};
use crate::service::schema::{Action, ListState};
use crate::service::{ExecContext, PreparedList};
use crate::tasks::ChildSummary;
use crate::tasks::state::{Snapshot, Task};
use crate::term::output::{self, Truncate};
use rmcp::model::CallToolResult;
use std::sync::Arc;

pub(super) fn render_list_agent(
    prepared: PreparedList,
    ctx: &ExecContext<'_>,
    delivery: &AgentDelivery<'_>,
) -> Result<
    (
        CallToolResult,
        Option<Lease>,
        Option<TaskMeta>,
        crate::lease::Owner,
    ),
    ServiceError,
> {
    let PreparedList {
        mut workspace,
        permit: _permit,
        total_count,
        items,
        parent_filter,
        state_filter,
        view,
    } = prepared;
    let launch = ctx.launch();
    let session = ctx.session;
    let mut blocks = Vec::with_capacity(items.len());
    for item in items {
        let running = item.snapshot.state == "running";
        let size = item.task.pty.then(|| item.task.size());
        let summary_text = item
            .children
            .as_ref()
            .and_then(|summary| format_child_summary(summary, running));
        let via_parent = if parent_filter.is_none() {
            item.parent_task_id.as_deref()
        } else {
            None
        };
        let top_row = TopRow::new(
            &item.task,
            &item.snapshot,
            size,
            summary_text.as_deref(),
            via_parent,
        );
        let mut block_lines = vec![top_row.full_line()];
        let mut delivered_tasks = Vec::new();
        let mut folded_finished = Vec::new();
        let mut folded_count = 0usize;
        if let Some(children) = item.children {
            folded_count = children.done + children.failed;
            folded_finished = children.folded_finished;
            delivered_tasks.reserve(1 + children.active_tasks.len());
            let parent_task_id_str = item.task.id.clone();
            let active_shown = children.active_tasks.len();
            let mut child_rows = Vec::with_capacity(active_shown);
            for (child, child_snapshot) in children.active_tasks {
                let child_size = child.pty.then(|| child.size());
                block_lines.push(format_child_row(&child, &child_snapshot, child_size));
                child_rows.push((child, child_snapshot));
            }
            if state_filter != Some(ListState::Finished) && children.active > active_shown {
                let more = children.active - active_shown;
                block_lines.push(format!(
                    "  └─ ... {more} more active; list with parentTaskId=\"{parent_task_id_str}\" state=\"active\""
                ));
            }
            delivered_tasks.push((item.task, item.snapshot));
            delivered_tasks.extend(child_rows);
        } else {
            delivered_tasks.push((item.task, item.snapshot));
        }
        blocks.push(ListBlock {
            top_row,
            lines: block_lines,
            delivered_tasks,
            folded_finished,
            folded_count,
        });
    }

    let (kept, text) = if blocks.is_empty() {
        (Vec::new(), "No tasks.".to_string())
    } else {
        fit_lines(&blocks, view.truncate, view.max_bytes)
    };
    let folded_children: usize = kept
        .iter()
        .map(|item| blocks[item.index].folded_count)
        .sum();
    let truncated = total_count > blocks.len();
    let omitted_by_budget = kept.len() < blocks.len();
    let shown = kept.len();
    let showing_note = match (truncated, folded_children > 0) {
        (true, true) if omitted_by_budget => Some(format!(
            "Showing {shown} of {total_count} top-level tasks ({folded_children} folded children; raise maxBytes or limit, or pass parentTaskId to inspect)."
        )),
        (true, true) => Some(format!(
            "Showing {shown} of {total_count} top-level tasks ({folded_children} folded children; pass parentTaskId to inspect)."
        )),
        (false, true) => Some(format!(
            "{folded_children} folded codemode children hidden; pass parentTaskId to inspect."
        )),
        (true, false) if omitted_by_budget => Some(format!(
            "Showing {shown} of {total_count} tasks; raise maxBytes or limit to see more."
        )),
        (true, false) => Some(format!(
            "Showing {shown} of {total_count} tasks; raise limit to see more."
        )),
        (false, false) => None,
    };
    let warnings: Vec<String> = kept
        .iter()
        .flat_map(|item| &blocks[item.index].delivered_tasks[..item.kept_tasks])
        .flat_map(|(task, snapshot)| {
            task_warnings(snapshot)
                .into_iter()
                .map(|warning| format!("{}: {warning}", task.id))
        })
        .collect();
    let mut suffix = warning_text(&warnings);
    if let Some(note) = showing_note {
        suffix.push('\n');
        suffix.push_str(&note);
    }
    let listed = success(text, true, &suffix);

    let outbound = measure_outbound_len(delivery, &listed)?;
    if outbound > launch.outbound_limit() {
        return Err(ServiceError::Overload(
            format!(
                "The list response was {outbound} bytes, over the frame limit; retry list with a smaller limit."
            ),
            None,
        ));
    }
    let payload = result_payload_capacity(&listed);
    let wire = if let Some(daemon_ipc) = &delivery.daemon_ipc {
        ipc_wire_len(daemon_ipc, &listed)?
    } else {
        outbound.saturating_mul(if delivery.http_response { 0 } else { 1 })
    };
    let retained = payload.saturating_add(wire);
    if retained > workspace.bytes() {
        resize_metadata_workspace(&mut workspace, retained, launch, Action::List)
            .map_err(|message| ServiceError::Overload(message, None))?;
    }
    if ctx.cancel.is_cancelled() {
        return Err(ServiceError::Cancelled(
            "Cancelled while listing tasks; retry list.".into(),
        ));
    }
    // Only an admitted candidate commits the fragments it actually displays.
    for item in kept {
        let block = &blocks[item.index];
        for (row_index, (task, snapshot)) in
            block.delivered_tasks[..item.kept_tasks].iter().enumerate()
        {
            let ack = if row_index == 0 {
                item.top_ack
            } else {
                full_ack(snapshot, false)
            };
            if let Some(entry) = session.existing(task) {
                if ack.root {
                    entry.mark_root_reported(snapshot);
                }
                if ack.tree_ended {
                    entry.mark_tree_end_reported();
                }
                if ack.keeps {
                    entry.mark_keeps_reported(snapshot);
                }
            }
        }
        if item.top_ack.summary {
            for (child, tree_ended) in &block.folded_finished {
                if let Some(entry) = session.existing(child) {
                    entry.mark_finished_reported(*tree_ended);
                }
            }
        }
    }
    drop(blocks);
    workspace.shrink_to(retained);
    let owner = crate::lease::Owner::new(workspace.split_off(payload));
    Ok((listed, Some(workspace), None, owner))
}

#[cfg(test)]
pub(crate) fn render_list_items_for_test(
    _service: &Service,
    items: Vec<crate::tasks::ListedTask>,
    max_bytes: usize,
    ctx: ExecContext<'_>,
) -> String {
    let prepared = PreparedList {
        total_count: items.len(),
        items,
        parent_filter: None,
        state_filter: None,
        view: crate::service::schema::View {
            inline_markers: false,
            keep_nul: false,
            truncate: Truncate::HeadTail,
            max_bytes,
            raw: false,
            encoding: None,
            screen: false,
            conditions: false,
        },
        workspace: Lease::empty(ctx.launch().memory()),
        permit: None,
    };
    let (result, _lease, _meta, _owner) =
        render_list_agent(prepared, &ctx, &AgentDelivery::default()).expect("test list render");
    result.content[0].as_text().unwrap().text.clone()
}

#[derive(Clone, Copy, Default)]
struct RowAck {
    root: bool,
    tree_ended: bool,
    keeps: bool,
    summary: bool,
}

fn full_ack(snapshot: &Snapshot, has_summary: bool) -> RowAck {
    RowAck {
        root: true,
        tree_ended: snapshot.tree_ended,
        keeps: snapshot.kept && snapshot.held_known,
        summary: has_summary,
    }
}

struct KeptBlock {
    index: usize,
    kept_tasks: usize,
    top_ack: RowAck,
}

struct TopRow {
    head: String,
    tree_full: Option<String>,
    tree_brief: Option<String>,
    tree_count: Option<String>,
    elapsed_pty: String,
    summary: Option<String>,
    tail: String,
}

impl TopRow {
    fn new(
        task: &Task,
        snapshot: &Snapshot,
        size: Option<(u16, u16)>,
        summary: Option<&str>,
        via_parent: Option<&str>,
    ) -> Self {
        let code = snapshot
            .exit_code
            .map_or(String::new(), |code| format!(" {code}"));
        let head = format!("{} [{}{code}]", task.id, snapshot.state);
        let tree_full = tree_marker(task, snapshot);
        let (tree_brief, tree_count) =
            if snapshot.kept && snapshot.held_known && !snapshot.held.is_empty() {
                let first = &snapshot.held[0];
                let name =
                    clean_member_name(&first.name).unwrap_or_else(|| format!("pid {}", first.pid));
                let suffix = if snapshot.kill_error.is_some() {
                    " (kill failed)"
                } else {
                    ""
                };
                let brief = if snapshot.held.len() == 1 {
                    format!(" · keeps {name}{suffix}")
                } else {
                    format!(" · keeps {name} +{}{suffix}", snapshot.held.len() - 1)
                };
                let count = if snapshot.held.len() == 1 {
                    format!(" · keeps 1 process{suffix}")
                } else {
                    format!(" · keeps {} processes{suffix}", snapshot.held.len())
                };
                (Some(brief), Some(count))
            } else {
                (None, None)
            };
        let sep = if tree_full.is_some() { " · " } else { " " };
        let pty = match size {
            Some((cols, rows)) => format!(" pty {cols}x{rows}"),
            None => String::new(),
        };
        let elapsed_pty = format!("{sep}{:.1}s{pty}", snapshot.elapsed.as_secs_f64());
        let summary = summary.map(|summary| format!(" · {summary}"));
        let owner = match &task.started_by {
            Some((_, call)) => format!("{} call {call}", task.owner),
            None => task.owner.clone(),
        };
        let via = match via_parent {
            Some(pid) => format!(" · via {pid}"),
            None => String::new(),
        };
        let tail = format!(" · {owner}{via} · {}", format_command(&task.command));
        Self {
            head,
            tree_full,
            tree_brief,
            tree_count,
            elapsed_pty,
            summary,
            tail,
        }
    }

    fn full_line(&self) -> String {
        self.line_with_tree(self.tree_full.as_deref().unwrap_or(""))
    }

    fn line_with_tree(&self, tree: &str) -> String {
        format!(
            "{}{tree}{}{}{}",
            self.head,
            self.elapsed_pty,
            self.summary.as_deref().unwrap_or(""),
            self.tail
        )
    }
}

struct ListBlock {
    top_row: TopRow,
    lines: Vec<String>,
    delivered_tasks: Vec<(Arc<Task>, Snapshot)>,
    folded_finished: Vec<(Arc<Task>, bool)>,
    folded_count: usize,
}

impl ListBlock {
    fn full_top_ack(&self) -> RowAck {
        full_ack(&self.delivered_tasks[0].1, self.top_row.summary.is_some())
    }
}

fn format_command(command: &str) -> String {
    command
        .chars()
        .take(120)
        .map(|c| {
            if c.is_control() || matches!(c, '\u{2028}' | '\u{2029}') {
                ' '
            } else {
                c
            }
        })
        .collect()
}

fn tree_marker(task: &Task, snapshot: &Snapshot) -> Option<String> {
    if snapshot.state == "running" {
        return task.keep.then(|| " · keepTree".into());
    }
    if snapshot.kept {
        let kill_failed = snapshot.kill_error.is_some();
        let body = if !snapshot.held_known {
            if kill_failed {
                "processes (unconfirmed, kill failed)".into()
            } else {
                "processes (unconfirmed)".into()
            }
        } else if kill_failed {
            format!("{} (kill failed)", members(&snapshot.held))
        } else {
            members(&snapshot.held)
        };
        return Some(format!(" · keeps {body}"));
    }
    if snapshot.tree_ended {
        return Some(" · kept processes ended".into());
    }
    task.keep.then(|| " · keepTree".into())
}

fn format_child_summary(summary: &ChildSummary, running: bool) -> Option<String> {
    if !summary.should_show(running) {
        return None;
    }
    if summary.total == 0 {
        return Some(if running {
            format!("{} queued", summary.queued)
        } else {
            format!("{} cancelled", summary.cancelled)
        });
    }
    let mut parts = Vec::new();
    if summary.done > 0 {
        parts.push(format!("{} done", summary.done));
    }
    if summary.failed > 0 {
        parts.push(format!("{} failed", summary.failed));
    }
    if summary.warned > 0 {
        parts.push(format!("{} warned", summary.warned));
    }
    if summary.tree_ended > 0 {
        parts.push(format!("{} kept ended", summary.tree_ended));
    }
    if summary.active > 0 {
        parts.push(format!("{} active", summary.active));
    }
    if running && summary.queued > 0 {
        parts.push(format!("{} queued", summary.queued));
    }
    if !running && summary.cancelled > 0 {
        parts.push(format!("{} cancelled", summary.cancelled));
    }
    let noun = if summary.total == 1 { "task" } else { "tasks" };
    Some(format!("{} {noun} ({})", summary.total, parts.join(", ")))
}

fn format_child_row(task: &Task, snapshot: &Snapshot, size: Option<(u16, u16)>) -> String {
    let code = snapshot
        .exit_code
        .map_or(String::new(), |code| format!(" {code}"));
    let tree = tree_marker(task, snapshot);
    let sep = if tree.is_some() { " · " } else { " " };
    let pty = match size {
        Some((cols, rows)) => format!(" pty {cols}x{rows}"),
        None => String::new(),
    };
    format!(
        "  └─ {} [{}{code}]{}{sep}{:.1}s{pty} · {}",
        task.id,
        snapshot.state,
        tree.as_deref().unwrap_or(""),
        snapshot.elapsed.as_secs_f64(),
        format_command(&task.command),
    )
}

fn cut_candidate(mut candidate_full: String, budget: usize) -> String {
    const MARK: &str = " … [cut]";
    if candidate_full.len() + MARK.len() <= budget {
        candidate_full.push_str(MARK);
    } else {
        let mut end = budget - MARK.len();
        while !candidate_full.is_char_boundary(end) {
            end -= 1;
        }
        candidate_full.truncate(end);
        candidate_full.push_str(MARK);
    }
    candidate_full
}

fn fit_row(row: &TopRow, snapshot: &Snapshot, budget: usize) -> (String, RowAck) {
    const MARK_LEN: usize = " … [cut]".len();
    let full_line = row.full_line();
    let has_summary = row.summary.is_some();
    if full_line.len() <= budget {
        return (full_line, full_ack(snapshot, has_summary));
    }
    if budget >= 20 {
        let head_len = row.head.len();
        let tree_len = row.tree_full.as_deref().map_or(0, str::len);
        let elapsed_len = row.elapsed_pty.len();
        let summary_len = row.summary.as_deref().map_or(0, str::len);
        let keeps_full = snapshot.kept && snapshot.held_known;

        // Tier 1: head + tree_full + elapsed_pty + summary.
        if head_len + tree_len + elapsed_len + summary_len + MARK_LEN <= budget {
            return (
                cut_candidate(full_line, budget),
                full_ack(snapshot, has_summary),
            );
        }
        // Tier 2: head + tree_full + elapsed_pty (drop summary ack).
        if has_summary && head_len + tree_len + elapsed_len + MARK_LEN <= budget {
            return (
                cut_candidate(full_line, budget),
                RowAck {
                    root: true,
                    tree_ended: snapshot.tree_ended,
                    keeps: keeps_full,
                    summary: false,
                },
            );
        }
        // Tier 3: head + tree_full.
        if row.tree_full.is_some() && head_len + tree_len + MARK_LEN <= budget {
            return (
                cut_candidate(full_line, budget),
                RowAck {
                    root: true,
                    tree_ended: snapshot.tree_ended,
                    keeps: keeps_full,
                    summary: false,
                },
            );
        }
        // Tier 4: head + tree_brief (same granularity as footer brief; confirms keeps).
        if let Some(tree_brief) = &row.tree_brief
            && head_len + tree_brief.len() + MARK_LEN <= budget
        {
            return (
                cut_candidate(row.line_with_tree(tree_brief), budget),
                RowAck {
                    root: true,
                    tree_ended: false,
                    keeps: true,
                    summary: false,
                },
            );
        }
        // Tier 5: head + tree_count (count only; keeps stays unconfirmed).
        if let Some(tree_count) = &row.tree_count
            && head_len + tree_count.len() + MARK_LEN <= budget
        {
            return (
                cut_candidate(row.line_with_tree(tree_count), budget),
                RowAck {
                    root: true,
                    tree_ended: false,
                    keeps: false,
                    summary: false,
                },
            );
        }
        // Tier 6: head only.
        if head_len + MARK_LEN <= budget {
            return (
                cut_candidate(full_line, budget),
                RowAck {
                    root: true,
                    tree_ended: false,
                    keeps: false,
                    summary: false,
                },
            );
        }
    }
    // Tier 7: hard cut, confirm nothing.
    let mut line = full_line;
    output::cut_to(&mut line, budget + 1);
    (line, RowAck::default())
}

fn block_len(lines: &[String]) -> usize {
    lines.iter().map(String::len).sum::<usize>() + lines.len().saturating_sub(1)
}

fn fit_single_block(block: &ListBlock, budget: usize) -> (usize, RowAck, String) {
    let mut kept_lines = block.lines.len();
    while kept_lines > 1 && block_len(&block.lines[..kept_lines]) > budget {
        kept_lines -= 1;
    }
    let kept_tasks = kept_lines.min(block.delivered_tasks.len());
    if kept_lines == 1 && block.lines[0].len() > budget {
        let (line, ack) = fit_row(&block.top_row, &block.delivered_tasks[0].1, budget);
        return (kept_tasks, ack, line);
    }
    (
        kept_tasks,
        block.full_top_ack(),
        block.lines[..kept_lines].join("\n"),
    )
}

fn fit_lines(blocks: &[ListBlock], truncate: Truncate, budget: usize) -> (Vec<KeptBlock>, String) {
    let total_bytes: usize = blocks
        .iter()
        .flat_map(|block| &block.lines)
        .map(|line| line.len() + 1)
        .sum();
    if total_bytes <= budget + 1 {
        let kept = blocks
            .iter()
            .enumerate()
            .map(|(index, block)| KeptBlock {
                index,
                kept_tasks: block.delivered_tasks.len(),
                top_ack: block.full_top_ack(),
            })
            .collect();
        let text = blocks
            .iter()
            .flat_map(|block| block.lines.iter().map(String::as_str))
            .collect::<Vec<_>>()
            .join("\n");
        return (kept, text);
    }
    if blocks.len() == 1 {
        let (kept_tasks, top_ack, text) = fit_single_block(&blocks[0], budget);
        return (
            vec![KeptBlock {
                index: 0,
                kept_tasks,
                top_ack,
            }],
            text,
        );
    }
    let marker =
        |omitted: usize| format!("... [{omitted} tasks omitted; raise maxBytes to see them] ...");
    let mut room = budget.saturating_sub(marker(blocks.len()).len());
    let mut head = 0;
    let mut tail = blocks.len();
    let mut want_head = truncate != Truncate::Tail;
    let mut want_tail = truncate != Truncate::Head;
    while head < tail && (want_head || want_tail) {
        if want_head {
            let cost = block_len(&blocks[head].lines) + 1;
            if cost <= room {
                head += 1;
                room -= cost;
            } else {
                want_head = false;
            }
        }
        if want_tail && head < tail {
            let cost = block_len(&blocks[tail - 1].lines) + 1;
            if cost <= room {
                tail -= 1;
                room -= cost;
            } else {
                want_tail = false;
            }
        }
    }
    if head == 0 && tail == blocks.len() {
        let edge = if truncate == Truncate::Tail {
            blocks.len() - 1
        } else {
            0
        };
        let note = marker(blocks.len() - 1);
        let edge_budget = budget.saturating_sub(note.len() + 1);
        if blocks[edge].lines.len() > 1 && blocks[edge].lines[0].len() <= edge_budget {
            let (kept_tasks, top_ack, trimmed) = fit_single_block(&blocks[edge], edge_budget);
            let text = if truncate == Truncate::Tail {
                format!("{note}\n{trimmed}")
            } else {
                format!("{trimmed}\n{note}")
            };
            return (
                vec![KeptBlock {
                    index: edge,
                    kept_tasks,
                    top_ack,
                }],
                text,
            );
        }
    }
    let mut note = marker(tail - head);
    output::cut_to(&mut note, budget + 1);
    let text = blocks[..head]
        .iter()
        .flat_map(|block| block.lines.iter().map(String::as_str))
        .chain([note.as_str()])
        .chain(
            blocks[tail..]
                .iter()
                .flat_map(|block| block.lines.iter().map(String::as_str)),
        )
        .collect::<Vec<_>>()
        .join("\n");
    let kept = (0..head)
        .chain(tail..blocks.len())
        .map(|index| KeptBlock {
            index,
            kept_tasks: blocks[index].delivered_tasks.len(),
            top_ack: blocks[index].full_top_ack(),
        })
        .collect();
    (kept, text)
}
