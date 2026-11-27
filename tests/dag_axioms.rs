//! Lexical and module-path architecture gates for Phase 2 decoupling (Spec §3, §7.1, §7.2).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

/// Final 24-module strict lower-triangular allowlist `D(u)` from Spec §2.1 (`rank(v) < rank(u)`).
/// Every cross-top-module dependency in non-test `src/` code must appear in this table, and the
/// set of top-level modules declared in `src/main.rs` must match these 24 entries one-to-one.
const TARGET_DAG_ALLOWLIST: &[(&str, &[&str])] = &[
    // S0 primitives (#1..#3, 3x3 diagonal block)
    ("id", &[]),
    ("lease", &[]),
    ("transport", &[]),
    // S1a launch environment (#4)
    ("env", &["lease"]),
    // S1b host capabilities (#5..#7, 3x3 diagonal block)
    ("media", &["lease"]),
    ("term", &["lease"]),
    ("platform", &["id", "lease", "transport", "env"]),
    // S1c files and configuration (#8..#10, {transfer, config} 2x2 diagonal block)
    ("files", &["lease", "transport", "env", "media", "platform"]),
    ("transfer", &["id", "lease", "env", "files"]),
    ("config", &["env", "files"]),
    // S2 events, tasks, and wait (#11..#13)
    ("events", &["id", "lease"]),
    (
        "tasks",
        &[
            "id", "lease", "env", "media", "term", "platform", "files", "events",
        ],
    ),
    ("wait", &["lease", "term", "files", "tasks"]),
    // S3 domain orchestration (#14)
    (
        "service",
        &[
            "id", "lease", "env", "term", "platform", "files", "events", "tasks", "wait",
        ],
    ),
    // S3b read-only skills catalog (#15)
    ("skills", &["lease", "media", "platform", "files"]),
    // S4 projection and wire (#16..#18, 3x3 diagonal block)
    (
        "render",
        &[
            "lease", "env", "media", "term", "platform", "files", "transfer", "events", "tasks",
            "wait", "service",
        ],
    ),
    (
        "codemode",
        &[
            "id", "lease", "env", "media", "term", "platform", "files", "tasks", "wait", "service",
        ],
    ),
    ("ipc", &["lease", "env", "events"]),
    // S5a HTTP wire and IPC client (#19..#20, 2x2 diagonal block)
    ("http_wire", &["lease", "files", "transfer"]),
    (
        "endpoint",
        &[
            "id",
            "lease",
            "transport",
            "env",
            "platform",
            "events",
            "wait",
            "render",
            "ipc",
        ],
    ),
    // S5b daemon and MCP handler (#21..#22, 2x2 diagonal block)
    (
        "daemon",
        &[
            "lease",
            "transport",
            "env",
            "platform",
            "transfer",
            "config",
            "events",
            "tasks",
            "service",
            "skills",
            "render",
            "codemode",
            "ipc",
            "http_wire",
        ],
    ),
    (
        "mcp",
        &[
            "id", "lease", "env", "files", "transfer", "config", "events", "tasks", "wait",
            "service", "skills", "render", "codemode", "ipc", "endpoint",
        ],
    ),
    // S5c HTTP server and CLI entrypoints (#23..#24)
    (
        "http",
        &[
            "lease",
            "env",
            "transfer",
            "config",
            "tasks",
            "service",
            "skills",
            "http_wire",
            "mcp",
        ],
    ),
    (
        "cli",
        &[
            "id",
            "lease",
            "transport",
            "env",
            "platform",
            "transfer",
            "config",
            "events",
            "tasks",
            "service",
            "skills",
            "ipc",
            "http_wire",
            "endpoint",
            "daemon",
            "mcp",
            "http",
        ],
    ),
];

/// Cross-layer one-way edges eliminated in Checkpoint 1.
const CP1_FORBIDDEN_EDGES: &[(&str, &str)] = &[
    ("platform", "ipc"),
    ("files", "ipc"),
    ("transfer", "tasks"),
    ("transfer", "ipc"),
    ("codemode", "ipc"),
    ("endpoint", "tasks"),
];

/// Allowed sibling submodules inside `src/tasks/` and their exact permitted sibling directed edges
/// `(from_sub, to_sub)` (Spec §4.2.2, §7.2.5).
const TASKS_SUBMODULES: &[&str] = &["script", "state", "workers", "session"];
const TASKS_ALLOWED_SIBLING_EDGES: &[(&str, &str)] = &[
    ("state", "script"),
    ("workers", "state"),
    ("session", "state"),
];

const RQUICKJS_ALLOWED: &[&str] = &["codemode"];
const ALACRITTY_ALLOWED: &[&str] = &["term"];
const HYPER_ALLOWED: &[&str] = &["http_wire", "http"];
const RMCP_ALLOWED: &[&str] = &["render", "ipc", "endpoint", "daemon", "mcp", "http", "cli"];
const SERDE_SAPHYR_ALLOWED: &[&str] = &["skills"];

#[derive(Clone, Debug, PartialEq, Eq)]
enum Tok {
    Ident(String),
    RawIdent(String),
    Lifetime,
    Literal,
    ColonColon,
    Arrow,
    FatArrow,
    Le,
    Ge,
    Shl,
    Shr,
    ShlEq,
    ShrEq,
    Lt,
    Gt,
    Punct(char),
}

#[derive(Clone, Debug)]
struct Token {
    kind: Tok,
    line: usize,
}

impl Token {
    fn is_kw(&self, kw: &str) -> bool {
        matches!(&self.kind, Tok::Ident(s) if s == kw)
    }

    fn ident_name(&self) -> Option<&str> {
        match &self.kind {
            Tok::Ident(s) | Tok::RawIdent(s) => Some(s.as_str()),
            _ => None,
        }
    }

    fn is_punct(&self, ch: char) -> bool {
        self.kind == Tok::Punct(ch)
    }
}

/// Matches a `use` import keyword while excluding Rust 2024 precise-capturing `+ use<'a, T>` bounds.
fn is_use_kw(tokens: &[Token], idx: usize) -> bool {
    tokens.get(idx).is_some_and(|t| t.is_kw("use"))
        && tokens.get(idx + 1).map(|t| &t.kind) != Some(&Tok::Lt)
}

fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic() || (!ch.is_ascii() && ch.is_alphabetic())
}

fn is_ident_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric() || (!ch.is_ascii() && ch.is_alphanumeric())
}

/// Checks if `src[pos..]` starts a raw string (`r#*"`, `br#*"`, `rb#*"`, `cr#*"`, `rc#*"`),
/// returning `(prefix_len_before_opening_quote, hash_count)`.
fn raw_string_prefix(src: &str, pos: usize) -> Option<(usize, usize)> {
    let bytes = src.as_bytes();
    let r_offset = match (bytes.get(pos).copied(), bytes.get(pos + 1).copied()) {
        (Some(b'b' | b'c'), Some(b'r')) | (Some(b'r'), Some(b'b' | b'c')) => 2,
        (Some(b'r'), _) => 1,
        _ => return None,
    };
    let mut hashes = 0;
    while bytes.get(pos + r_offset + hashes) == Some(&b'#') {
        hashes += 1;
    }
    (bytes.get(pos + r_offset + hashes) == Some(&b'"')).then_some((r_offset + hashes, hashes))
}

/// Scans Rust source with a character state machine, stripping `//` and nested `/* ... */`
/// comments while collapsing string, raw-string, char, and numeric literals to `Tok::Literal`.
fn lex(src: &str) -> Vec<Token> {
    let bytes = src.as_bytes();
    let len = bytes.len();
    let mut pos = 0;
    let mut line = 1usize;
    let mut out = Vec::new();
    let push = |out: &mut Vec<Token>, kind: Tok, line: usize| out.push(Token { kind, line });

    while pos < len {
        let b = bytes[pos];
        if b == b'\n' {
            line += 1;
            pos += 1;
            continue;
        }
        if b.is_ascii_whitespace() {
            pos += 1;
            continue;
        }

        if b == b'/' && bytes.get(pos + 1) == Some(&b'/') {
            pos += 2;
            while pos < len && bytes[pos] != b'\n' {
                pos += 1;
            }
            continue;
        }

        if b == b'/' && bytes.get(pos + 1) == Some(&b'*') {
            pos += 2;
            let mut depth = 1usize;
            while pos < len && depth > 0 {
                if bytes[pos] == b'/' && bytes.get(pos + 1) == Some(&b'*') {
                    depth += 1;
                    pos += 2;
                } else if bytes[pos] == b'*' && bytes.get(pos + 1) == Some(&b'/') {
                    depth -= 1;
                    pos += 2;
                } else {
                    if bytes[pos] == b'\n' {
                        line += 1;
                    }
                    pos += 1;
                }
            }
            continue;
        }

        if let Some((prefix_len, hashes)) = raw_string_prefix(src, pos) {
            let tok_line = line;
            pos += prefix_len + 1;
            while pos < len {
                if bytes[pos] == b'\n' {
                    line += 1;
                    pos += 1;
                    continue;
                }
                if bytes[pos] == b'"' {
                    let mut matched = 0;
                    while matched < hashes && bytes.get(pos + 1 + matched) == Some(&b'#') {
                        matched += 1;
                    }
                    if matched == hashes {
                        pos += 1 + hashes;
                        break;
                    }
                }
                pos += 1;
            }
            push(&mut out, Tok::Literal, tok_line);
            continue;
        }

        if b == b'"' || ((b == b'b' || b == b'c') && bytes.get(pos + 1) == Some(&b'"')) {
            let tok_line = line;
            pos += if b == b'"' { 1 } else { 2 };
            while pos < len {
                match bytes[pos] {
                    b'\\' => {
                        pos += 1;
                        if pos < len {
                            if bytes[pos] == b'\n' {
                                line += 1;
                            }
                            let ch = src[pos..].chars().next().unwrap();
                            pos += ch.len_utf8();
                        }
                    }
                    b'"' => {
                        pos += 1;
                        break;
                    }
                    b'\n' => {
                        line += 1;
                        pos += 1;
                    }
                    _ => pos += 1,
                }
            }
            push(&mut out, Tok::Literal, tok_line);
            continue;
        }

        if b == b'b' && bytes.get(pos + 1) == Some(&b'\'') {
            let tok_line = line;
            pos += 2;
            if bytes.get(pos) == Some(&b'\\') {
                pos += 2.min(len - pos);
            }
            while pos < len {
                if bytes[pos] == b'\n' {
                    line += 1;
                }
                if bytes[pos] == b'\'' {
                    pos += 1;
                    break;
                }
                pos += 1;
            }
            push(&mut out, Tok::Literal, tok_line);
            continue;
        }

        // Distinguish character literals (`'{'`, `'\''`, `'\"'`, `'\x7f'`, `'\u{...}'`) from lifetimes (`'a`, `'_`, `'static`).
        if b == b'\'' {
            let tok_line = line;
            let rest = &src[pos + 1..];
            if rest.starts_with('\\') {
                pos += 2;
                if let Some(escaped) = src[pos..].chars().next() {
                    if escaped == '\n' {
                        line += 1;
                    }
                    pos += escaped.len_utf8();
                }
                while pos < len {
                    if bytes[pos] == b'\n' {
                        line += 1;
                    }
                    if bytes[pos] == b'\'' {
                        pos += 1;
                        break;
                    }
                    pos += 1;
                }
                push(&mut out, Tok::Literal, tok_line);
                continue;
            }
            if let Some(ch) = rest.chars().next() {
                let after = &rest[ch.len_utf8()..];
                if ch != '\'' && ch != '\n' && ch != '\r' && ch != '\t' && after.starts_with('\'') {
                    pos += 1 + ch.len_utf8() + 1;
                    push(&mut out, Tok::Literal, tok_line);
                    continue;
                }
                if is_ident_start(ch) {
                    pos += 1;
                    if src[pos..].starts_with("r#") {
                        pos += 2;
                    }
                    while let Some(c) = src[pos..].chars().next() {
                        if is_ident_continue(c) {
                            pos += c.len_utf8();
                        } else {
                            break;
                        }
                    }
                    push(&mut out, Tok::Lifetime, tok_line);
                    continue;
                }
            }
            pos += 1;
            if bytes.get(pos) == Some(&b'\'') {
                pos += 1;
            }
            push(&mut out, Tok::Literal, tok_line);
            continue;
        }

        if b.is_ascii_digit() {
            let tok_line = line;
            pos += 1;
            while pos < len {
                let c = bytes[pos];
                if c.is_ascii_alphanumeric() || c == b'_' {
                    if (c == b'e' || c == b'E')
                        && matches!(bytes.get(pos + 1), Some(b'+' | b'-'))
                        && bytes.get(pos + 2).is_some_and(u8::is_ascii_digit)
                    {
                        pos += 2;
                    } else {
                        pos += 1;
                    }
                } else if c == b'.'
                    && bytes.get(pos + 1) != Some(&b'.')
                    && !src[pos + 1..].chars().next().is_some_and(is_ident_start)
                {
                    pos += 1;
                } else {
                    break;
                }
            }
            push(&mut out, Tok::Literal, tok_line);
            continue;
        }

        if b == b'r'
            && bytes.get(pos + 1) == Some(&b'#')
            && src[pos + 2..].chars().next().is_some_and(is_ident_start)
        {
            let tok_line = line;
            pos += 2;
            let start = pos;
            while let Some(c) = src[pos..].chars().next() {
                if is_ident_continue(c) {
                    pos += c.len_utf8();
                } else {
                    break;
                }
            }
            push(
                &mut out,
                Tok::RawIdent(src[start..pos].to_owned()),
                tok_line,
            );
            continue;
        }

        let ch = src[pos..].chars().next().unwrap();
        if is_ident_start(ch) {
            let tok_line = line;
            let start = pos;
            pos += ch.len_utf8();
            while let Some(c) = src[pos..].chars().next() {
                if is_ident_continue(c) {
                    pos += c.len_utf8();
                } else {
                    break;
                }
            }
            push(&mut out, Tok::Ident(src[start..pos].to_owned()), tok_line);
            continue;
        }

        let tok_line = line;
        let next = bytes.get(pos + 1).copied();
        let next2 = bytes.get(pos + 2).copied();
        let (kind, step) = match (b, next, next2) {
            (b':', Some(b':'), _) => (Tok::ColonColon, 2),
            (b'-', Some(b'>'), _) => (Tok::Arrow, 2),
            (b'=', Some(b'>'), _) => (Tok::FatArrow, 2),
            (b'<', Some(b'='), _) => (Tok::Le, 2),
            (b'>', Some(b'='), _) => (Tok::Ge, 2),
            (b'<', Some(b'<'), Some(b'=')) => (Tok::ShlEq, 3),
            (b'>', Some(b'>'), Some(b'=')) => (Tok::ShrEq, 3),
            (b'<', Some(b'<'), _) => (Tok::Shl, 2),
            (b'>', Some(b'>'), _) => (Tok::Shr, 2),
            (b'<', _, _) => (Tok::Lt, 1),
            (b'>', _, _) => (Tok::Gt, 1),
            _ => (Tok::Punct(ch), ch.len_utf8()),
        };
        pos += step;
        push(&mut out, kind, tok_line);
    }

    out
}

/// Kleene three-valued truth values for `#[cfg(...)]` satisfiability with `test = false`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Truth {
    True,
    False,
    Unknown,
}

impl Truth {
    fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }

    fn and(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::True, Self::True) => Self::True,
            _ => Self::Unknown,
        }
    }

    fn or(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::False, Self::False) => Self::False,
            _ => Self::Unknown,
        }
    }
}

fn skip_balanced_delim(tokens: &[Token], open_idx: usize) -> usize {
    let (open, close) = match tokens.get(open_idx).map(|t| &t.kind) {
        Some(Tok::Punct('(')) => ('(', ')'),
        Some(Tok::Punct('[')) => ('[', ']'),
        Some(Tok::Punct('{')) => ('{', '}'),
        _ => return open_idx + 1,
    };
    let mut depth = 1usize;
    let mut i = open_idx + 1;
    while i < tokens.len() && depth > 0 {
        if tokens[i].is_punct(open) {
            depth += 1;
        } else if tokens[i].is_punct(close) {
            depth -= 1;
        }
        i += 1;
    }
    i
}

fn split_top_level_commas(tokens: &[Token]) -> Vec<&[Token]> {
    let mut parts = Vec::new();
    let mut start = 0;
    let (mut paren, mut bracket, mut brace) = (0usize, 0usize, 0usize);
    for (idx, tok) in tokens.iter().enumerate() {
        match tok.kind {
            Tok::Punct('(') => paren += 1,
            Tok::Punct(')') => paren = paren.saturating_sub(1),
            Tok::Punct('[') => bracket += 1,
            Tok::Punct(']') => bracket = bracket.saturating_sub(1),
            Tok::Punct('{') => brace += 1,
            Tok::Punct('}') => brace = brace.saturating_sub(1),
            Tok::Punct(',') if paren == 0 && bracket == 0 && brace == 0 => {
                if start < idx {
                    parts.push(&tokens[start..idx]);
                }
                start = idx + 1;
            }
            _ => {}
        }
    }
    if start < tokens.len() {
        parts.push(&tokens[start..]);
    }
    parts
}

/// Evaluates a `cfg(...)` predicate slice under `test = False`, `true = True`, `false = False`,
/// and all platform/feature atoms as `Unknown`.
fn eval_cfg_expr(tokens: &[Token]) -> Truth {
    if tokens.is_empty() {
        return Truth::Unknown;
    }
    if tokens.len() >= 3
        && let Some(op) = tokens[0].ident_name()
        && tokens[1].is_punct('(')
    {
        let end = skip_balanced_delim(tokens, 1);
        if end == tokens.len() {
            let inner = &tokens[2..end - 1];
            match op {
                "all" => {
                    return split_top_level_commas(inner)
                        .into_iter()
                        .fold(Truth::True, |acc, part| acc.and(eval_cfg_expr(part)));
                }
                "any" => {
                    return split_top_level_commas(inner)
                        .into_iter()
                        .fold(Truth::False, |acc, part| acc.or(eval_cfg_expr(part)));
                }
                "not" => return eval_cfg_expr(inner).not(),
                _ => return Truth::Unknown,
            }
        }
    }
    if tokens.len() == 1
        && let Tok::Ident(atom) = &tokens[0].kind
    {
        return match atom.as_str() {
            "test" | "false" => Truth::False,
            "true" => Truth::True,
            _ => Truth::Unknown,
        };
    }
    Truth::Unknown
}

fn parse_attribute(tokens: &[Token], idx: usize) -> Option<(usize, bool, Option<Truth>)> {
    if !tokens.get(idx)?.is_punct('#') {
        return None;
    }
    let (is_inner, bracket_idx) = if tokens.get(idx + 1).is_some_and(|t| t.is_punct('!'))
        && tokens.get(idx + 2).is_some_and(|t| t.is_punct('['))
    {
        (true, idx + 2)
    } else if tokens.get(idx + 1).is_some_and(|t| t.is_punct('[')) {
        (false, idx + 1)
    } else {
        return None;
    };
    let after_bracket = skip_balanced_delim(tokens, bracket_idx);
    if after_bracket <= bracket_idx + 1 {
        return Some((after_bracket, is_inner, None));
    }
    let attr_body = &tokens[bracket_idx + 1..after_bracket - 1];
    if attr_body.len() >= 3 && attr_body[0].is_kw("cfg") && attr_body[1].is_punct('(') {
        let after_paren = skip_balanced_delim(attr_body, 1);
        if after_paren == attr_body.len() {
            let truth = eval_cfg_expr(&attr_body[2..after_paren - 1]);
            return Some((after_bracket, is_inner, Some(truth)));
        }
    }
    Some((after_bracket, is_inner, None))
}

/// True when `prev` forces a following `{ ... }` to be an inner operand/closure/modifier block
/// rather than the outer body of `if` / `while` / `for` / `match`.
fn forces_operand_block(prev: &Token) -> bool {
    matches!(
        &prev.kind,
        Tok::Punct('=' | '|' | '&' | '!' | '+' | '-' | '*' | '/' | '%' | '^' | ',' | ':')
            | Tok::Arrow
            | Tok::FatArrow
            | Tok::Le
            | Tok::Ge
            | Tok::Shl
            | Tok::ShlEq
            | Tok::ShrEq
    ) || prev.is_kw("unsafe")
        || prev.is_kw("const")
        || prev.is_kw("async")
        || prev.is_kw("move")
}

/// Consumes a control-flow header and its body `{ ... }` at `()` and `[]` depth 0 so closures or
/// `unsafe` blocks inside conditions do not terminate early before the real statement body.
fn skip_control_body(tokens: &[Token], start: usize, has_header: bool) -> usize {
    let mut i = start;
    let (mut paren, mut bracket) = (0usize, 0usize);
    while i < tokens.len() {
        if paren == 0 && bracket == 0 && tokens[i].is_punct('{') {
            let is_inner_expr_block =
                has_header && i > start && forces_operand_block(&tokens[i - 1]);
            if !is_inner_expr_block {
                return skip_balanced_delim(tokens, i);
            }
            i = skip_balanced_delim(tokens, i);
            continue;
        }
        match tokens[i].kind {
            Tok::Punct('(') => paren += 1,
            Tok::Punct(')') => paren = paren.saturating_sub(1),
            Tok::Punct('[') => bracket += 1,
            Tok::Punct(']') => bracket = bracket.saturating_sub(1),
            Tok::Punct('{') => {
                i = skip_balanced_delim(tokens, i);
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    i
}

/// Advances past an item, statement, control-flow block, or field/argument gated by a `False`
/// `#[cfg(...)]` attribute (§7.2.3 four-category syntax-aware skipper).
fn skip_pure_test_target(tokens: &[Token], mut i: usize) -> usize {
    while let Some((after_attr, false, _)) = parse_attribute(tokens, i) {
        i = after_attr;
    }

    loop {
        if tokens.get(i).is_some_and(|t| t.is_kw("pub")) {
            i += 1;
            if tokens.get(i).is_some_and(|t| t.is_punct('(')) {
                i = skip_balanced_delim(tokens, i);
            }
            continue;
        }
        if tokens
            .get(i)
            .is_some_and(|t| t.is_kw("default") || t.is_kw("async") || t.is_kw("unsafe"))
        {
            i += 1;
            if tokens.get(i).is_some_and(|t| t.is_kw("move"))
                && tokens.get(i + 1).is_some_and(|t| t.is_punct('{'))
            {
                i += 1;
            }
            continue;
        }
        if tokens.get(i).is_some_and(|t| t.is_kw("const"))
            && tokens.get(i + 1).is_some_and(|next| {
                next.is_kw("fn")
                    || next.is_kw("unsafe")
                    || next.is_kw("extern")
                    || next.is_punct('{')
            })
        {
            i += 1;
            continue;
        }
        // Keep `extern crate foo;` together for Category 1 while stripping `extern "C"` ABI
        // modifiers before `fn` or `{ ... }` extern blocks (without adding `crate` to Category 1,
        // which would overshoot call arguments like `foo(#[cfg(test)] crate::id::x(), prod())`).
        if tokens.get(i).is_some_and(|t| t.is_kw("extern"))
            && !tokens.get(i + 1).is_some_and(|t| t.is_kw("crate"))
        {
            i += 1;
            if matches!(tokens.get(i).map(|t| &t.kind), Some(Tok::Literal)) {
                i += 1;
            }
            continue;
        }
        break;
    }

    // Optional loop/block label (`'label: loop { ... }`).
    if tokens.get(i).is_some_and(|t| t.kind == Tok::Lifetime)
        && tokens.get(i + 1).is_some_and(|t| t.is_punct(':'))
    {
        i += 2;
    }

    let Some(lead) = tokens.get(i) else {
        return i;
    };

    // Category 1: Semicolon-terminated declarations (`type`, `const`, `static`, `use`, `extern crate`)
    // and `let` statements (`let ... else { ... };`).
    let is_extern_crate =
        lead.is_kw("extern") && tokens.get(i + 1).is_some_and(|t| t.is_kw("crate"));
    if lead.is_kw("type")
        || lead.is_kw("const")
        || lead.is_kw("static")
        || is_use_kw(tokens, i)
        || lead.is_kw("let")
        || is_extern_crate
    {
        let (mut paren, mut bracket, mut brace) = (0usize, 0usize, 0usize);
        i += 1;
        while i < tokens.len() {
            match tokens[i].kind {
                Tok::Punct('(') => paren += 1,
                Tok::Punct(')') => paren = paren.saturating_sub(1),
                Tok::Punct('[') => bracket += 1,
                Tok::Punct(']') => bracket = bracket.saturating_sub(1),
                Tok::Punct('{') => brace += 1,
                Tok::Punct('}') => brace = brace.saturating_sub(1),
                Tok::Punct(';') if paren == 0 && bracket == 0 && brace == 0 => return i + 1,
                _ => {}
            }
            i += 1;
        }
        return i;
    }

    // Category 2: Items with `{ ... }` body or `;` terminator (never truncated at `,`), plus
    // brace-delimited macro items (`ident! { ... }`).
    let is_brace_macro = lead.ident_name().is_some()
        && tokens.get(i + 1).is_some_and(|t| t.is_punct('!'))
        && tokens.get(i + 2).is_some_and(|t| t.is_punct('{'));
    if lead.is_kw("fn")
        || lead.is_kw("struct")
        || lead.is_kw("enum")
        || lead.is_kw("union")
        || lead.is_kw("trait")
        || lead.is_kw("impl")
        || lead.is_kw("mod")
        || lead.is_kw("macro_rules")
        || is_brace_macro
    {
        let (mut paren, mut bracket) = (0usize, 0usize);
        i += 1;
        while i < tokens.len() {
            match tokens[i].kind {
                Tok::Punct('(') => paren += 1,
                Tok::Punct(')') => paren = paren.saturating_sub(1),
                Tok::Punct('[') => bracket += 1,
                Tok::Punct(']') => bracket = bracket.saturating_sub(1),
                Tok::Punct(';') if paren == 0 && bracket == 0 => return i + 1,
                Tok::Punct('{') if paren == 0 && bracket == 0 => {
                    return skip_balanced_delim(tokens, i);
                }
                _ => {}
            }
            i += 1;
        }
        return i;
    }

    // Category 3: Control-flow expressions/statements (`if` + `else` chain, `match`, `loop`,
    // `while`, `for`) or direct block `{ ... }`.
    if lead.is_punct('{') {
        i = skip_balanced_delim(tokens, i);
        if tokens
            .get(i)
            .is_some_and(|t| t.is_punct(';') || t.is_punct(','))
        {
            i += 1;
        }
        return i;
    }
    if lead.is_kw("if") {
        i = skip_control_body(tokens, i + 1, true);
        while tokens.get(i).is_some_and(|t| t.is_kw("else")) {
            i += 1;
            if tokens.get(i).is_some_and(|t| t.is_kw("if")) {
                i = skip_control_body(tokens, i + 1, true);
            } else if tokens.get(i).is_some_and(|t| t.is_punct('{')) {
                i = skip_balanced_delim(tokens, i);
                break;
            } else {
                break;
            }
        }
        if tokens
            .get(i)
            .is_some_and(|t| t.is_punct(';') || t.is_punct(','))
        {
            i += 1;
        }
        return i;
    }
    if lead.is_kw("loop") {
        i = skip_control_body(tokens, i + 1, false);
        if tokens
            .get(i)
            .is_some_and(|t| t.is_punct(';') || t.is_punct(','))
        {
            i += 1;
        }
        return i;
    }
    if lead.is_kw("match") || lead.is_kw("while") || lead.is_kw("for") {
        i = skip_control_body(tokens, i + 1, true);
        if tokens
            .get(i)
            .is_some_and(|t| t.is_punct(';') || t.is_punct(','))
        {
            i += 1;
        }
        return i;
    }

    // Category 4: Struct/enum fields, struct literal fields, parameters, call arguments.
    let (mut paren, mut bracket, mut brace, mut angle) = (0usize, 0usize, 0usize, 0usize);
    while i < tokens.len() {
        if paren == 0 && bracket == 0 && brace == 0 {
            if tokens[i].is_punct('}') || tokens[i].is_punct(']') || tokens[i].is_punct(')') {
                return i;
            }
            if angle == 0 && (tokens[i].is_punct(',') || tokens[i].is_punct(';')) {
                return i + 1;
            }
            match tokens[i].kind {
                Tok::Lt => angle += 1,
                Tok::Gt => angle = angle.saturating_sub(1),
                Tok::Shr => angle = angle.saturating_sub(2),
                _ => {}
            }
        }
        match tokens[i].kind {
            Tok::Punct('(') => paren += 1,
            Tok::Punct(')') => paren = paren.saturating_sub(1),
            Tok::Punct('[') => bracket += 1,
            Tok::Punct(']') => bracket = bracket.saturating_sub(1),
            Tok::Punct('{') => brace += 1,
            Tok::Punct('}') => brace = brace.saturating_sub(1),
            _ => {}
        }
        i += 1;
    }
    i
}

/// Removes all pure-test `#![cfg(...)]` scopes and `#[cfg(...)]` targets where 3-valued evaluation
/// under `test = False` produces `Truth::False`.
fn strip_pure_test_code(tokens: &[Token]) -> Vec<Token> {
    let mut out = Vec::with_capacity(tokens.len());
    let mut scope_out_starts: Vec<usize> = Vec::new();
    let mut brace_depth = 0usize;
    let mut i = 0;
    while i < tokens.len() {
        if let Some((after_attr, true, Some(Truth::False))) = parse_attribute(tokens, i) {
            if brace_depth == 0 {
                out.clear();
                break;
            }
            if let Some(&start) = scope_out_starts.last() {
                out.truncate(start);
            }
            let target_depth = brace_depth - 1;
            i = after_attr;
            while i < tokens.len() {
                if tokens[i].is_punct('{') {
                    brace_depth += 1;
                } else if tokens[i].is_punct('}') {
                    // Stop at the enclosing block's closing `}` without decrementing here so
                    // the outer loop processes that `}` once and keeps `brace_depth` exact.
                    if brace_depth - 1 == target_depth {
                        break;
                    }
                    brace_depth -= 1;
                }
                i += 1;
            }
            continue;
        }
        if matches!(parse_attribute(tokens, i), Some((_, false, _))) {
            let mut cursor = i;
            let mut any_false = false;
            while let Some((after_attr, false, truth)) = parse_attribute(tokens, cursor) {
                if truth == Some(Truth::False) {
                    any_false = true;
                }
                cursor = after_attr;
            }
            if any_false {
                i = skip_pure_test_target(tokens, cursor);
                continue;
            }
        }
        if tokens[i].is_punct('{') {
            brace_depth += 1;
            out.push(tokens[i].clone());
            scope_out_starts.push(out.len());
            i += 1;
            continue;
        } else if tokens[i].is_punct('}') {
            brace_depth = brace_depth.saturating_sub(1);
            scope_out_starts.pop();
        }
        out.push(tokens[i].clone());
        i += 1;
    }
    out
}

#[derive(Clone, Debug)]
struct ExpandedUse {
    leading_coloncolon: bool,
    segments: Vec<String>,
    bound_name: Option<String>,
    is_glob: bool,
    line: usize,
}

fn expand_use_tree(
    tokens: &[Token],
    prefix: &[String],
    leading_coloncolon: bool,
    default_line: usize,
    out: &mut Vec<ExpandedUse>,
) {
    for part in split_top_level_commas(tokens) {
        if part.is_empty() {
            continue;
        }
        let mut i = 0;
        let mut leading = leading_coloncolon;
        if prefix.is_empty() && part.first().is_some_and(|t| t.kind == Tok::ColonColon) {
            leading = true;
            i = 1;
        }
        let line = part.get(i).map_or(default_line, |t| t.line);
        let mut segs: Vec<String> = prefix.to_vec();

        loop {
            if i >= part.len() {
                break;
            }
            if part[i].is_punct('{') {
                let end = skip_balanced_delim(part, i);
                expand_use_tree(&part[i + 1..end - 1], &segs, leading, part[i].line, out);
                break;
            }
            if part[i].is_punct('*') {
                out.push(ExpandedUse {
                    leading_coloncolon: leading,
                    segments: segs,
                    bound_name: None,
                    is_glob: true,
                    line: part[i].line,
                });
                break;
            }
            let Some(ident) = part[i].ident_name() else {
                break;
            };
            i += 1;
            if part.get(i).is_some_and(|t| t.kind == Tok::ColonColon) {
                segs.push(ident.to_owned());
                i += 1;
                continue;
            }
            let alias = if part.get(i).is_some_and(|t| t.is_kw("as")) {
                part.get(i + 1)
                    .and_then(Token::ident_name)
                    .map(str::to_owned)
            } else {
                None
            };
            let (final_segs, bound_name) = if ident == "self" {
                let bound = match alias.as_deref() {
                    Some("_") => None,
                    Some(name) => Some(name.to_owned()),
                    None => segs.last().cloned(),
                };
                (segs, bound)
            } else {
                segs.push(ident.to_owned());
                let bound = match alias.as_deref() {
                    Some("_") => None,
                    Some(name) => Some(name.to_owned()),
                    None => Some(ident.to_owned()),
                };
                (segs, bound)
            };
            out.push(ExpandedUse {
                leading_coloncolon: leading,
                segments: final_segs,
                bound_name,
                is_glob: false,
                line,
            });
            break;
        }
    }
}

/// Canonicalizes a path (`crate::...`, `super::...`, `self::...`, imported binding, or bare
/// declared submodule `<sub>::...`) into crate-relative segments (`vec![<top>, ...]`).
fn resolve_internal_path(
    leading_coloncolon: bool,
    segments: &[String],
    mod_path: &[String],
    declared_submods: &BTreeSet<String>,
    imports: &HashMap<String, Vec<String>>,
) -> Option<Vec<String>> {
    if leading_coloncolon || segments.is_empty() {
        return None;
    }
    match segments[0].as_str() {
        "crate" => {
            let rest = segments[1..].to_vec();
            (!rest.is_empty()).then_some(rest)
        }
        "self" | "super" => {
            let mut base = mod_path.to_vec();
            let mut idx = 0;
            while idx < segments.len() {
                match segments[idx].as_str() {
                    "self" => idx += 1,
                    "super" => {
                        base.pop();
                        idx += 1;
                    }
                    _ => break,
                }
            }
            base.extend_from_slice(&segments[idx..]);
            (!base.is_empty()).then_some(base)
        }
        first => {
            if let Some(prefix) = imports.get(first) {
                let mut full = prefix.clone();
                full.extend_from_slice(&segments[1..]);
                return (!full.is_empty()).then_some(full);
            }
            if declared_submods.contains(first) {
                let mut full = mod_path.to_vec();
                full.extend_from_slice(segments);
                return (!full.is_empty()).then_some(full);
            }
            None
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EdgeRef {
    file: String,
    line: usize,
    from_mod: Vec<String>,
    to_path: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InherentImplViolation {
    file: String,
    line: usize,
    from_top: String,
    target_top: String,
    target_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CrateLeak {
    crate_name: String,
    file: String,
    line: usize,
    from_top: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AllowSuppression {
    file: String,
    line: usize,
    is_inner: bool,
    lints: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DeclaredMod {
    name: String,
    line: usize,
    is_inline: bool,
}

#[derive(Default, Debug)]
struct ScanReport {
    main_declared_mods: Vec<DeclaredMod>,
    top_modules: BTreeSet<String>,
    file_submodules: BTreeMap<String, BTreeSet<String>>,
    modules: BTreeSet<Vec<String>>,
    edges: Vec<EdgeRef>,
    inherent_impls: Vec<InherentImplViolation>,
    crate_leaks: Vec<CrateLeak>,
    allow_suppressions: Vec<AllowSuppression>,
}

fn format_attr_tokens(tokens: &[Token]) -> String {
    let mut out = String::new();
    for tok in tokens {
        match &tok.kind {
            Tok::Ident(s) => out.push_str(s),
            Tok::RawIdent(s) => {
                out.push_str("r#");
                out.push_str(s);
            }
            Tok::Lifetime => out.push_str("'lt"),
            Tok::Literal => out.push_str("\"...\""),
            Tok::ColonColon => out.push_str("::"),
            Tok::Punct(',') => out.push_str(", "),
            Tok::Punct('=') => out.push_str(" = "),
            Tok::Arrow => out.push_str("->"),
            Tok::FatArrow => out.push_str("=>"),
            Tok::Le => out.push_str("<="),
            Tok::Ge => out.push_str(">="),
            Tok::Shl => out.push_str("<<"),
            Tok::Shr => out.push_str(">>"),
            Tok::ShlEq => out.push_str("<<="),
            Tok::ShrEq => out.push_str(">>="),
            Tok::Lt => out.push('<'),
            Tok::Gt => out.push('>'),
            Tok::Punct(ch) => out.push(*ch),
        }
    }
    out
}

fn collect_meta_allows(
    meta: &[Token],
    is_inner: bool,
    file_label: &str,
    out: &mut Vec<AllowSuppression>,
) -> bool {
    if meta.is_empty() {
        return false;
    }
    let mut p = 0;
    if meta.first().is_some_and(|t| t.kind == Tok::ColonColon) {
        p = 1;
    }
    let Some(first) = meta.get(p).and_then(Token::ident_name) else {
        return false;
    };
    let line = meta[p].line;
    let mut segs = vec![first];
    p += 1;
    while meta.get(p).is_some_and(|t| t.kind == Tok::ColonColon)
        && let Some(next) = meta.get(p + 1).and_then(Token::ident_name)
    {
        segs.push(next);
        p += 2;
    }
    let last = *segs.last().unwrap();
    if last == "allow" {
        let lints = if meta.get(p).is_some_and(|t| t.is_punct('('))
            && skip_balanced_delim(meta, p) == meta.len()
        {
            format_attr_tokens(&meta[p + 1..meta.len() - 1])
        } else if p == meta.len() {
            String::new()
        } else {
            return false;
        };
        out.push(AllowSuppression {
            file: file_label.to_owned(),
            line,
            is_inner,
            lints,
        });
        true
    } else if last == "cfg_attr"
        && meta.get(p).is_some_and(|t| t.is_punct('('))
        && skip_balanced_delim(meta, p) == meta.len()
    {
        let parts = split_top_level_commas(&meta[p + 1..meta.len() - 1]);
        if let Some((&pred, rest)) = parts.split_first()
            && eval_cfg_expr(pred) != Truth::False
        {
            for &sub_meta in rest {
                collect_meta_allows(sub_meta, is_inner, file_label, out);
            }
        }
        true
    } else {
        false
    }
}

fn scan_tokens_allow_suppressions(
    file_label: &str,
    tokens: &[Token],
    out: &mut Vec<AllowSuppression>,
) {
    let mut i = 0;
    while i < tokens.len() {
        if let Some((after_attr, is_inner, _)) = parse_attribute(tokens, i) {
            let bracket_idx = if is_inner { i + 2 } else { i + 1 };
            if after_attr > bracket_idx + 1 {
                let attr_body = &tokens[bracket_idx + 1..after_attr - 1];
                if collect_meta_allows(attr_body, is_inner, file_label, out) {
                    i = after_attr;
                    continue;
                }
            }
        }
        i += 1;
    }
}

fn skip_generic_params(tokens: &[Token], open_lt_idx: usize) -> usize {
    if tokens.get(open_lt_idx).map(|t| &t.kind) != Some(&Tok::Lt) {
        return open_lt_idx;
    }
    let (mut paren, mut bracket, mut brace, mut angle) = (0usize, 0usize, 0usize, 1usize);
    let mut i = open_lt_idx + 1;
    while i < tokens.len() && angle > 0 {
        if paren == 0 && bracket == 0 && brace == 0 {
            match tokens[i].kind {
                Tok::Lt => angle += 1,
                Tok::Gt => angle = angle.saturating_sub(1),
                Tok::Shr => angle = angle.saturating_sub(2),
                _ => {}
            }
        }
        match tokens[i].kind {
            Tok::Punct('(') => paren += 1,
            Tok::Punct(')') => paren = paren.saturating_sub(1),
            Tok::Punct('[') => bracket += 1,
            Tok::Punct(']') => bracket = bracket.saturating_sub(1),
            Tok::Punct('{') => brace += 1,
            Tok::Punct('}') => brace = brace.saturating_sub(1),
            _ => {}
        }
        i += 1;
    }
    i
}

/// Peels outer parentheses, reference prefixes (`&`, optional `'a`, optional `mut`), and raw
/// pointer prefixes (`*const`, `*mut`) before extracting the leading type path.
fn extract_leading_path(mut slice: &[Token]) -> Option<(bool, Vec<String>)> {
    loop {
        if slice.first().is_some_and(|t| t.is_punct('('))
            && skip_balanced_delim(slice, 0) == slice.len()
        {
            slice = &slice[1..slice.len() - 1];
            continue;
        }
        if slice.first().is_some_and(|t| t.is_punct('&')) {
            slice = &slice[1..];
            if slice.first().is_some_and(|t| t.kind == Tok::Lifetime) {
                slice = &slice[1..];
            }
            if slice.first().is_some_and(|t| t.is_kw("mut")) {
                slice = &slice[1..];
            }
            continue;
        }
        if slice.first().is_some_and(|t| t.is_punct('*'))
            && slice
                .get(1)
                .is_some_and(|t| t.is_kw("const") || t.is_kw("mut"))
        {
            slice = &slice[2..];
            continue;
        }
        break;
    }
    let mut p = 0;
    let mut leading_cc = false;
    if slice.first().is_some_and(|t| t.kind == Tok::ColonColon) {
        leading_cc = true;
        p = 1;
    }
    let mut segs = Vec::new();
    while let Some(name) = slice.get(p).and_then(Token::ident_name) {
        segs.push(name.to_owned());
        p += 1;
        if slice.get(p).is_some_and(|t| t.kind == Tok::ColonColon)
            && slice.get(p + 1).and_then(Token::ident_name).is_some()
        {
            p += 1;
        } else {
            break;
        }
    }
    (!segs.is_empty()).then_some((leading_cc, segs))
}

/// Inspects an `impl` item header starting at `impl_idx` (`tokens[impl_idx]` is `impl`).
/// If it has no top-level trait `for`, extracts the leading path of the inherent target type.
fn extract_inherent_impl_target(tokens: &[Token], impl_idx: usize) -> Option<(bool, Vec<String>)> {
    let mut i = impl_idx + 1;
    if tokens.get(i).is_some_and(|t| t.kind == Tok::Lt) {
        i = skip_generic_params(tokens, i);
    }
    let header_start = i;
    let (mut paren, mut bracket, mut brace, mut angle) = (0usize, 0usize, 0usize, 0usize);
    let mut type_end = None;
    while i < tokens.len() {
        if paren == 0 && bracket == 0 && brace == 0 && angle == 0 {
            if tokens[i].is_punct('{') {
                type_end.get_or_insert(i);
                break;
            }
            if tokens[i].is_punct(';') {
                return None;
            }
            if tokens[i].is_kw("where") {
                type_end.get_or_insert(i);
            }
            if tokens[i].is_kw("for")
                && !(tokens.get(i + 1).is_some_and(|t| t.kind == Tok::Lt)
                    && tokens.get(i + 2).is_some_and(|t| t.kind == Tok::Lifetime))
            {
                return None;
            }
        }
        if paren == 0 && bracket == 0 && brace == 0 {
            match tokens[i].kind {
                Tok::Lt => angle += 1,
                Tok::Gt => angle = angle.saturating_sub(1),
                Tok::Shr => angle = angle.saturating_sub(2),
                _ => {}
            }
        }
        match tokens[i].kind {
            Tok::Punct('(') => paren += 1,
            Tok::Punct(')') => paren = paren.saturating_sub(1),
            Tok::Punct('[') => bracket += 1,
            Tok::Punct(']') => bracket = bracket.saturating_sub(1),
            Tok::Punct('{') => brace += 1,
            Tok::Punct('}') => brace = brace.saturating_sub(1),
            _ => {}
        }
        i += 1;
    }
    extract_leading_path(&tokens[header_start..type_end?])
}

fn collect_defined_types(tokens: &[Token], out: &mut BTreeSet<String>) {
    let mut i = 0;
    while i + 1 < tokens.len() {
        if (tokens[i].is_kw("struct") || tokens[i].is_kw("enum") || tokens[i].is_kw("union"))
            && let Some(name) = tokens[i + 1].ident_name()
        {
            out.insert(name.to_owned());
        }
        i += 1;
    }
}

fn check_crate_token(tok: &Token, current_top: &str, file_label: &str, report: &mut ScanReport) {
    if let Some(ident) = tok.ident_name() {
        let forbidden = match ident {
            "rquickjs" => !RQUICKJS_ALLOWED.contains(&current_top),
            "alacritty_terminal" => !ALACRITTY_ALLOWED.contains(&current_top),
            "hyper" | "hyper_util" | "http_body_util" | "tower_service" | "axum" | "rustls"
            | "tokio_rustls" => !HYPER_ALLOWED.contains(&current_top),
            "rmcp" => !RMCP_ALLOWED.contains(&current_top),
            "serde_saphyr" => !SERDE_SAPHYR_ALLOWED.contains(&current_top),
            _ => false,
        };
        if forbidden {
            report.crate_leaks.push(CrateLeak {
                crate_name: ident.to_owned(),
                file: file_label.to_owned(),
                line: tok.line,
                from_top: current_top.to_owned(),
            });
        }
    }
}

#[derive(Clone)]
struct BlockScope {
    imports: HashMap<String, Vec<String>>,
    glob_tops: BTreeSet<String>,
}

fn scan_slice_paths_and_crates(
    slice: &[Token],
    file_label: &str,
    mod_path: &[String],
    declared_submods: &BTreeSet<String>,
    imports: &HashMap<String, Vec<String>>,
    report: &mut ScanReport,
) {
    let current_top = &mod_path[0];
    let mut i = 0;
    while i < slice.len() {
        let leading_cc = slice[i].kind == Tok::ColonColon
            && !slice
                .get(i.wrapping_sub(1))
                .is_some_and(|prev| prev.ident_name().is_some() || prev.is_punct('>'));
        let path_start = if leading_cc { i + 1 } else { i };
        let preceded_by_dot = !leading_cc && i > 0 && slice[i - 1].is_punct('.');
        if !preceded_by_dot
            && let Some(first_seg) = slice.get(path_start).and_then(Token::ident_name)
        {
            let mut segs = vec![first_seg.to_owned()];
            check_crate_token(&slice[path_start], current_top, file_label, report);
            let mut p = path_start + 1;
            while slice.get(p).is_some_and(|t| t.kind == Tok::ColonColon)
                && let Some(next_seg) = slice.get(p + 1).and_then(Token::ident_name)
            {
                check_crate_token(&slice[p + 1], current_top, file_label, report);
                segs.push(next_seg.to_owned());
                p += 2;
            }
            if segs.len() >= 2
                && let Some(canon) =
                    resolve_internal_path(leading_cc, &segs, mod_path, declared_submods, imports)
            {
                report.edges.push(EdgeRef {
                    file: file_label.to_owned(),
                    line: slice[path_start].line,
                    from_mod: mod_path.to_vec(),
                    to_path: canon,
                });
            }
            i = p;
            continue;
        }
        check_crate_token(&slice[i], current_top, file_label, report);
        i += 1;
    }
}

fn analyze_module_scope(
    file_label: &str,
    mod_path: &[String],
    tokens: &[Token],
    defined_types_by_top: &HashMap<String, BTreeSet<String>>,
    report: &mut ScanReport,
) {
    let current_top = &mod_path[0];
    for len in 1..=mod_path.len() {
        report.modules.insert(mod_path[..len].to_vec());
    }

    let mut declared_submods = BTreeSet::new();
    let mut excluded_spans: Vec<(usize, usize)> = Vec::new();
    let mut inline_mods: Vec<(String, usize, usize)> = Vec::new();
    {
        let (mut paren, mut bracket, mut brace) = (0usize, 0usize, 0usize);
        let mut i = 0;
        while i < tokens.len() {
            if paren == 0
                && bracket == 0
                && brace == 0
                && tokens[i].is_kw("mod")
                && let Some(sub) = tokens.get(i + 1).and_then(Token::ident_name)
            {
                if tokens.get(i + 2).is_some_and(|t| t.is_punct(';')) {
                    declared_submods.insert(sub.to_owned());
                    excluded_spans.push((i, i + 3));
                    i += 3;
                    continue;
                }
                if tokens.get(i + 2).is_some_and(|t| t.is_punct('{')) {
                    let after_body = skip_balanced_delim(tokens, i + 2);
                    declared_submods.insert(sub.to_owned());
                    excluded_spans.push((i, after_body));
                    inline_mods.push((sub.to_owned(), i + 3, after_body - 1));
                    i = after_body;
                    continue;
                }
            }
            match tokens[i].kind {
                Tok::Punct('(') => paren += 1,
                Tok::Punct(')') => paren = paren.saturating_sub(1),
                Tok::Punct('[') => bracket += 1,
                Tok::Punct(']') => bracket = bracket.saturating_sub(1),
                Tok::Punct('{') => brace += 1,
                Tok::Punct('}') => brace = brace.saturating_sub(1),
                _ => {}
            }
            i += 1;
        }
    }

    for sub in &declared_submods {
        let mut child = mod_path.to_vec();
        child.push(sub.clone());
        report.modules.insert(child);
    }

    let in_excluded = |idx: usize| {
        excluded_spans
            .iter()
            .any(|&(start, end)| idx >= start && idx < end)
    };

    // Pre-seed module-level (`brace == 0`) `use` trees and `type Alias = Path;` items so forward
    // references resolve regardless of declaration order without leaking block-local `use` items.
    let mut module_uses: Vec<ExpandedUse> = Vec::new();
    let mut module_type_aliases: Vec<(String, bool, Vec<String>)> = Vec::new();
    {
        let (mut paren, mut bracket, mut brace) = (0usize, 0usize, 0usize);
        let mut i = 0;
        while i < tokens.len() {
            if in_excluded(i) {
                i += 1;
                continue;
            }
            if paren == 0 && bracket == 0 && brace == 0 {
                if is_use_kw(tokens, i) {
                    let use_line = tokens[i].line;
                    i += 1;
                    let tree_start = i;
                    let (mut up, mut ub, mut uc) = (0usize, 0usize, 0usize);
                    while i < tokens.len() {
                        match tokens[i].kind {
                            Tok::Punct('(') => up += 1,
                            Tok::Punct(')') => up = up.saturating_sub(1),
                            Tok::Punct('[') => ub += 1,
                            Tok::Punct(']') => ub = ub.saturating_sub(1),
                            Tok::Punct('{') => uc += 1,
                            Tok::Punct('}') => uc = uc.saturating_sub(1),
                            Tok::Punct(';') if up == 0 && ub == 0 && uc == 0 => break,
                            _ => {}
                        }
                        i += 1;
                    }
                    expand_use_tree(
                        &tokens[tree_start..i],
                        &[],
                        false,
                        use_line,
                        &mut module_uses,
                    );
                    i = (i + 1).min(tokens.len());
                    continue;
                }
                if tokens[i].is_kw("type")
                    && let Some(alias) = tokens.get(i + 1).and_then(Token::ident_name)
                {
                    let mut k = i + 2;
                    if tokens.get(k).is_some_and(|t| t.kind == Tok::Lt) {
                        k = skip_generic_params(tokens, k);
                    }
                    if tokens.get(k).is_some_and(|t| t.is_punct('=')) {
                        let rhs_start = k + 1;
                        let mut end = rhs_start;
                        let (mut tp, mut tb, mut tc) = (0usize, 0usize, 0usize);
                        while end < tokens.len() {
                            match tokens[end].kind {
                                Tok::Punct('(') => tp += 1,
                                Tok::Punct(')') => tp = tp.saturating_sub(1),
                                Tok::Punct('[') => tb += 1,
                                Tok::Punct(']') => tb = tb.saturating_sub(1),
                                Tok::Punct('{') => tc += 1,
                                Tok::Punct('}') => tc = tc.saturating_sub(1),
                                Tok::Punct(';') if tp == 0 && tb == 0 && tc == 0 => break,
                                _ => {}
                            }
                            end += 1;
                        }
                        if let Some((lcc, segs)) = extract_leading_path(&tokens[rhs_start..end]) {
                            module_type_aliases.push((alias.to_owned(), lcc, segs));
                        }
                    }
                }
            }
            match tokens[i].kind {
                Tok::Punct('(') => paren += 1,
                Tok::Punct(')') => paren = paren.saturating_sub(1),
                Tok::Punct('[') => bracket += 1,
                Tok::Punct(']') => bracket = bracket.saturating_sub(1),
                Tok::Punct('{') => brace += 1,
                Tok::Punct('}') => brace = brace.saturating_sub(1),
                _ => {}
            }
            i += 1;
        }
    }

    let mut root_scope = BlockScope {
        imports: HashMap::new(),
        glob_tops: BTreeSet::new(),
    };
    for _ in 0..3 {
        let mut changed = false;
        for item in &module_uses {
            if let Some(canon) = resolve_internal_path(
                item.leading_coloncolon,
                &item.segments,
                mod_path,
                &declared_submods,
                &root_scope.imports,
            ) {
                if item.is_glob && &canon[0] != current_top {
                    root_scope.glob_tops.insert(canon[0].clone());
                }
                if let Some(bound) = &item.bound_name
                    && root_scope.imports.get(bound) != Some(&canon)
                {
                    root_scope.imports.insert(bound.clone(), canon);
                    changed = true;
                }
            }
        }
        for (alias, lcc, segs) in &module_type_aliases {
            if let Some(canon) =
                resolve_internal_path(*lcc, segs, mod_path, &declared_submods, &root_scope.imports)
                && root_scope.imports.get(alias) != Some(&canon)
            {
                root_scope.imports.insert(alias.clone(), canon);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    // Walk all tokens with a `{ ... }` lexical scope stack. No `paren == 0 && bracket == 0` guard
    // is applied to `is_use_kw`, so `tokio::spawn(async move { use crate::...; })` inside call
    // parentheses enters the enclosing block's scope while `+ use<'a>` bounds are ignored.
    let mut scope_stack = vec![root_scope];
    let mut at_item_boundary = true;
    let mut i = 0;
    while i < tokens.len() {
        if in_excluded(i) {
            at_item_boundary = true;
            i += 1;
            continue;
        }

        if is_use_kw(tokens, i) {
            let use_line = tokens[i].line;
            check_crate_token(&tokens[i], current_top, file_label, report);
            i += 1;
            let tree_start = i;
            let (mut up, mut ub, mut uc) = (0usize, 0usize, 0usize);
            while i < tokens.len() {
                check_crate_token(&tokens[i], current_top, file_label, report);
                match tokens[i].kind {
                    Tok::Punct('(') => up += 1,
                    Tok::Punct(')') => up = up.saturating_sub(1),
                    Tok::Punct('[') => ub += 1,
                    Tok::Punct(']') => ub = ub.saturating_sub(1),
                    Tok::Punct('{') => uc += 1,
                    Tok::Punct('}') => uc = uc.saturating_sub(1),
                    Tok::Punct(';') if up == 0 && ub == 0 && uc == 0 => break,
                    _ => {}
                }
                i += 1;
            }
            let mut expanded = Vec::new();
            expand_use_tree(&tokens[tree_start..i], &[], false, use_line, &mut expanded);
            let active = scope_stack.last_mut().unwrap();
            for item in expanded {
                if let Some(canon) = resolve_internal_path(
                    item.leading_coloncolon,
                    &item.segments,
                    mod_path,
                    &declared_submods,
                    &active.imports,
                ) {
                    if item.is_glob && &canon[0] != current_top {
                        active.glob_tops.insert(canon[0].clone());
                    }
                    if let Some(bound) = item.bound_name {
                        active.imports.insert(bound, canon.clone());
                    }
                    report.edges.push(EdgeRef {
                        file: file_label.to_owned(),
                        line: item.line,
                        from_mod: mod_path.to_vec(),
                        to_path: canon,
                    });
                }
            }
            at_item_boundary = true;
            i = (i + 1).min(tokens.len());
            continue;
        }

        if let Some((after_attr, _, _)) = parse_attribute(tokens, i) {
            let active = scope_stack.last().unwrap();
            scan_slice_paths_and_crates(
                &tokens[i + 1..after_attr],
                file_label,
                mod_path,
                &declared_submods,
                &active.imports,
                report,
            );
            i = after_attr;
            continue;
        }

        if at_item_boundary
            && tokens[i].is_kw("impl")
            && let Some((leading_cc, target_segs)) = extract_inherent_impl_target(tokens, i)
        {
            let active = scope_stack.last().unwrap();
            let resolved = resolve_internal_path(
                leading_cc,
                &target_segs,
                mod_path,
                &declared_submods,
                &active.imports,
            )
            .or_else(|| {
                if !leading_cc && target_segs.len() == 1 {
                    let ty = &target_segs[0];
                    let local_has = defined_types_by_top
                        .get(current_top)
                        .is_some_and(|s| s.contains(ty));
                    if !local_has {
                        for gtop in &active.glob_tops {
                            if defined_types_by_top
                                .get(gtop)
                                .is_some_and(|s| s.contains(ty))
                            {
                                return Some(vec![gtop.clone(), ty.clone()]);
                            }
                        }
                    }
                }
                None
            });
            if let Some(canon) = resolved
                && &canon[0] != current_top
            {
                report.inherent_impls.push(InherentImplViolation {
                    file: file_label.to_owned(),
                    line: tokens[i].line,
                    from_top: current_top.clone(),
                    target_top: canon[0].clone(),
                    target_path: canon.join("::"),
                });
            }
        }

        let leading_cc = tokens[i].kind == Tok::ColonColon
            && !tokens
                .get(i.wrapping_sub(1))
                .is_some_and(|prev| prev.ident_name().is_some() || prev.is_punct('>'));
        let path_start = if leading_cc { i + 1 } else { i };
        let preceded_by_dot = !leading_cc && i > 0 && tokens[i - 1].is_punct('.');
        if !preceded_by_dot
            && let Some(first_seg) = tokens.get(path_start).and_then(Token::ident_name)
        {
            let mut segs = vec![first_seg.to_owned()];
            check_crate_token(&tokens[path_start], current_top, file_label, report);
            let mut p = path_start + 1;
            while tokens.get(p).is_some_and(|t| t.kind == Tok::ColonColon)
                && let Some(next_seg) = tokens.get(p + 1).and_then(Token::ident_name)
            {
                check_crate_token(&tokens[p + 1], current_top, file_label, report);
                segs.push(next_seg.to_owned());
                p += 2;
            }
            let active = scope_stack.last().unwrap();
            if segs.len() >= 2
                && let Some(canon) = resolve_internal_path(
                    leading_cc,
                    &segs,
                    mod_path,
                    &declared_submods,
                    &active.imports,
                )
            {
                report.edges.push(EdgeRef {
                    file: file_label.to_owned(),
                    line: tokens[path_start].line,
                    from_mod: mod_path.to_vec(),
                    to_path: canon,
                });
            }
            at_item_boundary = at_item_boundary
                && segs.len() == 1
                && matches!(first_seg, "default" | "unsafe" | "const");
            i = p;
            continue;
        }

        match tokens[i].kind {
            Tok::Punct('{') => {
                let close = skip_balanced_delim(tokens, i);
                let mut child_scope = scope_stack.last().unwrap().clone();
                let body = &tokens[i + 1..close.saturating_sub(1)];
                let mut inner_brace = 0usize;
                let mut k = 0;
                while k < body.len() {
                    if !in_excluded(i + 1 + k) && inner_brace == 0 && is_use_kw(body, k) {
                        let use_line = body[k].line;
                        k += 1;
                        let tree_start = k;
                        let (mut up, mut ub, mut uc) = (0usize, 0usize, 0usize);
                        while k < body.len() {
                            match body[k].kind {
                                Tok::Punct('(') => up += 1,
                                Tok::Punct(')') => up = up.saturating_sub(1),
                                Tok::Punct('[') => ub += 1,
                                Tok::Punct(']') => ub = ub.saturating_sub(1),
                                Tok::Punct('{') => uc += 1,
                                Tok::Punct('}') => uc = uc.saturating_sub(1),
                                Tok::Punct(';') if up == 0 && ub == 0 && uc == 0 => break,
                                _ => {}
                            }
                            k += 1;
                        }
                        let mut expanded = Vec::new();
                        expand_use_tree(&body[tree_start..k], &[], false, use_line, &mut expanded);
                        for item in expanded {
                            if let Some(canon) = resolve_internal_path(
                                item.leading_coloncolon,
                                &item.segments,
                                mod_path,
                                &declared_submods,
                                &child_scope.imports,
                            ) {
                                if item.is_glob && &canon[0] != current_top {
                                    child_scope.glob_tops.insert(canon[0].clone());
                                }
                                if let Some(bound) = item.bound_name {
                                    child_scope.imports.insert(bound, canon);
                                }
                            }
                        }
                        k = (k + 1).min(body.len());
                        continue;
                    }
                    match body[k].kind {
                        Tok::Punct('{') => inner_brace += 1,
                        Tok::Punct('}') => inner_brace = inner_brace.saturating_sub(1),
                        _ => {}
                    }
                    k += 1;
                }
                scope_stack.push(child_scope);
            }
            Tok::Punct('}') if scope_stack.len() > 1 => {
                scope_stack.pop();
            }
            _ => {}
        }

        check_crate_token(&tokens[i], current_top, file_label, report);
        at_item_boundary = matches!(tokens[i].kind, Tok::Punct(';' | '{' | '}'));
        i += 1;
    }

    for (sub_name, body_start, body_end) in inline_mods {
        let mut child_path = mod_path.to_vec();
        child_path.push(sub_name);
        analyze_module_scope(
            file_label,
            &child_path,
            &tokens[body_start..body_end],
            defined_types_by_top,
            report,
        );
    }
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
        .map(|entry| entry.expect("dir entry").path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

fn mod_path_from_rel(rel: &Path) -> Option<Vec<String>> {
    if rel == Path::new("main.rs") || rel.file_name().and_then(|s| s.to_str()) == Some("tests.rs") {
        return None;
    }
    let without_ext = rel.with_extension("");
    let mut comps: Vec<String> = without_ext
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    if comps.len() > 1 && comps.last().map(String::as_str) == Some("mod") {
        comps.pop();
    }
    (!comps.is_empty()).then_some(comps)
}

fn analyze_virtual_files(files: &[(&str, Vec<String>, &str)]) -> ScanReport {
    let prepared: Vec<(&str, Vec<String>, Vec<Token>)> = files
        .iter()
        .map(|(label, mod_path, src)| {
            let filtered = strip_pure_test_code(&lex(src));
            (*label, mod_path.clone(), filtered)
        })
        .collect();

    let mut defined_types_by_top: HashMap<String, BTreeSet<String>> = HashMap::new();
    let mut report = ScanReport::default();
    for (_, mod_path, tokens) in &prepared {
        report.top_modules.insert(mod_path[0].clone());
        if mod_path.len() >= 2 {
            report
                .file_submodules
                .entry(mod_path[0].clone())
                .or_default()
                .insert(mod_path[1].clone());
        }
        collect_defined_types(
            tokens,
            defined_types_by_top.entry(mod_path[0].clone()).or_default(),
        );
    }

    for (label, mod_path, tokens) in &prepared {
        scan_tokens_allow_suppressions(label, tokens, &mut report.allow_suppressions);
        analyze_module_scope(label, mod_path, tokens, &defined_types_by_top, &mut report);
    }
    report
}

fn extract_top_level_mods(tokens: &[Token]) -> Vec<DeclaredMod> {
    let mut out = Vec::new();
    let (mut paren, mut bracket, mut brace) = (0usize, 0usize, 0usize);
    let mut i = 0;
    while i < tokens.len() {
        if paren == 0
            && bracket == 0
            && brace == 0
            && tokens[i].is_kw("mod")
            && let Some(name) = tokens.get(i + 1).and_then(Token::ident_name)
        {
            if tokens.get(i + 2).is_some_and(|t| t.is_punct(';')) {
                out.push(DeclaredMod {
                    name: name.to_owned(),
                    line: tokens[i].line,
                    is_inline: false,
                });
                i += 3;
                continue;
            }
            if tokens.get(i + 2).is_some_and(|t| t.is_punct('{')) {
                out.push(DeclaredMod {
                    name: name.to_owned(),
                    line: tokens[i].line,
                    is_inline: true,
                });
                i = skip_balanced_delim(tokens, i + 2);
                continue;
            }
        }
        match tokens[i].kind {
            Tok::Punct('(') => paren += 1,
            Tok::Punct(')') => paren = paren.saturating_sub(1),
            Tok::Punct('[') => bracket += 1,
            Tok::Punct(']') => bracket = bracket.saturating_sub(1),
            Tok::Punct('{') => brace += 1,
            Tok::Punct('}') => brace = brace.saturating_sub(1),
            _ => {}
        }
        i += 1;
    }
    out
}

fn scan_repo_src() -> ScanReport {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let src_dir = root.join("src");
    let mut paths = Vec::new();
    collect_rs_files(&src_dir, &mut paths);

    let mut raw_files: Vec<(String, Vec<String>, String)> = Vec::new();
    let mut main_src: Option<String> = None;
    for path in paths {
        let rel = path.strip_prefix(&src_dir).expect("under src/");
        if rel == Path::new("main.rs") {
            main_src = Some(
                fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display())),
            );
            continue;
        }
        let Some(mod_path) = mod_path_from_rel(rel) else {
            continue;
        };
        let label = format!(
            "src/{}",
            rel.to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/")
        );
        let content = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
        raw_files.push((label, mod_path, content));
    }

    let refs: Vec<(&str, Vec<String>, &str)> = raw_files
        .iter()
        .map(|(label, mod_path, content)| (label.as_str(), mod_path.clone(), content.as_str()))
        .collect();
    let mut report = analyze_virtual_files(&refs);
    let main_content = main_src.expect("src/main.rs must exist");
    let filtered = strip_pure_test_code(&lex(&main_content));
    report.main_declared_mods = extract_top_level_mods(&filtered);
    scan_tokens_allow_suppressions("src/main.rs", &filtered, &mut report.allow_suppressions);
    report
        .allow_suppressions
        .sort_by(|a, b| (&a.file, a.line).cmp(&(&b.file, b.line)));
    report
}

fn top_level_directed_edges(report: &ScanReport) -> BTreeMap<(String, String), Vec<String>> {
    let mut map: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for edge in &report.edges {
        let from_top = &edge.from_mod[0];
        let to_top = &edge.to_path[0];
        if from_top != to_top {
            let witness = format!(
                "{}:{} (`crate::{}`)",
                edge.file,
                edge.line,
                edge.to_path.join("::")
            );
            let list = map.entry((from_top.clone(), to_top.clone())).or_default();
            if !list.contains(&witness) {
                list.push(witness);
            }
        }
    }
    map
}

fn compute_two_cycles(
    directed: &BTreeMap<(String, String), Vec<String>>,
) -> BTreeSet<(String, String)> {
    let mut cycles = BTreeSet::new();
    for (u, v) in directed.keys() {
        if u < v && directed.contains_key(&(v.clone(), u.clone())) {
            cycles.insert((u.clone(), v.clone()));
        }
    }
    cycles
}

/// Tarjan's strongly connected components algorithm. Returns all non-trivial SCCs (`len >= 2`)
/// sorted deterministically.
fn tarjan_cyclic_sccs(
    nodes: &BTreeSet<String>,
    adj: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<Vec<String>> {
    struct State<'a> {
        adj: &'a BTreeMap<String, BTreeSet<String>>,
        next_index: usize,
        indices: HashMap<String, usize>,
        lowlink: HashMap<String, usize>,
        stack: Vec<String>,
        on_stack: BTreeSet<String>,
        cycles: Vec<Vec<String>>,
    }

    fn strongconnect(v: &str, state: &mut State<'_>) {
        let idx = state.next_index;
        state.next_index += 1;
        state.indices.insert(v.to_owned(), idx);
        state.lowlink.insert(v.to_owned(), idx);
        state.stack.push(v.to_owned());
        state.on_stack.insert(v.to_owned());

        if let Some(neighbors) = state.adj.get(v) {
            for w in neighbors {
                if w == v {
                    continue;
                }
                if !state.indices.contains_key(w) {
                    strongconnect(w, state);
                    let low_w = state.lowlink[w];
                    let low_v = state.lowlink.get_mut(v).unwrap();
                    *low_v = (*low_v).min(low_w);
                } else if state.on_stack.contains(w) {
                    let idx_w = state.indices[w];
                    let low_v = state.lowlink.get_mut(v).unwrap();
                    *low_v = (*low_v).min(idx_w);
                }
            }
        }

        if state.lowlink[v] == state.indices[v] {
            let mut comp = Vec::new();
            while let Some(w) = state.stack.pop() {
                state.on_stack.remove(&w);
                let done = w == v;
                comp.push(w);
                if done {
                    break;
                }
            }
            if comp.len() >= 2 {
                comp.sort();
                state.cycles.push(comp);
            }
        }
    }

    let mut state = State {
        adj,
        next_index: 0,
        indices: HashMap::new(),
        lowlink: HashMap::new(),
        stack: Vec::new(),
        on_stack: BTreeSet::new(),
        cycles: Vec::new(),
    };
    for node in nodes {
        if !state.indices.contains_key(node) {
            strongconnect(node, &mut state);
        }
    }
    state.cycles.sort();
    state.cycles
}

struct SubmoduleGraph {
    nodes: BTreeSet<String>,
    adj: BTreeMap<String, BTreeSet<String>>,
    witnesses: BTreeMap<(String, String), Vec<String>>,
}

/// Resolves each path to its longest declared module prefix, including inline and nested modules.
/// Only the Spec's tasks aggregate root may be omitted; descendants remain distinct nodes.
fn submodule_directed_edges(report: &ScanReport, top: &str, include_root: bool) -> SubmoduleGraph {
    let modules: Vec<&Vec<String>> = report
        .modules
        .iter()
        .filter(|path| path.first().map(String::as_str) == Some(top))
        .collect();
    let nodes = modules
        .iter()
        .filter(|path| include_root || path.len() > 1)
        .map(|path| path.join("::"))
        .collect();

    let node_of = |path: &[String]| -> Option<String> {
        let module = modules
            .iter()
            .filter(|module| path.starts_with(module.as_slice()))
            .max_by_key(|module| module.len())?;
        (include_root || module.len() > 1).then(|| module.join("::"))
    };

    let mut adj: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut witnesses: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for edge in &report.edges {
        let (Some(u), Some(v)) = (node_of(&edge.from_mod), node_of(&edge.to_path)) else {
            continue;
        };
        if u == v {
            continue;
        }
        adj.entry(u.clone()).or_default().insert(v.clone());
        let w = format!(
            "{}:{} (`crate::{}`)",
            edge.file,
            edge.line,
            edge.to_path.join("::")
        );
        let list = witnesses.entry((u, v)).or_default();
        if !list.contains(&w) {
            list.push(w);
        }
    }
    SubmoduleGraph {
        nodes,
        adj,
        witnesses,
    }
}

// ---------------------------------------------------------------------------
// Architecture Gate Tests (Final state: zero cycles, zero transitional edges)
// ---------------------------------------------------------------------------

#[test]
fn target_dag_allowlist_is_strictly_lower_triangular() {
    const EXPECTED_RANK_ORDER: &[&str] = &[
        "id",
        "lease",
        "transport",
        "env",
        "media",
        "term",
        "platform",
        "files",
        "transfer",
        "config",
        "events",
        "tasks",
        "wait",
        "service",
        "skills",
        "render",
        "codemode",
        "ipc",
        "http_wire",
        "endpoint",
        "daemon",
        "mcp",
        "http",
        "cli",
    ];
    const DIAGONAL_BLOCKS: &[&[&str]] = &[
        &["id", "lease", "transport"],
        &["media", "term", "platform"],
        &["transfer", "config"],
        &["render", "codemode", "ipc"],
        &["http_wire", "endpoint"],
        &["daemon", "mcp"],
    ];

    let actual_order: Vec<&str> = TARGET_DAG_ALLOWLIST.iter().map(|&(m, _)| m).collect();
    assert_eq!(
        actual_order, EXPECTED_RANK_ORDER,
        "TARGET_DAG_ALLOWLIST must match the 24 Spec §2.1 modules in exact #1..#24 rank order"
    );

    let mut ranks: HashMap<&str, usize> = HashMap::new();
    let mut deps_by_mod: HashMap<&str, BTreeSet<&str>> = HashMap::new();
    for (idx, &(module, deps)) in TARGET_DAG_ALLOWLIST.iter().enumerate() {
        assert!(
            ranks.insert(module, idx).is_none(),
            "duplicate module `{module}` in TARGET_DAG_ALLOWLIST"
        );
        deps_by_mod.insert(module, deps.iter().copied().collect());
    }
    for &(u, deps) in TARGET_DAG_ALLOWLIST {
        let rank_u = ranks[u];
        for &v in deps {
            let Some(&rank_v) = ranks.get(v) else {
                panic!("`{u}` lists unknown target `{v}` in TARGET_DAG_ALLOWLIST");
            };
            assert!(
                rank_v < rank_u,
                "TARGET_DAG_ALLOWLIST violates strict lower-triangular order: `{u}` (#{}) -> `{v}` (#{})",
                rank_u + 1,
                rank_v + 1
            );
        }
    }
    for &block in DIAGONAL_BLOCKS {
        for &u in block {
            for &v in block {
                if u != v {
                    assert!(
                        !deps_by_mod[u].contains(v),
                        "diagonal orthogonal block {block:?} contains lateral edge `{u}` -> `{v}`"
                    );
                }
            }
        }
    }
}

fn top_level_module_bijection_violations(
    main_mods: &[DeclaredMod],
    src_tops: &BTreeSet<String>,
) -> Vec<String> {
    let expected: BTreeSet<&str> = TARGET_DAG_ALLOWLIST.iter().map(|&(m, _)| m).collect();
    let mut violations = Vec::new();
    let mut seen_in_main: BTreeMap<&str, usize> = BTreeMap::new();

    for decl in main_mods {
        if decl.is_inline {
            violations.push(format!(
                "  - src/main.rs:{} declares inline top-level module `mod {} {{ ... }}` (must be file module)",
                decl.line, decl.name
            ));
        }
        if let Some(&first_line) = seen_in_main.get(decl.name.as_str()) {
            violations.push(format!(
                "  - src/main.rs:{} duplicates `mod {};` (first declared at line {first_line})",
                decl.line, decl.name
            ));
        } else {
            seen_in_main.insert(decl.name.as_str(), decl.line);
        }
        if !expected.contains(decl.name.as_str()) {
            violations.push(format!(
                "  - src/main.rs:{} declares non-target top-level module `mod {};`",
                decl.line, decl.name
            ));
        }
    }

    for &exp in &expected {
        if !seen_in_main.contains_key(exp) {
            violations.push(format!(
                "  - src/main.rs is missing target top-level module `mod {exp};`"
            ));
        }
    }

    for discovered in src_tops {
        if !expected.contains(discovered.as_str()) {
            violations.push(format!(
                "  - src/ contains non-target top-level module `src/{discovered}`"
            ));
        }
    }

    for &exp in &expected {
        if !src_tops.contains(exp) {
            violations.push(format!(
                "  - src/ is missing target top-level module `{exp}`"
            ));
        }
    }

    violations
}

#[test]
fn main_rs_declares_exact_target_top_level_modules() {
    let report = scan_repo_src();
    let violations =
        top_level_module_bijection_violations(&report.main_declared_mods, &report.top_modules);
    assert!(
        violations.is_empty(),
        "top-level module declarations do not match the 24 target modules in TARGET_DAG_ALLOWLIST:\n{}",
        violations.join("\n")
    );
}

fn unauthorized_cross_module_edges(report: &ScanReport) -> Vec<String> {
    let allowed_modules: BTreeSet<&str> = TARGET_DAG_ALLOWLIST.iter().map(|&(m, _)| m).collect();
    let mut violations = Vec::new();
    for discovered in &report.top_modules {
        if !allowed_modules.contains(discovered.as_str()) {
            violations.push(format!(
                "  - unregistered top-level module `src/{discovered}` (not in TARGET_DAG_ALLOWLIST)"
            ));
        }
    }

    let mut allowed_edges: BTreeSet<(&str, &str)> = BTreeSet::new();
    for &(u, deps) in TARGET_DAG_ALLOWLIST {
        for &v in deps {
            allowed_edges.insert((u, v));
        }
    }

    let directed = top_level_directed_edges(report);
    for ((u, v), sites) in &directed {
        if !allowed_edges.contains(&(u.as_str(), v.as_str())) {
            violations.push(format!(
                "  - {u} -> {v} ({} site(s)): {}",
                sites.len(),
                sites.join(", ")
            ));
        }
    }
    violations
}

#[test]
fn every_cross_module_edge_belongs_to_target_dag_allowlist() {
    let report = scan_repo_src();
    let unauthorized = unauthorized_cross_module_edges(&report);
    assert!(
        unauthorized.is_empty(),
        "found {} unauthorized cross-top-module edge(s) outside TARGET_DAG_ALLOWLIST:\n{}",
        unauthorized.len(),
        unauthorized.join("\n")
    );
}

#[test]
fn top_level_two_cycles_are_zero() {
    let report = scan_repo_src();
    let directed = top_level_directed_edges(&report);
    let actual_cycles = compute_two_cycles(&directed);
    if !actual_cycles.is_empty() {
        let mut msg = format!("found {} top-level 2-cycle(s):\n", actual_cycles.len());
        for (a, b) in &actual_cycles {
            let ab = &directed[&(a.clone(), b.clone())];
            let ba = &directed[&(b.clone(), a.clone())];
            msg.push_str(&format!(
                "  - {a} <-> {b}:\n      {a} -> {b}: {}\n      {b} -> {a}: {}\n",
                ab.join(", "),
                ba.join(", ")
            ));
        }
        panic!("{msg}");
    }
}

fn top_level_cyclic_sccs(
    report: &ScanReport,
    directed: &BTreeMap<(String, String), Vec<String>>,
) -> Vec<Vec<String>> {
    let mut nodes: BTreeSet<String> = TARGET_DAG_ALLOWLIST
        .iter()
        .map(|&(m, _)| m.to_owned())
        .collect();
    nodes.extend(report.top_modules.iter().cloned());
    let mut adj: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (u, v) in directed.keys() {
        nodes.insert(u.clone());
        nodes.insert(v.clone());
        adj.entry(u.clone()).or_default().insert(v.clone());
    }
    tarjan_cyclic_sccs(&nodes, &adj)
}

#[test]
fn top_level_module_graph_is_strictly_acyclic() {
    let report = scan_repo_src();
    let directed = top_level_directed_edges(&report);
    let sccs = top_level_cyclic_sccs(&report, &directed);
    if !sccs.is_empty() {
        let mut msg = format!(
            "found {} cyclic strongly connected component(s) in top-level module graph:\n",
            sccs.len()
        );
        for scc in &sccs {
            let mut details = Vec::new();
            for u in scc {
                for v in scc {
                    if let Some(sites) = directed.get(&(u.clone(), v.clone())) {
                        details.push(format!("      {u} -> {v}: {}", sites.join(", ")));
                    }
                }
            }
            msg.push_str(&format!(
                "  - SCC {{{}}}:\n{}\n",
                scc.join(", "),
                details.join("\n")
            ));
        }
        panic!("{msg}");
    }
}

#[test]
fn cp1_forbidden_cross_layer_edges_are_zero() {
    let report = scan_repo_src();
    let directed = top_level_directed_edges(&report);
    let mut violations = Vec::new();
    for &(from, to) in CP1_FORBIDDEN_EDGES {
        if let Some(witnesses) = directed.get(&(from.to_owned(), to.to_owned())) {
            violations.push(format!(
                "  - {from} -> {to} ({} site(s)): {}",
                witnesses.len(),
                witnesses.join(", ")
            ));
        }
    }
    assert!(
        violations.is_empty(),
        "found {} forbidden cross-layer edge kind(s):\n{}",
        violations.len(),
        violations.join("\n")
    );
}

#[test]
fn submodule_graphs_have_zero_strongly_connected_cycles() {
    let report = scan_repo_src();
    let mut failures = Vec::new();
    let mut check = |top: &str, include_root: bool, kind: &str| {
        let graph = submodule_directed_edges(&report, top, include_root);
        for scc in tarjan_cyclic_sccs(&graph.nodes, &graph.adj) {
            let mut details = Vec::new();
            for u in &scc {
                for v in &scc {
                    if let Some(sites) = graph.witnesses.get(&(u.clone(), v.clone())) {
                        details.push(format!("      {u} -> {v}: {}", sites.join(", ")));
                    }
                }
            }
            failures.push(format!(
                "  - `{top}` {kind} SCC {{{}}}:\n{}",
                scc.join(", "),
                details.join("\n")
            ));
        }
    };

    for top in &report.top_modules {
        check(top, top != "tasks", "module-tree");
    }

    assert!(
        failures.is_empty(),
        "found {} submodule strongly connected cycle(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

fn tasks_submodule_contract_violations(report: &ScanReport) -> Vec<String> {
    let allowed_subs: BTreeSet<&str> = TASKS_SUBMODULES.iter().copied().collect();
    let allowed_sibling_edges: BTreeSet<(&str, &str)> =
        TASKS_ALLOWED_SIBLING_EDGES.iter().copied().collect();

    let mut known_subs: BTreeSet<&str> = allowed_subs.clone();
    let mut unregistered_subs: BTreeSet<String> = BTreeSet::new();
    if let Some(discovered) = report.file_submodules.get("tasks") {
        for sub in discovered {
            known_subs.insert(sub.as_str());
            if !allowed_subs.contains(sub.as_str()) {
                unregistered_subs.insert(sub.clone());
            }
        }
    }

    let mut bad_edges: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for edge in &report.edges {
        if edge.from_mod.first().map(String::as_str) != Some("tasks") {
            continue;
        }
        let Some(from_sub) = edge.from_mod.get(1).map(String::as_str) else {
            continue;
        };
        if !allowed_subs.contains(from_sub) {
            unregistered_subs.insert(from_sub.to_owned());
        }
        if edge.to_path.first().map(String::as_str) != Some("tasks") {
            continue;
        }

        let witness = format!(
            "{}:{} (`crate::{}`)",
            edge.file,
            edge.line,
            edge.to_path.join("::")
        );
        if let Some(to_sub) = edge
            .to_path
            .get(1)
            .map(String::as_str)
            .filter(|cand| known_subs.contains(cand))
        {
            if from_sub == to_sub {
                continue;
            }
            if !allowed_sibling_edges.contains(&(from_sub, to_sub)) {
                let list = bad_edges
                    .entry((format!("tasks::{from_sub}"), format!("tasks::{to_sub}")))
                    .or_default();
                if !list.contains(&witness) {
                    list.push(witness);
                }
            }
        } else if matches!(from_sub, "script" | "session") {
            let list = bad_edges
                .entry((format!("tasks::{from_sub}"), "tasks".to_owned()))
                .or_default();
            if !list.contains(&witness) {
                list.push(witness);
            }
        }
    }

    let mut violations = Vec::new();
    for sub in unregistered_subs {
        violations.push(format!(
            "  - unregistered `tasks` submodule `tasks::{sub}` (allowed: {{{}}})",
            TASKS_SUBMODULES.join(", ")
        ));
    }
    for ((u, v), sites) in bad_edges {
        violations.push(format!(
            "  - {u} -> {v} ({} site(s)): {}",
            sites.len(),
            sites.join(", ")
        ));
    }
    violations
}

#[test]
fn tasks_internal_submodule_contract_holds() {
    let report = scan_repo_src();
    let violations = tasks_submodule_contract_violations(&report);
    assert!(
        violations.is_empty(),
        "found {} `src/tasks/` internal submodule contract violation(s):\n{}",
        violations.len(),
        violations.join("\n")
    );
}

#[test]
fn cross_top_module_inherent_impls_are_zero() {
    let report = scan_repo_src();
    if !report.inherent_impls.is_empty() {
        let lines: Vec<String> = report
            .inherent_impls
            .iter()
            .map(|v| {
                format!(
                    "  - {}:{} (`{}` defines inherent `impl crate::{}` belonging to `{}`)",
                    v.file, v.line, v.from_top, v.target_path, v.target_top
                )
            })
            .collect();
        panic!(
            "found {} cross-top-module inherent impl block(s):\n{}",
            lines.len(),
            lines.join("\n")
        );
    }
}

#[test]
fn third_party_protocol_and_engine_crates_stay_isolated() {
    let report = scan_repo_src();
    if !report.crate_leaks.is_empty() {
        let lines: Vec<String> = report
            .crate_leaks
            .iter()
            .map(|leak| {
                format!(
                    "  - {}:{} (top module `{}` references `{}`)",
                    leak.file, leak.line, leak.from_top, leak.crate_name
                )
            })
            .collect();
        panic!(
            "found {} third-party crate isolation violation(s):\n{}",
            lines.len(),
            lines.join("\n")
        );
    }
}

fn format_allow_violations(report: &ScanReport) -> Vec<String> {
    report
        .allow_suppressions
        .iter()
        .map(|sup| {
            let bang = if sup.is_inner { "!" } else { "" };
            if sup.lints.is_empty() {
                format!("  - {}:{} (#{bang}[allow])", sup.file, sup.line)
            } else {
                format!(
                    "  - {}:{} (#{bang}[allow({})])",
                    sup.file, sup.line, sup.lints
                )
            }
        })
        .collect()
}

#[test]
fn non_test_production_code_has_zero_allow_suppressions() {
    let report = scan_repo_src();
    let violations = format_allow_violations(&report);
    assert!(
        violations.is_empty(),
        "found {} non-test `#[allow(...)]` suppression(s) in `src/`:\n{}",
        violations.len(),
        violations.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Parser, Skipper, Path & SCC Self-Tests (§7.2.1 - §7.2.5)
// ---------------------------------------------------------------------------

#[test]
fn lexer_strips_urls_strings_nested_comments_and_distinguishes_chars_from_lifetimes() {
    let src = r####"
        //! Module doc mentioning rmcp and crate::ipc::Fake
        /// Item doc mentioning hyper_util and "unclosed quote
        const URL: &str = "http://127.0.0.1:8080/mcp // not a comment /* crate::tasks::Fake";
        const BYTES: &[u8] = b"crate::daemon::Fake \"escaped\"";
        const CSTR: &core::ffi::CStr = c"crate::http::Fake \n";
        const RAW: &str = r##"raw "http://..." with "#" and crate::mcp::Fake"##;
        const BRAW: &[u8] = br#"crate::events::Fake"#;
        const CRAW: &core::ffi::CStr = cr#"crate::service::Fake"#;
        /* Outer block comment crate::ipc::Fake
           /* Nested block comment */ still inside outer comment: rmcp::Fake
        */
        fn chars_and_lifetimes<'a, 'static_lt>(x: &'a mut Holder<'_, 'static>) -> [char; 5] {
            let _ = crate::lease::BytePool::new(1);
            ['{', '\'', '\"', '\x7f', '\u{1F980}']
        }
    "####;

    let report = analyze_virtual_files(&[("src/media.rs", vec!["media".into()], src)]);
    assert!(
        report.crate_leaks.is_empty(),
        "unexpected crate leaks: {:?}",
        report.crate_leaks
    );
    let targets: Vec<Vec<String>> = report.edges.into_iter().map(|e| e.to_path).collect();
    assert_eq!(
        targets,
        vec![vec![
            "lease".to_owned(),
            "BytePool".to_owned(),
            "new".to_owned()
        ]]
    );
}

#[test]
fn cfg_three_valued_logic_skips_only_unsatisfiable_test_predicates() {
    let src = r#"
        #[cfg(test)]
        use crate::ipc::PureTestUse;

        #[cfg(all(test, unix, not(target_os = "macos")))]
        fn pure_test_unix() {
            let _ = crate::tasks::random_chars(8);
        }

        #[cfg(any(test, all(unix, test)))]
        mod pure_test_mod {
            use crate::daemon::Daemon;
        }

        #[cfg(not(test))]
        type ProdWriter = crate::transport::Client;

        #[cfg(any(windows, test))]
        fn cross_platform_helper() {
            let _ = crate::lease::process_pool();
        }

        #[cfg(any(target_os = "macos", test))]
        fn macos_or_test_helper() {
            let _ = crate::events::EventBus::new();
        }
    "#;

    let report = analyze_virtual_files(&[(
        "src/files/path.rs",
        vec!["files".into(), "path".into()],
        src,
    )]);
    let tops: BTreeSet<String> = report.edges.iter().map(|e| e.to_path[0].clone()).collect();
    assert_eq!(
        tops,
        BTreeSet::from([
            "events".to_owned(),
            "lease".to_owned(),
            "transport".to_owned(),
        ])
    );
}

#[test]
fn cfg_skipper_handles_generic_commas_let_statements_closure_if_crate_args_and_inner_cfg() {
    let src = r#"
        #[cfg(test)]
        extern crate alloc;

        #[cfg(test)]
        #[allow(clippy::type_complexity)]
        pub(super) async fn generic_comma_fn<W, T>(
            writer: &mut W,
            msg: &T,
        ) -> Result<(String, Vec<usize>, crate::ipc::Owned<u8>), String>
        where
            W: Unpin,
            T: Sized,
        {
            let _ = crate::tasks::random_chars(8);
            Err(String::new())
        }

        struct Container {
            kept: crate::lease::Lease,
            #[cfg(test)]
            hook_mid: Option<fn(usize, usize) -> Result<(), crate::daemon::Error>>,
            #[cfg(test)]
            hook_last: std::collections::HashMap<String, Option<Vec<crate::ipc::Owned<u8>>>>
        }

        pub mod outer_mod {
            mod first_inner_test_mod {
                #![cfg(test)]
                pub fn dropped_first() {
                    let _ = crate::http::dropped_one();
                }
            }

            mod second_inner_test_mod {
                #![cfg(test)]
                pub fn dropped_second() {
                    let _ = crate::mcp::dropped_two();
                }
            }

            pub fn kept_inside_outer() {
                let _ = crate::id::random_chars(4);
            }
        }

        pub fn prod_workflow() {
            #[cfg(test)]
            let _test_binding = crate::http::PUBLIC_URL;

            #[cfg(test)]
            let Some(_guard) = crate::mcp::check() else {
                let _ = crate::service::abort();
                return;
            };

            #[cfg(test)]
            if let Ok(exe) = std::env::current_exe().map(|d| {
                let _ = crate::ipc::in_condition_closure(&d);
                d
            }) {
                let _ = crate::tasks::in_if_body(exe);
            } else if unsafe { crate::codemode::in_unsafe_cond() } {
                let _ = crate::codemode::in_else_if();
            } else {
                let _ = crate::daemon::in_else();
            }

            #[cfg(test)]
            'probe: loop {
                let _ = crate::http::in_labeled_loop();
                break 'probe;
            }

            let _ = call_fn(
                #[cfg(test)]
                crate::ipc::pure_test_first_arg(4),
                crate::events::prod_second_arg(),
                #[cfg(test)]
                (1usize << 4)
            );
            let _ = crate::transport::after_all_skips();
        }
    "#;

    let report = analyze_virtual_files(&[("src/wait.rs", vec!["wait".into()], src)]);
    let tops: BTreeSet<String> = report.edges.iter().map(|e| e.to_path[0].clone()).collect();
    assert_eq!(
        tops,
        BTreeSet::from([
            "events".to_owned(),
            "id".to_owned(),
            "lease".to_owned(),
            "transport".to_owned(),
        ]),
        "pure-test items leaked or swallowed production code: {:?}",
        report.edges
    );
}

#[test]
fn paren_nested_local_use_is_captured_and_rust2024_precise_capturing_use_lt_is_ignored() {
    let src = r#"
        pub fn precise_capture<'a>(
            input: &'a str,
        ) -> impl core::future::Future<Output = usize> + use<'a> {
            let _ = crate::lease::BytePool::new(input.len());
            async move { input.len() }
        }

        pub fn spawn_background() {
            tokio::spawn(async move {
                let _ = daemon::run();
                let _ = Owned::new(1u8);
                use crate::{daemon, ipc::Owned};
            });
        }
    "#;

    let report = analyze_virtual_files(&[("src/endpoint.rs", vec!["endpoint".into()], src)]);
    let tops: BTreeSet<String> = report.edges.iter().map(|e| e.to_path[0].clone()).collect();
    assert_eq!(
        tops,
        BTreeSet::from(["daemon".to_owned(), "ipc".to_owned(), "lease".to_owned()]),
        "got edges: {:?}",
        report.edges
    );
    assert!(
        report
            .edges
            .iter()
            .any(|e| e.to_path == vec!["daemon", "run"]),
        "forward call before block-local `use` inside `tokio::spawn(...)` did not resolve: {:?}",
        report.edges
    );
}

#[test]
fn allowlist_gate_rejects_upward_diagonal_and_three_cycle_violations() {
    let report = analyze_virtual_files(&[
        // Upward violation (#1 id -> #22 http)
        (
            "src/id.rs",
            vec!["id".into()],
            "pub fn bad() { let _ = crate::http::serve(); }",
        ),
        // Upward violation (#2 lease -> #12 tasks)
        (
            "src/lease.rs",
            vec!["lease".into()],
            "pub fn bad() { let _ = crate::tasks::Limits; }",
        ),
        // Diagonal S1b breach (#5 media -> #6 term)
        (
            "src/media.rs",
            vec!["media".into()],
            "pub fn bad() { let _ = crate::term::keys::Modes; }",
        ),
        // Diagonal S1c breach (#10 config -> #9 transfer)
        (
            "src/config.rs",
            vec!["config".into()],
            "pub fn bad() { let _ = crate::transfer::Transfers; }",
        ),
        // Three-cycle (`events -> wait -> tasks -> events`, where `events -> wait` is illegal)
        (
            "src/events.rs",
            vec!["events".into()],
            "pub fn c1() { let _ = crate::wait::WaitEnd; }",
        ),
        (
            "src/wait.rs",
            vec!["wait".into()],
            "pub fn c2() { let _ = crate::tasks::Tasks; }",
        ),
        (
            "src/tasks.rs",
            vec!["tasks".into()],
            "pub fn c3() { let _ = crate::events::EventBus; }",
        ),
    ]);

    let violations = unauthorized_cross_module_edges(&report);
    assert_eq!(violations.len(), 5, "got: {violations:?}");
    assert!(
        violations
            .iter()
            .any(|v| v.contains("id -> http") && v.contains("src/id.rs:1"))
    );
    assert!(
        violations
            .iter()
            .any(|v| v.contains("lease -> tasks") && v.contains("src/lease.rs:1"))
    );
    assert!(
        violations
            .iter()
            .any(|v| v.contains("media -> term") && v.contains("src/media.rs:1"))
    );
    assert!(
        violations
            .iter()
            .any(|v| v.contains("config -> transfer") && v.contains("src/config.rs:1"))
    );
    assert!(
        violations
            .iter()
            .any(|v| v.contains("events -> wait") && v.contains("src/events.rs:1"))
    );
}

#[test]
fn path_canonicalizer_resolves_nested_use_trees_super_self_inline_mods_and_bare_submodules() {
    let parent_src = r#"
        pub(crate) mod child;
        #[cfg(test)]
        mod tests;

        use self::child::Helper;
        use crate::{
            lease::{self as mem, BytePool},
            events::{EventBus as Bus, *},
        };

        pub fn run() {
            let _ = child::invoke();
            let _ = Helper::new();
            let _ = mem::json_len(&1u8);
            let _ = BytePool::new(8);
            let _ = Bus::new();
        }
    "#;

    let child_src = r#"
        use super::Helper;

        pub mod nested {
            use super::super::child;
            pub fn call_up() {
                let _ = super::super::super::transport::connect();
                let _ = child::invoke();
            }
        }

        pub fn invoke() {
            let _ = Helper::new();
            let _ = nested::call_up();
            let _ = super::super::config::load();
        }
    "#;

    let report = analyze_virtual_files(&[
        ("src/alpha.rs", vec!["alpha".into()], parent_src),
        (
            "src/alpha/child.rs",
            vec!["alpha".into(), "child".into()],
            child_src,
        ),
    ]);

    let directed = top_level_directed_edges(&report);
    let edge_pairs: BTreeSet<(&str, &str)> = directed
        .keys()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    assert_eq!(
        edge_pairs,
        BTreeSet::from([
            ("alpha", "config"),
            ("alpha", "events"),
            ("alpha", "lease"),
            ("alpha", "transport"),
        ])
    );

    assert!(
        report
            .edges
            .iter()
            .any(|e| e.to_path == vec!["alpha", "child", "invoke"])
    );
}

#[test]
fn tarjan_scc_detects_three_node_submodule_cycles_and_passes_dags() {
    let root_src = r#"
        pub mod a;
        pub mod b;
        pub mod c;
        pub mod leaf;
    "#;
    let a_src = "pub fn a() { let _ = super::b::b(); let _ = super::leaf::ok(); }";
    let b_src = "pub fn b() { let _ = super::c::c(); }";
    let c_src = "pub fn c() { let _ = super::a::a(); }";
    let leaf_src = "pub fn ok() {}";

    let report = analyze_virtual_files(&[
        ("src/pkg.rs", vec!["pkg".into()], root_src),
        ("src/pkg/a.rs", vec!["pkg".into(), "a".into()], a_src),
        ("src/pkg/b.rs", vec!["pkg".into(), "b".into()], b_src),
        ("src/pkg/c.rs", vec!["pkg".into(), "c".into()], c_src),
        (
            "src/pkg/leaf.rs",
            vec!["pkg".into(), "leaf".into()],
            leaf_src,
        ),
    ]);

    let graph = submodule_directed_edges(&report, "pkg", true);
    let cycles = tarjan_cyclic_sccs(&graph.nodes, &graph.adj);
    assert_eq!(
        cycles,
        vec![vec![
            "pkg::a".to_owned(),
            "pkg::b".to_owned(),
            "pkg::c".to_owned()
        ]]
    );
}

#[test]
fn inherent_impl_detector_flags_direct_imported_and_ref_foreign_types_without_false_positives() {
    let session_src = r#"
        pub struct LaunchContext;
        pub struct GlobImportedType;
        pub struct LocalCollision;
    "#;

    let service_root_src = r#"
        pub mod render;
        pub struct Service;
    "#;

    let service_render_src = r#"
        use super::Service;
        impl Service {
            pub fn same_top_module_is_allowed(&self) {}
        }
    "#;

    let ipc_src = r#"
        use crate::session::LaunchContext as ImportedCtx;
        use crate::session::*;
        type TypeAliasedCtx = crate::session::LaunchContext;

        pub struct LocalWire;
        pub struct LocalCollision;

        fn block_local_import_does_not_taint_outer_impl() {
            use crate::session::LocalCollision;
            let _ = LocalCollision;
        }

        impl LocalCollision {
            pub fn local_is_allowed(&self) {}
        }

        impl LocalWire {
            pub fn ok() -> impl core::future::Future<Output = crate::session::LaunchContext> {
                async { crate::session::LaunchContext }
            }
        }

        impl From<ImportedCtx> for LocalWire {
            fn from(_: ImportedCtx) -> Self {
                Self
            }
        }

        impl crate::session::LaunchContext {
            pub fn direct_foreign() {}
        }

        impl<'a> ImportedCtx {
            pub fn aliased_foreign() {}
        }

        impl TypeAliasedCtx {
            pub fn type_aliased_foreign() {}
        }

        impl GlobImportedType {
            pub fn glob_foreign() {}
        }

        impl &crate::session::LaunchContext {
            pub fn shared_ref_foreign() {}
        }

        impl<'a> &'a mut ImportedCtx {
            pub fn mut_ref_foreign() {}
        }
    "#;

    let report = analyze_virtual_files(&[
        ("src/session.rs", vec!["session".into()], session_src),
        ("src/service.rs", vec!["service".into()], service_root_src),
        (
            "src/service/render.rs",
            vec!["service".into(), "render".into()],
            service_render_src,
        ),
        ("src/ipc.rs", vec!["ipc".into()], ipc_src),
    ]);

    assert_eq!(
        report.inherent_impls.len(),
        6,
        "got: {:?}",
        report.inherent_impls
    );
    for idx in [0, 1, 2, 4, 5] {
        assert_eq!(
            report.inherent_impls[idx].target_path,
            "session::LaunchContext"
        );
    }
    assert_eq!(
        report.inherent_impls[3].target_path,
        "session::GlobImportedType"
    );
}

#[test]
fn tasks_submodule_contract_accepts_target_dag_and_rejects_script_session_and_state_violations() {
    let tasks_root = r#"
        pub mod script;
        pub mod state;
        pub mod session;
        mod workers;

        pub struct Tasks;
        impl Tasks {
            pub fn touch() {
                let _ = self::script::ScriptControl;
                let _ = self::state::Task;
                let _ = self::workers::OutputWorkers;
                let _ = self::session::Session;
            }
        }
    "#;
    let valid_script = r#"
        pub struct ScriptControl;
        pub fn id_and_lease() {
            let _ = crate::id::random_chars(8);
            let _ = crate::lease::BytePool::new(1);
        }
    "#;
    let valid_state = r#"
        use super::Tasks;
        use super::script::ScriptControl;
        pub struct Task;
        pub struct ReadState;
        pub fn check(_: &Tasks, _: &ScriptControl) {}
    "#;
    let valid_workers = r#"
        use super::Tasks;
        use super::state::Task;
        pub struct OutputWorkers;
        pub fn run(_: &Tasks, _: &Task) {}
    "#;
    let valid_session = r#"
        use super::state::{ReadState, Task};
        pub struct Session;
        pub fn cursor(_: &ReadState, _: &Task) {}
    "#;

    let clean_report = analyze_virtual_files(&[
        ("src/tasks.rs", vec!["tasks".into()], tasks_root),
        (
            "src/tasks/script.rs",
            vec!["tasks".into(), "script".into()],
            valid_script,
        ),
        (
            "src/tasks/state.rs",
            vec!["tasks".into(), "state".into()],
            valid_state,
        ),
        (
            "src/tasks/workers.rs",
            vec!["tasks".into(), "workers".into()],
            valid_workers,
        ),
        (
            "src/tasks/session.rs",
            vec!["tasks".into(), "session".into()],
            valid_session,
        ),
    ]);
    let clean_violations = tasks_submodule_contract_violations(&clean_report);
    assert!(
        clean_violations.is_empty(),
        "valid tasks submodule DAG unexpectedly failed: {clean_violations:?}"
    );

    let bad_report = analyze_virtual_files(&[
        ("src/tasks.rs", vec!["tasks".into()], tasks_root),
        (
            "src/tasks/script.rs",
            vec!["tasks".into(), "script".into()],
            "pub fn bad() { let _ = super::state::Task; let _ = super::Tasks; }",
        ),
        (
            "src/tasks/state.rs",
            vec!["tasks".into(), "state".into()],
            "pub struct Task; pub fn bad() { let _ = super::workers::OutputWorkers; }",
        ),
        (
            "src/tasks/workers.rs",
            vec!["tasks".into(), "workers".into()],
            "pub struct OutputWorkers; pub fn bad() { let _ = super::script::ScriptControl; }",
        ),
        (
            "src/tasks/session.rs",
            vec!["tasks".into(), "session".into()],
            "pub fn bad() { let _ = super::Tasks; let _ = super::workers::OutputWorkers; }",
        ),
        (
            "src/tasks/extra.rs",
            vec!["tasks".into(), "extra".into()],
            "pub fn rogue() {}",
        ),
    ]);
    let violations = tasks_submodule_contract_violations(&bad_report);
    assert_eq!(violations.len(), 7, "got: {violations:?}");
    assert!(
        violations
            .iter()
            .any(|v| v.contains("unregistered `tasks` submodule `tasks::extra`"))
    );
    assert!(violations.iter().any(|v| {
        v.contains("tasks::script -> tasks::state")
            && v.contains("src/tasks/script.rs:1 (`crate::tasks::state::Task`)")
    }));
    assert!(violations.iter().any(|v| {
        v.contains("tasks::script -> tasks (")
            && v.contains("src/tasks/script.rs:1 (`crate::tasks::Tasks`)")
    }));
    assert!(violations.iter().any(|v| {
        v.contains("tasks::session -> tasks (")
            && v.contains("src/tasks/session.rs:1 (`crate::tasks::Tasks`)")
    }));
    assert!(violations.iter().any(|v| {
        v.contains("tasks::session -> tasks::workers")
            && v.contains("src/tasks/session.rs:1 (`crate::tasks::workers::OutputWorkers`)")
    }));
    assert!(violations.iter().any(|v| {
        v.contains("tasks::state -> tasks::workers")
            && v.contains("src/tasks/state.rs:1 (`crate::tasks::workers::OutputWorkers`)")
    }));
    assert!(violations.iter().any(|v| {
        v.contains("tasks::workers -> tasks::script")
            && v.contains("src/tasks/workers.rs:1 (`crate::tasks::script::ScriptControl`)")
    }));
}

#[test]
fn third_party_crate_isolation_enforces_final_rmcp_hyper_and_rquickjs_boundaries() {
    let report = analyze_virtual_files(&[
        (
            "src/files.rs",
            vec!["files".into()],
            "pub fn bad() -> rmcp::model::CallToolResult { todo!() }",
        ),
        (
            "src/transfer.rs",
            vec!["transfer".into()],
            "pub fn bad() -> rmcp::model::CallToolResult { todo!() }",
        ),
        (
            "src/service.rs",
            vec!["service".into()],
            "pub fn bad() -> (rmcp::model::CallToolResult, rquickjs::Error) { todo!() }",
        ),
        (
            "src/ingress.rs",
            vec!["ingress".into()],
            "pub fn bad() -> rmcp::model::RequestId { todo!() }",
        ),
        (
            "src/http_wire.rs",
            vec!["http_wire".into()],
            "pub fn wire(_: hyper::Version, _: hyper_util::rt::TokioExecutor, _: http_body_util::Full<bytes::Bytes>, _: &dyn tower_service::Service<()>) -> rmcp::ErrorData { todo!() }",
        ),
        (
            "src/http.rs",
            vec!["http".into()],
            "pub fn srv(_: http_body_util::Limited<()>, _: &dyn tower_service::Service<()>) -> rmcp::ErrorData { todo!() }",
        ),
        (
            "src/term/terminal.rs",
            vec!["term".into(), "terminal".into()],
            "pub fn vt(_: alacritty_terminal::index::Line) {}",
        ),
        (
            "src/daemon.rs",
            vec!["daemon".into()],
            "pub fn bad(_: http_body_util::Full<bytes::Bytes>, _: &dyn tower_service::Service<()>) {}",
        ),
        (
            "src/cli/stdio.rs",
            vec!["cli".into(), "stdio".into()],
            "pub fn bad(_: http_body_util::Empty<bytes::Bytes>, _: &dyn tower_service::Service<()>) {}",
        ),
        (
            "src/tasks.rs",
            vec!["tasks".into()],
            "pub fn bad(_: alacritty_terminal::index::Column) {}",
        ),
        (
            "src/wait.rs",
            vec!["wait".into()],
            "pub fn bad(_: alacritty_terminal::grid::Scroll) {}",
        ),
        (
            "src/config.rs",
            vec!["config".into()],
            "pub fn bad() -> Result<(), serde_saphyr::Error> { Ok(()) }",
        ),
        (
            "src/skills.rs",
            vec!["skills".into()],
            "pub fn ok() -> serde_saphyr::Options { todo!() }",
        ),
        (
            "src/mcp/ingress.rs",
            vec!["mcp".into(), "ingress".into()],
            "pub fn ok() -> rmcp::model::RequestId { todo!() }",
        ),
        (
            "src/render.rs",
            vec!["render".into()],
            "pub fn ok() -> rmcp::model::CallToolResult { todo!() }",
        ),
        (
            "src/codemode.rs",
            vec!["codemode".into()],
            "pub fn ok(_: rquickjs::Runtime) {}",
        ),
    ]);
    let leaks: Vec<(&str, &str)> = report
        .crate_leaks
        .iter()
        .map(|l| (l.from_top.as_str(), l.crate_name.as_str()))
        .collect();
    assert_eq!(
        leaks,
        vec![
            ("files", "rmcp"),
            ("transfer", "rmcp"),
            ("service", "rmcp"),
            ("service", "rquickjs"),
            ("ingress", "rmcp"),
            ("http_wire", "rmcp"),
            ("daemon", "http_body_util"),
            ("daemon", "tower_service"),
            ("cli", "http_body_util"),
            ("cli", "tower_service"),
            ("tasks", "alacritty_terminal"),
            ("wait", "alacritty_terminal"),
            ("config", "serde_saphyr"),
        ]
    );
}

#[test]
fn top_level_module_bijection_gate_rejects_missing_extra_duplicate_and_inline_modules() {
    let mut full_main = String::new();
    let mut full_tops = BTreeSet::new();
    for &(name, _) in TARGET_DAG_ALLOWLIST {
        full_main.push_str(&format!("mod {name};\n"));
        full_tops.insert(name.to_owned());
    }
    full_main.push_str("#[cfg(test)]\nmod tests;\nfn main() { mod inner_fn_local {} }\n");
    let clean_decls = extract_top_level_mods(&strip_pure_test_code(&lex(&full_main)));
    assert!(
        top_level_module_bijection_violations(&clean_decls, &full_tops).is_empty(),
        "exact 24-module bijection unexpectedly failed"
    );

    let mut bad_main = full_main
        .replace("mod http_wire;\n", "")
        .replace("mod wait;\n", "mod wait {}\n");
    bad_main.push_str("mod id;\nmod session;\nmod ingress;\n");
    let mut bad_tops = full_tops;
    bad_tops.remove("http_wire");
    bad_tops.insert("ingress".to_owned());

    let bad_decls = extract_top_level_mods(&strip_pure_test_code(&lex(&bad_main)));
    let violations = top_level_module_bijection_violations(&bad_decls, &bad_tops);
    assert_eq!(violations.len(), 7, "got: {violations:?}");
    assert!(
        violations
            .iter()
            .any(|v| v.contains("declares inline top-level module `mod wait { ... }`"))
    );
    assert!(
        violations
            .iter()
            .any(|v| v.contains("duplicates `mod id;`"))
    );
    assert!(
        violations
            .iter()
            .any(|v| v.contains("declares non-target top-level module `mod session;`"))
    );
    assert!(
        violations
            .iter()
            .any(|v| v.contains("declares non-target top-level module `mod ingress;`"))
    );
    assert!(
        violations
            .iter()
            .any(|v| v.contains("src/main.rs is missing target top-level module `mod http_wire;`"))
    );
    assert!(
        violations
            .iter()
            .any(|v| v.contains("src/ contains non-target top-level module `src/ingress`"))
    );
    assert!(
        violations
            .iter()
            .any(|v| v.contains("src/ is missing target top-level module `http_wire`"))
    );
}

#[test]
fn top_level_tarjan_scc_and_mcp_ingress_submodule_tree_detect_cycles() {
    let three_cycle = analyze_virtual_files(&[
        (
            "src/id.rs",
            vec!["id".into()],
            "pub fn a() { let _ = crate::lease::b(); }",
        ),
        (
            "src/lease.rs",
            vec!["lease".into()],
            "pub fn b() { let _ = crate::transport::c(); }",
        ),
        (
            "src/transport.rs",
            vec!["transport".into()],
            "pub fn c() { let _ = crate::id::a(); }",
        ),
    ]);
    let directed = top_level_directed_edges(&three_cycle);
    assert!(compute_two_cycles(&directed).is_empty());
    let sccs = top_level_cyclic_sccs(&three_cycle, &directed);
    assert_eq!(
        sccs,
        vec![vec![
            "id".to_owned(),
            "lease".to_owned(),
            "transport".to_owned(),
        ]]
    );

    let clean_mcp = analyze_virtual_files(&[
        (
            "src/mcp.rs",
            vec!["mcp".into()],
            "pub(crate) mod ingress; pub struct Server; pub fn run() { let _ = ingress::Buffer; }",
        ),
        (
            "src/mcp/ingress.rs",
            vec!["mcp".into(), "ingress".into()],
            "pub struct Buffer; pub fn ok() { let _ = crate::lease::BytePool::new(1); }",
        ),
    ]);
    let clean_mcp_graph = submodule_directed_edges(&clean_mcp, "mcp", true);
    assert!(tarjan_cyclic_sccs(&clean_mcp_graph.nodes, &clean_mcp_graph.adj).is_empty());

    let cyclic_mcp = analyze_virtual_files(&[
        (
            "src/mcp.rs",
            vec!["mcp".into()],
            "pub(crate) mod ingress; pub struct Server; pub fn run() { let _ = ingress::Buffer; }",
        ),
        (
            "src/mcp/ingress.rs",
            vec!["mcp".into(), "ingress".into()],
            "pub struct Buffer; pub fn bad() { let _ = super::Server; }",
        ),
    ]);
    let cyclic_mcp_graph = submodule_directed_edges(&cyclic_mcp, "mcp", true);
    assert_eq!(
        tarjan_cyclic_sccs(&cyclic_mcp_graph.nodes, &cyclic_mcp_graph.adj),
        vec![vec!["mcp".to_owned(), "mcp::ingress".to_owned()]]
    );
}

#[test]
fn inline_and_nested_modules_remain_distinct_in_cycle_detection() {
    let render_root = r#"
        pub mod files;
        pub struct Shared;
        pub fn run() { detail::call_sibling(); }

        mod detail {
            pub fn call_sibling() {
                let _ = super::files::entry();
            }
        }
    "#;
    let render_files = r#"
        pub fn entry() {
            let _ = super::Shared;
        }
    "#;

    let cyclic_report = analyze_virtual_files(&[
        ("src/render.rs", vec!["render".into()], render_root),
        (
            "src/render/files.rs",
            vec!["render".into(), "files".into()],
            render_files,
        ),
    ]);
    let full_graph = submodule_directed_edges(&cyclic_report, "render", true);
    assert_eq!(
        tarjan_cyclic_sccs(&full_graph.nodes, &full_graph.adj),
        vec![vec![
            "render".to_owned(),
            "render::detail".to_owned(),
            "render::files".to_owned(),
        ]]
    );
    let sibling_only = submodule_directed_edges(&cyclic_report, "render", false);
    assert!(tarjan_cyclic_sccs(&sibling_only.nodes, &sibling_only.adj).is_empty());

    let pure_test_inline_root = r#"
        pub mod files;
        pub struct Shared;

        #[cfg(test)]
        mod detail {
            pub fn call_sibling() {
                let _ = super::files::entry();
            }
        }
    "#;
    let clean_report = analyze_virtual_files(&[
        (
            "src/render.rs",
            vec!["render".into()],
            pure_test_inline_root,
        ),
        (
            "src/render/files.rs",
            vec!["render".into(), "files".into()],
            render_files,
        ),
    ]);
    let clean_graph = submodule_directed_edges(&clean_report, "render", true);
    assert!(tarjan_cyclic_sccs(&clean_graph.nodes, &clean_graph.adj).is_empty());
}

#[test]
fn nested_file_and_inline_module_cycles_are_checked_without_group_exclusions() {
    for top in ["files", "platform", "codemode", "daemon"] {
        let root = "mod outer;";
        let outer = r#"
            mod nested;
            pub struct Shared;
            pub fn run() { nested::back(); }
            mod left { pub fn run() { super::right::run(); } }
            mod right { pub fn run() { super::left::run(); } }
        "#;
        let nested = "pub fn back() { let _ = super::Shared; }";
        let report = analyze_virtual_files(&[
            ("src/root.rs", vec![top.into()], root),
            ("src/root/outer.rs", vec![top.into(), "outer".into()], outer),
            (
                "src/root/outer/nested.rs",
                vec![top.into(), "outer".into(), "nested".into()],
                nested,
            ),
        ]);
        let graph = submodule_directed_edges(&report, top, true);
        assert_eq!(
            tarjan_cyclic_sccs(&graph.nodes, &graph.adj),
            vec![
                vec![format!("{top}::outer"), format!("{top}::outer::nested")],
                vec![
                    format!("{top}::outer::left"),
                    format!("{top}::outer::right")
                ],
            ]
        );
    }
}

#[test]
fn inherent_impl_detector_tracks_generic_targets_and_distinguishes_trait_impls() {
    let report = analyze_virtual_files(&[
        (
            "src/lease.rs",
            vec!["lease".into()],
            "pub struct Owned<T>(T); pub struct Flag<const B: bool>;",
        ),
        (
            "src/files.rs",
            vec!["files".into()],
            r#"
            use crate::lease::Owned as Imported;
            struct Local;
            trait Trait<T> {}
            impl<T> crate::lease::Owned<T> {}
            impl<T> Imported<T> {}
            impl crate::lease::Owned<Vec<u8>> {}
            impl crate::lease::Flag<{ 2 > 1 }> {}
            impl<T> Trait<T> for Local {}
            impl<T> Trait<Vec<T>> for crate::lease::Owned<T> {}
        "#,
        ),
    ]);
    let targets: Vec<&str> = report
        .inherent_impls
        .iter()
        .map(|violation| violation.target_path.as_str())
        .collect();
    assert_eq!(
        targets,
        [
            "lease::Owned",
            "lease::Owned",
            "lease::Owned",
            "lease::Flag"
        ]
    );
}

#[test]
fn allow_suppression_gate_flags_production_and_cfg_platform_allows_while_ignoring_pure_tests() {
    let clean_src = r##"
        //! Comments with #[allow(dead_code)] and strings do not trigger.
        const LITERAL: &str = "#[allow(clippy::too_many_arguments)]";

        #[expect(dead_code, reason = "explicit expect is permitted")]
        struct Guarded(u8);

        #[cfg(test)]
        #[allow(clippy::too_many_arguments)]
        fn test_first_order() {}

        #[allow(clippy::type_complexity)]
        #[cfg(all(test, unix))]
        fn allow_before_cfg_test() {}

        #[cfg_attr(all(test, windows), allow(dead_code))]
        fn pure_test_cfg_attr() {}

        mod inner_tests {
            #![allow(dead_code)]
            #![cfg(test)]
            fn helper() {}
        }
    "##;
    let clean_report = analyze_virtual_files(&[("src/clean.rs", vec!["clean".into()], clean_src)]);
    assert!(
        clean_report.allow_suppressions.is_empty(),
        "unexpected suppressions on clean source: {:?}",
        clean_report.allow_suppressions
    );

    let bad_mod_src = r#"
        #![allow(missing_docs)]

        #[allow(clippy::too_many_arguments)]
        pub fn prod_fn() {}

        #[cfg(windows)]
        #[allow(dead_code)]
        fn windows_prod_fn() {}

        #[cfg_attr(target_os = "macos", allow(clippy::type_complexity))]
        fn macos_prod_fn() {}

        #[cfg(any(test, unix))]
        #[allow(unused_variables)]
        fn mixed_cfg_still_compiles_in_prod() {}
    "#;
    let bad_report =
        analyze_virtual_files(&[("src/service.rs", vec!["service".into()], bad_mod_src)]);
    let violations = format_allow_violations(&bad_report);
    assert_eq!(violations.len(), 5, "got: {violations:?}");
    assert!(violations[0].contains("src/service.rs:2 (#![allow(missing_docs)])"));
    assert!(violations[1].contains("src/service.rs:4 (#[allow(clippy::too_many_arguments)])"));
    assert!(violations[2].contains("src/service.rs:8 (#[allow(dead_code)])"));
    assert!(violations[3].contains("src/service.rs:11 (#[allow(clippy::type_complexity)])"));
    assert!(violations[4].contains("src/service.rs:15 (#[allow(unused_variables)])"));
}
