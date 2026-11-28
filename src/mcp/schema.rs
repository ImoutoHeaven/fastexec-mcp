use crate::codemode;
use crate::env::LaunchContext;
use crate::files;
use crate::lease;
use rmcp::model::ToolAnnotations;
use std::sync::Arc;

// Keep the advertised schema while returning invalid tool arguments as tool errors.
#[derive(serde::Deserialize)]
#[serde(transparent)]
pub(super) struct CodemodeArguments(pub(super) serde_json::Value);

impl schemars::JsonSchema for CodemodeArguments {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        codemode::control::Request::schema_name()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        codemode::control::Request::json_schema(generator)
    }
}

/// The `exec` tool's description: the static text, the Windows paragraph on Windows, and one
/// line saying where this server's tasks run. Nothing in it changes while the server runs.
pub(super) fn exec_description(launch: &LaunchContext, codemode: bool) -> String {
    let mut description = include_str!("../description.md").trim_end().to_owned();
    if cfg!(windows) {
        description.push_str("\n\n");
        description.push_str(include_str!("../description-windows.md").trim_end());
    }
    if codemode {
        description.push_str("\n\npoll, kill, and list also take codemode tasks. A codemode task takes no input, eof, keys,\nresize, screen, raw, encoding, or transcript; killing it cancels only the calls the script is\nawaiting, and the tasks it started keep running.");
    }
    description.push_str("\n\nHost: ");
    description.push_str(&host_line(launch));
    description
}

/// `os arch · hostname · user <user> · home <home> · default cwd <cwd>`, with `unknown` for a
/// value the launch environment does not hold.
pub(crate) fn host_line(launch: &LaunchContext) -> String {
    let value = |name: &str| {
        launch
            .var(name)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string_lossy().into_owned())
    };
    let hostname = hostname(launch).unwrap_or_else(|| "unknown".to_owned());
    let user = value("USER")
        .or_else(|| value("USERNAME"))
        .unwrap_or_else(|| "unknown".to_owned());
    let home = files::path::resolve_home(launch).map_or_else(
        || "unknown".to_owned(),
        |home| files::path::display_path(&home),
    );
    format!(
        "{} {} · {hostname} · user {user} · home {home} · default cwd {}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        files::path::display_path(&launch.cwd)
    )
}

/// This host's name: the launch environment's `COMPUTERNAME` on Windows, the kernel's answer on
/// Unix.
#[cfg(windows)]
fn hostname(launch: &LaunchContext) -> Option<String> {
    launch
        .var("COMPUTERNAME")
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string_lossy().into_owned())
}

#[cfg(unix)]
fn hostname(launch: &LaunchContext) -> Option<String> {
    let _ = launch;
    let mut name = [0u8; 256];
    // SAFETY: `name` is writable for the length passed, as `gethostname` requires.
    let answer = unsafe { libc::gethostname(name.as_mut_ptr().cast(), name.len()) };
    if answer != 0 {
        return None;
    }
    let end = name.iter().position(|byte| *byte == 0)?;
    String::from_utf8(name[..end].to_vec()).ok()
}

pub(super) fn prepare_tool(mut attr: rmcp::model::Tool, yolo: bool) -> rmcp::model::Tool {
    let raw = serde_json::Value::Object((*attr.input_schema).clone());
    if let serde_json::Value::Object(portable) = portable_schema(raw) {
        attr.input_schema = Arc::new(portable);
    }
    if yolo {
        let title = attr
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.title.clone());
        attr.annotations = Some(ToolAnnotations::from_raw(
            title,
            Some(true),
            Some(false),
            Some(false),
            Some(false),
        ));
    }
    attr
}

fn is_null_schema(value: &serde_json::Value) -> bool {
    value
        .as_object()
        .is_some_and(|obj| obj.len() == 1 && obj.get("type") == Some(&serde_json::json!("null")))
}

/// Rewrites a schemars-generated JSON Schema into the portable subset understood across MCP
/// clients: no `$ref`, `$defs`, `$schema`, `anyOf`, `oneOf`, array `type`, or non-standard `format`.
pub(crate) fn portable_schema(mut schema: serde_json::Value) -> serde_json::Value {
    let defs = schema
        .as_object_mut()
        .and_then(|root| {
            root.remove("$schema");
            root.remove("$defs")
        })
        .and_then(|value| match value {
            serde_json::Value::Object(map) => Some(map),
            _ => None,
        })
        .unwrap_or_default();
    rewrite_schema_node(schema, &defs)
}

fn rewrite_schema_node(
    node: serde_json::Value,
    defs: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    let serde_json::Value::Object(mut obj) = node else {
        return node;
    };
    obj.remove("$schema");

    loop {
        let mut progressed = false;
        if let Some(serde_json::Value::String(reference)) = obj.remove("$ref") {
            let name = reference.strip_prefix("#/$defs/").unwrap_or(&reference);
            if let Some(serde_json::Value::Object(mut target)) = defs.get(name).cloned() {
                if obj.contains_key("description") {
                    target.remove("description");
                }
                for (key, value) in target {
                    obj.entry(key).or_insert(value);
                }
                progressed = true;
            }
        }
        if let Some(serde_json::Value::Array(items)) = obj.get("anyOf")
            && items.len() == 2
            && items.iter().any(is_null_schema)
            && let Some(non_null) = items.iter().find(|item| !is_null_schema(item)).cloned()
        {
            obj.remove("anyOf");
            if let serde_json::Value::Object(mut inner) = non_null {
                if obj.contains_key("description") {
                    inner.remove("description");
                }
                for (key, value) in inner {
                    obj.entry(key).or_insert(value);
                }
            }
            progressed = true;
        }
        if !progressed {
            break;
        }
    }

    if let Some(serde_json::Value::Array(variants)) = obj.get("oneOf")
        && !variants.is_empty()
        && variants
            .iter()
            .all(|v| v.get("const").and_then(serde_json::Value::as_str).is_some())
    {
        let Some(serde_json::Value::Array(variants)) = obj.remove("oneOf") else {
            unreachable!();
        };
        let mut values = Vec::with_capacity(variants.len());
        let mut notes = Vec::new();
        for variant in variants {
            let val = variant["const"].as_str().unwrap().to_owned();
            if let Some(desc) = variant
                .get("description")
                .and_then(serde_json::Value::as_str)
            {
                notes.push(format!("{val}: {desc}"));
            }
            values.push(serde_json::Value::String(val));
        }
        obj.insert("type".into(), serde_json::Value::String("string".into()));
        obj.insert("enum".into(), serde_json::Value::Array(values));
        if !notes.is_empty() {
            let suffix = notes.join(" ");
            let merged = match obj.get("description").and_then(serde_json::Value::as_str) {
                Some(existing) if !existing.is_empty() => format!("{existing} {suffix}"),
                _ => suffix,
            };
            obj.insert("description".into(), serde_json::Value::String(merged));
        }
    }

    if let Some(serde_json::Value::Array(types)) = obj.get("type")
        && types.len() == 2
        && types.iter().any(|t| t.as_str() == Some("null"))
        && let Some(non_null) = types.iter().find(|t| t.as_str() != Some("null")).cloned()
    {
        obj.insert("type".into(), non_null);
    }

    if let Some(serde_json::Value::String(fmt)) = obj.get("format")
        && matches!(
            fmt.as_str(),
            "uint"
                | "uint8"
                | "uint16"
                | "uint32"
                | "uint64"
                | "int"
                | "int8"
                | "int16"
                | "int32"
                | "int64"
                | "float"
                | "double"
        )
    {
        obj.remove("format");
    }

    for (key, value) in &mut obj {
        let taken = std::mem::take(value);
        *value = match (key.as_str(), taken) {
            (
                "properties" | "patternProperties" | "dependentSchemas",
                serde_json::Value::Object(map),
            ) => serde_json::Value::Object(
                map.into_iter()
                    .map(|(prop, schema)| (prop, rewrite_schema_node(schema, defs)))
                    .collect(),
            ),
            (_, serde_json::Value::Object(sub)) => {
                rewrite_schema_node(serde_json::Value::Object(sub), defs)
            }
            ("items" | "prefixItems", serde_json::Value::Array(items)) => serde_json::Value::Array(
                items
                    .into_iter()
                    .map(|item| rewrite_schema_node(item, defs))
                    .collect(),
            ),
            (_, other) => other,
        };
    }
    serde_json::Value::Object(obj)
}

pub(super) fn tool_metadata_copy_bytes(tool: &rmcp::model::Tool) -> usize {
    use std::borrow::Cow;
    let owned = |text: &Cow<'static, str>| match text {
        Cow::Borrowed(_) => 0,
        Cow::Owned(text) => text.len(),
    };
    // Schemas are shared Arc values; list_all only clones the independent fields.
    owned(&tool.name)
        .saturating_add(tool.title.as_ref().map_or(0, String::len))
        .saturating_add(tool.description.as_ref().map_or(0, owned))
        .saturating_add(lease::json_len(&tool.annotations).unwrap_or(usize::MAX))
        .saturating_add(lease::json_len(&tool.icons).unwrap_or(usize::MAX))
        .saturating_add(lease::json_len(&tool.meta).unwrap_or(usize::MAX))
}
