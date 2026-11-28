use super::events::{build_event_notification, send_event_notification};
use super::leases::{Principal, application_input_owner, inferred_base, reserve_response_content};
use super::schema::portable_schema;
use super::server::{Backend, Server};
use crate::config::{InstructionsToolMode, Tools, compose_instructions};
use crate::env::LaunchContext;
use crate::events;
use crate::id;
use crate::lease;
use crate::service::Service;
use crate::tasks::session::Session;
use crate::transfer::{self, TransferEndpoint};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, InitializeRequestParams};
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServerHandler};
use std::sync::{Arc, OnceLock};

mod portable_schema_tests {
    use super::*;
    use crate::tasks::{Limits, Tasks};
    use serde_json::{Value, json};

    /// Simulates Gemini web's MCP JSON Schema -> OpenAPI 3.0 declaration converter.
    fn gemini_convert(node: &Value) -> Value {
        let Some(obj) = node.as_object() else {
            return json!({});
        };
        let mut out = serde_json::Map::new();
        if let Some(desc) = obj.get("description") {
            out.insert("description".into(), desc.clone());
        }
        if let Some(ty) = obj.get("type").and_then(Value::as_str) {
            out.insert("type".into(), json!(ty.to_ascii_uppercase()));
        }
        if let Some(en) = obj.get("enum") {
            out.insert("enum".into(), en.clone());
        }
        if let Some(req) = obj.get("required") {
            out.insert("required".into(), req.clone());
        }
        if let Some(items) = obj.get("items") {
            out.insert("items".into(), gemini_convert(items));
        }
        if let Some(props) = obj.get("properties").and_then(Value::as_object) {
            let mut converted = serde_json::Map::new();
            for (k, v) in props {
                converted.insert(k.clone(), gemini_convert(v));
            }
            out.insert("properties".into(), Value::Object(converted));
        }
        if let Some(any_of) = obj.get("anyOf").and_then(Value::as_array) {
            out.insert(
                "any_of".into(),
                Value::Array(any_of.iter().map(gemini_convert).collect()),
            );
        }
        Value::Object(out)
    }

    fn assert_portable_and_gemini_typed(path: &str, schema: &Value, gemini: &Value) {
        let obj = schema
            .as_object()
            .unwrap_or_else(|| panic!("{path}: not an object"));
        for forbidden in [
            "$ref",
            "$defs",
            "definitions",
            "$schema",
            "anyOf",
            "oneOf",
            "allOf",
            "not",
            "prefixItems",
            "additionalItems",
            "patternProperties",
            "dependentSchemas",
            "format",
        ] {
            assert!(
                !obj.contains_key(forbidden),
                "{path} still contains {forbidden}: {schema}"
            );
        }
        assert!(
            obj.get("type").and_then(Value::as_str).is_some(),
            "{path} lacks scalar string type: {schema}"
        );
        assert!(
            gemini.get("type").and_then(Value::as_str).is_some(),
            "{path} lost its type under Gemini conversion: {gemini}"
        );
        if let Some(en) = obj.get("enum") {
            assert_eq!(
                obj.get("type"),
                Some(&json!("string")),
                "{path}: enum must be string"
            );
            assert_eq!(gemini.get("enum"), Some(en), "{path}: Gemini lost enum");
        }
        if let Some(items) = obj.get("items") {
            assert_portable_and_gemini_typed(&format!("{path}.items"), items, &gemini["items"]);
        }
        if let Some(props) = obj.get("properties").and_then(Value::as_object) {
            for (k, v) in props {
                assert_portable_and_gemini_typed(
                    &format!("{path}.{k}"),
                    v,
                    &gemini["properties"][k],
                );
            }
        }
    }

    #[tokio::test]
    async fn every_tool_schema_is_portable_and_preserves_types_in_a_modeled_gemini_conversion() {
        let tasks = Tasks::new(Limits::OFF).unwrap();
        let launch = Arc::new(LaunchContext::current().unwrap());
        let backend = Backend::Local {
            service: Arc::new(Service::new(tasks, true)),
            session: Arc::new(OnceLock::new()),
            launch: Arc::clone(&launch),
            daemon: true,
        };
        let transfer = Some(TransferEndpoint::Fixed {
            table: Arc::new(transfer::Transfers::new()),
            base: Arc::from("http://127.0.0.1:1/"),
        });
        let server = Server::new(
            backend,
            &launch,
            transfer,
            Tools {
                codemode: true,
                transfer: true,
                yolo: false,
            },
            None,
            InstructionsToolMode::Off,
        );
        let tools = server.router.list_all();
        assert_eq!(tools.len(), 6);
        let mut by_name = std::collections::HashMap::new();
        for tool in tools {
            let schema = Value::Object((*tool.input_schema).clone());
            let gemini = gemini_convert(&schema);
            assert_portable_and_gemini_typed(&tool.name, &schema, &gemini);
            assert_eq!(
                schema["additionalProperties"],
                json!(false),
                "{}",
                tool.name
            );
            assert!(
                !tool
                    .description
                    .as_deref()
                    .unwrap_or_default()
                    .contains("libproc")
            );
            by_name.insert(tool.name.into_owned(), (schema, gemini));
        }
        let (exec_s, exec_g) = &by_name["exec"];
        let (code_s, _) = &by_name["codemode"];
        let (edit_s, edit_g) = &by_name["edit"];
        assert_eq!(
            exec_s["properties"]["action"]["enum"],
            json!(["start", "poll", "kill", "list", "transcript"])
        );
        assert_eq!(exec_g["properties"]["action"]["type"], json!("STRING"));
        assert_eq!(
            exec_g["properties"]["action"]["enum"],
            json!(["start", "poll", "kill", "list", "transcript"])
        );
        assert!(edit_s["properties"]["edits"]["items"]["properties"]["oldText"].is_object());
        assert!(edit_s["properties"]["edits"]["items"]["properties"]["newText"].is_object());
        assert_eq!(
            edit_g["properties"]["edits"]["items"]["properties"]["oldText"]["type"],
            json!("STRING")
        );
        // The wire constraints match. Each tool describes its own behavior and defaults.
        let constraints = |field: &Value| {
            let mut schema = field.as_object().unwrap().clone();
            schema.remove("description");
            schema
        };
        for shared in ["waitMs", "killAfterMs", "truncate", "maxBytes"] {
            assert_eq!(
                constraints(&code_s["properties"][shared]),
                constraints(&exec_s["properties"][shared]),
                "codemode.{shared} diverged from exec.{shared}"
            );
        }
        for rw_field in ["outputContains", "quietMs"] {
            assert_eq!(
                constraints(&code_s["properties"]["returnWhen"]["properties"][rw_field]),
                constraints(&exec_s["properties"]["returnWhen"]["properties"][rw_field]),
                "returnWhen.{rw_field} diverged"
            );
        }
        assert!(
            code_s["properties"]["killAfterMs"]["description"]
                .as_str()
                .unwrap()
                .contains("Tasks the script already started keep running")
        );
        assert_eq!(
            exec_s["properties"]["truncate"]["enum"],
            json!(["head_tail", "head", "tail"])
        );
        assert_eq!(
            exec_s["properties"]["maxBytes"]["maximum"],
            json!(4_194_304)
        );
        assert!(code_s["properties"].get("raw").is_none());

        // Chained $ref -> nullable anyOf -> oneOf inside $defs flattens cleanly while
        // preserving property names such as "format" and "type" inside `properties`.
        let tricky = portable_schema(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {
                "Mode": {
                    "oneOf": [
                        {"const": "fast", "description": "quick"},
                        {"const": "full", "description": "complete"}
                    ]
                },
                "NullableMode": {
                    "anyOf": [{"$ref": "#/$defs/Mode"}, {"type": "null"}]
                }
            },
            "type": "object",
            "properties": {
                "mode": {"description": "Run mode.", "$ref": "#/$defs/NullableMode"},
                "format": {"type": ["string", "null"], "description": "Output format."}
            }
        }));
        let tricky_g = gemini_convert(&tricky);
        assert_portable_and_gemini_typed("tricky", &tricky, &tricky_g);
        assert_eq!(
            tricky["properties"]["mode"]["enum"],
            json!(["fast", "full"])
        );
        assert_eq!(
            tricky["properties"]["mode"]["description"],
            json!("Run mode. fast: quick full: complete")
        );
        assert_eq!(tricky["properties"]["format"]["type"], json!("string"));
    }
}

mod instructions_tool_tests {
    use super::*;
    use crate::config::{
        INSTRUCTIONS_TOOL, MOVE_INSTRUCTIONS_POINTER, instructions_tool_mode_of,
        server_instructions,
    };
    use serde_json::{Value, json};
    use std::ffi::OsStr;

    #[test]
    fn parser_accepts_strict_lowercase_values_and_rejects_invalid() {
        assert_eq!(
            instructions_tool_mode_of(None),
            Ok(InstructionsToolMode::Off)
        );
        assert_eq!(
            instructions_tool_mode_of(Some(OsStr::new("off"))),
            Ok(InstructionsToolMode::Off)
        );
        assert_eq!(
            instructions_tool_mode_of(Some(OsStr::new("move"))),
            Ok(InstructionsToolMode::Move)
        );
        assert_eq!(
            instructions_tool_mode_of(Some(OsStr::new("copy"))),
            Ok(InstructionsToolMode::Copy)
        );

        for bad in [
            "", "true", "false", "on", "OFF", "Move", "COPY", " off ", "move\n", "1", "0",
        ] {
            let err = instructions_tool_mode_of(Some(OsStr::new(bad))).unwrap_err();
            assert_eq!(
                err,
                format!("invalid {INSTRUCTIONS_TOOL} {bad:?}; expected off, move, or copy")
            );
        }

        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            let err = instructions_tool_mode_of(Some(OsStr::from_bytes(b"\xff"))).unwrap_err();
            assert!(
                err.starts_with(&format!("invalid {INSTRUCTIONS_TOOL} "))
                    && err.ends_with("; expected off, move, or copy"),
                "{err}"
            );
        }

        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStringExt;
            let value = std::ffi::OsString::from_wide(&[0xD800]);
            let err = instructions_tool_mode_of(Some(&value)).unwrap_err();
            assert!(
                err.starts_with(&format!("invalid {INSTRUCTIONS_TOOL} "))
                    && err.ends_with("; expected off, move, or copy"),
                "{err}"
            );
        }
    }

    #[test]
    fn compose_instructions_matches_spec_matrix() {
        let extra = "    host rule 1\n第二行\n";
        for daemon_owned in [false, true] {
            for codemode in [false, true] {
                let base = server_instructions(daemon_owned, codemode, None);
                let full = server_instructions(daemon_owned, codemode, Some(extra));

                let (proto, tool) =
                    compose_instructions(daemon_owned, codemode, None, InstructionsToolMode::Off);
                assert_eq!(proto, base);
                assert_eq!(tool, None);

                let (proto, tool) = compose_instructions(
                    daemon_owned,
                    codemode,
                    Some(extra),
                    InstructionsToolMode::Off,
                );
                assert_eq!(proto, full);
                assert_eq!(tool, None);

                let (proto, tool) =
                    compose_instructions(daemon_owned, codemode, None, InstructionsToolMode::Move);
                assert_eq!(proto, base);
                assert_eq!(tool.as_deref(), Some(base.as_ref()));

                let (proto, tool) = compose_instructions(
                    daemon_owned,
                    codemode,
                    Some(extra),
                    InstructionsToolMode::Move,
                );
                assert_eq!(
                    proto.as_ref(),
                    format!("{base}\n\n{MOVE_INSTRUCTIONS_POINTER}")
                );
                assert_eq!(tool.as_deref(), Some(extra));

                let (proto, tool) =
                    compose_instructions(daemon_owned, codemode, None, InstructionsToolMode::Copy);
                assert_eq!(proto, base);
                assert_eq!(tool.as_deref(), Some(base.as_ref()));

                let (proto, tool) = compose_instructions(
                    daemon_owned,
                    codemode,
                    Some(extra),
                    InstructionsToolMode::Copy,
                );
                assert_eq!(proto, full);
                assert_eq!(tool.as_deref(), Some(full.as_ref()));
            }
        }
    }

    #[test]
    fn build_router_registers_instructions_tool_with_exact_schema_and_annotations() {
        let launch = LaunchContext::current().unwrap();
        let expected_schema = json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false,
        });
        let description = "    host-specific directive\n第二行\n";

        for yolo in [false, true] {
            let tools = Tools {
                codemode: false,
                transfer: false,
                yolo,
            };

            let absent = Server::build_router(&launch, tools, None);
            assert!(
                absent
                    .list_all()
                    .iter()
                    .all(|tool| tool.name != "instructions")
            );

            let present = Server::build_router(&launch, tools, Some(description));
            let tool = present
                .list_all()
                .into_iter()
                .find(|tool| tool.name == "instructions")
                .expect("instructions tool registered");
            assert_eq!(tool.description.as_deref(), Some(description));
            assert_eq!(Value::Object((*tool.input_schema).clone()), expected_schema);
            let annotations = tool.annotations.expect("instructions annotations");
            assert_eq!(annotations.title.as_deref(), Some("Server instructions"));
            assert_eq!(annotations.read_only_hint, Some(true));
            assert_eq!(annotations.destructive_hint, Some(false));
            assert_eq!(annotations.idempotent_hint, Some(!yolo));
            assert_eq!(annotations.open_world_hint, Some(false));
        }
    }
}

mod metadata_budget_tests {
    use super::*;

    #[test]
    fn application_input_admission_precedes_copies_and_keeps_control_for_management() {
        let args = serde_json::json!({"action":"sleep","path":"sample.txt"});
        let id = rmcp::model::RequestId::String("ordinary-id".into());
        let borrowed = (&args, &id);
        let needed = 4 * lease::json_len(&borrowed).unwrap();
        for (limit, fits) in [(needed - 1, false), (needed, true)] {
            let pool = lease::BytePool::new(limit);
            let control = lease::BytePool::new(needed);
            let result = application_input_owner(&borrowed, None, &pool, &control, false);
            assert_eq!(result.is_ok(), fits);
            assert_eq!(control.used(), 0, "ordinary tools cannot use control space");
            drop(result);
            assert_eq!(pool.used(), 0);
        }
        let full = lease::BytePool::new(0);
        let control = lease::BytePool::new(needed);
        let held = application_input_owner(&borrowed, None, &full, &control, true).unwrap();
        assert_eq!(control.used(), needed);
        let inherited =
            application_input_owner(&borrowed, Some(&held), &full, &full, false).unwrap();
        drop(held);
        assert_eq!(
            control.used(),
            needed,
            "an existing owner is shared, not recharged"
        );
        drop(inherited);
        assert_eq!(control.used(), 0);
    }

    #[test]
    fn metadata_workspace_refuses_before_copy_and_releases_at_handoff() {
        let pool = lease::BytePool::new(32);
        let held = reserve_response_content(&pool, 32).unwrap();
        assert!(reserve_response_content(&pool, 1).is_err());
        let result = CallToolResult::success(vec![ContentBlock::text("ordinary metadata")]);
        assert_eq!(pool.used(), 32);
        // Returning this official SDK value does not transfer the application's
        // workspace. SDK-owned later allocations are explicitly out of scope.
        drop(held);
        assert_eq!(pool.used(), 0);
        assert_eq!(
            result.content[0].as_text().unwrap().text,
            "ordinary metadata"
        );
    }
}

mod event_notification_tests {
    use super::*;
    use crate::tasks::{Limits, Tasks};
    use rmcp::model::{ClientJsonRpcMessage, ServerJsonRpcMessage};
    use rmcp::transport::Transport;
    use tokio::sync::{mpsc, oneshot};

    struct HeldNotification {
        message: ServerJsonRpcMessage,
        complete: oneshot::Sender<()>,
    }

    struct TestServer;

    impl ServerHandler for TestServer {}

    struct PausedTransport(mpsc::Sender<HeldNotification>);

    impl Transport<RoleServer> for PausedTransport {
        type Error = std::io::Error;

        fn send(
            &mut self,
            message: ServerJsonRpcMessage,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
            let outgoing = self.0.clone();
            async move {
                let (complete, waiting) = oneshot::channel();
                outgoing
                    .send(HeldNotification { message, complete })
                    .await
                    .map_err(|_| std::io::Error::other("test receiver closed"))?;
                waiting
                    .await
                    .map_err(|_| std::io::Error::other("test send dropped"))
            }
        }

        async fn receive(&mut self) -> Option<ClientJsonRpcMessage> {
            std::future::pending().await
        }

        async fn close(&mut self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn event_notification_backpressure_does_not_delay_stream_cancel() {
        let tasks = Tasks::new(Limits::OFF).unwrap();
        let service = Arc::new(Service::new(tasks, false));
        let bus = Arc::clone(service.event_bus());
        let launch = Arc::new(LaunchContext::new(std::env::temp_dir(), Vec::new()));
        let server = Server::new(
            Backend::Local {
                service,
                session: Arc::new(OnceLock::new()),
                launch: Arc::clone(&launch),
                daemon: false,
            },
            &launch,
            None,
            Tools::default(),
            None,
            InstructionsToolMode::Off,
        );
        let (outgoing, mut queued) = mpsc::channel(1);
        let mut peer =
            rmcp::service::serve_directly(server.clone(), PausedTransport(outgoing), None);
        let context = RequestContext::new(
            rmcp::model::RequestId::String("stream-cancel".into()),
            peer.peer().clone(),
        );
        let cancel = context.ct.clone();
        let mut streaming = tokio::spawn(async move {
            server
                .handle_events_stream(
                    Some(serde_json::json!({"name": events::EVENT_EXTERNAL})),
                    context,
                )
                .await
        });
        let pending = tokio::time::timeout(std::time::Duration::from_secs(5), queued.recv())
            .await
            .unwrap()
            .unwrap();
        let value = serde_json::to_value(&pending.message).unwrap();
        assert_eq!(value["method"], "notifications/events/active");
        assert_eq!(bus.active_streams(), 1);

        cancel.cancel();
        let completed = tokio::time::timeout(std::time::Duration::from_secs(5), &mut streaming)
            .await
            .expect("cancel must release the subscription before the notification can be written")
            .unwrap()
            .unwrap();
        assert_eq!(completed.0["resultType"], "complete");
        assert_eq!(bus.active_streams(), 0);
        drop(pending.message);
        pending.complete.send(()).unwrap();
        peer.close().await.unwrap();
    }
    #[tokio::test]
    async fn event_notification_cancel_keeps_owners_until_transport_drops_it() {
        let (outgoing, mut queued) = mpsc::channel(1);
        let mut peer = rmcp::service::serve_directly(TestServer, PausedTransport(outgoing), None);
        let context = RequestContext::new(
            rmcp::model::RequestId::String("stream-owner".into()),
            peer.peer().clone(),
        );
        let cancel = context.ct.clone();
        let payload_pool = lease::BytePool::new(64);
        let metadata_pool = lease::BytePool::new(64);
        let payload = serde_json::json!({"cursor": "current"});
        let payload_bytes = lease::json_len(&payload).unwrap();
        let metadata_bytes = lease::json_len(&context.id).unwrap();
        let params = lease::Owned::new(
            payload,
            lease::Owner::new(payload_pool.try_acquire(payload_bytes).unwrap()),
        );
        let metadata = lease::Owner::new(metadata_pool.try_acquire(metadata_bytes).unwrap());
        let mut sending = tokio::spawn(async move {
            send_event_notification(
                &context,
                "notifications/events/heartbeat",
                params,
                &context.id,
                &metadata,
            )
            .await
        });
        let pending = tokio::time::timeout(std::time::Duration::from_secs(5), queued.recv())
            .await
            .unwrap()
            .unwrap();

        cancel.cancel();
        assert!(
            !tokio::time::timeout(std::time::Duration::from_secs(5), &mut sending)
                .await
                .expect("notification send must observe cancellation while transport is paused")
                .unwrap()
        );
        assert_eq!(payload_pool.used(), payload_bytes);
        assert_eq!(metadata_pool.used(), metadata_bytes);
        drop(pending.message);
        assert_eq!(payload_pool.used(), 0);
        assert_eq!(metadata_pool.used(), 0);
        pending.complete.send(()).unwrap();
        peer.close().await.unwrap();
    }

    #[tokio::test]
    async fn event_notification_pre_cancel_sends_no_notification() {
        let (outgoing, mut queued) = mpsc::channel(1);
        let mut peer = rmcp::service::serve_directly(TestServer, PausedTransport(outgoing), None);
        let context = RequestContext::new(
            rmcp::model::RequestId::String("stream-pre-cancel".into()),
            peer.peer().clone(),
        );
        context.ct.cancel();
        assert!(
            !tokio::time::timeout(
                std::time::Duration::from_secs(5),
                send_event_notification(
                    &context,
                    "notifications/events/active",
                    lease::Owned::new(serde_json::json!({}), lease::Owner::default()),
                    &context.id,
                    &lease::Owner::default(),
                ),
            )
            .await
            .expect("a pre-cancelled request must not wait for the transport")
        );

        let after_cancel = tokio::spawn(async move {
            context
                .peer
                .send_notification(build_event_notification(
                    "notifications/events/heartbeat",
                    serde_json::json!({"cursor": "after-cancel"}),
                    &context.id,
                ))
                .await
        });
        let pending = tokio::time::timeout(std::time::Duration::from_secs(5), queued.recv())
            .await
            .unwrap()
            .unwrap();
        let value = serde_json::to_value(&pending.message).unwrap();
        assert_eq!(value["method"], "notifications/events/heartbeat");
        assert_eq!(value["params"]["cursor"], "after-cancel");
        drop(pending.message);
        pending.complete.send(()).unwrap();
        after_cancel.await.unwrap().unwrap();
        peer.close().await.unwrap();
    }

    #[tokio::test]
    async fn http_stateless_requests_share_launch_while_stateful_sessions_isolate_caches() {
        let tasks = Tasks::new(Limits::OFF).unwrap();
        let service = Arc::new(Service::new(tasks, true));
        let shared_launch = Arc::new(LaunchContext::new(std::env::temp_dir(), Vec::new()));
        let principal_session = Session::new(
            "t1".into(),
            "http-t1".into(),
            Arc::new(shared_launch.for_session()),
        );
        let principals: Arc<[Arc<Session>]> = Arc::from([Arc::clone(&principal_session)]);
        let make_http_server = || {
            let mut server = Server::new(
                Backend::Local {
                    service: Arc::clone(&service),
                    session: Arc::new(OnceLock::new()),
                    launch: Arc::clone(&shared_launch),
                    daemon: true,
                },
                &shared_launch,
                None,
                Tools::default(),
                None,
                InstructionsToolMode::Off,
            );
            server.principals = Some(Arc::clone(&principals));
            server
        };

        // Stateless HTTP call uses the pre-built principal session without initializing OnceLock.
        let stateless_server = make_http_server();
        let (tx0, _rx0) = mpsc::channel(1);
        let mut peer0 = rmcp::service::serve_directly(TestServer, PausedTransport(tx0), None);
        let mut stateless_ctx = RequestContext::new(
            rmcp::model::RequestId::String("stateless".into()),
            peer0.peer().clone(),
        );
        let (mut parts, ()) = ::http::Request::new(()).into_parts();
        parts.extensions.insert(Principal(0));
        stateless_ctx.extensions.insert(parts);
        let resolved = stateless_server.session(&stateless_ctx).unwrap();
        assert!(Arc::ptr_eq(&resolved, &principal_session));
        if let Backend::Local {
            session, launch, ..
        } = &stateless_server.backend
        {
            assert!(session.get().is_none());
            assert!(Arc::ptr_eq(launch, &shared_launch));
        }
        peer0.close().await.unwrap();

        // Stateful HTTP initialize isolates each session's LaunchContext from shared_launch and peers.
        let init_params: InitializeRequestParams = serde_json::from_value(serde_json::json!({
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": { "name": "http-stateful", "version": "1.0" }
        }))
        .unwrap();
        let stateful_a = make_http_server();
        let (txa, _rxa) = mpsc::channel(1);
        let mut peera = rmcp::service::serve_directly(TestServer, PausedTransport(txa), None);
        stateful_a
            .initialize(
                init_params.clone(),
                RequestContext::new(
                    rmcp::model::RequestId::String("init-a".into()),
                    peera.peer().clone(),
                ),
            )
            .await
            .unwrap();
        let sess_a = stateful_a
            .session(&RequestContext::new(
                rmcp::model::RequestId::String("call-a".into()),
                peera.peer().clone(),
            ))
            .unwrap();
        peera.close().await.unwrap();

        let stateful_b = make_http_server();
        let (txb, _rxb) = mpsc::channel(1);
        let mut peerb = rmcp::service::serve_directly(TestServer, PausedTransport(txb), None);
        stateful_b
            .initialize(
                init_params,
                RequestContext::new(
                    rmcp::model::RequestId::String("init-b".into()),
                    peerb.peer().clone(),
                ),
            )
            .await
            .unwrap();
        let sess_b = stateful_b
            .session(&RequestContext::new(
                rmcp::model::RequestId::String("call-b".into()),
                peerb.peer().clone(),
            ))
            .unwrap();
        peerb.close().await.unwrap();

        assert!(!Arc::ptr_eq(&sess_a.launch, &shared_launch));
        assert!(!Arc::ptr_eq(&sess_b.launch, &shared_launch));
        assert!(!Arc::ptr_eq(&sess_a.launch, &sess_b.launch));

        // Stdio server (principals: None) shares its single LaunchContext directly.
        let stdio_server = Server::new(
            Backend::Local {
                service: Arc::clone(&service),
                session: Arc::new(OnceLock::new()),
                launch: Arc::clone(&shared_launch),
                daemon: false,
            },
            &shared_launch,
            None,
            Tools::default(),
            None,
            InstructionsToolMode::Off,
        );
        let (txs, _rxs) = mpsc::channel(1);
        let mut peers = rmcp::service::serve_directly(TestServer, PausedTransport(txs), None);
        let stdio_sess = stdio_server
            .session(&RequestContext::new(
                rmcp::model::RequestId::String("stdio".into()),
                peers.peer().clone(),
            ))
            .unwrap();
        assert!(Arc::ptr_eq(&stdio_sess.launch, &shared_launch));
        peers.close().await.unwrap();
    }

    #[test]
    fn instance_pools_never_touch_globals_and_control_plane_falls_back_to_control_memory() {
        const EXACT_NAME: &str = "mcp::tests::event_notification_tests::instance_pools_never_touch_globals_and_control_plane_falls_back_to_control_memory";
        if !std::env::args().any(|arg| arg == "--exact") {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", EXACT_NAME, "--nocapture"])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(verify_instance_pool_isolation());
    }

    async fn verify_instance_pool_isolation() {
        assert_eq!(lease::process_pool().used(), 0);
        assert_eq!(lease::control_pool().used(), 0);

        let init_params: InitializeRequestParams = serde_json::from_value(serde_json::json!({
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": { "name": "pool-isolation", "version": "1.0" }
        }))
        .unwrap();

        let make_server = |main_limit: usize, control_limit: usize| {
            let pool = lease::BytePool::new(main_limit);
            let control = lease::BytePool::new(control_limit);
            let launch = Arc::new(
                LaunchContext::new(std::env::temp_dir(), Vec::new())
                    .with_memory(Arc::clone(&pool))
                    .with_control_memory(Arc::clone(&control)),
            );
            let tasks = Tasks::new(Limits::OFF).unwrap();
            let server = Server::new(
                Backend::Local {
                    service: Arc::new(Service::new(tasks, false)),
                    session: Arc::new(OnceLock::new()),
                    launch: Arc::clone(&launch),
                    daemon: false,
                },
                &launch,
                None,
                Tools::default(),
                Some("extra directive"),
                InstructionsToolMode::Copy,
            );
            (server, pool, control)
        };

        // 1. Both isolated pools saturated (0 bytes): all 6 paths refuse/drop without touching globals.
        {
            let (server, pool, control) = make_server(0, 0);
            let (outgoing, mut queued) = mpsc::channel(4);
            let mut peer =
                rmcp::service::serve_directly(TestServer, PausedTransport(outgoing), None);
            let ctx = |id: &str| {
                RequestContext::new(
                    rmcp::model::RequestId::String(id.into()),
                    peer.peer().clone(),
                )
            };

            let init_err = server
                .initialize(init_params.clone(), ctx("init-full"))
                .await
                .unwrap_err();
            assert!(
                init_err
                    .message
                    .contains("Server active memory budget is full")
            );
            let instr_err = server
                .instructions(Parameters(serde_json::Map::new()), ctx("instr-full"))
                .await
                .unwrap_err();
            assert!(
                instr_err
                    .message
                    .contains("Server active memory budget is full")
            );
            let disc_err = server.discover(ctx("disc-full")).await.unwrap_err();
            assert!(
                disc_err
                    .message
                    .contains("Server active memory budget is full")
            );
            let list_err = server.list_tools(None, ctx("list-full")).await.unwrap_err();
            assert!(
                list_err
                    .message
                    .contains("Server active memory budget is full")
            );
            let ev_err = server
                .handle_events_poll(
                    Some(serde_json::json!({"name": "no.such.event"})),
                    ctx("poll-full"),
                )
                .await
                .unwrap_err();
            assert_eq!(ev_err.code.0, -32025);
            assert!(
                ev_err.message.contains(
                    "Server active memory budget is full while preparing the event error"
                )
            );

            let mut prog_ctx = ctx("prog-full");
            prog_ctx.meta.set_progress_token(rmcp::model::ProgressToken(
                rmcp::model::NumberOrString::Number(1),
            ));
            let (sender, forwarder) = server.progress(&prog_ctx);
            sender
                .as_ref()
                .unwrap()
                .send(Some((1.0, "dropped-progress".to_owned())))
                .unwrap();
            tokio::task::yield_now().await;
            drop(sender);
            drop(forwarder);
            assert!(queued.try_recv().is_err());

            assert_eq!(pool.used(), 0);
            assert_eq!(control.used(), 0);
            assert_eq!(lease::process_pool().used(), 0);
            assert_eq!(lease::control_pool().used(), 0);
            peer.close().await.unwrap();
        }

        // 2. Main pool saturated (0 bytes), control pool ample: initialize and instructions stay
        // refused, while discover, list_tools, event_error_to_rmcp, and progress fall back to control.
        {
            let (server, pool, control) = make_server(0, 64 * 1024);
            let (outgoing, mut queued) = mpsc::channel(4);
            let mut peer =
                rmcp::service::serve_directly(TestServer, PausedTransport(outgoing), None);
            let ctx = |id: &str| {
                RequestContext::new(
                    rmcp::model::RequestId::String(id.into()),
                    peer.peer().clone(),
                )
            };

            assert!(
                server
                    .initialize(init_params.clone(), ctx("init-ctrl"))
                    .await
                    .is_err()
            );
            assert!(
                server
                    .instructions(Parameters(serde_json::Map::new()), ctx("instr-ctrl"))
                    .await
                    .is_err()
            );
            assert_eq!(control.used(), 0);

            let discovered = server.discover(ctx("disc-ctrl")).await.unwrap();
            assert!(!discovered.supported_versions.is_empty());
            let listed = server.list_tools(None, ctx("list-ctrl")).await.unwrap();
            assert!(!listed.tools.is_empty());
            let ev_err = server
                .handle_events_poll(
                    Some(serde_json::json!({"name": "no.such.event"})),
                    ctx("poll-ctrl"),
                )
                .await
                .unwrap_err();
            assert_eq!(ev_err.code.0, -32023);

            let mut prog_ctx = ctx("prog-ctrl");
            prog_ctx.meta.set_progress_token(rmcp::model::ProgressToken(
                rmcp::model::NumberOrString::Number(2),
            ));
            let (sender, _forwarder) = server.progress(&prog_ctx);
            let msg = "control-progress-payload";
            sender
                .as_ref()
                .unwrap()
                .send(Some((2.0, msg.to_owned())))
                .unwrap();
            let pending = tokio::time::timeout(std::time::Duration::from_secs(5), queued.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(pool.used(), 0);
            assert_eq!(control.used(), msg.len());
            assert_eq!(lease::process_pool().used(), 0);
            assert_eq!(lease::control_pool().used(), 0);
            drop(pending.message);
            assert_eq!(control.used(), 0);
            pending.complete.send(()).unwrap();
            peer.close().await.unwrap();
        }

        // 3. Main pool ample, control pool zero: initialize and instructions succeed on instance main pool.
        {
            let (server, pool, control) = make_server(64 * 1024, 0);
            let (outgoing, _queued) = mpsc::channel(1);
            let mut peer =
                rmcp::service::serve_directly(TestServer, PausedTransport(outgoing), None);
            let ctx = |id: &str| {
                RequestContext::new(
                    rmcp::model::RequestId::String(id.into()),
                    peer.peer().clone(),
                )
            };

            server
                .initialize(init_params, ctx("init-main"))
                .await
                .unwrap();
            let instr = server
                .instructions(Parameters(serde_json::Map::new()), ctx("instr-main"))
                .await
                .unwrap();
            assert_eq!(instr.is_error, Some(false));
            assert_eq!(pool.used(), 0);
            assert_eq!(control.used(), 0);
            assert_eq!(lease::process_pool().used(), 0);
            assert_eq!(lease::control_pool().used(), 0);
            peer.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn inferred_base_and_transfer_support_http2_authority_and_reject_userinfo_or_duplicate_hosts()
     {
        let dir = std::env::temp_dir().join(format!(
            "fastexec-mcp-transfer-{}",
            id::random_chars(10).unwrap()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let sample = dir.join("item.txt");
        std::fs::write(&sample, b"hello").unwrap();

        let tasks = Tasks::new(Limits::OFF).unwrap();
        let service = Arc::new(Service::new(tasks, true));
        let launch = Arc::new(LaunchContext::new(dir.clone(), Vec::new()));
        let table = Arc::new(transfer::Transfers::new());
        let server = Server::new(
            Backend::Local {
                service,
                session: Arc::new(OnceLock::new()),
                launch: Arc::clone(&launch),
                daemon: true,
            },
            &launch,
            Some(TransferEndpoint::Http {
                table: Arc::clone(&table),
                public_root: None,
            }),
            Tools {
                codemode: false,
                transfer: true,
                yolo: false,
            },
            None,
            InstructionsToolMode::Off,
        );
        let (tx, _rx) = mpsc::channel(1);
        let mut peer = rmcp::service::serve_directly(TestServer, PausedTransport(tx), None);

        let call_transfer = |parts: ::http::request::Parts| {
            let mut ctx =
                RequestContext::new(rmcp::model::RequestId::Number(1), peer.peer().clone());
            ctx.extensions.insert(parts);
            let path = sample.to_string_lossy().into_owned();
            server.transfer(Parameters(transfer::Request { path }), ctx)
        };

        // HTTP/2 request without Host header uses :authority and :scheme.
        let (h2_https_parts, ()) = ::http::Request::builder()
            .uri("https://127.0.0.1:8787/mcp")
            .body(())
            .unwrap()
            .into_parts();
        assert_eq!(
            inferred_base(&h2_https_parts).unwrap(),
            "https://127.0.0.1:8787/"
        );
        let ok_res = call_transfer(h2_https_parts).await;
        assert_eq!(ok_res.is_error, Some(false));
        let ok_text = &ok_res.content[0].as_text().unwrap().text;
        assert!(
            ok_text.starts_with("https://127.0.0.1:8787/t/"),
            "{ok_text}"
        );

        // X-Forwarded-Proto overrides :scheme in both directions.
        let (xfp_https, ()) = ::http::Request::builder()
            .uri("http://[::1]:8787/mcp")
            .header("x-forwarded-proto", "https, http")
            .body(())
            .unwrap()
            .into_parts();
        assert_eq!(inferred_base(&xfp_https).unwrap(), "https://[::1]:8787/");
        let (xfp_http, ()) = ::http::Request::builder()
            .uri("https://localhost:8787/mcp")
            .header("x-forwarded-proto", "http")
            .body(())
            .unwrap()
            .into_parts();
        assert_eq!(inferred_base(&xfp_http).unwrap(), "http://localhost:8787/");

        // Missing both Host and :authority fails with the canonical message.
        let (no_host, ()) = ::http::Request::builder()
            .uri("/mcp")
            .body(())
            .unwrap()
            .into_parts();
        assert_eq!(
            inferred_base(&no_host).unwrap_err(),
            "Cannot make a transfer URL: the request names no Host."
        );

        // Empty host (`:8787`) is rejected.
        let (empty_host, ()) = ::http::Request::builder()
            .uri("/mcp")
            .header(::http::header::HOST, ":8787")
            .body(())
            .unwrap()
            .into_parts();
        assert!(inferred_base(&empty_host).is_err());

        // Userinfo in Host or HTTP/2 :authority is rejected without echoing credentials.
        let (evil_header, ()) = ::http::Request::builder()
            .uri("/mcp")
            .header(::http::header::HOST, "evil@127.0.0.1:8787")
            .body(())
            .unwrap()
            .into_parts();
        let err = inferred_base(&evil_header).unwrap_err();
        assert!(!err.contains("evil"), "{err}");
        let bad_res = call_transfer(evil_header).await;
        assert_eq!(bad_res.is_error, Some(true));

        let evil_uri = ::http::Uri::builder()
            .scheme("http")
            .authority("evil@127.0.0.1:8787")
            .path_and_query("/mcp")
            .build()
            .unwrap();
        let (evil_h2, ()) = ::http::Request::builder()
            .uri(evil_uri)
            .body(())
            .unwrap()
            .into_parts();
        let err = inferred_base(&evil_h2).unwrap_err();
        assert!(!err.contains("evil"), "{err}");
        let bad_h2_res = call_transfer(evil_h2).await;
        assert_eq!(bad_h2_res.is_error, Some(true));

        // Duplicate Host headers are rejected even when identical.
        for second in ["127.0.0.1:8787", "localhost:8787"] {
            let (mut dup_parts, ()) = ::http::Request::builder()
                .uri("http://127.0.0.1:8787/mcp")
                .body(())
                .unwrap()
                .into_parts();
            dup_parts
                .headers
                .append(::http::header::HOST, "127.0.0.1:8787".parse().unwrap());
            dup_parts
                .headers
                .append(::http::header::HOST, second.parse().unwrap());
            assert!(inferred_base(&dup_parts).is_err());
            let dup_res = call_transfer(dup_parts).await;
            assert_eq!(dup_res.is_error, Some(true));
        }

        peer.close().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn concurrent_backend_local_drops_always_remove_event_bus_session_cursor() {
        let tasks = Tasks::new(Limits::OFF).unwrap();
        let service = Arc::new(Service::new(tasks, false));
        let bus = Arc::clone(service.event_bus());
        bus.publish_external(events::ExternalEventInput {
            event_type: None,
            source: "ci".to_owned(),
            topic: None,
            name: "done".to_owned(),
            event_id: "ev-1".to_owned(),
            timestamp: None,
            data: serde_json::json!({}),
        })
        .unwrap();

        let launch = Arc::new(LaunchContext::new(std::env::temp_dir(), Vec::new()));
        let (tx, _rx) = mpsc::channel(1);
        let mut peer = rmcp::service::serve_directly(TestServer, PausedTransport(tx), None);

        for round in 0..128 {
            let backend = Backend::Local {
                service: Arc::clone(&service),
                session: Arc::new(OnceLock::new()),
                launch: Arc::clone(&launch),
                daemon: false,
            };
            let server = Server::new(
                backend.clone(),
                &launch,
                None,
                Tools::default(),
                None,
                InstructionsToolMode::Off,
            );
            let sess = server
                .session(&RequestContext::new(
                    rmcp::model::RequestId::Number(round),
                    peer.peer().clone(),
                ))
                .unwrap();
            let sess_id = sess.id.clone();
            drop(sess);
            drop(server);

            assert!(bus.external_background_summary(&sess_id).is_some());
            assert!(bus.external_background_summary(&sess_id).is_none());

            let clones: Vec<Backend> = (0..8).map(|_| backend.clone()).collect();
            drop(backend);
            let barrier = std::sync::Barrier::new(clones.len());
            std::thread::scope(|scope| {
                for clone in clones {
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        drop(clone);
                    });
                }
            });

            assert!(
                bus.external_background_summary(&sess_id).is_some(),
                "round {round}: session cursor for {sess_id} was not removed on concurrent drop"
            );
            bus.remove_session(&sess_id);
        }

        peer.close().await.unwrap();
    }
}
