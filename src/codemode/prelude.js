(function (__write, __call, __attach, __exit) {
    const parse = Function.prototype.call.bind(JSON.parse, JSON);
    const stringify = Function.prototype.call.bind(JSON.stringify, JSON);
    const get = Function.prototype.call.bind(Map.prototype.get);
    const set = Function.prototype.call.bind(Map.prototype.set);
    const remove = Function.prototype.call.bind(Map.prototype.delete);
    const then = Function.prototype.call.bind(Promise.prototype.then);
    const PromiseCtor = Promise;
    const ErrorCtor = Error;
    const TypeErrorCtor = TypeError;
    const RangeErrorCtor = RangeError;
    const StringCtor = String;
    const Uint8ArrayCtor = Uint8Array;
    const ArrayBufferCtor = ArrayBuffer;
    const DataViewCtor = DataView;
    const SharedArrayBufferCtor = typeof SharedArrayBuffer === "function" ? SharedArrayBuffer : undefined;
    const TypedArrayProto = Object.getPrototypeOf(Uint8ArrayCtor.prototype);
    const taTag = Function.prototype.call.bind(
        Object.getOwnPropertyDescriptor(TypedArrayProto, Symbol.toStringTag).get
    );
    const taBuffer = Function.prototype.call.bind(
        Object.getOwnPropertyDescriptor(TypedArrayProto, "buffer").get
    );
    const taByteOffset = Function.prototype.call.bind(
        Object.getOwnPropertyDescriptor(TypedArrayProto, "byteOffset").get
    );
    const taByteLength = Function.prototype.call.bind(
        Object.getOwnPropertyDescriptor(TypedArrayProto, "byteLength").get
    );
    const dvBuffer = Function.prototype.call.bind(
        Object.getOwnPropertyDescriptor(DataViewCtor.prototype, "buffer").get
    );
    const dvByteOffset = Function.prototype.call.bind(
        Object.getOwnPropertyDescriptor(DataViewCtor.prototype, "byteOffset").get
    );
    const dvByteLength = Function.prototype.call.bind(
        Object.getOwnPropertyDescriptor(DataViewCtor.prototype, "byteLength").get
    );
    const abByteLength = Function.prototype.call.bind(
        Object.getOwnPropertyDescriptor(ArrayBufferCtor.prototype, "byteLength").get
    );
    const abDetachedDesc = Object.getOwnPropertyDescriptor(ArrayBufferCtor.prototype, "detached");
    const abDetached = abDetachedDesc && abDetachedDesc.get
        ? Function.prototype.call.bind(abDetachedDesc.get)
        : undefined;
    const abImmutableDesc = Object.getOwnPropertyDescriptor(ArrayBufferCtor.prototype, "immutable");
    const abImmutable = abImmutableDesc && abImmutableDesc.get
        ? Function.prototype.call.bind(abImmutableDesc.get)
        : undefined;
    const sabByteLength = SharedArrayBufferCtor
        ? Function.prototype.call.bind(
            Object.getOwnPropertyDescriptor(SharedArrayBufferCtor.prototype, "byteLength").get
        )
        : undefined;
    const objectToString = Function.prototype.call.bind(Object.prototype.toString);
    const charCodeAt = Function.prototype.call.bind(String.prototype.charCodeAt);
    const stringIndexOf = Function.prototype.call.bind(String.prototype.indexOf);
    const regexpExec = Function.prototype.call.bind(RegExp.prototype.exec);
    const sourceFrame = /^(    at (?:.* \()?codemode\.js:)([0-9]+)(:[0-9]+\)?)$/;
    const stringSlice = Function.prototype.call.bind(String.prototype.slice);
    const stringToLowerCase = Function.prototype.call.bind(String.prototype.toLowerCase);
    const fromCharCode = String.fromCharCode;
    const reflectApply = Reflect.apply;
    const weakSetAdd = Function.prototype.call.bind(WeakSet.prototype.add);
    const weakSetHas = Function.prototype.call.bind(WeakSet.prototype.has);
    const weakMapSet = Function.prototype.call.bind(WeakMap.prototype.set);
    const weakMapGet = Function.prototype.call.bind(WeakMap.prototype.get);
    const defineProperty = Object.defineProperty;
    const objectFreeze = Object.freeze;
    const hasOwn = Function.prototype.call.bind(Object.prototype.hasOwnProperty);
    const numberIsFinite = Number.isFinite;
    const arrayJoin = Function.prototype.call.bind(Array.prototype.join);
    const toStringTag = Symbol.toStringTag;
    const isArray = Array.isArray;
    const errorStackDesc = Object.getOwnPropertyDescriptor(ErrorCtor.prototype, "stack");
    const getErrorStack = Function.prototype.call.bind(errorStackDesc.get);
    // The wrapper occupies one line; normalize native stacks at their shared getter.
    defineProperty(ErrorCtor.prototype, "stack", {
        __proto__: null,
        get() { return sourceStack(getErrorStack(this)); },
        set: errorStackDesc.set,
        enumerable: errorStackDesc.enumerable,
        configurable: errorStackDesc.configurable,
    });
    const pending = new Map();
    let waiting = 0;
    let state = 0;
    let failure;

    function sourceStack(raw) {
        if (typeof raw !== "string") return raw;
        let out = "";
        let pos = 0;
        while (pos < raw.length) {
            const nl = stringIndexOf(raw, "\n", pos);
            let line = nl === -1 ? stringSlice(raw, pos) : stringSlice(raw, pos, nl);
            const frame = regexpExec(sourceFrame, line);
            if (frame && +frame[2] > 1) line = frame[1] + (+frame[2] - 1) + frame[3];
            out += line;
            if (nl === -1) break;
            out += "\n";
            pos = nl + 1;
        }
        return out;
    }
    function stripPreludeFrames(raw) {
        if (typeof raw !== "string" || raw.length === 0) return "";
        let out = "";
        let pos = 0;
        while (pos < raw.length) {
            const nl = stringIndexOf(raw, "\n", pos);
            const line = nl === -1 ? stringSlice(raw, pos) : stringSlice(raw, pos, nl);
            if (line.length > 0 && stringIndexOf(line, "codemode-prelude.js") === -1) {
                out += out.length === 0 ? line : "\n" + line;
            }
            if (nl === -1) break;
            pos = nl + 1;
        }
        return out;
    }
    function readErrorStack(error) {
        if (error === null || typeof error !== "object") return "";
        try { return stripPreludeFrames(error.stack); }
        catch { return ""; }
    }
    function captureCallStack() {
        try { return readErrorStack(new ErrorCtor()); }
        catch { return ""; }
    }
    function withCallStack(error, stack) {
        if (error !== null && typeof error === "object") {
            const chosen = readErrorStack(error) || stack;
            if (chosen) {
                try {
                    defineProperty(error, "stack", {
                        __proto__: null,
                        value: chosen,
                        writable: true,
                        enumerable: false,
                        configurable: true,
                    });
                } catch {}
            }
        }
        return error;
    }

    function safeString(value) {
        try { return StringCtor(value); }
        catch {
            try { return objectToString(value); }
            catch { return "[unprintable]"; }
        }
    }
    function text(value) {
        const json = typeof value === "string" ? value : stringify(value);
        __write(json === undefined ? "undefined" : json);
    }
    function format(value) {
        let isErr = false;
        try { isErr = value instanceof ErrorCtor; } catch {}
        if (!isErr && value !== null && typeof value === "object") {
            try { isErr = objectToString(value) === "[object Error]"; } catch {}
        }
        if (isErr) {
            let name = "Error";
            let message = "";
            let stack = "";
            try { if (value.name !== undefined) name = safeString(value.name); } catch {}
            try { if (value.message !== undefined) message = safeString(value.message); } catch {}
            try { if (value.stack) stack = safeString(value.stack); } catch {}
            const head = name + ": " + message;
            return stack ? head + "\n" + stack : head;
        }
        if (typeof value === "string" || typeof value === "number") return StringCtor(value);
        try {
            const json = stringify(value);
            return json === undefined ? safeString(value) : json;
        } catch { return safeString(value); }
    }
    const console = {};
    for (const level of ["log", "info", "warn", "error", "debug"]) {
        console[level] = (...args) => {
            let line = "";
            for (let i = 0; i < args.length; i++) line += (i ? " " : "") + format(args[i]);
            __write(line);
        };
    }
    function toolErrorMessage(value) {
        if (value && typeof value === "object") {
            if (typeof value.error === "string" && value.error.length > 0) return value.error;
            const parts = [];
            if (typeof value.inputError === "string" && value.inputError.length > 0) {
                parts[parts.length] = value.inputError;
            }
            if (typeof value.captureError === "string" && value.captureError.length > 0) {
                parts[parts.length] = "output capture failed: " + value.captureError;
            }
            if (typeof value.logError === "string" && value.logError.length > 0) {
                parts[parts.length] = "log write failed: " + value.logError;
            }
            if (parts.length > 0) return arrayJoin(parts, "; ");
        }
        return "Tool operation failed.";
    }
    class ToolError extends ErrorCtor {
        constructor(tool, code, message, result) {
            super(typeof message === "string" && message.length > 0 ? message : toolErrorMessage(result));
            defineProperty(this, "name", {
                __proto__: null,
                value: "ToolError",
                writable: true,
                enumerable: false,
                configurable: true,
            });
            defineProperty(this, "tool", {
                __proto__: null,
                value: tool,
                writable: true,
                enumerable: true,
                configurable: true,
            });
            defineProperty(this, "code", {
                __proto__: null,
                value: code || "IO",
                writable: true,
                enumerable: true,
                configurable: true,
            });
            defineProperty(this, "result", {
                __proto__: null,
                value: result,
                writable: true,
                enumerable: true,
                configurable: true,
            });
        }
    }
    const members = Object.create(null);
    for (const name of ["exec", "read", "write", "edit"]) {
        members[name] = args => {
            const callStack = captureCallStack();
            return new PromiseCtor((resolve, reject) => {
                if (args === null || typeof args !== "object" || isArray(args)) {
                    reject(withCallStack(new TypeErrorCtor("Tool arguments must be an object."), callStack));
                    return;
                }
                try {
                    const res = __call(name, stringify(args));
                    if (charCodeAt(res, 0) === 123) {
                        const err = parse(res);
                        reject(withCallStack(new ToolError(name, err.code, err.message, undefined), callStack));
                    } else {
                        const id = +res;
                        set(pending, id, { resolve, reject, tool: name, stack: callStack });
                        waiting++;
                    }
                } catch (error) { reject(withCallStack(error, callStack)); }
            });
        };
    }
    function sleep(ms) {
        const callStack = captureCallStack();
        return new PromiseCtor((resolve, reject) => {
            if (typeof ms !== "number" || !numberIsFinite(ms)) {
                reject(withCallStack(new TypeErrorCtor("sleep requires a finite number of milliseconds."), callStack));
                return;
            }
            try {
                const res = __call("sleep", ms <= 0 ? "0" : StringCtor(ms));
                if (charCodeAt(res, 0) === 123) {
                    const err = parse(res);
                    reject(withCallStack(new ToolError("sleep", err.code, err.message, undefined), callStack));
                } else {
                    const id = +res;
                    set(pending, id, { resolve, reject, tool: "sleep", stack: callStack });
                    waiting++;
                }
            } catch (error) { reject(withCallStack(error, callStack)); }
        });
    }
    const tools = new Proxy(members, {
        __proto__: null,
        get(object, name) {
            if (typeof name !== "string") return undefined;
            if (name === "exec" || name === "read" || name === "write" || name === "edit") return object[name];
            throw new TypeErrorCtor("tools." + name + " does not exist. Available: exec, read, write, edit.");
        }
    });
    const shHelpers = {
        quote(value) {
            let str;
            if (typeof value === "string") {
                str = value;
            } else if (typeof value === "number") {
                if (!numberIsFinite(value)) {
                    throw new TypeErrorCtor("sh.quote requires a string or finite number.");
                }
                str = StringCtor(value);
            } else {
                throw new TypeErrorCtor("sh.quote requires a string or finite number.");
            }
            if (stringIndexOf(str, "\0") !== -1) {
                throw new TypeErrorCtor("sh.quote string must not contain NUL (U+0000).");
            }
            let out = "'";
            let start = 0;
            while (true) {
                const at = stringIndexOf(str, "'", start);
                if (at === -1) {
                    out += start === 0 ? str : stringSlice(str, start);
                    break;
                }
                out += stringSlice(str, start, at) + "'\\''";
                start = at + 1;
            }
            return out + "'";
        },
        join(values) {
            if (!isArray(values)) {
                throw new TypeErrorCtor("sh.join requires an array.");
            }
            const len = values.length;
            if (typeof len !== "number" || (len >>> 0) !== len) {
                throw new TypeErrorCtor("sh.join array has an invalid length.");
            }
            let out = "";
            for (let i = 0; i < len; i++) {
                if (!hasOwn(values, i)) {
                    throw new TypeErrorCtor("sh.join array must not contain holes.");
                }
                const word = quote(values[i]);
                out += i === 0 ? word : " " + word;
            }
            return out;
        },
        sh(strings) {
            if (strings === null || typeof strings !== "object" || !hasOwn(strings, "raw")) {
                throw new TypeErrorCtor("sh must be called as a tagged template.");
            }
            const raw = strings.raw;
            if (!isArray(raw)) {
                throw new TypeErrorCtor("sh template strings.raw must be an array.");
            }
            const len = raw.length;
            if (typeof len !== "number" || (len >>> 0) !== len || len !== arguments.length) {
                throw new TypeErrorCtor("sh template strings.raw length does not match values.");
            }
            let out = "";
            for (let i = 0; i < len; i++) {
                if (!hasOwn(raw, i)) {
                    throw new TypeErrorCtor("sh template strings.raw must not contain holes.");
                }
                const seg = raw[i];
                if (typeof seg !== "string") {
                    throw new TypeErrorCtor("sh template segments must be strings.");
                }
                if (stringIndexOf(seg, "\0") !== -1) {
                    throw new TypeErrorCtor("sh template must not contain NUL (U+0000).");
                }
                out += seg;
                if (i + 1 < len) {
                    const val = arguments[i + 1];
                    out += isArray(val) ? join(val) : quote(val);
                }
            }
            return out;
        },
    };
    const quote = objectFreeze(shHelpers.quote);
    const join = objectFreeze(shHelpers.join);
    const sh = shHelpers.sh;
    defineProperty(sh, "quote", {
        __proto__: null,
        value: quote,
        writable: false,
        enumerable: true,
        configurable: false,
    });
    defineProperty(sh, "join", {
        __proto__: null,
        value: join,
        writable: false,
        enumerable: true,
        configurable: false,
    });
    objectFreeze(sh);
    const attachmentHandles = new WeakMap();
    function parseDataUrl(value) {
        if (value.length < 12) return undefined;
        if (stringToLowerCase(stringSlice(value, 0, 5)) !== "data:") return undefined;
        const comma = stringIndexOf(value, ",", 5);
        if (comma < 12) {
            throw new TypeErrorCtor("attach data URL must include ';base64,'.");
        }
        const header = stringSlice(value, 5, comma);
        if (stringToLowerCase(stringSlice(header, header.length - 7)) !== ";base64") {
            throw new TypeErrorCtor("attach data URL must use ;base64 encoding.");
        }
        return {
            mime: stringSlice(header, 0, header.length - 7),
            data: stringSlice(value, comma + 1),
        };
    }
    function attach(value) {
        if (value !== null && typeof value === "object" && !isArray(value)) {
            const hid = weakMapGet(attachmentHandles, value);
            if (hid !== undefined) {
                return parse(__attach(0, StringCtor(hid), "", ""));
            }
            if (value.attachable === false) {
                const note = typeof value.note === "string" && value.note.length > 0
                    ? value.note
                    : "The media file cannot be attached.";
                throw new TypeErrorCtor(note);
            }
            if (typeof value.path === "string" && value.type === undefined) {
                return parse(__attach(1, value.path, "", ""));
            }
            if ((value.type === "image" || value.type === "audio") && typeof value.data === "string") {
                if (value.mimeType !== undefined && typeof value.mimeType !== "string") {
                    throw new TypeErrorCtor("MCP " + value.type + " block mimeType must be a string.");
                }
                return parse(__attach(2, value.data, value.mimeType || "", value.type));
            }
            if (
                value.type === "resource" &&
                value.resource !== null &&
                typeof value.resource === "object" &&
                typeof value.resource.blob === "string"
            ) {
                const res = value.resource;
                if (res.mimeType !== undefined && typeof res.mimeType !== "string") {
                    throw new TypeErrorCtor("MCP resource block mimeType must be a string.");
                }
                if (res.uri !== undefined && typeof res.uri !== "string") {
                    throw new TypeErrorCtor("MCP resource block uri must be a string.");
                }
                return parse(__attach(2, res.blob, res.mimeType || "", ""));
            }
        }
        if (typeof value === "string") {
            const dataUrl = parseDataUrl(value);
            if (dataUrl !== undefined) {
                return parse(__attach(2, dataUrl.data, dataUrl.mime, ""));
            }
            return parse(__attach(1, value, "", ""));
        }
        throw new TypeErrorCtor(
            "attach expects a file path, a read media descriptor, an opaque task attachment handle, an MCP image/audio/resource(blob) block, or a base64 data URL."
        );
    }
    function sealNestedAttachments(val) {
        if (
            val !== null &&
            typeof val === "object" &&
            val.codemode !== null &&
            typeof val.codemode === "object" &&
            isArray(val.codemode.attachments)
        ) {
            const rawList = val.codemode.attachments;
            const out = [];
            for (let i = 0; i < rawList.length; i++) {
                const item = rawList[i];
                const hid = item.__hid;
                const handle = objectFreeze({
                    index: item.index,
                    kind: item.kind,
                    mimeType: item.mimeType,
                    bytes: item.bytes,
                });
                if (typeof hid === "number") {
                    weakMapSet(attachmentHandles, handle, hid);
                }
                out[i] = handle;
            }
            val.codemode.attachments = objectFreeze(out);
        }
        return val;
    }
    function exit() {
        __exit();
        // QuickJS's interrupt is uncatchable, including inside a script's try/catch.
        while (true) {}
    }

    const encoders = new WeakSet();
    class TextEncoder {
        constructor() {
            weakSetAdd(encoders, this);
        }
        get encoding() {
            if (!weakSetHas(encoders, this)) throw new TypeErrorCtor("Illegal invocation");
            return "utf-8";
        }
        encode(input = "") {
            if (!weakSetHas(encoders, this)) throw new TypeErrorCtor("Illegal invocation");
            const str = input === undefined ? "" : `${input}`;
            const len = str.length;
            let total = 0;
            for (let i = 0; i < len; i++) {
                const c = charCodeAt(str, i);
                if (c < 0x80) total += 1;
                else if (c < 0x800) total += 2;
                else if (c >= 0xD800 && c <= 0xDBFF && i + 1 < len) {
                    const d = charCodeAt(str, i + 1);
                    if (d >= 0xDC00 && d <= 0xDFFF) { total += 4; i++; }
                    else total += 3;
                } else total += 3;
            }
            const out = new Uint8ArrayCtor(total);
            let pos = 0;
            for (let i = 0; i < len; i++) {
                const c = charCodeAt(str, i);
                if (c < 0x80) {
                    out[pos++] = c;
                } else if (c < 0x800) {
                    out[pos++] = 0xC0 | (c >> 6);
                    out[pos++] = 0x80 | (c & 0x3F);
                } else if (c >= 0xD800 && c <= 0xDFFF) {
                    if (c <= 0xDBFF && i + 1 < len) {
                        const d = charCodeAt(str, i + 1);
                        if (d >= 0xDC00 && d <= 0xDFFF) {
                            const cp = 0x10000 + ((c - 0xD800) << 10) + (d - 0xDC00);
                            out[pos++] = 0xF0 | (cp >> 18);
                            out[pos++] = 0x80 | ((cp >> 12) & 0x3F);
                            out[pos++] = 0x80 | ((cp >> 6) & 0x3F);
                            out[pos++] = 0x80 | (cp & 0x3F);
                            i++;
                            continue;
                        }
                    }
                    out[pos++] = 0xEF;
                    out[pos++] = 0xBF;
                    out[pos++] = 0xBD;
                } else {
                    out[pos++] = 0xE0 | (c >> 12);
                    out[pos++] = 0x80 | ((c >> 6) & 0x3F);
                    out[pos++] = 0x80 | (c & 0x3F);
                }
            }
            return out;
        }
        encodeInto(source, destination) {
            if (!weakSetHas(encoders, this)) throw new TypeErrorCtor("Illegal invocation");
            if (arguments.length < 2) {
                throw new TypeErrorCtor("TextEncoder.encodeInto requires 2 arguments.");
            }
            const str = `${source}`;
            if (destination === null || typeof destination !== "object" || taTag(destination) !== "Uint8Array") {
                throw new TypeErrorCtor("TextEncoder.encodeInto destination must be a Uint8Array.");
            }
            if (abImmutable) {
                const buf = taBuffer(destination);
                let ro = false;
                try { ro = abImmutable(buf); } catch {}
                if (ro) throw new TypeErrorCtor("TextEncoder.encodeInto destination buffer is immutable.");
            }
            const cap = taByteLength(destination);
            const len = str.length;
            let read = 0;
            let written = 0;
            while (read < len) {
                const c = charCodeAt(str, read);
                if (c < 0x80) {
                    if (written + 1 > cap) break;
                    destination[written++] = c;
                    read++;
                } else if (c < 0x800) {
                    if (written + 2 > cap) break;
                    destination[written++] = 0xC0 | (c >> 6);
                    destination[written++] = 0x80 | (c & 0x3F);
                    read++;
                } else if (c >= 0xD800 && c <= 0xDFFF) {
                    if (c <= 0xDBFF && read + 1 < len) {
                        const d = charCodeAt(str, read + 1);
                        if (d >= 0xDC00 && d <= 0xDFFF) {
                            if (written + 4 > cap) break;
                            const cp = 0x10000 + ((c - 0xD800) << 10) + (d - 0xDC00);
                            destination[written++] = 0xF0 | (cp >> 18);
                            destination[written++] = 0x80 | ((cp >> 12) & 0x3F);
                            destination[written++] = 0x80 | ((cp >> 6) & 0x3F);
                            destination[written++] = 0x80 | (cp & 0x3F);
                            read += 2;
                            continue;
                        }
                    }
                    if (written + 3 > cap) break;
                    destination[written++] = 0xEF;
                    destination[written++] = 0xBF;
                    destination[written++] = 0xBD;
                    read++;
                } else {
                    if (written + 3 > cap) break;
                    destination[written++] = 0xE0 | (c >> 12);
                    destination[written++] = 0x80 | ((c >> 6) & 0x3F);
                    destination[written++] = 0x80 | (c & 0x3F);
                    read++;
                }
            }
            return { read, written };
        }
    }
    defineProperty(TextEncoder.prototype, toStringTag, {
        __proto__: null,
        value: "TextEncoder",
        configurable: true,
    });

    function asciiLabel(raw) {
        const str = `${raw}`;
        let start = 0;
        let end = str.length;
        while (start < end) {
            const c = charCodeAt(str, start);
            if (c === 0x09 || c === 0x0A || c === 0x0C || c === 0x0D || c === 0x20) start++;
            else break;
        }
        while (end > start) {
            const c = charCodeAt(str, end - 1);
            if (c === 0x09 || c === 0x0A || c === 0x0C || c === 0x0D || c === 0x20) end--;
            else break;
        }
        let out = "";
        for (let i = start; i < end; i++) {
            const c = charCodeAt(str, i);
            if (c > 0x7F) return "";
            out += c >= 0x41 && c <= 0x5A ? fromCharCode(c + 0x20) : fromCharCode(c);
        }
        return out;
    }

    function isUtf8Label(label) {
        return (
            label === "utf-8" ||
            label === "utf8" ||
            label === "unicode-1-1-utf-8" ||
            label === "unicode11utf8" ||
            label === "unicode20utf8" ||
            label === "x-unicode20utf8"
        );
    }

    function isDetached(buf) {
        if (!abDetached) return false;
        try { return abDetached(buf); } catch { return false; }
    }

    function isBufferSource(input) {
        if (input === undefined) return true;
        if (input === null || typeof input !== "object") return false;
        if (taTag(input) !== undefined) return true;
        try { dvBuffer(input); return true; } catch {}
        try { abByteLength(input); return true; } catch {}
        if (sabByteLength) {
            try { sabByteLength(input); return true; } catch {}
        }
        return false;
    }

    function asByteView(input) {
        if (input === undefined) return new Uint8ArrayCtor(0);
        if (taTag(input) !== undefined) {
            const buf = taBuffer(input);
            if (isDetached(buf)) return new Uint8ArrayCtor(0);
            const len = taByteLength(input);
            return len === 0 ? new Uint8ArrayCtor(0) : new Uint8ArrayCtor(buf, taByteOffset(input), len);
        }
        let dvBuf;
        let isDv = false;
        try { dvBuf = dvBuffer(input); isDv = true; } catch {}
        if (isDv) {
            if (isDetached(dvBuf)) return new Uint8ArrayCtor(0);
            let off = 0;
            let len = 0;
            try {
                off = dvByteOffset(input);
                len = dvByteLength(input);
            } catch {
                return new Uint8ArrayCtor(0);
            }
            return len === 0 ? new Uint8ArrayCtor(0) : new Uint8ArrayCtor(dvBuf, off, len);
        }
        let isAb = false;
        try { abByteLength(input); isAb = true; } catch {}
        if (isAb) {
            return isDetached(input) ? new Uint8ArrayCtor(0) : new Uint8ArrayCtor(input);
        }
        if (sabByteLength) {
            let isSab = false;
            try { sabByteLength(input); isSab = true; } catch {}
            if (isSab) return new Uint8ArrayCtor(input);
        }
        throw new TypeErrorCtor("TextDecoder.decode input must be an ArrayBuffer, SharedArrayBuffer, or ArrayBufferView.");
    }

    const decodeUnits = [];
    for (let i = 0; i < 1024; i++) decodeUnits[i] = 0;
    const decoders = new WeakMap();
    class TextDecoder {
        constructor(label = "utf-8", options = undefined) {
            const normalized = label === undefined ? "utf-8" : asciiLabel(label);
            if (options !== undefined && options !== null && typeof options !== "object" && typeof options !== "function") {
                throw new TypeErrorCtor("TextDecoder options must be an object.");
            }
            const fatal = Boolean(options && options.fatal);
            const ignoreBOM = Boolean(options && options.ignoreBOM);
            if (!isUtf8Label(normalized)) {
                throw new RangeErrorCtor("TextDecoder only supports utf-8 encoding.");
            }
            weakMapSet(decoders, this, {
                fatal,
                ignoreBOM,
                doNotFlush: false,
                bomSeen: false,
                pending: undefined,
                cp: 0,
                seen: 0,
                needed: 0,
                lower: 0x80,
                upper: 0xBF,
            });
        }
        get encoding() {
            if (!weakMapGet(decoders, this)) throw new TypeErrorCtor("Illegal invocation");
            return "utf-8";
        }
        get fatal() {
            const s = weakMapGet(decoders, this);
            if (!s) throw new TypeErrorCtor("Illegal invocation");
            return s.fatal;
        }
        get ignoreBOM() {
            const s = weakMapGet(decoders, this);
            if (!s) throw new TypeErrorCtor("Illegal invocation");
            return s.ignoreBOM;
        }
        decode(input = undefined, options = undefined) {
            const s = weakMapGet(decoders, this);
            if (!s) throw new TypeErrorCtor("Illegal invocation");
            if (!isBufferSource(input)) {
                throw new TypeErrorCtor("TextDecoder.decode input must be an ArrayBuffer, SharedArrayBuffer, or ArrayBufferView.");
            }
            if (options !== undefined && options !== null && typeof options !== "object" && typeof options !== "function") {
                throw new TypeErrorCtor("TextDecoder decode options must be an object.");
            }
            const stream = Boolean(options && options.stream);
            let bytes = asByteView(input);
            const reset = () => {
                s.cp = 0;
                s.seen = 0;
                s.needed = 0;
                s.lower = 0x80;
                s.upper = 0xBF;
            };
            if (!s.doNotFlush) {
                reset();
                s.bomSeen = false;
                s.pending = undefined;
            }
            s.doNotFlush = stream;
            if (s.pending !== undefined) {
                const head = s.pending;
                const headLen = taByteLength(head);
                const tailLen = taByteLength(bytes);
                const joined = new Uint8ArrayCtor(headLen + tailLen);
                for (let i = 0; i < headLen; i++) joined[i] = head[i];
                for (let i = 0; i < tailLen; i++) joined[headLen + i] = bytes[i];
                bytes = joined;
                s.pending = undefined;
            }
            let bomSeen = s.bomSeen;
            const len = taByteLength(bytes);
            const fail = start => {
                // Fatal streaming errors leave unread input queued for the next decode.
                if (stream && start < len) {
                    const pending = new Uint8ArrayCtor(len - start);
                    for (let i = start; i < len; i++) pending[i - start] = bytes[i];
                    s.pending = pending;
                }
                throw new TypeErrorCtor("The encoded data was not valid utf-8.");
            };
            let out = "";
            let unitCount = 0;
            const pushUnit = unit => {
                decodeUnits[unitCount++] = unit;
                if (unitCount === 1024) {
                    out += reflectApply(fromCharCode, null, decodeUnits);
                    unitCount = 0;
                }
            };
            const emit = scalar => {
                if (!bomSeen) {
                    bomSeen = true;
                    if (!s.ignoreBOM && scalar === 0xFEFF) return;
                }
                if (scalar <= 0xFFFF) {
                    pushUnit(scalar);
                } else {
                    const offset = scalar - 0x10000;
                    pushUnit(0xD800 + (offset >> 10));
                    pushUnit(0xDC00 + (offset & 0x3FF));
                }
            };
            for (let i = 0; i < len; i++) {
                const byte = bytes[i];
                if (s.needed === 0) {
                    if (byte <= 0x7F) {
                        emit(byte);
                    } else if (byte >= 0xC2 && byte <= 0xDF) {
                        s.needed = 1;
                        s.cp = byte & 0x1F;
                    } else if (byte >= 0xE0 && byte <= 0xEF) {
                        if (byte === 0xE0) s.lower = 0xA0;
                        else if (byte === 0xED) s.upper = 0x9F;
                        s.needed = 2;
                        s.cp = byte & 0x0F;
                    } else if (byte >= 0xF0 && byte <= 0xF4) {
                        if (byte === 0xF0) s.lower = 0x90;
                        else if (byte === 0xF4) s.upper = 0x8F;
                        s.needed = 3;
                        s.cp = byte & 0x07;
                    } else if (s.fatal) {
                        reset();
                        fail(i + 1);
                    } else {
                        emit(0xFFFD);
                    }
                } else if (byte < s.lower || byte > s.upper) {
                    reset();
                    if (s.fatal) fail(i);
                    emit(0xFFFD);
                    i--;
                } else {
                    s.lower = 0x80;
                    s.upper = 0xBF;
                    s.cp = (s.cp << 6) | (byte & 0x3F);
                    s.seen++;
                    if (s.seen === s.needed) {
                        const scalar = s.cp;
                        reset();
                        emit(scalar);
                    }
                }
            }
            if (!stream && s.needed !== 0) {
                reset();
                if (s.fatal) fail(len);
                emit(0xFFFD);
            }
            s.bomSeen = bomSeen;
            for (let j = 0; j < unitCount; j++) {
                out += fromCharCode(decodeUnits[j]);
            }
            return out;
        }
    }
    defineProperty(TextDecoder.prototype, toStringTag, {
        __proto__: null,
        value: "TextDecoder",
        configurable: true,
    });

    defineProperty(globalThis, "TextEncoder", {
        __proto__: null,
        value: TextEncoder,
        writable: true,
        enumerable: false,
        configurable: true,
    });
    defineProperty(globalThis, "TextDecoder", {
        __proto__: null,
        value: TextDecoder,
        writable: true,
        enumerable: false,
        configurable: true,
    });
    globalThis.ToolError = ToolError;
    globalThis.tools = tools;
    globalThis.sh = sh;
    globalThis.console = console;
    globalThis.text = text;
    globalThis.attach = attach;
    globalThis.sleep = sleep;
    globalThis.exit = exit;
    return {
        run(fn) {
            then(fn(), value => {
                try {
                    if (value !== undefined) {
                        if (typeof value === "string") {
                            __write(value);
                        } else {
                            let json;
                            try {
                                json = stringify(value);
                            } catch {
                                throw new TypeErrorCtor("the return value is not JSON-serializable");
                            }
                            if (json === undefined) {
                                throw new TypeErrorCtor("the return value is not JSON-serializable");
                            }
                            __write(json);
                        }
                    }
                    state = 1;
                }
                catch (error) { failure = error; state = 2; }
            }, error => { failure = error; state = 2; });
        },
        settle(id, ok, payload, errCode, taskId, logPath) {
            const entry = get(pending, id);
            if (!entry) return;
            remove(pending, id);
            waiting--;
            if (!ok) {
                const partial = taskId ? { taskId, logPath } : undefined;
                entry.reject(withCallStack(new ToolError(entry.tool, errCode, payload, partial), entry.stack));
                return;
            }
            if (entry.tool === "sleep") {
                entry.resolve(undefined);
                return;
            }
            let envelope;
            try { envelope = parse(payload); } catch (error) { entry.reject(withCallStack(error, entry.stack)); return; }
            if (envelope.ok) {
                entry.resolve(sealNestedAttachments(envelope.value));
            } else {
                const partial = envelope.result !== undefined ? sealNestedAttachments(envelope.result) : undefined;
                entry.reject(withCallStack(new ToolError(entry.tool, envelope.code, envelope.error, partial), entry.stack));
            }
        },
        status() { return state || (waiting ? 0 : 3); },
        failure() { return failure; }
    };
})