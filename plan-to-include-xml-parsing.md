# Plan: Add XML receive/send support to the Solace branch of Feldera

## Context

`faa-continuum-feldera` runs on the `feldera-solace` fork (`~/feldera-solace`, branch
`feat/solace-connector`), which adds `solace_input`/`solace_output` connectors over Solace SMF.
Today the pipeline speaks only **JSON** (and the input-only **raw** blob format). The immediate
need is genuine **FIXM/AIXM SWIM ingestion** — real aviation XML off Solace queues — plus general
enterprise XML send/receive.

FIXM/AIXM documents use patterns that disqualify naive XML libraries:
- `gml:id` on virtually every element — a **namespace-prefixed attribute** (GML namespace URI,
  not just a plain attribute)
- `xlink:href` for feature cross-references — another namespaced attribute (XLink namespace)
- 10+ namespace declarations on the root element
- Deep nesting (10+ levels), multiple SWIM feature types per message

Verified against real AIXM NOTAM sample (`aixm/donlon` — official EUROCONTROL/FAA data).

**Key architectural finding from exploration:** the Solace connector is completely
format-agnostic. Input transport delivers raw `&[u8]` + `ConnectorMetadata` to a generic
`Box<dyn Parser>`; the output sink publishes pre-encoded bytes produced by a generic
`Box<dyn Encoder>`. Formats are a compiled-in registry keyed by name string
(`INPUT_FORMATS`/`OUTPUT_FORMATS` in `crates/adapters/src/format.rs`). **Therefore adding XML is
purely a new format module + registry entry — zero changes to the Solace connector, controller,
or SQL layer.** `format='xml'` in SQL then resolves automatically via `get_input_format`/
`get_output_format`.

**Chosen approach:**
- **`roxmltree` (input) + `quick-xml` (output)** — pure Rust, no native deps, correct namespace
  handling. `roxmltree` resolves namespace URIs per spec: `node.attribute(("http://www.opengis.net/gml/3.2", "id"))` retrieves `gml:id` correctly. Output uses `quick-xml` for
  serialization. Both feed through `RecordFormat::Json` internally so typed columns work and
  **no `RecordFormat::Xml` variant and no `feldera-adapterlib` changes are needed**.
- **`with-xml` Cargo feature, included in `default`** (mirrors `with-avro`), so every image built
  by `solace-ci.yml` ships it.
- **Fork capability only** — `config/feldera/faa_program.sql` is left untouched.

## Files to create / modify (all in `~/feldera-solace`)

### 1. Config structs — `crates/feldera-types/src/format/xml.rs` (new)
Model on `crates/feldera-types/src/format/raw.rs`. Define serde `Deserialize/Serialize + ToSchema`
config structs:

- `XmlParserConfig` — input options:
  - `namespace_map: HashMap<String, String>` — maps user-defined prefix → namespace URI, e.g.
    `{"gml": "http://www.opengis.net/gml/3.2", "aixm": "http://www.aixm.aero/schema/5.1.1",
    "xlink": "http://www.w3.org/1999/xlink"}`. Used to resolve prefixes in `row_element` and
    column paths without depending on whatever prefix appears in the document.
  - `row_element: Option<String>` — prefixed element name (e.g. `"aixm:Airspace"`) whose
    occurrences each become one record. If unset, the whole document is one record.
  - `columns: Vec<XmlColumnConfig>` — per-column extraction config (see below). If empty,
    behaviour falls back to the whole-document JSON bridge for simple/flat XML.
  - `raw_column: Option<String>` — if set, the full raw XML bytes of each record node are also
    stored as a VARCHAR in this column. Enables re-emit-verbatim on the output side: the SQL view
    simply selects this column and the output sink publishes it unchanged.
  - `XmlColumnConfig { column: String, path: String }` — `path` is a dot-separated sequence of
    prefixed element names optionally ending with `@prefix:attr` or `@attr` for an attribute value,
    e.g. `"aixm:timeSlice.aixm:AirspaceTimeSlice.aixm:type"` or `"@gml:id"` (attribute on the
    current record node). Namespace prefixes are resolved via `namespace_map`.

- `XmlEncoderConfig` — output options:
  - `passthrough_column: Option<String>` — if set, find this VARCHAR column in each row, emit its
    UTF-8 string value directly as the message payload. No XML construction occurs. Takes
    precedence over all other encoder options. Used for the filter/route pattern (ingest FIXM →
    extract type for SQL filtering → re-emit original document verbatim).
  - `root_element: Option<String>` — wrapping root element (used when `passthrough_column` is
    absent; omit for bare row elements),
  - `row_element: String` (default `"row"`) — element name wrapping each row's column values,
  - `namespace_map: HashMap<String, String>` — prefix → URI declarations emitted on the root,
  - `declaration: bool` — emit `<?xml version="1.0"?>` (default `true`).

Register the module in `crates/feldera-types/src/format.rs` with `pub mod xml;` (ungated — plain
structs, no heavy deps).

### 2. Input + output implementation — `crates/adapters/src/format/xml.rs` (new)
Model input on `crates/adapters/src/format/raw.rs` and output on
`crates/adapters/src/format/json/output.rs`.

**Input (`XmlInputFormat` impl `InputFormat`, `XmlParser` impl `Parser`):**
- `new_parser` (mirror `raw.rs:62` / `json/input.rs:204`): deserialize `XmlParserConfig`, then
  `input_stream.handle.configure_deserializer(RecordFormat::Json(JsonFlavor::Default))` — i.e.
  drive the **JSON** deserializer, exactly as `json/input.rs:217` does. Store the resulting
  `Box<dyn DeCollectionStream>` on `XmlParser`.
- `XmlParser::parse(&[u8], metadata)` (mirror `raw.rs:169`):
  1. Parse the bytes with `roxmltree::Document::parse(str::from_utf8(data)?)`.
  2. Build an inverse prefix map from `namespace_map` (URI → config-prefix) for path resolution.
  3. If `columns` is non-empty: for each record node (matched by `row_element` using full URI
     comparison — `node.tag_name().namespace() == Some(uri)` and `node.tag_name().name() == local`),
     walk each `XmlColumnConfig.path` segment by segment resolving namespace URIs, with `@attr`
     suffix handled via `node.attribute((namespace_uri, local_name))` using
     `roxmltree`'s tuple API. Build a `serde_json::Map<String, Value>` keyed by column name.
     If `raw_column` is set, also include an entry keyed by that column name whose value is
     the serialized XML of the record node subtree (extracted via
     `&input_bytes[node.range()]` — `roxmltree` exposes byte ranges for every node, so this
     is a zero-copy slice of the original input). Serialize the map to JSON bytes for
     `input_stream.insert`.
  4. If `columns` is empty: convert the whole document to a `serde_json::Value` (recursive
     element→object walk, attributes included as `@`-prefixed keys) and serialize — simple/flat
     XML fallback. `raw_column` still applies at the document level.
  5. Push parse failures as `ParseError` like `RawParser::parse_record`.
- `splitter()` → `SpongeSplitter` (XML documents must be seen whole).
- `fork()` and `stage()` mirror `RawParser`.

**Output (`XmlOutputFormat` impl `OutputFormat`, `XmlEncoder` impl `Encoder`):**
- `new_encoder` (mirror `json/output.rs:54`): deserialize `XmlEncoderConfig`, build `XmlEncoder`
  holding the `OutputConsumer`.
- `encode(batch)` (mirror `json/output.rs:240`): get a cursor with
  `batch.cursor(RecordFormat::Json(JsonFlavor::Default))`. For each record:
  - **Passthrough mode** (`passthrough_column` is set): parse the JSON bytes to
    `serde_json::Value`, extract the named column's string value, emit its UTF-8 bytes directly
    via `self.output_consumer.push_buffer(col_str.as_bytes(), 1)` — no XML construction.
  - **Constructed mode** (`passthrough_column` absent): parse JSON to `serde_json::Value`,
    convert to an XML string via `quick-xml`'s `Writer` honoring `namespace_map`, `declaration`,
    `root_element`, and `row_element`, then `push_buffer`.
  Keys/headers are not used by the Solace sink so `push_key` mirrors JSON's minimal path.

### 3. Register the format — `crates/adapters/src/format.rs`
- Add `#[cfg(feature = "with-xml")] pub(crate) mod xml;` next to the `raw`/`json`/`csv` mods
  (line ~9-12), and the gated `use self::xml::{XmlInputFormat, XmlOutputFormat};`.
- Add `#[cfg(feature = "with-xml")] ("xml", Box::new(XmlInputFormat) as Box<dyn InputFormat>)`
  to `INPUT_FORMATS` (line ~40) and the `XmlOutputFormat` entry to `OUTPUT_FORMATS` (line ~59) —
  mirroring the `#[cfg(feature = "with-avro")]` avro entries exactly.

### 4. Cargo wiring — `crates/adapters/Cargo.toml`
- Add feature: `with-xml = ["roxmltree", "quick-xml"]` in `[features]` (model on `with-avro`
  at line ~43). No `feldera-adapterlib/with-xml` sub-feature required — we reuse
  `RecordFormat::Json` (no adapterlib variant added).
- Add `with-xml` to the `default = [...]` list alongside `with-solace` (line ~13).
- Add both deps as optional:
  - `roxmltree = { workspace = true, optional = true }` — for input DOM parsing with correct
    namespace/attribute resolution (`node.attribute(("uri", "local"))` API confirmed).
  - `quick-xml = { workspace = true, optional = true, features = ["serialize"] }` — for output
    XML serialization via `Writer`.
- If either crate is not yet in the root `Cargo.toml` `[workspace.dependencies]`, add them
  (current stable: `roxmltree = "0.19"`, `quick-xml = "0.37"`).

## Verification (end-to-end)

1. **Compile + unit tests** (fastest signal), from `~/feldera-solace`:
   - `cargo build -p dbsp_adapters --features with-solace,with-xml`
   - Add a `#[cfg(test)] mod test` in `xml.rs` modeled on `raw.rs:342` using
     `mock_parser_pipeline`. Two test tiers:
     - **Flat XML:** `<flight><id>A1</id><alt>350</alt></flight>` with empty `columns` config
       (JSON bridge fallback) → assert typed columns `id="A1"`, `alt="350"`.
     - **AIXM/namespaced:** a minimal AIXM fragment with `gml:id` attribute and `xlink:href`
       element, with a `namespace_map`, `columns`, and `raw_column` config → assert typed columns
       extracted correctly AND `raw_column` contains the original XML subtree bytes.
     - **Passthrough output:** encoder with `passthrough_column` set emits the VARCHAR value
       verbatim (no escaping, no wrapping).
     - **Constructed output:** encoder without `passthrough_column` produces well-formed XML with
       declared namespace prefixes on the root element.
   - `cargo test -p dbsp_adapters --features with-solace,with-xml xml`
2. **Local pipeline smoke test** against the running stack:
   - Bring up `docker compose up -d` in `faa-continuum-feldera` (rebuild fork image first).
   - Create a scratch pipeline (`PUT /v0/pipelines/xml-smoke`) with one
     `CREATE TABLE ... WITH (... "format":{"name":"xml","config":{"namespace_map":{...},"row_element":"aixm:Airspace","columns":[...]}})` bound to a `solace_input` queue, and one
     `CREATE MATERIALIZED VIEW ... "format":{"name":"xml"}` bound to a `solace_output` topic.
   - Publish the real `DN_SAA.NEW_SAR.xml` AIXM NOTAM sample to the input queue, subscribe to
     the output topic, and confirm extracted column values (airspace type `"R"`, altitude `"500"`)
     appear correctly. `POST .../query?sql=SELECT * FROM <view>` for additional verification.
   - Tear down (`stop?force=true` → `clear` → `DELETE`).

## Out of scope / notes
- No changes to the Solace connector, controller, SQL engine, or `faa_program.sql`.
- No `RecordFormat::Xml` variant — routing through `RecordFormat::Json` internally is sufficient
  and avoids touching `feldera-adapterlib`.
- XPath is intentionally out of scope — the config-driven dot-path + `@attr` syntax covers
  FIXM/AIXM extraction without needing an XPath engine. Full XPath (sxd-xpath or libxml2) can be
  added later if sub-element filtering or predicates are needed.
- The fork's `default` features already include `with-solace`; adding `with-xml` there keeps the
  shipped image self-consistent.
- `roxmltree` confirmed correct for FIXM/AIXM: `node.attribute(("http://www.opengis.net/gml/3.2", "id"))` resolves `gml:id` by namespace URI — verified against real AIXM NOTAM sample
  (`DN_SAA.NEW_SAR.xml`, official EUROCONTROL/FAA). `quick-xml` handles output serialization.
  Both are pure Rust — no native library dependencies added to the Docker build.
