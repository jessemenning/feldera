// Config struct lives in feldera-types so it appears in the OpenAPI schema.
pub use feldera_types::transport::solace::SolaceInputConfig;

/// Extract named captures from a Solace destination topic using a pattern.
///
/// Pattern: `"demo/events/{region}/{event_type}"`
/// Topic:   `"demo/events/us-east/order"`
/// Returns: `[("region", "us-east"), ("event_type", "order")]`
///
/// Segments without braces are static and produce no captures.
pub fn parse_topic_fields(pattern: &str, topic: &str) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    for (pat_seg, topic_seg) in pattern.split('/').zip(topic.split('/')) {
        if pat_seg.starts_with('{') && pat_seg.ends_with('}') {
            let name = &pat_seg[1..pat_seg.len() - 1];
            if !name.is_empty() {
                fields.push((name.to_string(), topic_seg.to_string()));
            }
        }
    }
    fields
}
