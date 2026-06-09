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

#[cfg(test)]
mod tests {
    use super::parse_topic_fields;

    #[test]
    fn single_field() {
        assert_eq!(
            parse_topic_fields("demo/{region}", "demo/us-east"),
            vec![("region".to_string(), "us-east".to_string())]
        );
    }

    #[test]
    fn multiple_fields() {
        assert_eq!(
            parse_topic_fields("demo/events/{region}/{event_type}", "demo/events/us-east/order"),
            vec![
                ("region".to_string(), "us-east".to_string()),
                ("event_type".to_string(), "order".to_string()),
            ]
        );
    }

    #[test]
    fn static_segments_produce_no_captures() {
        assert!(parse_topic_fields("a/b/c", "a/b/c").is_empty());
    }

    #[test]
    fn mixed_static_and_named() {
        assert_eq!(
            parse_topic_fields("app/v1/{tenant}/events", "app/v1/acme/events"),
            vec![("tenant".to_string(), "acme".to_string())]
        );
    }

    #[test]
    fn pattern_longer_than_topic_stops_gracefully() {
        // zip stops at the shorter iterator — no panic.
        assert_eq!(
            parse_topic_fields("a/{b}/{c}", "a/x"),
            vec![("b".to_string(), "x".to_string())]
        );
    }

    #[test]
    fn topic_longer_than_pattern_ignores_extra_levels() {
        assert_eq!(
            parse_topic_fields("a/{b}", "a/x/y/z"),
            vec![("b".to_string(), "x".to_string())]
        );
    }

    #[test]
    fn empty_brace_name_is_skipped() {
        // `{}` has an empty name — must not produce a capture.
        assert!(parse_topic_fields("{}/end", "val/end").is_empty());
    }

    #[test]
    fn empty_pattern_and_topic() {
        assert!(parse_topic_fields("", "").is_empty());
    }
}
