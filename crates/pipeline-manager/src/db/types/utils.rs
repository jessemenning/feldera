use crate::db::error::DBError;
use crate::db::types::program::{ProgramConfig, ProgramInfo};
use crate::pipeline_env::validate_pipeline_env;
use feldera_types::config::{PipelineConfig, RuntimeConfig};
use feldera_types::runtime_status::StorageStatusDetails;
use regex::Regex;
use serde::Serialize;
use thiserror::Error as ThisError;
use tracing::error;

// Utility functions related to types which are stored in the database.
// The functions center around serialization, deserialization and validation.

/// Pattern that every name must adhere to.
pub const PATTERN_VALID_NAME: &str = r"^[a-zA-Z0-9_-]+$";

/// Maximum name length.
pub const MAXIMUM_NAME_LENGTH: usize = 100;

/// Pattern that every tag must adhere to: one or more ASCII letters (a-z,
/// A-Z), digits (0-9), or one of space, `.`, `_`, `/`, `|`, `\`, `:`, `=` and
/// `-`.
pub const PATTERN_VALID_TAG: &str = r"^[a-zA-Z0-9 ._/|\\:=-]+$";

/// Maximum length of a single tag, in characters.
pub const MAXIMUM_TAG_LENGTH: usize = 50;

/// Maximum number of tags a single pipeline may carry.
pub const MAXIMUM_TAGS_PER_PIPELINE: usize = 50;

/// Maximum length of a pipeline description, in characters.
pub const MAXIMUM_DESCRIPTION_LENGTH: usize = 300;

/// Checks whether the provided name is valid.
/// The constraints are as follows:
/// - It cannot be empty
/// - It must be at most 100 characters long
/// - It must contain only characters which are lowercase (a-z), uppercase (A-Z),
//    numbers (0-9), underscores (_) or hyphens (-)
pub fn validate_name(name: &str) -> Result<(), DBError> {
    if name.is_empty() {
        Err(DBError::EmptyName)
    } else if name.len() > MAXIMUM_NAME_LENGTH {
        Err(DBError::TooLongName {
            name: name.to_string(),
            length: name.len(),
            maximum: MAXIMUM_NAME_LENGTH,
        })
    } else {
        let re = Regex::new(PATTERN_VALID_NAME).expect("Pattern for name must be valid");
        if re.is_match(name) {
            Ok(())
        } else {
            Err(DBError::NameDoesNotMatchPattern {
                name: name.to_string(),
            })
        }
    }
}

/// Checks whether the provided description is valid: it must be at most
/// [`MAXIMUM_DESCRIPTION_LENGTH`] characters long. The content is otherwise
/// free-form.
pub fn validate_description(description: &str) -> Result<(), DBError> {
    let length = description.chars().count();
    if length > MAXIMUM_DESCRIPTION_LENGTH {
        Err(DBError::TooLongDescription {
            length,
            maximum: MAXIMUM_DESCRIPTION_LENGTH,
        })
    } else {
        Ok(())
    }
}

/// Checks whether the provided tags are valid. There may be at most
/// [`MAXIMUM_TAGS_PER_PIPELINE`] tags, and each tag:
/// - cannot be empty;
/// - must be at most [`MAXIMUM_TAG_LENGTH`] characters long;
/// - must contain only ASCII letters (a-z, A-Z), digits (0-9), or one of
///   space, `.`, `_`, `/`, `|`, `\`, `:`, `=` and `-`.
pub fn validate_tags(tags: &[String]) -> Result<(), DBError> {
    if tags.len() > MAXIMUM_TAGS_PER_PIPELINE {
        return Err(DBError::TooManyTags {
            count: tags.len(),
            maximum: MAXIMUM_TAGS_PER_PIPELINE,
        });
    }
    let re = Regex::new(PATTERN_VALID_TAG).expect("Pattern for tag must be valid");
    for tag in tags {
        let length = tag.chars().count();
        let reason = if tag.is_empty() {
            Some("it cannot be empty".to_string())
        } else if length > MAXIMUM_TAG_LENGTH {
            Some(format!(
                "it is {length} characters long, but the maximum is {MAXIMUM_TAG_LENGTH}"
            ))
        } else if !re.is_match(tag) {
            Some(
                "it may contain only ASCII letters (a-z, A-Z), numbers (0-9), or one of space, \
                 '.', '_', '/', '|', '\\', ':', '=' and '-'"
                    .to_string(),
            )
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(DBError::InvalidTag {
                tag: tag.clone(),
                reason,
            });
        }
    }
    Ok(())
}

/// Errors that can happen when deserializing a generic JSON value
/// to its actual object and performing any validation.
#[derive(Debug, Serialize, ThisError)]
pub enum ValidationError {
    #[error("could not deserialize due to: {0}")]
    DeserializationFailed(String),
    #[error("enterprise feature: {0}")]
    EnterpriseFeature(String),
    #[error("invalid pipeline environment: {0}")]
    InvalidPipelineEnv(String),
}

/// Deserializes generic JSON value into [`RuntimeConfig`] and performs any additional validation.
/// It should log an error if it was not used to validate initial user input.
pub(crate) fn validate_runtime_config(
    value: &serde_json::Value,
    log_if_invalid: bool,
) -> Result<RuntimeConfig, ValidationError> {
    let deserialize_result = serde_json::from_value::<RuntimeConfig>(value.clone())
        .map_err(|e| ValidationError::DeserializationFailed(e.to_string()));
    match deserialize_result {
        Ok(runtime_config) => {
            #[cfg(not(feature = "feldera-enterprise"))]
            if runtime_config.fault_tolerance.is_enabled() {
                let e = ValidationError::EnterpriseFeature("fault tolerance".to_string());
                if log_if_invalid {
                    error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid runtime configuration due to: {e}");
                }
                return Err(e);
            }
            if let Err(e) = validate_pipeline_env(&runtime_config.env) {
                let e = ValidationError::InvalidPipelineEnv(e);
                if log_if_invalid {
                    error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid runtime configuration due to: {e}");
                }
                return Err(e);
            }
            Ok(runtime_config)
        }
        Err(e) => {
            if log_if_invalid {
                error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid runtime configuration due to: {e}");
            }
            Err(e)
        }
    }
}

/// Deserializes generic JSON value into [`ProgramConfig`] and performs any additional validation.
/// It should log an error if it was not used to validate initial user input.
pub(crate) fn validate_program_config(
    value: &serde_json::Value,
    log_if_invalid: bool,
) -> Result<ProgramConfig, ValidationError> {
    let deserialize_result = serde_json::from_value(value.clone())
        .map_err(|e| ValidationError::DeserializationFailed(e.to_string()));
    if let Err(e) = &deserialize_result {
        if log_if_invalid {
            error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid program configuration due to: {e}");
        }
    }
    deserialize_result
}

/// Deserializes the generic JSON value into [`ProgramInfo`] and performs any additional validation.
pub(crate) fn validate_program_info(
    value: &serde_json::Value,
) -> Result<ProgramInfo, ValidationError> {
    let deserialize_result = serde_json::from_value(value.clone())
        .map_err(|e| ValidationError::DeserializationFailed(e.to_string()));
    if let Err(e) = &deserialize_result {
        error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid program information due to: {e}");
    }
    deserialize_result
}

/// Deserializes the generic JSON value into [`PipelineConfig`] and performs any additional validation.
pub(crate) fn validate_deployment_config(
    value: &serde_json::Value,
) -> Result<PipelineConfig, ValidationError> {
    let deserialize_result = serde_json::from_value::<PipelineConfig>(value.clone())
        .map_err(|e| ValidationError::DeserializationFailed(e.to_string()));
    match deserialize_result {
        Ok(deployment_config) => {
            if let Err(e) = validate_pipeline_env(&deployment_config.global.env) {
                let e = ValidationError::InvalidPipelineEnv(e);
                error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid deployment configuration due to: {e}");
                Err(e)
            } else {
                Ok(deployment_config)
            }
        }
        Err(e) => {
            error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid deployment configuration due to: {e}");
            Err(e)
        }
    }
}

/// Deserializes the generic JSON value into [`StorageStatusDetails`] and performs any additional validation.
pub(crate) fn validate_storage_status_details(
    value: &serde_json::Value,
) -> Result<StorageStatusDetails, ValidationError> {
    let deserialize_result = serde_json::from_value(value.clone())
        .map_err(|e| ValidationError::DeserializationFailed(e.to_string()));
    if let Err(e) = &deserialize_result {
        error!("Backward incompatibility detected: the following JSON:\n{value:#}\n\n... is no longer a valid storage status details due to: {e}");
    }
    deserialize_result
}

#[cfg(test)]
mod tests {
    use super::{
        validate_deployment_config, validate_description, validate_name, validate_program_config,
        validate_program_info, validate_runtime_config, validate_tags, ValidationError,
        MAXIMUM_DESCRIPTION_LENGTH, MAXIMUM_TAGS_PER_PIPELINE, MAXIMUM_TAG_LENGTH,
    };
    use crate::db::error::DBError;
    use crate::db::types::program::{CompilationProfile, ProgramConfig, ProgramInfo};
    use feldera_types::config::{PipelineConfig, RuntimeConfig};
    use feldera_types::program_schema::ProgramSchema;
    use serde_json::json;

    #[test]
    fn test_valid_names() {
        let valid = vec![
            "a",
            "z",
            "A",
            "Z",
            "0",
            "9",
            "-",
            "_",
            "exampleExample",
            "example-1",
            "example-of-this",
            "Aa0_-",
            "example_2",
            "Example",
            "EXAMPLE_example-example1234",
        ];
        for name in valid {
            assert!(validate_name(name).is_ok());
        }
    }

    #[test]
    fn test_invalid_names() {
        assert!(matches!(validate_name(""), Err(DBError::EmptyName)));
        assert!(
            matches!(validate_name("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), Err(DBError::TooLongName {
            name, length, maximum
        }) if name == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" && length == 101 && maximum == 100)
        );
        let invalid_due_to_pattern = vec!["%", "$", "abc@", "example example", " ", "%20"];
        for invalid_name in invalid_due_to_pattern {
            assert!(
                matches!(validate_name(invalid_name), Err(DBError::NameDoesNotMatchPattern {
                name
            }) if name == invalid_name)
            );
        }
    }

    #[test]
    fn test_valid_tags() {
        let max_length = "a".repeat(MAXIMUM_TAG_LENGTH);
        let valid = vec![
            "a",
            "Z",
            "0",
            "prod",
            "team-billing",
            "env/staging",
            "a.b.c",
            "with space",
            "pipe|sep",
            "back\\slash",
            "key:value",
            "key=value",
            "Mixed-1_2/3|4.5:6=7 8",
            max_length.as_str(),
        ];
        for tag in valid {
            assert!(
                validate_tags(&[tag.to_string()]).is_ok(),
                "expected '{tag}' to be valid"
            );
        }
        // An empty list (no tags) is valid.
        assert!(validate_tags(&[]).is_ok());
        // Several valid tags together.
        assert!(validate_tags(&["a".to_string(), "b/c".to_string()]).is_ok());
        // Up to the maximum number of tags is valid.
        let max_tags: Vec<String> = (0..MAXIMUM_TAGS_PER_PIPELINE)
            .map(|i| format!("tag{i}"))
            .collect();
        assert!(validate_tags(&max_tags).is_ok());
    }

    #[test]
    fn test_invalid_tags() {
        // Empty tag.
        assert!(matches!(
            validate_tags(&["".to_string()]),
            Err(DBError::InvalidTag { tag, .. }) if tag.is_empty()
        ));
        // Too long.
        let too_long = "a".repeat(MAXIMUM_TAG_LENGTH + 1);
        assert!(matches!(
            validate_tags(&[too_long.clone()]),
            Err(DBError::InvalidTag { tag, .. }) if tag == too_long
        ));
        // Disallowed characters.
        for invalid in ["%", "café", "a,b", "tab\tchar", "emoji😀", "a;b", "a@b"] {
            assert!(
                matches!(
                    validate_tags(&[invalid.to_string()]),
                    Err(DBError::InvalidTag { tag, .. }) if tag == invalid
                ),
                "expected '{invalid}' to be rejected"
            );
        }
        // A single bad tag among good ones is rejected.
        assert!(matches!(
            validate_tags(&["ok".to_string(), "bad,tag".to_string()]),
            Err(DBError::InvalidTag { tag, .. }) if tag == "bad,tag"
        ));
        // More than the maximum number of tags is rejected, even when every
        // individual tag is valid.
        let too_many: Vec<String> = (0..MAXIMUM_TAGS_PER_PIPELINE + 1)
            .map(|i| format!("tag{i}"))
            .collect();
        assert!(matches!(
            validate_tags(&too_many),
            Err(DBError::TooManyTags { count, maximum })
                if count == MAXIMUM_TAGS_PER_PIPELINE + 1 && maximum == MAXIMUM_TAGS_PER_PIPELINE
        ));
    }

    #[test]
    fn test_description_length() {
        // Empty and up-to-the-limit descriptions are accepted.
        assert!(validate_description("").is_ok());
        assert!(validate_description("a short description").is_ok());
        assert!(validate_description(&"a".repeat(MAXIMUM_DESCRIPTION_LENGTH)).is_ok());
        // Length is counted in characters, not bytes: a multi-byte string of
        // MAXIMUM_DESCRIPTION_LENGTH characters is still accepted.
        assert!(validate_description(&"é".repeat(MAXIMUM_DESCRIPTION_LENGTH)).is_ok());
        // One character over the limit is rejected.
        let too_long = "a".repeat(MAXIMUM_DESCRIPTION_LENGTH + 1);
        assert!(matches!(
            validate_description(&too_long),
            Err(DBError::TooLongDescription { length, maximum })
                if length == MAXIMUM_DESCRIPTION_LENGTH + 1 && maximum == MAXIMUM_DESCRIPTION_LENGTH
        ));
    }

    #[test]
    fn runtime_config_validation() {
        // RuntimeConfig -> JSON -> RuntimeConfig is the same as original
        let runtime_config = RuntimeConfig::default();
        let value = serde_json::to_value(runtime_config.clone()).unwrap();
        assert_eq!(
            runtime_config,
            validate_runtime_config(&value, true).unwrap()
        );

        // Invalid JSON for RuntimeConfig
        assert!(matches!(
            validate_runtime_config(&json!({ "workers": "not-a-number" }), true),
            Err(ValidationError::DeserializationFailed(_))
        ));

        #[cfg(feature = "feldera-enterprise")]
        assert!(
            validate_runtime_config(&json!({ "fault_tolerance": {} }), true)
                .unwrap()
                .fault_tolerance
                .model
                .is_some()
        );

        #[cfg(not(feature = "feldera-enterprise"))]
        assert!(matches!(
            validate_runtime_config(&json!({ "fault_tolerance": {} }), true),
            Err(ValidationError::EnterpriseFeature(s)) if s == "fault tolerance"
        ));

        assert!(matches!(
            validate_runtime_config(&json!({ "env": { "TOKIO_WORKER_THREADS": "1" } }), true),
            Err(ValidationError::InvalidPipelineEnv(_))
        ));
    }

    #[test]
    fn program_config_validation() {
        // ProgramConfig -> JSON -> ProgramConfig is the same as original
        let program_config = ProgramConfig {
            profile: Some(CompilationProfile::Unoptimized),
            cache: false,
            runtime_version: None,
        };
        let value = serde_json::to_value(program_config.clone()).unwrap();
        assert_eq!(
            program_config,
            validate_program_config(&value, true).unwrap()
        );

        // Invalid JSON for ProgramConfig
        assert!(matches!(
            validate_program_config(&json!({ "profile": "non-existent-profile" }), true),
            Err(ValidationError::DeserializationFailed(_))
        ));
    }

    #[test]
    fn program_info_validation() {
        // ProgramInfo -> JSON -> ProgramInfo is the same as original
        let program_info = ProgramInfo {
            schema: serde_json::to_value(ProgramSchema {
                inputs: vec![],
                outputs: vec![],
            })
            .unwrap(),
            main_rust: "".to_string(),
            udf_stubs: "".to_string(),
            input_connectors: Default::default(),
            output_connectors: Default::default(),
            dataflow: None,
        };
        let value = serde_json::to_value(program_info.clone()).unwrap();
        assert_eq!(program_info, validate_program_info(&value).unwrap());

        // Invalid JSON for ProgramInfo
        assert!(matches!(
            validate_program_info(&json!({ "main_rust": 123 })),
            Err(ValidationError::DeserializationFailed(_))
        ));
    }

    #[test]
    fn deployment_config_validation() {
        // PipelineConfig -> JSON -> PipelineConfig is the same as original
        let deployment_config = PipelineConfig {
            global: Default::default(),
            multihost: None,
            name: None,
            given_name: None,
            storage_config: None,
            secrets_dir: None,
            inputs: Default::default(),
            outputs: Default::default(),
            program_ir: None,
        };
        let value = serde_json::to_value(deployment_config.clone()).unwrap();
        assert_eq!(
            deployment_config,
            validate_deployment_config(&value).unwrap()
        );

        // Invalid JSON for PipelineConfig
        assert!(matches!(
            validate_deployment_config(&json!({ "name": 123 })),
            Err(ValidationError::DeserializationFailed(_))
        ));

        assert!(matches!(
            validate_deployment_config(
                &json!({ "workers": 8, "env": { "TOKIO_WORKER_THREADS": "1" } })
            ),
            Err(ValidationError::InvalidPipelineEnv(_))
        ));
    }
}
