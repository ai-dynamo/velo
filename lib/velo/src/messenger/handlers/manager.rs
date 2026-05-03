// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Handler registration manager for the messenger active message system.

use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;

use super::{ActiveMessageDispatcher, Handler};

/// Manager for registering handlers with the dispatcher.
///
/// Inserts directly into the shared DashMap so registration is immediately
/// visible to the dispatcher — no async task needs to run first.
#[derive(Clone)]
pub(crate) struct HandlerManager {
    handlers: Arc<DashMap<String, Arc<dyn ActiveMessageDispatcher>>>,
}

impl HandlerManager {
    /// Create a new handler manager backed by the given DashMap.
    pub(crate) fn new(handlers: Arc<DashMap<String, Arc<dyn ActiveMessageDispatcher>>>) -> Self {
        Self { handlers }
    }

    /// Register a public handler (handler names cannot start with `_`).
    pub fn register_handler(&self, handler: Handler) -> Result<()> {
        let name = handler.name();

        if name.starts_with('_') {
            anyhow::bail!(
                "Handler name '{}' cannot start with '_'. System handlers must be registered via internal APIs.",
                name
            );
        }

        self.register_dispatcher(handler.dispatcher)
    }

    /// Register a system/internal handler (allows names starting with `_`).
    pub(crate) fn register_internal_handler(&self, handler: Handler) -> Result<()> {
        self.register_dispatcher(handler.dispatcher)
    }

    /// Insert a dispatcher directly into the shared DashMap.
    fn register_dispatcher(&self, dispatcher: Arc<dyn ActiveMessageDispatcher>) -> Result<()> {
        let name = dispatcher.name().to_string();
        self.handlers.insert(name, dispatcher);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messenger::server::dispatcher::HandlerContext;

    struct MockDispatcher {
        name: String,
    }

    impl ActiveMessageDispatcher for MockDispatcher {
        fn name(&self) -> &str {
            &self.name
        }

        fn dispatch(&self, _ctx: HandlerContext) {
            // No-op for testing
        }
    }

    fn make_manager() -> HandlerManager {
        HandlerManager::new(Arc::new(DashMap::new()))
    }

    #[test]
    fn test_register_handler_blocks_underscore() {
        let manager = make_manager();

        let dispatcher = Arc::new(MockDispatcher {
            name: "_system".to_string(),
        });

        let handler = Handler { dispatcher };

        let result = manager.register_handler(handler);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot start with '_'")
        );
    }

    #[test]
    fn test_register_handler_allows_normal_names() {
        let manager = make_manager();

        let dispatcher = Arc::new(MockDispatcher {
            name: "my_handler".to_string(),
        });

        let handler = Handler { dispatcher };

        let result = manager.register_handler(handler);
        assert!(result.is_ok());
        assert!(manager.handlers.contains_key("my_handler"));
    }

    #[test]
    fn test_register_internal_handler_allows_underscore() {
        let manager = make_manager();

        let dispatcher = Arc::new(MockDispatcher {
            name: "_hello".to_string(),
        });

        let handler = Handler { dispatcher };

        let result = manager.register_internal_handler(handler);
        assert!(result.is_ok());
        assert!(manager.handlers.contains_key("_hello"));
    }

    #[test]
    fn test_register_internal_handler_allows_normal_names() {
        let manager = make_manager();

        let dispatcher = Arc::new(MockDispatcher {
            name: "internal_util".to_string(),
        });

        let handler = Handler { dispatcher };

        let result = manager.register_internal_handler(handler);
        assert!(result.is_ok());
        assert!(manager.handlers.contains_key("internal_util"));
    }
}
