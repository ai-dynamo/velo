// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! One error type for every route, mapped to [`ErrorBody`].

use axum::Json;
use axum::response::{IntoResponse, Response};

use super::authority::AuthorityError;
use super::extension::Rejection;
use crate::protocol::{ErrorBody, ErrorCode};
use crate::store::StoreError;

/// A route's failure: an [`ErrorCode`] and a message. Extension routes may
/// return it too.
#[derive(Debug, Clone)]
pub struct HubError {
    code: ErrorCode,
    message: String,
    /// See [`ErrorBody::not_ready`].
    not_ready: bool,
}

impl HubError {
    /// An error with `code`.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            not_ready: false,
        }
    }

    /// 503 because the hub is not ready, before it did anything for the
    /// request: a client with more than one host tries the next one. Only
    /// for answers that changed nothing; see [`ErrorBody::not_ready`].
    pub(crate) fn not_ready(message: impl Into<String>) -> Self {
        Self {
            not_ready: true,
            ..Self::new(ErrorCode::Unavailable, message)
        }
    }

    /// 400.
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::BadRequest, message)
    }

    /// 404.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }

    /// 503.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unavailable, message)
    }

    /// 500.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }

    /// The code.
    pub fn code(&self) -> ErrorCode {
        self.code
    }
}

impl std::fmt::Display for HubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for HubError {}

impl From<AuthorityError> for HubError {
    fn from(error: AuthorityError) -> Self {
        let code = match error {
            AuthorityError::Unauthorized { .. } => ErrorCode::Unauthorized,
            AuthorityError::Busy { .. } => ErrorCode::Busy,
            AuthorityError::NotFound { .. } => ErrorCode::NotFound,
            AuthorityError::StateChanged { .. } => ErrorCode::RegisterAgain,
            AuthorityError::Recovered { .. } => ErrorCode::Recovered,
            // Authority answers before any write: the store is not live.
            AuthorityError::Unavailable => return Self::not_ready(error.to_string()),
        };
        Self::new(code, error.to_string())
    }
}

impl From<StoreError> for HubError {
    fn from(error: StoreError) -> Self {
        let code = match error {
            StoreError::Conflict { .. } | StoreError::Stale { .. } => ErrorCode::Conflict,
            StoreError::NotFound(_) => ErrorCode::NotFound,
            StoreError::Indeterminate(_) => ErrorCode::Unavailable,
            StoreError::HookInstalled | StoreError::Backend(_) => ErrorCode::Internal,
        };
        Self::new(code, format!("store: {error}"))
    }
}

impl From<Rejection> for HubError {
    fn from(rejection: Rejection) -> Self {
        Self::new(rejection.code, rejection.message)
    }
}

impl IntoResponse for HubError {
    fn into_response(self) -> Response {
        (
            self.code.status(),
            Json(ErrorBody {
                code: self.code,
                message: self.message,
                not_ready: self.not_ready,
            }),
        )
            .into_response()
    }
}
