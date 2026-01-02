/// Health check endpoint handler.
///
/// Returns "Ok" if the gateway is running.
#[tracing::instrument]
pub async fn health_check() -> &'static str {
    "Ok"
}

#[cfg(test)]
mod tests {
    use crate::test::router::TestRouter;
    use reqwest::StatusCode;

    #[tokio::test]
    async fn get_request_health_check_succeeds() {
        let router = TestRouter::new();
        let (status_code, body) = router.request("/health").await;
        assert_eq!(status_code, StatusCode::OK);
        assert_eq!(body, "Ok");
    }
}
