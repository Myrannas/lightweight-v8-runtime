use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use hyper::client::HttpConnector;
use hyper::{Body, Client, Request, Response, StatusCode, Uri};
use serde::de::DeserializeOwned;
use serde::export::fmt::Error;
use serde::export::Formatter;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::fmt;
use std::fmt::Display;
use std::io::Bytes;
use std::str::FromStr;

pub struct AWSRuntimeAPIClient {
    runtime_api_url: String,
    client: Client<HttpConnector>,
}

pub struct RequestId(String);
pub struct Invocation<T> {
    request_id: RequestId,
    payload: T,
}

const REQUEST_ID_HEADER: &str = "Lambda-Runtime-Aws-Request-Id";
const LAMBDA_RUNTIME_API: &str = "AWS_LAMBDA_RUNTIME_API";

impl RequestId {
    fn from_request<T>(result: &Response<T>) -> Result<RequestId> {
        let request_id_header = result.headers().get(REQUEST_ID_HEADER).with_context(|| {
            format!(
                "Missing header {} from next unit of work response",
                REQUEST_ID_HEADER
            )
        })?;

        let request_id = request_id_header
            .to_str()
            .with_context(|| format!("Failed to parse header {}", REQUEST_ID_HEADER))?
            .to_string();

        Ok(RequestId(request_id))
    }
}

impl AWSRuntimeAPIClient {
    pub fn from_environment() -> Result<AWSRuntimeAPIClient> {
        Ok(AWSRuntimeAPIClient {
            runtime_api_url: std::env::var(LAMBDA_RUNTIME_API).context(format!(
                "Environmental variable {} was missing",
                LAMBDA_RUNTIME_API
            ))?,
            client: Client::new(),
        })
    }

    pub async fn get_next_unit_of_work<T>(&self) -> Result<Invocation<T>>
    where
        T: DeserializeOwned,
    {
        let uri = Uri::from_str(&format!("{}/runtime/invocation/next", self.runtime_api_url))?;

        let request = Request::builder()
            .uri(uri)
            .body(Body::from(""))
            .context("Error parsing URL for next invocation")?;

        let result = self
            .client
            .request(request)
            .await
            .context("Error fetching next unit of work from lambda API")?;

        ensure!(
            result.status() != StatusCode::OK,
            "Invalid status code returned from next unit of work API, got {}",
            result.status()
        );

        let request_id = RequestId::from_request(&result)?;
        let body_data = hyper::body::to_bytes(result.into_body()).await?;
        let result = serde_json::from_slice::<T>(body_data.borrow())?;

        Result::Ok(Invocation {
            payload: result,
            request_id,
        })
    }

    pub async fn report_success<T>(&self, request_id: RequestId, response: T) -> Result<()>
    where
        T: Serialize,
    {
        let uri = Uri::from_str(&format!(
            "{}/runtime/invocation/{}/response",
            self.runtime_api_url, request_id.0
        ))
        .unwrap();

        let request = Request::builder()
            .uri(uri)
            .body(Body::from(serde_json::to_string(&response)?))
            .context("Error fetching next unit of work from lambda API")?;

        let result = self
            .client
            .request(request)
            .await
            .context("Error reporting success to lambda API")?;

        ensure!(
            result.status() != StatusCode::OK,
            "Invalid status code returned from next unit of work API, got {}",
            result.status()
        );

        Result::Ok(())
    }

    pub async fn report_error<T>(&self, request_id: RequestId, error: T) -> Result<()>
    where
        T: Serialize,
    {
        let uri = Uri::from_str(&format!(
            "{}/runtime/invocation/{}/error",
            self.runtime_api_url, request_id.0
        ))
        .unwrap();

        let request = Request::builder()
            .uri(uri)
            .body(Body::from(serde_json::to_string(&error)?))
            .context("Error fetching next unit of work from lambda API")?;

        let result = self
            .client
            .request(request)
            .await
            .context("Error reporting error to lambda API")?;

        ensure!(
            result.status() != StatusCode::OK,
            "Invalid status code returned from next unit of work API, got {}",
            result.status()
        );

        Result::Ok(())
    }
}

pub struct LambdaRuntime<Input, Output> {
    client: AWSRuntimeAPIClient,
    task_handler: Box<dyn Handler<Input, Output>>,
}

#[derive(Serialize, Debug)]
pub enum HandlerError {
    ClientError,
    ServerError(String),
}

impl Display for HandlerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&format!("{:?}", self))
    }
}

#[async_trait]
pub trait Handler<Input, Output> {
    async fn handle(&self, input: &Input) -> Result<Output>;
}

impl<Input, Output> LambdaRuntime<Input, Output>
where
    Input: DeserializeOwned,
    Output: Serialize,
{
    pub fn new(
        client: AWSRuntimeAPIClient,
        task_handler: Box<dyn Handler<Input, Output>>,
    ) -> LambdaRuntime<Input, Output> {
        LambdaRuntime {
            client,
            task_handler,
        }
    }

    pub async fn start(&self) -> Result<()> {
        loop {
            let work = self.client.get_next_unit_of_work::<Input>().await?;

            let task_result = self.task_handler.handle(&work.payload).await;

            match task_result {
                Ok(result) => self.client.report_success(work.request_id, result).await?,
                Err(err) => {
                    let serializable_error = match err.downcast_ref::<HandlerError>() {
                        Some(HandlerError::ClientError) => HandlerError::ClientError,
                        _ => HandlerError::ServerError(format!("{}", err)),
                    };

                    self.client
                        .report_error(work.request_id, serializable_error)
                        .await?
                }
            }
        }
    }
}
