use crate::aws::Handler;
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use rusty_v8 as v8;
use rusty_v8::{Isolate, Local, ToLocal, Value};
use std::convert::TryInto;

use tokio;

mod aws;
mod tasks;

pub fn script_origin<'a>(
    s: &mut impl v8::ToLocal<'a>,
    resource_name: v8::Local<'a, v8::String>,
) -> v8::ScriptOrigin<'a> {
    let resource_line_offset = v8::Integer::new(s, 0);
    let resource_column_offset = v8::Integer::new(s, 0);
    let resource_is_shared_cross_origin = v8::Boolean::new(s, false);
    let script_id = v8::Integer::new(s, 123);
    let source_map_url = v8::String::new(s, "source_map_url").unwrap();
    let resource_is_opaque = v8::Boolean::new(s, true);
    let is_wasm = v8::Boolean::new(s, false);
    let is_module = v8::Boolean::new(s, false);
    v8::ScriptOrigin::new(
        resource_name.into(),
        resource_line_offset,
        resource_column_offset,
        resource_is_shared_cross_origin,
        script_id,
        source_map_url.into(),
        resource_is_opaque,
        is_wasm,
        is_module,
    )
}

fn execute_global_function<'s>(
    scope: &mut impl v8::ToLocal<'s>,
    context: v8::Local<v8::Context>,
    name: &str,
    parameter: v8::Local<v8::Value>,
) -> Result<String> {
    let accessor = v8::String::new(scope, name).unwrap();

    let reference = match context.global(scope).get(scope, context, accessor.into()) {
        Some(function) if function.is_function() => function,
        _ => bail!("Reference {} was not a function", name),
    };

    let function: v8::Local<v8::Function> = reference.try_into().unwrap();

    let undefined = v8::undefined(scope).into();

    let mut try_catch = v8::TryCatch::new(scope);
    let ts = try_catch.enter();
    let result = function.call(scope, context, undefined, &vec![parameter]);

    let p: v8::Local<v8::Promise> = result.into();
    p.then()

    match result {
        Some(value) => {
            let result_string: v8::Local<v8::String> = value.try_into()?;
            Ok(result_string.to_rust_string_lossy(scope))
        }
        None => bail!("Encountered exception"),
    }
}

struct JSPlatform {
    platform: *mut v8::Platform,
}

unsafe impl Sync for JSPlatform {}
impl JSPlatform {
    fn new() -> JSPlatform {
        JSPlatform {
            platform: v8::new_default_platform(),
        }
    }

    fn initialize_platform(&self) {
        v8::V8::initialize_platform(self.platform);
    }

    fn pump_message_loop(&self, isolate: &mut Isolate) {
        v8::pump_message_loop(self.platform, isolate);
    }
}

struct JSHandler {
    platform: JSPlatform,
    function: String,
}

impl JSHandler {
    fn new(entry_point: &str) -> Result<JSHandler> {
        let platform = JSPlatform::new();
        platform.initialize_platform();
        v8::V8::initialize();

        let function = std::fs::read_to_string(entry_point)
            .with_context(|| format!("Unable to load handler entrypoint {}", entry_point))?;

        Ok(JSHandler { platform, function })
    }
}

trait Transfer {
    fn transfer<'sc>(&self, cs: &mut impl ToLocal<'sc>) -> Result<v8::Local<'sc, v8::Value>>;
}

impl Transfer for serde_json::Value {
    fn transfer<'sc>(&self, sc: &mut impl ToLocal<'sc>) -> Result<Local<'sc, Value>> {
        Ok(match self {
            serde_json::Value::String(str) => {
                v8::Local::from(v8::String::new(sc, str).unwrap()) // TODO: Handle error
            }
            serde_json::Value::Object(_m) => v8::Local::from(v8::Object::new(sc)),
            _ => v8::Local::from(v8::undefined(sc)),
        })
    }
}

#[async_trait]
impl Handler<serde_json::Value, serde_json::Value> for JSHandler {
    async fn handle(&self, input: &serde_json::Value) -> Result<serde_json::Value> {
        let mut isolate_creation_params = v8::Isolate::create_params();
        isolate_creation_params.set_array_buffer_allocator(v8::new_default_allocator());

        let mut isolate = v8::Isolate::new(isolate_creation_params);

        let mut hs = v8::HandleScope::new(&mut isolate);
        let scope = hs.enter();

        let context = v8::Context::new(scope);
        let mut cs = v8::ContextScope::new(scope, context);
        let scope = cs.enter();

        let script_source = v8::String::new(scope, &self.function).unwrap();

        let mut script = v8::Script::compile(scope, context, script_source, None).unwrap();
        script.run(scope, context);

        let input_v8 = input.transfer(scope).unwrap(); // TODO: Better error handling
        let result = execute_global_function(scope, context, "a", input_v8)?;

        self.platform.pump_message_loop(scope.isolate());

        Ok(serde_json::Value::String(result))
    }
}

#[tokio::main(max_threads = 1)]
async fn main() -> Result<()> {
    // let api_client =
    //     AWSRuntimeAPIClient::from_environment().context("API Client initialization error")?;
    //
    // let runtime = LambdaRuntime::new(api_client, Box::new(JSHandler::new()));
    //
    // Ok(runtime
    //     .start()
    //     .await
    //     .context("Aborting event loop due to API Client execution error")?)

    let handler = JSHandler::new("test_code.js")?;

    let input = serde_json::Value::String("input".to_string());
    let result = handler.handle(&input).await?;

    println!("{}", result);

    Ok(())
}
