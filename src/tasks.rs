use rusty_v8 as v8;
use rusty_v8::{Context, Isolate, Local, OwnedIsolate};

struct Tasks {
    state: u64,
}

impl Tasks {
    fn install(&self, isolate: &mut OwnedIsolate, context: Local<Context>) {
        let mut hs = v8::HandleScope::new(isolate);
        let scope = hs.enter();

        let mut cs = v8::ContextScope::new(scope, context);
        let scope = cs.enter();
    }
}
