use super::Result;
use mlua::prelude::*;
use reqwest::{header::HeaderMap, Client, Method, RequestBuilder, Url};
use std::{collections::HashMap, fs::File, io::Read, path::Path};

const WRK_SCRIPT: &str = r#"
    wrk = {}
    function wrk.format(method, path, headers, body)
        headers["Content-Length"] = body and string.len(body)
        local request = {}
        request["method"]  = method
        request["path"]    = path
        request["headers"] = headers
        request["body"]    = body
        return request
    end
"#;

pub fn new_state(path: &Path) -> Result<Lua> {
    let lua = unsafe { Lua::unsafe_new() };
    lua.load(WRK_SCRIPT).exec()?;

    let mut script = String::new();
    File::open(path)?.read_to_string(&mut script)?;
    lua.load(&script).exec()?;
    Ok(lua)
}

pub fn build_request(client: &Client, mut url: Url, lua: &Lua) -> Result<RequestBuilder> {
    let t = lua.load("request()").eval::<LuaTable>().unwrap();

    let method: Method = t.get::<_, String>("method")?.parse()?;
    let path: Url = t.get::<_, String>("path")?.parse()?;
    let headers: HeaderMap = (&t.get::<_, HashMap<String, String>>("headers")?).try_into()?;
    let body = t.get::<_, Option<String>>("body")?;
    url.set_path(path.path());
    url.set_query(path.query());
    url.set_fragment(path.fragment());

    let mut builder = client.request(method, url).headers(headers);
    if let Some(body) = body {
        builder = builder.body(body);
    }
    Ok(builder)
}
