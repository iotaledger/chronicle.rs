use crate::get_config_async;

pub async fn send_alert(msg: String) -> anyhow::Result<()> {
    let config = get_config_async().await;
    let client = reqwest::Client::new();
    let mut errors = Vec::new();
    for request in config.alert_config.requests.iter() {
        if let Some(mut json) = request.json.clone() {
            // Create a stack of references so we can search the json for $msg tokens
            let mut values = vec![&mut json];
            // Iterate the values looking for strings to replace tokens in
            while let Some(value) = values.pop() {
                match value {
                    ron::Value::String(s) => {
                        *s = s.replace("$msg", &msg);
                    }
                    ron::Value::Map(m) => {
                        values.extend(m.values_mut());
                    }
                    ron::Value::Option(o) => {
                        if let Some(v) = o {
                            values.push(v.as_mut());
                        }
                    }
                    ron::Value::Seq(v) => {
                        values.extend(v.iter_mut());
                    }
                    _ => (),
                }
            }
            match client.post(request.url.clone()).json(&json).send().await {
                Ok(res) => {
                    if !res.status().is_success() {
                        let txt = format!(
                            "{}: {}",
                            res.status(),
                            res.text().await.unwrap_or("Response text unavailable!".to_owned())
                        );
                        log::error!("{}", txt);
                        errors.push(anyhow::anyhow!(txt))
                    }
                }
                Err(e) => errors.push(anyhow::anyhow!(e)),
            }
        } else {
            if let Err(e) = client.post(request.url.clone()).body(msg.clone()).send().await {
                errors.push(anyhow::anyhow!(e));
            }
        }
    }
    if !errors.is_empty() {
        anyhow::bail!("Errors while sending notifications: {:#?}", errors);
    }
    Ok(())
}

#[macro_export]
macro_rules! alert {
    ($lit:literal $(,)?) => ({
        log::error!($lit);
        $crate::send_alert(format!($lit))
    });
    ($err:expr $(,)?) => ({
        log::error!("{}", $err);
        $crate::send_alert($err.into())
    });
    ($fmt:expr, $($arg:tt)*) => ({
        log::error!($fmt, $($arg)*);
        $crate::send_alert(format!($fmt, $($arg)*))
    });
}
