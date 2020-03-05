use crate::node;

macro_rules! set_builder_option_field {
    ($i:ident, $t:ty) => {
        pub fn $i(mut self, $i: $t) -> Self {
            self.$i.replace($i);
            self
        }
    };
}

macro_rules! set_builder_field {
    ($i:ident, $t:ty) => {
        pub fn $i(mut self, $i: $t) -> Self {
            self.$i = $i;
            self
        }
    };
}

pub async fn run(address: &'static str, reporter_count: u8) -> Result<(), std::io::Error> {

    let address: String = String::from(address);
    node::SupervisorBuilder::new()
        .address(address)
        .reporter_count(reporter_count)
        .build()
        .run()
        .await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn run_engine() {
        run("0.0.0.0:9042", 1).await.unwrap();
    }
}
