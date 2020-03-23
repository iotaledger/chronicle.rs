
actor!(
    HelperBuilder {
        nodes: Vec<String>
});

impl HelperBuilder {

    pub fn build(self) -> Helper {
        Helper {
            nodes: self.nodes.unwrap(),
        }
    }
}

enum HelperEvent {

}

pub struct Helper {
    nodes: Vec<String>,
    // dashboard_tx
}
impl Helper {
    pub async fn run(mut self) {
        // create channel

    }
}
