use super::*;
use rocket::get;
use scylla::ring::*;

#[async_trait]
impl<H: LauncherSender<PermanodeBuilder<H>>> EventLoop<PermanodeSender<H>> for Listener {
    async fn event_loop(
        &mut self,
        status: Result<(), Need>,
        supervisor: &mut Option<PermanodeSender<H>>,
    ) -> Result<(), Need> {
        std::env::set_var("ROCKET_PORT", "3000");
        rocket::ignite()
            .mount(
                "/",
                routes![
                    get_message,
                    get_message_metadata,
                    get_message_children,
                    get_message_by_index
                ],
            )
            .launch()
            .await
            .map_err(|e| Need::Abort)
    }
}

#[get("/messages/<message_id>")]
async fn get_message(message_id: u32) -> String {
    "okay".to_string()
}

#[get("/messages/<message_id>/metadata")]
async fn get_message_metadata(message_id: u32) -> String {
    "okay".to_string()
}

#[get("/messages/<message_id>/children")]
async fn get_message_children(message_id: u32) -> String {
    "okay".to_string()
}

#[get("/messages?<index>")]
async fn get_message_by_index(index: u32) -> String {
    "okay".to_string()
}
