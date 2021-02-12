use super::*;
use std::borrow::Cow;

#[async_trait]
impl<H> Starter<H> for PermanodeBuilder<H>
where
    H: LauncherSender<PermanodeBuilder<H>>,
{
    type Ok = PermanodeSender<H>;

    type Error = Cow<'static, str>;

    type Input = Permanode<H>;

    async fn starter(self, handle: H, input: Option<Self::Input>) -> Result<Self::Ok, Self::Error> {
        let permanode = self.build();

        let supervisor = permanode.sender.clone();

        tokio::spawn(permanode.start(Some(handle)));

        Ok(supervisor)
    }
}
