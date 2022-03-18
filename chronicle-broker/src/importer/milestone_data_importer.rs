// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use backstage::core::AbortableUnboundedChannel;
use filter::FilterHandle;

/// MilestoneData Importer state
pub struct MilestoneDataImporter<T: FilterBuilder> {
    filter_handle: T::Handle,
}

impl<T: FilterBuilder> MilestoneDataImporter<T> {
    pub(crate) fn new(filter_handle: T::Handle) -> Self {
        Self { filter_handle }
    }
}

#[async_trait]
impl<T: FilterBuilder> Actor<ImporterHandle> for MilestoneDataImporter<T> {
    type Data = ();
    type Channel = AbortableUnboundedChannel<MilestoneData>;

    async fn init(&mut self, rt: &mut Rt<Self, ImporterHandle>) -> ActorResult<Self::Data> {
        Ok(())
    }

    async fn run(&mut self, rt: &mut Rt<Self, ImporterHandle>, _data: Self::Data) -> ActorResult<()> {
        while let Some(milestone_data) = rt.inbox_mut().next().await {
            let milestone_index = milestone_data.milestone_index().0;
            let milestone_data_builder = milestone_data.into();
            if let Err(e) = self.filter_handle.process_milestone_data(milestone_data_builder).await {
                error!(
                    "MilestoneDataImporter, id: {}, failed to import milestone data for index: {}, error: {}",
                    rt.service().directory().clone().unwrap_or_default(),
                    milestone_index,
                    e
                );
                rt.supervisor_handle()
                    .send(ImporterEvent::FromMilestoneDataImporter(
                        rt.scope_id(),
                        Err(milestone_index),
                    ))
                    .ok();
            } else {
                rt.supervisor_handle()
                    .send(ImporterEvent::FromMilestoneDataImporter(
                        rt.scope_id(),
                        Ok(milestone_index),
                    ))
                    .ok();
            };
        }
        Ok(())
    }
}
