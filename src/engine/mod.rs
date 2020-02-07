//use crate::cluster;

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

pub async fn cluster() {
    unimplemented!()
}
