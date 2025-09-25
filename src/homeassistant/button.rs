//! Tools for publishing a [Home Assistant button](https://www.home-assistant.io/integrations/button.mqtt/).
use core::ops::Deref;

use serde::Serialize;

use crate::{homeassistant::Component, Error, Topic};

/// The type of button.
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
#[allow(missing_docs)]
pub enum ButtonClass {
    Identify,
    Restart,
    Update,
}

/// A button that can be pressed.
#[derive(Serialize)]
pub struct Button {
    /// The type of button.
    pub device_class: Option<ButtonClass>,
}

impl Component for Button {
    type State = ();

    fn platform() -> &'static str {
        "button"
    }

    async fn publish_state<T: Deref<Target = str>>(
        &self,
        _topic: &Topic<T>,
        _state: Self::State,
    ) -> Result<(), Error> {
        // Buttons don't have a state
        Err(Error::Invalid)
    }
}
