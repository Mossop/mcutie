//! Tools for publishing a [Home Assistant sensor](https://www.home-assistant.io/integrations/sensor.mqtt/).
use core::ops::Deref;

use serde::Serialize;

use crate::{homeassistant::Component, Error, Publishable, Topic};

/// The type of sensor.
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
#[allow(missing_docs)]
pub enum SensorClass {
    ApparentPower,
    Aqi,
    AtmosphericPressure,
    Battery,
    CarbonDioxide,
    CarbonMonoxide,
    Current,
    DataRate,
    DataSize,
    Date,
    Distance,
    Duration,
    Energy,
    EnergyStorage,
    Enum,
    Frequency,
    Gas,
    Humidity,
    Illuminance,
    Irradiance,
    Moisture,
    Monetary,
    NitrogenDioxide,
    NitrogenMonoxide,
    NitrousOxide,
    Ozone,
    Ph,
    Pm1,
    Pm25,
    Pm10,
    PowerFactor,
    Power,
    Precipitation,
    PrecipitationIntensity,
    Pressure,
    ReactivePower,
    SignalStrength,
    SoundPressure,
    Speed,
    SulphurDioxide,
    Temperature,
    Timestamp,
    VolatileOrganicCompounds,
    VolatileOrganicCompoundsParts,
    Voltage,
    Volume,
    VolumeFlowRate,
    VolumeStorage,
    Water,
    Weight,
    WindSpeed,
}

/// The type of measurement that this entity publishes.
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SensorStateClass {
    /// A measurement at a singe point in time.
    Measurement,
    /// A cumulative total that can increase or decrease over time.
    Total,
    /// A cumulative total that can only increase.
    TotalIncreasing,
}

/// A binary sensor that can publish a [`f32`] value.
#[derive(Serialize)]
pub struct Sensor<'u> {
    /// The type of sensor.
    pub device_class: Option<SensorClass>,
    /// The type of measurement that this sensor reports.
    pub state_class: Option<SensorStateClass>,
    /// The unit of measurement for this sensor.
    pub unit_of_measurement: Option<&'u str>,
}

impl Component for Sensor<'_> {
    type State = f32;

    fn platform() -> &'static str {
        "sensor"
    }

    async fn publish_state<T: Deref<Target = str>>(
        &self,
        topic: &Topic<T>,
        state: Self::State,
    ) -> Result<(), Error> {
        topic.with_display(state).publish().await
    }
}

#[cfg(test)]
mod tests {
    use super::{Sensor, SensorClass, SensorStateClass};
    use crate::test_support::to_json;

    #[test]
    fn component_json() {
        assert_eq!(
            to_json(&Sensor {
                device_class: Some(SensorClass::Pm25),
                state_class: Some(SensorStateClass::TotalIncreasing),
                unit_of_measurement: Some("µg/m³"),
            }),
            r#"{"device_class":"pm25","state_class":"total_increasing","unit_of_measurement":"µg/m³"}"#
        );
        assert_eq!(
            to_json(&Sensor {
                device_class: None,
                state_class: None,
                unit_of_measurement: None,
            }),
            r#"{"device_class":null,"state_class":null,"unit_of_measurement":null}"#
        );
    }
}
