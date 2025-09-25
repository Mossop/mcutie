//! Tools for publishing a [Home Assistant light](https://www.home-assistant.io/integrations/light.mqtt/).
use core::{ops::Deref, str};

use serde::{ser::SerializeStruct, Deserialize, Serialize, Serializer};

use crate::{
    fmt::Debug2Format,
    homeassistant::{binary_sensor::BinarySensorState, ser::List, Component},
    Error, Payload, Publishable, Topic,
};

#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
#[allow(missing_docs)]
pub enum SupportedColorMode {
    OnOff,
    Brightness,
    #[serde(rename = "color_temp")]
    ColorTemp,
    Hs,
    Xy,
    Rgb,
    Rgbw,
    Rgbww,
    White,
}

#[derive(Serialize, Deserialize, Default)]
struct SerializedColor {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    h: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    s: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    x: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    y: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    r: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    g: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    b: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    w: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    c: Option<u8>,
}

#[derive(Deserialize)]
struct LedPayload<'a> {
    state: BinarySensorState,
    #[serde(default)]
    brightness: Option<u8>,
    #[serde(default)]
    color_temp: Option<u32>,
    #[serde(default)]
    color: Option<SerializedColor>,
    #[serde(default)]
    effect: Option<&'a str>,
}

/// The color of the light in various forms.
#[derive(Serialize)]
#[serde(rename_all = "lowercase", tag = "color_mode", content = "color")]
#[allow(missing_docs)]
pub enum Color {
    None,
    Brightness(u8),
    ColorTemp(u32),
    Hs {
        #[serde(rename = "h")]
        hue: f32,
        #[serde(rename = "s")]
        saturation: f32,
    },
    Xy {
        x: f32,
        y: f32,
    },
    Rgb {
        #[serde(rename = "r")]
        red: u8,
        #[serde(rename = "g")]
        green: u8,
        #[serde(rename = "b")]
        blue: u8,
    },
    Rgbw {
        #[serde(rename = "r")]
        red: u8,
        #[serde(rename = "g")]
        green: u8,
        #[serde(rename = "b")]
        blue: u8,
        #[serde(rename = "w")]
        white: u8,
    },
    Rgbww {
        #[serde(rename = "r")]
        red: u8,
        #[serde(rename = "g")]
        green: u8,
        #[serde(rename = "b")]
        blue: u8,
        #[serde(rename = "c")]
        cool_white: u8,
        #[serde(rename = "w")]
        warm_white: u8,
    },
}

/// The state of the light. This can be sent to the broker and received as a
/// command from Home Assistant.
pub struct LightState<'a> {
    /// Whether the light is on or off.
    pub state: BinarySensorState,
    /// The color of the light.
    pub color: Color,
    /// Any effect that is applied.
    pub effect: Option<&'a str>,
}

impl<'a> LightState<'a> {
    /// Parses the state from a command payload.
    pub fn from_payload(payload: &'a Payload) -> Result<Self, Error> {
        let parsed: LedPayload<'a> = match payload.deserialize_json() {
            Ok(p) => p,
            Err(e) => {
                warn!("Failed to deserialize packet: {:?}", Debug2Format(&e));
                if let Ok(s) = str::from_utf8(payload) {
                    trace!("{}", s);
                }
                return Err(Error::PacketError);
            }
        };

        let color = if let Some(color) = parsed.color {
            if let Some(x) = color.x {
                Color::Xy {
                    x,
                    y: color.y.unwrap_or_default(),
                }
            } else if let Some(h) = color.h {
                Color::Hs {
                    hue: h,
                    saturation: color.s.unwrap_or_default(),
                }
            } else if let Some(c) = color.c {
                Color::Rgbww {
                    red: color.r.unwrap_or_default(),
                    green: color.g.unwrap_or_default(),
                    blue: color.b.unwrap_or_default(),
                    cool_white: c,
                    warm_white: color.w.unwrap_or_default(),
                }
            } else if let Some(w) = color.w {
                Color::Rgbw {
                    red: color.r.unwrap_or_default(),
                    green: color.g.unwrap_or_default(),
                    blue: color.b.unwrap_or_default(),
                    white: w,
                }
            } else {
                Color::Rgb {
                    red: color.r.unwrap_or_default(),
                    green: color.g.unwrap_or_default(),
                    blue: color.b.unwrap_or_default(),
                }
            }
        } else if let Some(color_temp) = parsed.color_temp {
            Color::ColorTemp(color_temp)
        } else if let Some(brightness) = parsed.brightness {
            Color::Brightness(brightness)
        } else {
            Color::None
        };

        Ok(LightState {
            state: parsed.state,
            color,
            effect: parsed.effect,
        })
    }
}

impl Serialize for LightState<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut len = 1;

        if self.effect.is_some() {
            len += 1;
        }

        match self.color {
            Color::None => {}
            Color::Brightness(_) | Color::ColorTemp(_) => len += 1,
            _ => len += 2,
        }

        let mut serializer = serializer.serialize_struct("LightState", len)?;

        serializer.serialize_field("state", &self.state)?;

        if let Some(effect) = self.effect {
            serializer.serialize_field("effect", effect)?;
        } else {
            serializer.skip_field("effect")?;
        }

        match self.color {
            Color::None => {
                serializer.skip_field("brightness")?;
                serializer.skip_field("color_temp")?;
                serializer.skip_field("color")?;
            }
            Color::Brightness(b) => {
                serializer.skip_field("color_temp")?;
                serializer.skip_field("color")?;

                serializer.serialize_field("brightness", &b)?
            }
            Color::ColorTemp(c) => {
                serializer.skip_field("brightness")?;
                serializer.skip_field("color")?;

                serializer.serialize_field("color_temp", &c)?
            }
            Color::Hs { hue, saturation } => {
                serializer.skip_field("brightness")?;
                serializer.skip_field("color_temp")?;

                serializer.serialize_field("color_mode", "hs")?;

                let color = SerializedColor {
                    h: Some(hue),
                    s: Some(saturation),
                    ..Default::default()
                };

                serializer.serialize_field("color", &color)?
            }
            Color::Xy { x, y } => {
                serializer.skip_field("brightness")?;
                serializer.skip_field("color_temp")?;

                serializer.serialize_field("color_mode", "xy")?;

                let color = SerializedColor {
                    x: Some(x),
                    y: Some(y),
                    ..Default::default()
                };

                serializer.serialize_field("color", &color)?
            }
            Color::Rgb { red, green, blue } => {
                serializer.skip_field("brightness")?;
                serializer.skip_field("color_temp")?;

                serializer.serialize_field("color_mode", "rgb")?;

                let color = SerializedColor {
                    r: Some(red),
                    g: Some(green),
                    b: Some(blue),
                    ..Default::default()
                };

                serializer.serialize_field("color", &color)?
            }
            Color::Rgbw {
                red,
                green,
                blue,
                white,
            } => {
                serializer.skip_field("brightness")?;
                serializer.skip_field("color_temp")?;

                serializer.serialize_field("color_mode", "rgbw")?;

                let color = SerializedColor {
                    r: Some(red),
                    g: Some(green),
                    b: Some(blue),
                    w: Some(white),
                    ..Default::default()
                };

                serializer.serialize_field("color", &color)?
            }
            Color::Rgbww {
                red,
                green,
                blue,
                cool_white,
                warm_white,
            } => {
                serializer.skip_field("brightness")?;
                serializer.skip_field("color_temp")?;

                serializer.serialize_field("color_mode", "rgbww")?;

                let color = SerializedColor {
                    r: Some(red),
                    g: Some(green),
                    b: Some(blue),
                    c: Some(cool_white),
                    w: Some(warm_white),
                    ..Default::default()
                };

                serializer.serialize_field("color", &color)?
            }
        }

        serializer.end()
    }
}

/// A light entity
pub struct Light<'a, const C: usize, const E: usize> {
    /// The color modes supported by the light.
    pub supported_color_modes: [SupportedColorMode; C],
    /// Any effects that can be used.
    pub effects: [&'a str; E],
}

impl<const C: usize, const E: usize> Serialize for Light<'_, C, E> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut len = 2;

        if C > 0 {
            len += 1;
        }

        if E > 0 {
            len += 2;
        }

        let mut serializer = serializer.serialize_struct("Light", len)?;

        serializer.serialize_field("schema", "json")?;

        if C > 0 {
            serializer.serialize_field("sup_clrm", &List::new(&self.supported_color_modes))?;
        } else {
            serializer.skip_field("sup_clrm")?;
        }

        if E > 0 {
            serializer.serialize_field("effect", &true)?;
            serializer.serialize_field("fx_list", &List::new(&self.effects))?;
        } else {
            serializer.skip_field("effect")?;
            serializer.skip_field("fx_list")?;
        }

        serializer.end()
    }
}

impl<const C: usize, const E: usize> Component for Light<'_, C, E> {
    type State = LightState<'static>;

    fn platform() -> &'static str {
        "light"
    }

    async fn publish_state<T: Deref<Target = str>>(
        &self,
        topic: &Topic<T>,
        state: Self::State,
    ) -> Result<(), Error> {
        topic.with_json(state).publish().await
    }
}
