use core::ops::Deref;

use serde::{
    ser::{SerializeSeq, SerializeStruct},
    Serialize, Serializer,
};

use crate::{
    homeassistant::{AvailabilityTopics, Component, Entity},
    Topic,
};

#[derive(Serialize)]
pub(super) struct AvailabilityTopicItem<'a> {
    topic: Topic<&'a str>,
}

struct AvailabilityTopicList<'a, T: Deref<Target = str>, const N: usize> {
    list: &'a [Topic<T>; N],
}

impl<'a, const N: usize, T: Deref<Target = str>> AvailabilityTopicList<'a, T, N> {
    pub(super) fn new(list: &'a [Topic<T>; N]) -> Self {
        Self { list }
    }
}

impl<T: Deref<Target = str>, const N: usize> Serialize for AvailabilityTopicList<'_, T, N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut serializer = serializer.serialize_seq(Some(N))?;

        for topic in self.list {
            serializer.serialize_element(&AvailabilityTopicItem {
                topic: topic.as_ref(),
            })?;
        }

        serializer.end()
    }
}

pub(super) struct List<'a, T: Serialize, const N: usize> {
    list: &'a [T; N],
}

impl<'a, T: Serialize, const N: usize> List<'a, T, N> {
    pub(super) fn new(list: &'a [T; N]) -> Self {
        Self { list }
    }
}

impl<T: Serialize, const N: usize> Serialize for List<'_, T, N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut serializer = serializer.serialize_seq(Some(N))?;

        for item in self.list {
            serializer.serialize_element(item)?;
        }

        serializer.end()
    }
}

pub(super) struct DiscoverySerializer<'a, const A: usize, C: Component, S: Serializer> {
    pub(super) discovery: &'a Entity<'a, A, C>,
    pub(super) inner: S,
}

impl<const A: usize, C: Component, S: Serializer> Serializer for DiscoverySerializer<'_, A, C, S> {
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = S::SerializeSeq;
    type SerializeTuple = S::SerializeTuple;
    type SerializeTupleStruct = S::SerializeTupleStruct;
    type SerializeTupleVariant = S::SerializeTupleVariant;
    type SerializeMap = S::SerializeMap;
    type SerializeStruct = S::SerializeStruct;
    type SerializeStructVariant = S::SerializeStructVariant;

    fn serialize_struct(
        self,
        name: &'static str,
        mut len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        len += 5;
        if self.discovery.state_topic.is_some() {
            len += 1;
        }
        if self.discovery.command_topic.is_some() {
            len += 1;
        }
        if self.discovery.unique_id.is_some() {
            len += 1;
        }
        if !matches!(self.discovery.availability, AvailabilityTopics::None) {
            len += 2;
        }

        let mut serializer = self.inner.serialize_struct(name, len)?;

        serializer.serialize_field("dev", &self.discovery.device)?;
        serializer.serialize_field("o", &self.discovery.origin)?;
        serializer.serialize_field("p", C::platform())?;
        serializer.serialize_field("obj_id", self.discovery.object_id)?;

        serializer.serialize_field("name", self.discovery.name)?;

        if let Some(t) = self.discovery.state_topic {
            serializer.serialize_field("stat_t", &t)?;
        } else {
            serializer.skip_field("stat_t")?;
        }

        if let Some(t) = self.discovery.command_topic {
            serializer.serialize_field("cmd_t", &t)?;
        } else {
            serializer.skip_field("cmd_t")?;
        }

        match &self.discovery.availability {
            AvailabilityTopics::None => {
                serializer.skip_field("avty")?;
                serializer.skip_field("avty_mode")?;
            }
            AvailabilityTopics::All(topics) => {
                serializer.serialize_field("avty_mode", "all")?;
                serializer.serialize_field("avty", &AvailabilityTopicList::new(topics))?;
            }
            AvailabilityTopics::Any(topics) => {
                serializer.serialize_field("avty_mode", "any")?;
                serializer.serialize_field("avty", &AvailabilityTopicList::new(topics))?;
            }
            AvailabilityTopics::Latest(topics) => {
                serializer.serialize_field("avty_mode", "latest")?;
                serializer.serialize_field("avty", &AvailabilityTopicList::new(topics))?;
            }
        }

        if let Some(v) = self.discovery.unique_id {
            serializer.serialize_field("uniq_id", v)?;
        } else {
            serializer.skip_field("uniq_id")?;
        }

        Ok(serializer)
    }

    fn serialize_bool(self, _: bool) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_i8(self, _: i8) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_i16(self, _: i16) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_i32(self, _: i32) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_i64(self, _: i64) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_u8(self, _: u8) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_u16(self, _: u16) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_u32(self, _: u32) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_u64(self, _: u64) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_f32(self, _: f32) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_f64(self, _: f64) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_char(self, _: char) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_str(self, _: &str) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_bytes(self, _: &[u8]) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_some<T>(self, _: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_newtype_struct<T>(self, _: &'static str, _: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn serialize_newtype_variant<T>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        unimplemented!()
    }

    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Self::Error> {
        unimplemented!()
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        unimplemented!()
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        unimplemented!()
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        unimplemented!()
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        unimplemented!()
    }

    fn serialize_i128(self, _: i128) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_u128(self, _: u128) -> Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn collect_seq<I>(self, _: I) -> Result<Self::Ok, Self::Error>
    where
        I: IntoIterator,
        <I as IntoIterator>::Item: Serialize,
    {
        unimplemented!()
    }

    fn collect_map<K, V, I>(self, _: I) -> Result<Self::Ok, Self::Error>
    where
        K: Serialize,
        V: Serialize,
        I: IntoIterator<Item = (K, V)>,
    {
        unimplemented!()
    }

    fn collect_str<T>(self, _: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + core::fmt::Display,
    {
        unimplemented!()
    }

    fn is_human_readable(&self) -> bool {
        unimplemented!()
    }
}
