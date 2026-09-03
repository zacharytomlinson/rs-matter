/*
 * Copyright (c) 2026 Project CHIP Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use super::{Cluster, ClusterId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandardElement {
    pub id: u32,
    pub name: &'static str,
}

impl StandardElement {
    pub const fn new(id: u32, name: &'static str) -> Self {
        Self { id, name }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandardFieldDescriptor {
    pub id: u32,
    pub name: &'static str,
    pub data_type: &'static str,
    pub list: bool,
    pub maximum_length: Option<u64>,
    pub optional: bool,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandardAttributeDescriptor {
    pub field: StandardFieldDescriptor,
    pub read_only: bool,
    pub write_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandardCommandDescriptor {
    pub id: u32,
    pub name: &'static str,
    pub input_type: Option<&'static str>,
    pub output_type: &'static str,
    pub fields: &'static [StandardFieldDescriptor],
    pub timed: bool,
    pub fabric_scoped: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandardEventDescriptor {
    pub id: u32,
    pub name: &'static str,
    pub fields: &'static [StandardFieldDescriptor],
    pub fabric_scoped: bool,
}

#[derive(Debug)]
pub struct StandardClusterDescriptor {
    pub name: &'static str,
    pub metadata: &'static Cluster<'static>,
    pub features: &'static [StandardElement],
    pub attributes: &'static [StandardAttributeDescriptor],
    pub commands: &'static [StandardCommandDescriptor],
    pub events: &'static [StandardEventDescriptor],
}

impl StandardClusterDescriptor {
    pub const fn id(&self) -> ClusterId {
        self.metadata.id
    }

    pub fn attribute(&self, id: u32) -> Option<&StandardAttributeDescriptor> {
        self.attributes
            .iter()
            .find(|attribute| attribute.field.id == id)
    }

    pub fn command(&self, id: u32) -> Option<&StandardCommandDescriptor> {
        self.commands.iter().find(|command| command.id == id)
    }

    pub fn event(&self, id: u32) -> Option<&StandardEventDescriptor> {
        self.events.iter().find(|event| event.id == id)
    }
}
