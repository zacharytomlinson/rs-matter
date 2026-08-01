/*
 *
 *    Copyright (c) 2026 Project CHIP Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! Client-side subscribe tests exercising the `SubscribeSender` API
//! and the establishment-phase response handling
//! (`SubscribePrimingChunk::complete` →
//! [`SubscribeOutcome::Established`]).

use core::num::NonZeroU8;

use embassy_futures::block_on;
use embassy_futures::select::select;

use rs_matter::dm::clusters::app::on_off::{
    self, test::TestOnOffDeviceLogic, ClusterAsyncHandler as _, NoLevelControl,
};
use rs_matter::im::client::{ImClient, SubscribeOutcome, SubscriptionReportChunk, TxOutcome};
use rs_matter::im::{AttrPath, CmdDataTag, GenericPath};
use rs_matter::tlv::{TLVTag, TLVWrite};
use rs_matter::transport::exchange::Exchange;
use rs_matter::utils::select::Coalesce;

use crate::common::e2e::im::echo_cluster;
use crate::common::e2e::{new_default_runner, E2eRunner};
use crate::common::init_env_logger;

fn remote_peer_id<C: rs_matter::crypto::Crypto>(_: &E2eRunner<C>) -> u64 {
    E2eRunner::<C>::REMOTE_PEER_ID
}

/// `SubscribeSender::tx` + priming-chunk loop + terminal
/// `SubscribeEstablished`. Mirrors `test_client_read_sender_non_chunked`
/// but on the subscribe path: one priming `ReportData` chunk for the
/// single concrete attribute, followed by the `SubscribeResponse`
/// carrying `subscription_id` + `max_int`.
#[test]
fn test_client_subscribe_sender_non_chunked() {
    init_env_logger();

    let im = new_default_runner();
    im.add_default_acl();
    let handler = im.handler();

    block_on(
        select(im.run(handler), async {
            let exchange = im.initiate_exchange().await?;
            let mut sender = exchange.subscribe_sender().await?;

            let path = AttrPath::from_gp(&GenericPath::new(
                Some(0),
                Some(echo_cluster::ID),
                Some(echo_cluster::AttributesDiscriminants::Att1 as u32),
            ));
            let paths = [path];

            // Drive the retransmit loop. `min_int_floor=0`,
            // `max_int_ceil=60` are bounds the test responder
            // accepts; `keep_subs=true` is the typical client value.
            let mut chunk = loop {
                match sender.tx().await? {
                    TxOutcome::BuildRequest(builder) => {
                        sender = builder
                            .keep_subs(true)?
                            .min_int_floor(0)?
                            .max_int_ceil(60)?
                            .attr_requests_from(&paths)?
                            .fabric_filtered(false)?
                            .end()?;
                    }
                    TxOutcome::GotResponse(c) => break c,
                }
            };

            // One concrete attribute → one priming ReportData chunk →
            // SubscribeResponse. Walk the outcome enum.
            let mut chunk_count = 0u32;
            let mut attr_count = 0u32;
            let established = loop {
                chunk_count += 1;
                {
                    let resp = chunk.response()?;
                    if let Some(attr_reports) = &resp.attr_reports {
                        for attr_resp in attr_reports.iter() {
                            if attr_resp.is_ok() {
                                attr_count += 1;
                            }
                        }
                    }
                }
                match chunk.complete().await? {
                    SubscribeOutcome::NextChunk(next) => chunk = next,
                    SubscribeOutcome::Established(est) => break est,
                }
            };

            assert_eq!(
                chunk_count, 1,
                "Single-attr subscribe should have 1 priming chunk"
            );
            assert_eq!(attr_count, 1, "Should have received 1 attribute report");
            assert_ne!(
                established.subscription_id, 0,
                "Subscription id should be non-zero"
            );
            assert!(
                established.max_int >= 40,
                "Server should clamp max_int to at least 40s (saw {})",
                established.max_int
            );

            Ok(())
        })
        .coalesce(),
    )
    .unwrap()
}

/// Establish a subscription, mutate a standard cluster through another
/// Interaction Model transaction, and consume the resulting server-initiated
/// report exchange.
#[test]
fn test_client_subscription_active_report() {
    init_env_logger();

    let im = new_default_runner();
    im.add_default_acl();
    let handler = im.handler();

    block_on(
        select(im.run(handler), async {
            let cluster_id =
                on_off::OnOffHandler::<'_, TestOnOffDeviceLogic, NoLevelControl>::CLUSTER.id;
            let attr_id = on_off::AttributeId::OnOff as u32;

            let exchange = im.initiate_exchange().await?;
            let mut sender = exchange.subscribe_sender().await?;
            let paths = [AttrPath::from_gp(&GenericPath::new(
                Some(1),
                Some(cluster_id),
                Some(attr_id),
            ))];

            let mut chunk = loop {
                match sender.tx().await? {
                    TxOutcome::BuildRequest(builder) => {
                        sender = builder
                            .keep_subs(true)?
                            .min_int_floor(0)?
                            .max_int_ceil(60)?
                            .attr_requests_from(&paths)?
                            .fabric_filtered(false)?
                            .end()?;
                    }
                    TxOutcome::GotResponse(chunk) => break chunk,
                }
            };

            let established = loop {
                match chunk.complete().await? {
                    SubscribeOutcome::NextChunk(next) => chunk = next,
                    SubscribeOutcome::Established(established) => break established,
                }
            };

            assert_eq!(established.peer.fabric_index, NonZeroU8::new(1).unwrap());
            assert_eq!(established.peer.node_id, remote_peer_id(&im));

            let exchange = im.initiate_exchange().await?;
            let mut sender = exchange.invoke_sender(None).await?;
            let mut invoke_chunk = loop {
                match sender.tx().await? {
                    TxOutcome::BuildRequest(builder) => {
                        sender = builder
                            .suppress_response(false)?
                            .timed_request(false)?
                            .invoke_requests()?
                            .push()?
                            .path(1, cluster_id, on_off::CommandId::On as u32)?
                            .data(|w| w.u8(&TLVTag::Context(CmdDataTag::Data as u8), 1))?
                            .end()?
                            .end()?
                            .end()?;
                    }
                    TxOutcome::GotResponse(chunk) => break chunk,
                }
            };

            if !invoke_chunk.is_status_only() {
                let response = invoke_chunk
                    .response()?
                    .expect("non-status-only invoke must have a response");
                let mut statuses = response.statuses(cluster_id, on_off::CommandId::On as u32);
                let (endpoint, status) = statuses.next().expect("On command status");
                assert_eq!(endpoint, 1);
                status?;
            }

            while let Some(next) = invoke_chunk.complete().await? {
                invoke_chunk = next;
            }

            let exchange = Exchange::accept(im.matter_client()).await?;
            let mut report = SubscriptionReportChunk::receive(exchange).await?;
            assert!(report.matches(&established));
            assert_eq!(report.peer(), established.peer);
            assert_eq!(report.subscription_id(), established.subscription_id);

            let mut report_chunks = 0;
            let mut got_on = false;
            loop {
                report_chunks += 1;
                {
                    let response = report.response()?;
                    for (endpoint, value) in response.attrs::<bool>(cluster_id, attr_id) {
                        assert_eq!(endpoint, 1);
                        assert!(value?);
                        got_on = true;
                    }
                }

                match report.complete().await? {
                    Some(next) => report = next,
                    None => break,
                }
            }

            assert_eq!(report_chunks, 1);
            assert!(
                got_on,
                "active report should contain the updated OnOff value"
            );

            Ok(())
        })
        .coalesce(),
    )
    .unwrap()
}

/// Chunked wildcard subscribe — mirrors
/// `test_client_read_sender_chunked_wildcard`. Subscribing to every
/// attribute on every endpoint forces the priming-read side of the
/// establishment to chunk; the test verifies the chunk loop on
/// [`SubscribePrimingChunk`] correctly iterates and lands on the
/// terminal [`SubscribeOutcome::Established`].
#[test]
fn test_client_subscribe_sender_chunked_wildcard() {
    init_env_logger();

    let im = new_default_runner();
    im.add_default_acl();
    let handler = im.handler();

    block_on(
        select(im.run(handler), async {
            let exchange = im.initiate_exchange().await?;
            let mut sender = exchange.subscribe_sender().await?;

            let path = AttrPath::from_gp(&GenericPath::new(None, None, None));
            let paths = [path];

            let mut chunk = loop {
                match sender.tx().await? {
                    TxOutcome::BuildRequest(builder) => {
                        sender = builder
                            .keep_subs(true)?
                            .min_int_floor(0)?
                            .max_int_ceil(60)?
                            .attr_requests_from(&paths)?
                            .fabric_filtered(false)?
                            .end()?;
                    }
                    TxOutcome::GotResponse(c) => break c,
                }
            };

            let mut chunk_count = 0u32;
            let mut attr_count = 0u32;
            let established = loop {
                chunk_count += 1;
                {
                    let resp = chunk.response()?;
                    if let Some(attr_reports) = &resp.attr_reports {
                        for attr_resp in attr_reports.iter() {
                            if attr_resp.is_ok() {
                                attr_count += 1;
                            }
                        }
                    }
                }
                match chunk.complete().await? {
                    SubscribeOutcome::NextChunk(next) => chunk = next,
                    SubscribeOutcome::Established(est) => break est,
                }
            };

            assert!(
                chunk_count > 1,
                "Wildcard subscribe priming should chunk (got {})",
                chunk_count
            );
            assert!(
                attr_count > 1,
                "Wildcard subscribe should report many attributes (got {})",
                attr_count
            );
            assert_ne!(established.subscription_id, 0);

            Ok(())
        })
        .coalesce(),
    )
    .unwrap()
}
