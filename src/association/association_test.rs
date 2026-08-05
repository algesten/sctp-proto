use crate::chunk::{Chunk, chunk_init::ChunkInit};
use crate::config::generate_snap_token;

use super::*;

const ACCEPT_CH_SIZE: usize = 16;

fn create_association(config: TransportConfig) -> Association {
    Association::new(
        None,
        Arc::new(config),
        1400,
        0,
        SocketAddr::from_str("0.0.0.0:0").unwrap(),
        None,
        Instant::now(),
    )
}

#[test]
fn test_assoc_is_closing() {
    let closing_states = [
        AssociationState::ShutdownSent,
        AssociationState::ShutdownAckSent,
        AssociationState::ShutdownPending,
        AssociationState::ShutdownReceived,
    ];

    for state in [
        AssociationState::Closed,
        AssociationState::CookieWait,
        AssociationState::CookieEchoed,
        AssociationState::Established,
    ] {
        let a = Association {
            state,
            ..Default::default()
        };

        assert!(!a.is_closing(), "{state} should not be closing");
    }

    for state in closing_states {
        let a = Association {
            state,
            ..Default::default()
        };

        assert!(a.is_closing(), "{state} should be closing");
        assert!(!a.is_closed(), "{state} should not be closed");
    }
}

fn outgoing_reset(rsn: u32, stream_id: StreamId) -> ChunkReconfig {
    ChunkReconfig {
        param_a: Some(Box::new(ParamOutgoingResetRequest {
            reconfig_request_sequence_number: rsn,
            stream_identifiers: vec![stream_id],
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn insert_active_reset(a: &mut Association, rsn: u32, stream_id: StreamId) {
    a.reconfigs.insert(rsn, outgoing_reset(rsn, stream_id));
    a.active_reconfig = Some(rsn);
}

fn insert_queued_reset(a: &mut Association, rsn: u32, stream_id: StreamId) {
    let reset = outgoing_reset(rsn, stream_id);
    a.reconfigs.insert(rsn, reset.clone());
    let packet = a.create_packet(vec![Box::new(reset)]);
    a.control_queue.push_back(packet);
}

#[test]
fn test_reconfig_in_progress_timeout_does_not_consume_retry_budget() -> Result<()> {
    let now = Instant::now();
    let rsn = 7;
    let mut a = create_association(
        TransportConfig::default()
            .with_max_init_retransmits(Some(0))
            .with_rto_initial_ms(1),
    );

    insert_active_reset(&mut a, rsn, 1);
    a.timers.start(Timer::Reconfig, now, a.rto_mgr.get_rto());

    let response: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
        reconfig_response_sequence_number: rsn,
        result: ReconfigResult::InProgress,
    });
    a.handle_reconfig_param(&response, &mut vec![])?;

    // The outbound path restarts the timer after processing InProgress. RFC
    // 6525 section 5.2.7 H2 requires the next expiry to retransmit without
    // incrementing the error counter.
    a.timers
        .restart_if_stale(Timer::Reconfig, now, a.rto_mgr.get_rto());
    let deadline = a.timers.get(Timer::Reconfig).unwrap();
    a.handle_timeout(deadline);

    assert!(a.reconfigs.contains_key(&rsn));
    assert!(a.will_retransmit_reconfig);
    Ok(())
}

#[test]
fn test_reset_complete_only_for_successful_reconfig_response() -> Result<()> {
    let rsn = 7;
    let stream_id = 1;

    for result in [
        ReconfigResult::SuccessNop,
        ReconfigResult::SuccessPerformed,
        ReconfigResult::Denied,
        ReconfigResult::ErrorWrongSsn,
        ReconfigResult::ErrorRequestAlreadyInProgress,
        ReconfigResult::ErrorBadSequenceNumber,
        ReconfigResult::InProgress,
        ReconfigResult::Unknown,
    ] {
        let mut a = Association::default();
        a.pending_reset_completions.insert(stream_id);
        insert_active_reset(&mut a, rsn, stream_id);

        let response: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
            reconfig_response_sequence_number: rsn,
            result,
        });
        a.handle_reconfig_param(&response, &mut vec![])?;

        let completed = matches!(
            a.poll(),
            Some(Event::Stream(StreamEvent::ResetComplete { id })) if id == stream_id
        );
        let should_complete = matches!(
            result,
            ReconfigResult::SuccessNop | ReconfigResult::SuccessPerformed
        );
        assert_eq!(completed, should_complete, "unexpected result for {result}");
    }

    Ok(())
}

#[test]
fn test_reconfig_retransmission_failure_is_terminal() {
    let rsn = 7;
    let stream_id = 1;
    let mut a = Association::default();
    a.pending_reset_completions.insert(stream_id);
    insert_active_reset(&mut a, rsn, stream_id);

    a.on_retransmission_failure(Timer::Reconfig);

    assert!(matches!(
        a.poll(),
        Some(Event::Stream(StreamEvent::ResetFailed {
            id,
            reason: StreamResetError::Failed,
        })) if id == stream_id
    ));
}

#[test]
fn test_ambiguous_reset_failure_keeps_existing_stream_quarantined() -> Result<()> {
    let stream_id = 1;
    let mut a = Association {
        state: AssociationState::Established,
        my_next_tsn: 1,
        ..Default::default()
    };
    assert!(
        a.create_stream(stream_id, false, PayloadProtocolIdentifier::Binary)
            .is_some()
    );

    a.stream(stream_id)?.stop()?;
    let _ = a.gather_outbound(Instant::now());
    assert!(a.active_reconfig.is_some());

    a.on_retransmission_failure(Timer::Reconfig);

    assert!(a.failed_reset_streams.contains(&stream_id));
    assert!(
        !a.stream(stream_id)?.is_writable(),
        "an unacknowledged reset may have succeeded at the peer, so old SSNs remain unsafe"
    );
    Ok(())
}

#[test]
fn test_denied_reset_emits_terminal_event_and_remains_quarantined() -> Result<()> {
    let rsn = 7;
    let stream_id = 1;
    let mut a = Association::default();
    a.pending_reset_completions.insert(stream_id);
    insert_active_reset(&mut a, rsn, stream_id);

    let response: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
        reconfig_response_sequence_number: rsn,
        result: ReconfigResult::Denied,
    });
    a.handle_reconfig_param(&response, &mut vec![])?;

    assert!(matches!(
        a.poll(),
        Some(Event::Stream(StreamEvent::ResetFailed {
            id,
            reason: StreamResetError::Denied,
        })) if id == stream_id
    ));
    assert!(!a.pending_reset_completions.contains(&stream_id));
    assert!(
        matches!(
            a.open_stream(stream_id, PayloadProtocolIdentifier::Binary),
            Err(Error::ErrStreamResetPending)
        ),
        "the quarantine must prevent the failed stream ID from being reused"
    );
    Ok(())
}

#[test]
fn test_reset_complete_preserves_stream_generations() {
    let stream_id = 1;
    let mut a = Association::default();

    assert!(
        a.create_stream(stream_id, false, PayloadProtocolIdentifier::Binary)
            .is_some()
    );
    a.unregister_stream(stream_id, true);

    // Incoming DATA can recreate an id while the previous reciprocal reset is
    // still pending. A second reset of that id is a distinct generation.
    assert!(a.get_or_create_stream(stream_id).is_some());
    a.unregister_stream(stream_id, true);

    a.emit_reset_complete([stream_id]);
    a.emit_reset_complete([stream_id]);

    let mut finished = 0;
    let mut reset_complete = 0;
    while let Some(event) = a.poll() {
        match event {
            Event::Stream(StreamEvent::Finished { id }) if id == stream_id => finished += 1,
            Event::Stream(StreamEvent::ResetComplete { id }) if id == stream_id => {
                reset_complete += 1;
            }
            _ => {}
        }
    }

    assert_eq!(finished, 2);
    assert_eq!(reset_complete, 2);
}

#[test]
fn test_reset_complete_preserves_generations_through_responses() -> Result<()> {
    let stream_id = 1;
    let first_rsn = 7;
    let second_rsn = 8;
    let mut a = Association::default();

    // Two Finished events for the same stream ID represent two distinct
    // incarnations. Each successful reset response must complete one.
    a.pending_reset_completions.insert(stream_id);
    a.pending_reset_completions.insert(stream_id);
    a.reconfigs
        .insert(first_rsn, outgoing_reset(first_rsn, stream_id));
    a.reconfigs
        .insert(second_rsn, outgoing_reset(second_rsn, stream_id));

    for rsn in [first_rsn, second_rsn] {
        a.active_reconfig = Some(rsn);
        let response: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
            reconfig_response_sequence_number: rsn,
            result: ReconfigResult::SuccessPerformed,
        });
        a.handle_reconfig_param(&response, &mut vec![])?;
    }

    let reset_complete = core::iter::from_fn(|| a.poll())
        .filter(|event| {
            matches!(
                event,
                Event::Stream(StreamEvent::ResetComplete { id }) if *id == stream_id
            )
        })
        .count();

    assert_eq!(
        reset_complete, 2,
        "each completed stream generation needs its own ResetComplete"
    );
    Ok(())
}

#[test]
fn test_overlapping_generations_success_then_denied_reports_success() -> Result<()> {
    let stream_id = 1;
    let mut a = Association::default();

    a.pending_reset_completions.insert(stream_id);
    a.pending_reset_completions.insert(stream_id);
    a.reconfigs.insert(7, outgoing_reset(7, stream_id));
    a.reconfigs.insert(8, outgoing_reset(8, stream_id));

    for (rsn, result) in [
        (7, ReconfigResult::SuccessPerformed),
        (8, ReconfigResult::Denied),
    ] {
        a.active_reconfig = Some(rsn);
        let response: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
            reconfig_response_sequence_number: rsn,
            result,
        });
        a.handle_reconfig_param(&response, &mut vec![])?;
    }

    let reset_complete = core::iter::from_fn(|| a.poll())
        .filter(|event| {
            matches!(
                event,
                Event::Stream(StreamEvent::ResetComplete { id }) if *id == stream_id
            )
        })
        .count();

    assert_eq!(
        reset_complete, 1,
        "the successful generation still needs its ResetComplete"
    );
    Ok(())
}

#[test]
fn test_overlapping_generations_denied_then_success_reports_only_success() -> Result<()> {
    let stream_id = 1;
    let mut a = Association::default();

    a.pending_reset_completions.insert(stream_id);
    a.pending_reset_completions.insert(stream_id);
    a.reconfigs.insert(7, outgoing_reset(7, stream_id));
    a.reconfigs.insert(8, outgoing_reset(8, stream_id));

    for (rsn, result) in [
        (7, ReconfigResult::Denied),
        (8, ReconfigResult::SuccessPerformed),
    ] {
        a.active_reconfig = Some(rsn);
        let response: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
            reconfig_response_sequence_number: rsn,
            result,
        });
        a.handle_reconfig_param(&response, &mut vec![])?;
    }

    let reset_complete = core::iter::from_fn(|| a.poll())
        .filter(|event| {
            matches!(
                event,
                Event::Stream(StreamEvent::ResetComplete { id }) if *id == stream_id
            )
        })
        .count();

    assert_eq!(
        reset_complete, 1,
        "the denied generation must not inherit a later successful completion"
    );
    Ok(())
}

#[test]
fn test_outgoing_reset_implicitly_acknowledges_pending_request() -> Result<()> {
    let local_rsn = 7;
    let stream_id = 1;
    let mut a = Association::default();

    insert_active_reset(&mut a, local_rsn, stream_id);
    a.timers
        .start(Timer::Reconfig, Instant::now(), a.rto_mgr.get_rto());

    // RFC 6525 section 5.2.2 E1: the response sequence number carried by an
    // incoming Outgoing Reset Request acknowledges our matching request.
    let request: Box<dyn Param + Send + Sync> = Box::new(ParamOutgoingResetRequest {
        reconfig_request_sequence_number: 8,
        reconfig_response_sequence_number: local_rsn,
        sender_last_tsn: a.peer_last_tsn,
        stream_identifiers: vec![],
    });
    a.handle_reconfig_param(&request, &mut vec![])?;

    assert!(
        !a.reconfigs.contains_key(&local_rsn),
        "the implicitly acknowledged request must no longer be in flight"
    );
    assert!(
        a.timers.get(Timer::Reconfig).is_none(),
        "the timer must stop after the final in-flight request is acknowledged"
    );
    Ok(())
}

#[test]
fn test_outgoing_reset_does_not_ack_unsent_reciprocal() -> Result<()> {
    let stream_id = 1;
    let mut a = Association {
        my_next_tsn: 1,
        ..Default::default()
    };
    assert!(
        a.create_stream(stream_id, false, PayloadProtocolIdentifier::Binary)
            .is_some()
    );

    // Handling this request queues a reciprocal Outgoing Reset Request in
    // `reply`, but it has not been transmitted and its timer is not running.
    let request: Box<dyn Param + Send + Sync> = Box::new(ParamOutgoingResetRequest {
        reconfig_request_sequence_number: 7,
        reconfig_response_sequence_number: u32::MAX,
        sender_last_tsn: a.peer_last_tsn,
        stream_identifiers: vec![stream_id],
    });
    let mut reply = vec![];
    a.handle_reconfig_param(&request, &mut reply)?;

    let reciprocal_rsn = *a.reconfigs.keys().next().unwrap();
    assert!(!reply.is_empty());
    assert!(a.timers.get(Timer::Reconfig).is_none());

    // RFC 6525 section 5.2.2 E1 only acknowledges a request for which the
    // Re-configuration Timer is running. A peer must not be able to pre-ack
    // the queued reciprocal before the application polls it for transmission.
    let premature_ack: Box<dyn Param + Send + Sync> = Box::new(ParamOutgoingResetRequest {
        reconfig_request_sequence_number: 8,
        reconfig_response_sequence_number: reciprocal_rsn,
        sender_last_tsn: a.peer_last_tsn,
        stream_identifiers: vec![],
    });
    a.handle_reconfig_param(&premature_ack, &mut vec![])?;

    let reset_complete = core::iter::from_fn(|| a.poll())
        .filter(|event| {
            matches!(
                event,
                Event::Stream(StreamEvent::ResetComplete { id }) if *id == stream_id
            )
        })
        .count();
    assert_eq!(
        (a.reconfigs.contains_key(&reciprocal_rsn), reset_complete),
        (true, 0),
        "an unsent reciprocal must remain pending and cannot complete a reset"
    );
    Ok(())
}

#[test]
fn test_reset_complete_does_not_override_newer_failure() -> Result<()> {
    let stream_id = 1;
    let mut a = Association::default();

    a.pending_reset_completions.insert(stream_id);
    a.pending_reset_completions.insert(stream_id);
    a.reconfigs.insert(7, outgoing_reset(7, stream_id));
    a.reconfigs.insert(8, outgoing_reset(8, stream_id));

    for (rsn, result) in [
        (7, ReconfigResult::SuccessPerformed),
        (8, ReconfigResult::Denied),
    ] {
        a.active_reconfig = Some(rsn);
        let response: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
            reconfig_response_sequence_number: rsn,
            result,
        });
        a.handle_reconfig_param(&response, &mut vec![])?;
    }

    assert!(matches!(
        a.poll(),
        Some(Event::Stream(StreamEvent::ResetComplete { id })) if id == stream_id
    ));
    assert!(matches!(
        a.poll(),
        Some(Event::Stream(StreamEvent::ResetFailed {
            id,
            reason: StreamResetError::Denied,
        })) if id == stream_id
    ));
    assert!(matches!(
        a.open_stream(stream_id, PayloadProtocolIdentifier::Binary),
        Err(Error::ErrStreamResetPending)
    ));
    Ok(())
}

#[test]
fn test_reconfig_response_does_not_ack_unsent_reciprocal() -> Result<()> {
    let stream_id = 1;
    let mut a = Association {
        my_next_tsn: 1,
        ..Default::default()
    };
    assert!(
        a.create_stream(stream_id, false, PayloadProtocolIdentifier::Binary)
            .is_some()
    );

    let request: Box<dyn Param + Send + Sync> = Box::new(ParamOutgoingResetRequest {
        reconfig_request_sequence_number: 7,
        reconfig_response_sequence_number: u32::MAX,
        sender_last_tsn: a.peer_last_tsn,
        stream_identifiers: vec![stream_id],
    });
    let mut reply = vec![];
    a.handle_reconfig_param(&request, &mut reply)?;

    let reciprocal_rsn = *a.reconfigs.keys().next().unwrap();
    assert!(!reply.is_empty());
    assert!(a.timers.get(Timer::Reconfig).is_none());

    // The reciprocal is only queued in the reply and has not been transmitted.
    // RFC 6525 H1 says to ignore a response for an RSN whose timer is not running.
    let premature_response: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
        reconfig_response_sequence_number: reciprocal_rsn,
        result: ReconfigResult::SuccessPerformed,
    });
    a.handle_reconfig_param(&premature_response, &mut vec![])?;

    let reset_complete = core::iter::from_fn(|| a.poll())
        .filter(|event| {
            matches!(
                event,
                Event::Stream(StreamEvent::ResetComplete { id }) if *id == stream_id
            )
        })
        .count();
    assert_eq!(
        (a.reconfigs.contains_key(&reciprocal_rsn), reset_complete),
        (true, 0),
        "RFC 6525 H1 requires a response for an RSN whose timer is not running to be ignored"
    );
    Ok(())
}

#[test]
fn test_outgoing_reset_does_not_ack_unsent_reciprocal_with_unrelated_timer() -> Result<()> {
    let stream_id = 1;
    let sent_rsn = 99;
    let mut a = Association {
        my_next_tsn: 1,
        ..Default::default()
    };
    assert!(
        a.create_stream(stream_id, false, PayloadProtocolIdentifier::Binary)
            .is_some()
    );

    insert_active_reset(&mut a, sent_rsn, 2);
    a.timers
        .start(Timer::Reconfig, Instant::now(), a.rto_mgr.get_rto());

    // The global timer belongs to the already-sent request above. Processing
    // this incoming request queues a distinct reciprocal, but does not send it.
    let request: Box<dyn Param + Send + Sync> = Box::new(ParamOutgoingResetRequest {
        reconfig_request_sequence_number: 7,
        reconfig_response_sequence_number: u32::MAX,
        sender_last_tsn: a.peer_last_tsn,
        stream_identifiers: vec![stream_id],
    });
    let mut reply = vec![];
    a.handle_reconfig_param(&request, &mut reply)?;

    let reciprocal_rsn = *a.reconfigs.keys().find(|&&rsn| rsn != sent_rsn).unwrap();
    assert!(!reply.is_empty());

    let premature_ack: Box<dyn Param + Send + Sync> = Box::new(ParamOutgoingResetRequest {
        reconfig_request_sequence_number: 8,
        reconfig_response_sequence_number: reciprocal_rsn,
        sender_last_tsn: a.peer_last_tsn,
        stream_identifiers: vec![3],
    });
    a.handle_reconfig_param(&premature_ack, &mut vec![])?;

    let reset_complete = core::iter::from_fn(|| a.poll())
        .filter(|event| {
            matches!(
                event,
                Event::Stream(StreamEvent::ResetComplete { id }) if *id == stream_id
            )
        })
        .count();
    assert_eq!(
        (a.reconfigs.contains_key(&reciprocal_rsn), reset_complete),
        (true, 0),
        "a timer running for another RSN must not make an unsent reciprocal acknowledgeable"
    );
    Ok(())
}

#[test]
fn test_failed_generation_stays_quarantined_after_other_success() -> Result<()> {
    let stream_id = 1;
    let mut a = Association::default();

    a.pending_reset_completions.insert(stream_id);
    a.pending_reset_completions.insert(stream_id);
    a.reconfigs.insert(7, outgoing_reset(7, stream_id));
    a.reconfigs.insert(8, outgoing_reset(8, stream_id));

    // The older generation completes, but the newer generation is denied.
    // A success for the former cannot make the latter safe to reuse.
    for (rsn, result) in [
        (7, ReconfigResult::SuccessPerformed),
        (8, ReconfigResult::Denied),
    ] {
        a.active_reconfig = Some(rsn);
        let response: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
            reconfig_response_sequence_number: rsn,
            result,
        });
        a.handle_reconfig_param(&response, &mut vec![])?;
    }

    assert!(matches!(
        a.open_stream(stream_id, PayloadProtocolIdentifier::Binary),
        Err(Error::ErrStreamResetPending)
    ));
    Ok(())
}

#[test]
fn test_second_reconfig_request_stays_buffered_while_timer_runs() -> Result<()> {
    let stream_id = 1;
    let sent_rsn = 99;
    let mut a = Association {
        state: AssociationState::Established,
        my_next_tsn: 1,
        ..Default::default()
    };
    assert!(
        a.create_stream(stream_id, false, PayloadProtocolIdentifier::Binary)
            .is_some()
    );

    insert_active_reset(&mut a, sent_rsn, 2);
    a.timers
        .start(Timer::Reconfig, Instant::now(), a.rto_mgr.get_rto());

    // Processing the peer's request creates a reciprocal request while another
    // local request is already in flight. RFC 6525 section 5.1.1 requires the
    // reciprocal request to remain buffered until the running timer stops.
    let request: Box<dyn Param + Send + Sync> = Box::new(ParamOutgoingResetRequest {
        reconfig_request_sequence_number: 7,
        reconfig_response_sequence_number: u32::MAX,
        sender_last_tsn: a.peer_last_tsn,
        stream_identifiers: vec![stream_id],
    });
    let mut reply = vec![];
    a.handle_reconfig_param(&request, &mut reply)?;

    let reciprocal_rsn = *a.reconfigs.keys().find(|&&rsn| rsn != sent_rsn).unwrap();
    a.control_queue.extend(reply);

    assert!(a.poll_transmit(Instant::now()).is_some());
    assert_eq!(
        a.active_reconfig,
        Some(sent_rsn),
        "a second request must remain buffered while the first request's timer runs"
    );
    assert!(a.reconfigs.contains_key(&reciprocal_rsn));
    assert_eq!(a.control_queue.len(), 1);

    let response: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
        reconfig_response_sequence_number: sent_rsn,
        result: ReconfigResult::SuccessPerformed,
    });
    a.handle_reconfig_param(&response, &mut vec![])?;
    assert!(a.poll_transmit(Instant::now()).is_some());
    assert_eq!(a.active_reconfig, Some(reciprocal_rsn));
    assert!(a.control_queue.is_empty());
    Ok(())
}

#[test]
fn test_peer_recreated_quarantined_stream_is_not_writable() -> Result<()> {
    let stream_id = 1;
    let mut a = Association::default();

    // The peer may start its new incoming generation before our reciprocal
    // outgoing reset is acknowledged. Reading that generation is safe, but
    // sending with a freshly initialized SSN is not yet safe.
    a.pending_reset_completions.insert(stream_id);
    insert_active_reset(&mut a, 7, stream_id);

    assert!(a.get_or_create_stream(stream_id).is_some());
    assert!(
        !a.stream(stream_id)?.is_writable(),
        "a pending reciprocal reset must quarantine the outgoing direction"
    );
    Ok(())
}

#[test]
fn test_pending_reset_blocks_existing_stream_writes() -> Result<()> {
    let stream_id = 1;
    let mut a = Association::default();
    assert!(
        a.create_stream(stream_id, false, PayloadProtocolIdentifier::Binary)
            .is_some()
    );
    insert_active_reset(&mut a, 7, stream_id);

    assert!(
        !a.stream(stream_id)?.is_writable(),
        "new SSNs must not be assigned while an outgoing reset is pending"
    );
    Ok(())
}

#[test]
fn test_reset_completion_does_not_reopen_finished_write_half() -> Result<()> {
    let stream_id = 1;
    let rsn = 7;
    let mut a = Association::default();
    assert!(
        a.create_stream(stream_id, false, PayloadProtocolIdentifier::Binary)
            .is_some()
    );
    insert_active_reset(&mut a, rsn, stream_id);

    // Finishing the write half is permanent, including while a reset request
    // temporarily makes the stream non-writable for protocol reasons.
    a.stream(stream_id)?.finish()?;

    let response: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
        reconfig_response_sequence_number: rsn,
        result: ReconfigResult::SuccessPerformed,
    });
    a.handle_reconfig_param(&response, &mut vec![])?;

    assert!(
        !a.stream(stream_id)?.is_writable(),
        "reset completion must not undo Stream::finish()"
    );
    Ok(())
}

#[test]
fn test_successful_outgoing_reset_restarts_stream_sequence_number() -> Result<()> {
    let stream_id = 1;
    let rsn = 7;
    let mut a = Association::default();
    assert!(
        a.create_stream(stream_id, false, PayloadProtocolIdentifier::Binary)
            .is_some()
    );
    a.streams.get_mut(&stream_id).unwrap().sequence_number = 9;
    insert_active_reset(&mut a, rsn, stream_id);

    let response: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
        reconfig_response_sequence_number: rsn,
        result: ReconfigResult::SuccessPerformed,
    });
    a.handle_reconfig_param(&response, &mut vec![])?;

    assert_eq!(
        a.streams.get(&stream_id).unwrap().sequence_number,
        0,
        "RFC 6525 section 5.2.7 H4 requires the affected outgoing SSN to reset"
    );
    Ok(())
}

#[test]
fn test_only_one_buffered_reconfig_is_sent_when_timer_is_idle() {
    let mut a = Association {
        state: AssociationState::Established,
        ..Default::default()
    };

    for (rsn, stream_id) in [(7, 1), (8, 2)] {
        insert_queued_reset(&mut a, rsn, stream_id);
    }
    assert!(a.timers.get(Timer::Reconfig).is_none());

    assert!(a.poll_transmit(Instant::now()).is_some());
    assert_eq!(a.active_reconfig, Some(7));
    assert_eq!(a.control_queue.len(), 1);
    assert!(a.reconfigs.contains_key(&8));

    let response: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
        reconfig_response_sequence_number: 7,
        result: ReconfigResult::SuccessPerformed,
    });
    a.handle_reconfig_param(&response, &mut vec![]).unwrap();
    assert!(a.poll_transmit(Instant::now()).is_some());
    assert_eq!(a.active_reconfig, Some(8));
    assert!(a.control_queue.is_empty());
}

#[test]
fn test_close_discards_buffered_reconfig_requests() -> Result<()> {
    let mut a = Association {
        state: AssociationState::Established,
        ..Default::default()
    };
    insert_active_reset(&mut a, 7, 1);
    insert_queued_reset(&mut a, 8, 2);
    a.timers
        .start(Timer::Reconfig, Instant::now(), a.rto_mgr.get_rto());

    a.close()?;

    assert!(
        a.poll_transmit(Instant::now()).is_none(),
        "a closed association must not send a buffered stream-reset request"
    );
    assert!(a.active_reconfig.is_none());
    assert!(a.reconfigs.is_empty());
    Ok(())
}

#[test]
fn test_in_progress_keeps_later_request_buffered() -> Result<()> {
    let mut a = Association {
        state: AssociationState::Established,
        ..Default::default()
    };

    insert_active_reset(&mut a, 7, 1);
    insert_queued_reset(&mut a, 8, 2);
    a.timers
        .start(Timer::Reconfig, Instant::now(), a.rto_mgr.get_rto());

    let response: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
        reconfig_response_sequence_number: 7,
        result: ReconfigResult::InProgress,
    });
    a.handle_reconfig_param(&response, &mut vec![])?;
    assert!(a.reconfigs.contains_key(&7));

    let _ = a.poll_transmit(Instant::now());
    assert_eq!(a.active_reconfig, Some(7));
    assert_eq!(a.control_queue.len(), 1);
    Ok(())
}

#[test]
fn test_local_reset_stays_queued_while_reconfig_timer_runs() -> Result<()> {
    let mut a = Association {
        state: AssociationState::Established,
        my_next_tsn: 1,
        ..Default::default()
    };

    insert_active_reset(&mut a, 7, 1);
    a.timers
        .start(Timer::Reconfig, Instant::now(), a.rto_mgr.get_rto());
    a.send_reset_request(2)?;

    let _ = a.poll_transmit(Instant::now());
    assert_eq!(
        a.reconfigs.len(),
        1,
        "a locally initiated reset must remain queued while another request is in flight"
    );
    assert_eq!(
        a.pending_reset_streams.iter().copied().collect::<Vec<_>>(),
        [2]
    );

    let response: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
        reconfig_response_sequence_number: 7,
        result: ReconfigResult::SuccessPerformed,
    });
    a.handle_reconfig_param(&response, &mut vec![])?;
    assert!(a.poll_transmit(Instant::now()).is_some());
    assert_ne!(a.active_reconfig, Some(7));
    assert!(a.active_reconfig.is_some());
    assert!(a.pending_reset_streams.is_empty());
    Ok(())
}

#[test]
fn test_local_reset_does_not_overtake_pending_data() -> Result<()> {
    let mut a = Association {
        state: AssociationState::Established,
        my_next_tsn: 1,
        ..Default::default()
    };
    a.inflight_queue.push_no_check(ChunkPayloadData {
        tsn: 1,
        user_data: Bytes::from_static(b"inflight"),
        ..Default::default()
    });
    a.pending_queue.push(ChunkPayloadData {
        stream_identifier: 1,
        beginning_fragment: true,
        ending_fragment: true,
        user_data: Bytes::from_static(b"pending"),
        ..Default::default()
    });
    a.send_reset_request(1)?;

    let _ = a.poll_transmit(Instant::now());
    assert!(a.active_reconfig.is_none());
    assert!(a.reconfigs.is_empty());
    assert_eq!(a.pending_reset_streams.len(), 1);
    Ok(())
}

#[test]
fn test_local_reset_is_not_blocked_by_unrelated_pending_data() -> Result<()> {
    let reset_stream_id = 1;
    let unrelated_stream_id = 2;
    let mut a = Association {
        state: AssociationState::Established,
        my_next_tsn: 2,
        cwnd: 0,
        rwnd: 0,
        mtu: 1400,
        ..Default::default()
    };
    a.inflight_queue.push_no_check(ChunkPayloadData {
        tsn: 1,
        stream_identifier: unrelated_stream_id,
        user_data: Bytes::from_static(b"inflight"),
        ..Default::default()
    });
    a.pending_queue.push(ChunkPayloadData {
        stream_identifier: unrelated_stream_id,
        beginning_fragment: true,
        ending_fragment: true,
        user_data: Bytes::from_static(b"flow controlled"),
        ..Default::default()
    });
    a.send_reset_request(reset_stream_id)?;

    let _ = a.gather_outbound(Instant::now());

    assert!(
        a.active_reconfig.is_some(),
        "flow-controlled DATA on another stream must not starve this reset"
    );
    assert!(
        !a.pending_queue.is_empty(),
        "the test requires the unrelated DATA to remain flow controlled"
    );
    Ok(())
}

#[test]
fn test_reset_sender_last_tsn_wraps_at_zero() -> Result<()> {
    let stream_id = 1;
    let mut a = Association {
        state: AssociationState::Established,
        my_next_tsn: 0,
        ..Default::default()
    };
    a.send_reset_request(stream_id)?;

    let _ = a.gather_outbound(Instant::now());

    let reset = a
        .reconfigs
        .get(&a.active_reconfig.unwrap())
        .unwrap()
        .param_a
        .as_ref()
        .and_then(|param| param.as_any().downcast_ref::<ParamOutgoingResetRequest>())
        .unwrap();
    assert_eq!(
        reset.sender_last_tsn,
        u32::MAX,
        "the TSN preceding zero must wrap to u32::MAX"
    );
    Ok(())
}

#[test]
fn test_reset_request_sequence_number_wraps() -> Result<()> {
    let stream_id = 1;
    let mut a = Association {
        state: AssociationState::Established,
        my_next_tsn: 1,
        my_next_rsn: u32::MAX,
        ..Default::default()
    };
    a.send_reset_request(stream_id)?;

    let _ = a.gather_outbound(Instant::now());

    assert_eq!(a.active_reconfig, Some(u32::MAX));
    assert_eq!(
        a.my_next_rsn, 0,
        "the re-configuration request sequence number must wrap"
    );
    Ok(())
}

#[test]
fn test_local_reset_acknowledges_latest_peer_request_sequence() -> Result<()> {
    let peer_rsn = 41;
    let mut a = Association {
        state: AssociationState::Established,
        my_next_tsn: 1,
        ..Default::default()
    };

    let peer_request: Box<dyn Param + Send + Sync> = Box::new(ParamOutgoingResetRequest {
        reconfig_request_sequence_number: peer_rsn,
        reconfig_response_sequence_number: u32::MAX,
        sender_last_tsn: a.peer_last_tsn,
        stream_identifiers: vec![],
    });
    a.handle_reconfig_param(&peer_request, &mut vec![])?;
    assert_eq!(a.max_completed_reconfig_rsn, Some(peer_rsn));

    a.send_reset_request(1)?;
    let _ = a.gather_outbound(Instant::now());

    let reset = a
        .reconfigs
        .get(&a.active_reconfig.unwrap())
        .unwrap()
        .param_a
        .as_ref()
        .and_then(|param| param.as_any().downcast_ref::<ParamOutgoingResetRequest>())
        .unwrap();
    assert_eq!(
        reset.reconfig_response_sequence_number, peer_rsn,
        "RFC 6525 section 5.1.2 A4 requires the latest peer RSN in the response field"
    );
    Ok(())
}

#[test]
fn test_reciprocal_reset_covers_preexisting_pending_data() -> Result<()> {
    let stream_id = 1;
    let mut a = Association {
        state: AssociationState::Established,
        my_next_tsn: 1,
        cwnd: 1400,
        rwnd: 1400,
        mtu: 1400,
        ..Default::default()
    };
    assert!(
        a.create_stream(stream_id, false, PayloadProtocolIdentifier::Binary)
            .is_some()
    );
    a.pending_queue.push(ChunkPayloadData {
        stream_identifier: stream_id,
        stream_sequence_number: 4,
        beginning_fragment: true,
        ending_fragment: true,
        user_data: Bytes::from_static(b"pending"),
        ..Default::default()
    });

    // Receiving the peer's outgoing reset creates a reciprocal outgoing reset.
    // DATA accepted before that request must be assigned a TSN covered by the
    // reciprocal request's Sender's Last Assigned TSN boundary.
    let request: Box<dyn Param + Send + Sync> = Box::new(ParamOutgoingResetRequest {
        reconfig_request_sequence_number: 7,
        reconfig_response_sequence_number: u32::MAX,
        sender_last_tsn: a.peer_last_tsn,
        stream_identifiers: vec![stream_id],
    });
    let mut reply = vec![];
    a.handle_reconfig_param(&request, &mut reply)?;
    a.control_queue.extend(reply);

    let _ = a.gather_outbound(Instant::now());

    let data_tsn = a.inflight_queue.get(1).unwrap().tsn;
    let reciprocal = a
        .reconfigs
        .get(&a.active_reconfig.unwrap())
        .unwrap()
        .param_a
        .as_ref()
        .and_then(|param| param.as_any().downcast_ref::<ParamOutgoingResetRequest>())
        .unwrap();
    assert!(
        sna32gte(reciprocal.sender_last_tsn, data_tsn),
        "the reciprocal reset boundary must cover DATA queued before the reset"
    );
    Ok(())
}

#[test]
fn test_retransmission_does_not_send_buffered_reconfigs() {
    let mut a = Association {
        state: AssociationState::Established,
        ..Default::default()
    };

    insert_active_reset(&mut a, 7, 1);
    insert_queued_reset(&mut a, 8, 2);
    a.timers
        .start(Timer::Reconfig, Instant::now(), a.rto_mgr.get_rto());
    a.will_retransmit_reconfig = true;

    let (packets, _) = a.gather_outbound(Instant::now());
    assert_eq!(
        packets.len(),
        1,
        "retransmission must include only the request associated with the running timer"
    );
}

#[test]
fn test_older_completion_does_not_make_newer_generation_writable() -> Result<()> {
    let stream_id = 1;
    let mut a = Association::default();

    a.pending_reset_completions.insert(stream_id);
    a.pending_reset_completions.insert(stream_id);
    insert_active_reset(&mut a, 7, stream_id);
    insert_queued_reset(&mut a, 8, stream_id);
    a.timers
        .start(Timer::Reconfig, Instant::now(), a.rto_mgr.get_rto());

    assert!(a.get_or_create_stream(stream_id).is_some());
    assert!(!a.stream(stream_id)?.is_writable());

    let response: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
        reconfig_response_sequence_number: 7,
        result: ReconfigResult::SuccessPerformed,
    });
    a.handle_reconfig_param(&response, &mut vec![])?;

    assert!(a.reconfigs.contains_key(&8));
    assert!(
        !a.stream(stream_id)?.is_writable(),
        "an older success cannot release the newer generation's outgoing quarantine"
    );
    Ok(())
}

#[test]
fn test_stale_completion_does_not_override_newer_denial() -> Result<()> {
    let stream_id = 1;
    let mut a = Association::default();

    a.pending_reset_completions.insert(stream_id);
    insert_active_reset(&mut a, 7, stream_id);

    let success: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
        reconfig_response_sequence_number: 7,
        result: ReconfigResult::SuccessPerformed,
    });
    a.handle_reconfig_param(&success, &mut vec![])?;

    // Before the application polls generation 7's completion, the peer opens
    // and uses generation 8, then resets it. Its nonzero outgoing sequence
    // means a denied reciprocal reset must quarantine the id.
    assert!(a.get_or_create_stream(stream_id).is_some());
    a.streams.get_mut(&stream_id).unwrap().sequence_number = 1;
    a.unregister_stream(stream_id, true);
    insert_active_reset(&mut a, 8, stream_id);
    a.timers
        .start(Timer::Reconfig, Instant::now(), a.rto_mgr.get_rto());

    let denied: Box<dyn Param + Send + Sync> = Box::new(ParamReconfigResponse {
        reconfig_response_sequence_number: 8,
        result: ReconfigResult::Denied,
    });
    a.handle_reconfig_param(&denied, &mut vec![])?;

    // Polling the stale completion must not resurrect reuse permission after
    // generation 8 has already failed.
    assert!(matches!(
        a.poll(),
        Some(Event::Stream(StreamEvent::ResetComplete { id })) if id == stream_id
    ));

    assert!(matches!(
        a.open_stream(stream_id, PayloadProtocolIdentifier::Binary),
        Err(Error::ErrStreamResetPending)
    ));
    Ok(())
}

#[test]
fn test_create_forward_tsn_forward_one_abandoned() -> Result<()> {
    let mut a = Association {
        cumulative_tsn_ack_point: 9,
        advanced_peer_tsn_ack_point: 10,
        ..Default::default()
    };

    a.inflight_queue.push_no_check(ChunkPayloadData {
        beginning_fragment: true,
        ending_fragment: true,
        tsn: 10,
        stream_identifier: 1,
        stream_sequence_number: 2,
        user_data: Bytes::from_static(b"ABC"),
        nsent: 1,
        abandoned: true,
        ..Default::default()
    });

    let fwdtsn = a.create_forward_tsn();

    assert_eq!(10, fwdtsn.new_cumulative_tsn, "should be able to serialize");
    assert_eq!(1, fwdtsn.streams.len(), "there should be one stream");
    assert_eq!(1, fwdtsn.streams[0].identifier, "si should be 1");
    assert_eq!(2, fwdtsn.streams[0].sequence, "ssn should be 2");

    Ok(())
}

#[test]
fn test_create_forward_tsn_forward_two_abandoned_with_the_same_si() -> Result<()> {
    let mut a = Association {
        cumulative_tsn_ack_point: 9,
        advanced_peer_tsn_ack_point: 12,
        ..Default::default()
    };

    a.inflight_queue.push_no_check(ChunkPayloadData {
        beginning_fragment: true,
        ending_fragment: true,
        tsn: 10,
        stream_identifier: 1,
        stream_sequence_number: 2,
        user_data: Bytes::from_static(b"ABC"),
        nsent: 1,
        abandoned: true,
        ..Default::default()
    });
    a.inflight_queue.push_no_check(ChunkPayloadData {
        beginning_fragment: true,
        ending_fragment: true,
        tsn: 11,
        stream_identifier: 1,
        stream_sequence_number: 3,
        user_data: Bytes::from_static(b"DEF"),
        nsent: 1,
        abandoned: true,
        ..Default::default()
    });
    a.inflight_queue.push_no_check(ChunkPayloadData {
        beginning_fragment: true,
        ending_fragment: true,
        tsn: 12,
        stream_identifier: 2,
        stream_sequence_number: 1,
        user_data: Bytes::from_static(b"123"),
        nsent: 1,
        abandoned: true,
        ..Default::default()
    });

    let fwdtsn = a.create_forward_tsn();

    assert_eq!(12, fwdtsn.new_cumulative_tsn, "should be able to serialize");
    assert_eq!(2, fwdtsn.streams.len(), "there should be two stream");

    let mut si1ok = false;
    let mut si2ok = false;
    for s in &fwdtsn.streams {
        match s.identifier {
            1 => {
                assert_eq!(3, s.sequence, "ssn should be 3");
                si1ok = true;
            }
            2 => {
                assert_eq!(1, s.sequence, "ssn should be 1");
                si2ok = true;
            }
            _ => panic!("unexpected stream indentifier"),
        }
    }
    assert!(si1ok, "si=1 should be present");
    assert!(si2ok, "si=2 should be present");

    Ok(())
}

#[test]
fn test_handle_forward_tsn_forward_3unreceived_chunks() -> Result<()> {
    let mut a = Association {
        use_forward_tsn: true,
        ..Default::default()
    };

    let prev_tsn = a.peer_last_tsn;

    let fwdtsn = ChunkForwardTsn {
        new_cumulative_tsn: a.peer_last_tsn + 3,
        streams: vec![ChunkForwardTsnStream {
            identifier: 0,
            sequence: 0,
        }],
    };

    let p = a.handle_forward_tsn(&fwdtsn)?;

    let delayed_ack_triggered = a.delayed_ack_triggered;
    let immediate_ack_triggered = a.immediate_ack_triggered;
    assert_eq!(
        a.peer_last_tsn,
        prev_tsn + 3,
        "peerLastTSN should advance by 3 "
    );
    assert!(delayed_ack_triggered, "delayed sack should be triggered");
    assert!(
        !immediate_ack_triggered,
        "immediate sack should NOT be triggered"
    );
    assert!(p.is_empty(), "should return empty");

    Ok(())
}

#[test]
fn test_handle_forward_tsn_forward_1for1_missing() -> Result<()> {
    let mut a = Association {
        use_forward_tsn: true,
        ..Default::default()
    };

    let prev_tsn = a.peer_last_tsn;

    // this chunk is blocked by the missing chunk at tsn=1
    a.payload_queue.push(
        ChunkPayloadData {
            beginning_fragment: true,
            ending_fragment: true,
            tsn: a.peer_last_tsn + 2,
            stream_identifier: 0,
            stream_sequence_number: 1,
            user_data: Bytes::from_static(b"ABC"),
            ..Default::default()
        },
        a.peer_last_tsn,
    );

    let fwdtsn = ChunkForwardTsn {
        new_cumulative_tsn: a.peer_last_tsn + 1,
        streams: vec![ChunkForwardTsnStream {
            identifier: 0,
            sequence: 1,
        }],
    };

    let p = a.handle_forward_tsn(&fwdtsn)?;

    let delayed_ack_triggered = a.delayed_ack_triggered;
    let immediate_ack_triggered = a.immediate_ack_triggered;
    assert_eq!(
        a.peer_last_tsn,
        prev_tsn + 2,
        "peerLastTSN should advance by 2"
    );
    assert!(delayed_ack_triggered, "delayed sack should be triggered");
    assert!(
        !immediate_ack_triggered,
        "immediate sack should NOT be triggered"
    );
    assert!(p.is_empty(), "should return empty");

    Ok(())
}

#[test]
fn test_handle_forward_tsn_forward_1for2_missing() -> Result<()> {
    let mut a = Association {
        use_forward_tsn: true,
        ..Default::default()
    };

    a.use_forward_tsn = true;
    let prev_tsn = a.peer_last_tsn;

    // this chunk is blocked by the missing chunk at tsn=1
    a.payload_queue.push(
        ChunkPayloadData {
            beginning_fragment: true,
            ending_fragment: true,
            tsn: a.peer_last_tsn + 3,
            stream_identifier: 0,
            stream_sequence_number: 1,
            user_data: Bytes::from_static(b"ABC"),
            ..Default::default()
        },
        a.peer_last_tsn,
    );

    let fwdtsn = ChunkForwardTsn {
        new_cumulative_tsn: a.peer_last_tsn + 1,
        streams: vec![ChunkForwardTsnStream {
            identifier: 0,
            sequence: 1,
        }],
    };

    let p = a.handle_forward_tsn(&fwdtsn)?;

    let immediate_ack_triggered = a.immediate_ack_triggered;
    assert_eq!(
        a.peer_last_tsn,
        prev_tsn + 1,
        "peerLastTSN should advance by 1"
    );
    assert!(
        immediate_ack_triggered,
        "immediate sack should be triggered"
    );
    assert!(p.is_empty(), "should return empty");

    Ok(())
}

#[test]
fn test_handle_forward_tsn_dup_forward_tsn_chunk_should_generate_sack() -> Result<()> {
    let mut a = Association {
        use_forward_tsn: true,
        ..Default::default()
    };

    let prev_tsn = a.peer_last_tsn;

    let fwdtsn = ChunkForwardTsn {
        new_cumulative_tsn: a.peer_last_tsn,
        streams: vec![ChunkForwardTsnStream {
            identifier: 0,
            sequence: 1,
        }],
    };

    let p = a.handle_forward_tsn(&fwdtsn)?;

    let ack_state = a.ack_state;
    assert_eq!(a.peer_last_tsn, prev_tsn, "peerLastTSN should not advance");
    assert_eq!(AckState::Immediate, ack_state, "sack should be requested");
    assert!(p.is_empty(), "should return empty");

    Ok(())
}

#[test]
fn test_assoc_create_new_stream() -> Result<()> {
    let mut a = Association::default();

    for i in 0..ACCEPT_CH_SIZE {
        let stream_identifier =
            if let Some(s) = a.create_stream(i as u16, true, PayloadProtocolIdentifier::Unknown) {
                s.stream_identifier
            } else {
                panic!("{} should success", i);
            };
        let result = a.streams.get(&stream_identifier);
        assert!(result.is_some(), "should be in a.streams map");
    }

    let new_si = ACCEPT_CH_SIZE as u16;
    let result = a.streams.get(&new_si);
    assert!(result.is_none(), "should NOT be in a.streams map");

    let to_be_ignored = ChunkPayloadData {
        beginning_fragment: true,
        ending_fragment: true,
        tsn: a.peer_last_tsn + 1,
        stream_identifier: new_si,
        user_data: Bytes::from_static(b"ABC"),
        ..Default::default()
    };

    let p = a.handle_data(&to_be_ignored)?;
    assert!(p.is_empty(), "should return empty");

    Ok(())
}

fn handle_init_test(name: &str, initial_state: AssociationState, expect_err: bool) {
    let mut a = create_association(TransportConfig::default());
    a.set_state(initial_state);
    let pkt = Packet {
        common_header: CommonHeader {
            source_port: 5001,
            destination_port: 5002,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut init = ChunkInit {
        initial_tsn: 1234,
        num_outbound_streams: 1001,
        num_inbound_streams: 1002,
        initiate_tag: 5678,
        advertised_receiver_window_credit: 512 * 1024,
        ..Default::default()
    };
    init.set_supported_extensions();

    let result = a.handle_init(&pkt, &init);
    if expect_err {
        assert!(result.is_err(), "{} should fail", name);
        return;
    } else {
        assert!(result.is_ok(), "{} should be ok", name);
    }
    assert_eq!(
        if init.initial_tsn == 0 {
            u32::MAX
        } else {
            init.initial_tsn - 1
        },
        a.peer_last_tsn,
        "{} should match",
        name
    );
    assert_eq!(1001, a.my_max_num_outbound_streams, "{} should match", name);
    assert_eq!(1002, a.my_max_num_inbound_streams, "{} should match", name);
    assert_eq!(5678, a.peer_verification_tag, "{} should match", name);
    assert_eq!(
        pkt.common_header.source_port, a.destination_port,
        "{} should match",
        name
    );
    assert_eq!(
        pkt.common_header.destination_port, a.source_port,
        "{} should match",
        name
    );
    assert!(a.use_forward_tsn, "{} should be set to true", name);
    assert_eq!(
        512 * 1024,
        a.rwnd,
        "{} rwnd should be initialized from peer's advertised_receiver_window_credit",
        name
    );
    assert_eq!(
        a.rwnd, a.ssthresh,
        "{} ssthresh should be initialized to rwnd",
        name
    );
}

#[test]
fn test_assoc_handle_init() -> Result<()> {
    handle_init_test("normal", AssociationState::Closed, false);

    handle_init_test(
        "unexpected state established",
        AssociationState::Established,
        true,
    );

    handle_init_test(
        "unexpected state shutdownAckSent",
        AssociationState::ShutdownAckSent,
        true,
    );

    handle_init_test(
        "unexpected state shutdownPending",
        AssociationState::ShutdownPending,
        true,
    );

    handle_init_test(
        "unexpected state shutdownReceived",
        AssociationState::ShutdownReceived,
        true,
    );

    handle_init_test(
        "unexpected state shutdownSent",
        AssociationState::ShutdownSent,
        true,
    );

    Ok(())
}

#[test]
fn test_assoc_max_send_message_size_default() -> Result<()> {
    let mut a = create_association(TransportConfig::default());
    assert_eq!(65536, a.max_send_message_size, "should match");

    let ppi = PayloadProtocolIdentifier::Unknown;
    let stream = a.create_stream(1, false, ppi);
    assert!(stream.is_some(), "should succeed");

    if let Some(mut s) = stream {
        let p = Bytes::from(vec![0u8; 65537]);

        if let Err(err) = s.write_sctp(&p.slice(..65536), ppi) {
            assert_ne!(
                Error::ErrOutboundPacketTooLarge,
                err,
                "should be not Error::ErrOutboundPacketTooLarge"
            );
        } else {
            panic!("should be error");
        }

        if let Err(err) = s.write_sctp(&p.slice(..65537), ppi) {
            assert_eq!(
                Error::ErrOutboundPacketTooLarge,
                err,
                "should be Error::ErrOutboundPacketTooLarge"
            );
        } else {
            panic!("should be error");
        }
    }

    Ok(())
}

#[test]
fn test_assoc_max_send_message_size_explicit() -> Result<()> {
    let mut a = create_association(TransportConfig::default().with_max_send_message_size(30000));
    assert_eq!(30000, a.max_send_message_size, "should match");

    let ppi = PayloadProtocolIdentifier::Unknown;
    let stream = a.create_stream(1, false, ppi);
    assert!(stream.is_some(), "should succeed");

    if let Some(mut s) = stream {
        let p = Bytes::from(vec![0u8; 30001]);

        if let Err(err) = s.write_sctp(&p.slice(..30000), ppi) {
            assert_ne!(
                Error::ErrOutboundPacketTooLarge,
                err,
                "should be not Error::ErrOutboundPacketTooLarge"
            );
        } else {
            panic!("should be error");
        }

        if let Err(err) = s.write_sctp(&p.slice(..30001), ppi) {
            assert_eq!(
                Error::ErrOutboundPacketTooLarge,
                err,
                "should be Error::ErrOutboundPacketTooLarge"
            );
        } else {
            panic!("should be error");
        }
    }

    Ok(())
}

#[test]
fn test_assoc_max_receive_message_size_default() -> Result<()> {
    let mut a = create_association(TransportConfig::default());
    assert_eq!(65536, a.max_receive_message_size, "should match");

    let p = Bytes::from(vec![0u8; 65537]);

    let size_ok = ChunkPayloadData {
        beginning_fragment: true,
        ending_fragment: true,
        tsn: a.peer_last_tsn + 1,
        stream_identifier: 1,
        user_data: p.slice(..65536),
        ..Default::default()
    };

    assert!(a.handle_data(&size_ok).is_ok(), "should succeed");

    let too_large = ChunkPayloadData {
        beginning_fragment: true,
        ending_fragment: true,
        tsn: a.peer_last_tsn + 1,
        stream_identifier: 1,
        user_data: p,
        ..Default::default()
    };

    if let Err(err) = a.handle_data(&too_large) {
        assert_eq!(
            Error::ErrInboundPacketTooLarge,
            err,
            "should be Error::ErrInboundPacketTooLarge"
        );
    } else {
        panic!("should be error");
    }

    Ok(())
}

#[test]
fn test_assoc_max_receive_message_size_explicit() -> Result<()> {
    let mut a = create_association(TransportConfig::default().with_max_receive_message_size(1024));
    assert_eq!(1024, a.max_receive_message_size, "should match");

    let first_chunk = ChunkPayloadData {
        beginning_fragment: true,
        ending_fragment: true,
        tsn: a.peer_last_tsn + 1,
        stream_identifier: 1,
        user_data: Bytes::from(vec![0u8; 512]),
        ..Default::default()
    };

    assert!(a.handle_data(&first_chunk).is_ok(), "should succeed");

    let second_chunk = ChunkPayloadData {
        beginning_fragment: true,
        ending_fragment: true,
        tsn: a.peer_last_tsn + 1,
        stream_identifier: 1,
        user_data: Bytes::from(vec![0u8; 513]),
        ..Default::default()
    };

    if let Err(err) = a.handle_data(&second_chunk) {
        assert_eq!(
            Error::ErrInboundPacketTooLarge,
            err,
            "should be Error::ErrInboundPacketTooLarge"
        );
    } else {
        panic!("should be error");
    }

    Ok(())
}

#[test]
fn test_assoc_max_message_size_asymmetric() -> Result<()> {
    let config = TransportConfig::default()
        .with_max_send_message_size(1024)
        .with_max_receive_message_size(30000);

    let mut a = create_association(config);
    assert_eq!(1024, a.max_send_message_size, "should match");
    assert_eq!(30000, a.max_receive_message_size, "should match");

    let ppi = PayloadProtocolIdentifier::Unknown;
    let stream = a.create_stream(1, false, ppi);
    assert!(stream.is_some(), "should succeed");

    if let Some(mut s) = stream {
        let p = Bytes::from(vec![0u8; 1025]);

        if let Err(err) = s.write_sctp(&p.slice(..1024), ppi) {
            assert_ne!(
                Error::ErrOutboundPacketTooLarge,
                err,
                "should be not Error::ErrOutboundPacketTooLarge"
            );
        } else {
            panic!("should be error");
        }

        if let Err(err) = s.write_sctp(&p.slice(..1025), ppi) {
            assert_eq!(
                Error::ErrOutboundPacketTooLarge,
                err,
                "should be Error::ErrOutboundPacketTooLarge"
            );
        } else {
            panic!("should be error");
        }
    }

    let p = Bytes::from(vec![0u8; 30001]);

    let size_ok = ChunkPayloadData {
        beginning_fragment: true,
        ending_fragment: true,
        tsn: a.peer_last_tsn + 1,
        stream_identifier: 1,
        user_data: p.slice(..30000),
        ..Default::default()
    };

    assert!(a.handle_data(&size_ok).is_ok(), "should succeed");

    let too_large = ChunkPayloadData {
        beginning_fragment: true,
        ending_fragment: true,
        tsn: a.peer_last_tsn + 1,
        stream_identifier: 1,
        user_data: p,
        ..Default::default()
    };

    if let Err(err) = a.handle_data(&too_large) {
        assert_eq!(
            Error::ErrInboundPacketTooLarge,
            err,
            "should be Error::ErrInboundPacketTooLarge"
        );
    } else {
        panic!("should be error");
    }

    Ok(())
}

#[test]
fn test_generate_out_of_band_init() {
    let config = TransportConfig::default();
    let init_bytes = generate_snap_token(&config).unwrap();

    // Parse it back to validate
    let parsed = ChunkInit::unmarshal(&init_bytes).unwrap();

    assert!(!parsed.is_ack, "Should be INIT, not INIT ACK");
    assert!(parsed.initiate_tag != 0, "Initiate tag should not be zero");
    // Token always advertises u16::MAX for stream counts;
    // actual limits are applied from TransportConfig during negotiation.
    assert_eq!(
        parsed.num_outbound_streams,
        u16::MAX,
        "Outbound streams should always be u16::MAX in token"
    );
    assert_eq!(
        parsed.num_inbound_streams,
        u16::MAX,
        "Inbound streams should always be u16::MAX in token"
    );
    assert_eq!(
        parsed.advertised_receiver_window_credit,
        config.max_receive_buffer_size(),
        "ARWND should match config"
    );
}

#[test]
fn test_generate_out_of_band_init_with_custom_config() {
    let config = TransportConfig::default()
        .with_max_receive_buffer_size(2_000_000)
        .with_max_num_outbound_streams(256)
        .with_max_num_inbound_streams(512);

    let init_bytes = generate_snap_token(&config).unwrap();
    let parsed = ChunkInit::unmarshal(&init_bytes).unwrap();

    // Token always advertises u16::MAX for stream counts;
    // actual limits are applied from TransportConfig during negotiation.
    assert_eq!(parsed.num_outbound_streams, u16::MAX);
    assert_eq!(parsed.num_inbound_streams, u16::MAX);
    assert_eq!(parsed.advertised_receiver_window_credit, 2_000_000);
}

#[test]
fn test_generate_out_of_band_init_uniqueness() {
    // Each call to generate_snap_token creates a new INIT with unique random tags.
    let config1 = TransportConfig::default();
    let config2 = TransportConfig::default();

    let init1 = generate_snap_token(&config1).unwrap();
    let init2 = generate_snap_token(&config2).unwrap();

    let parsed1 = ChunkInit::unmarshal(&init1).unwrap();
    let parsed2 = ChunkInit::unmarshal(&init2).unwrap();

    // Initiate tags should be different (random)
    assert_ne!(
        parsed1.initiate_tag, parsed2.initiate_tag,
        "Initiate tags should be unique across calls"
    );

    // Initial TSNs should be different (random)
    assert_ne!(
        parsed1.initial_tsn, parsed2.initial_tsn,
        "Initial TSNs should be unique across calls"
    );
}

#[test]
fn test_out_of_band_association_creation() {
    let local_config = Arc::new(TransportConfig::default());
    let remote_config = TransportConfig::default();
    let max_payload_size = 1200;

    let local_init_bytes = generate_snap_token(&local_config).unwrap();
    let remote_init_bytes = generate_snap_token(&remote_config).unwrap();

    let local_init = ChunkInit::unmarshal(&local_init_bytes).unwrap();
    let remote_init = ChunkInit::unmarshal(&remote_init_bytes).unwrap();

    let remote_addr: SocketAddr = "192.168.1.1:5000".parse().unwrap();

    let assoc = Association::new_with_out_of_band_init(
        local_config.clone(),
        max_payload_size,
        remote_addr,
        None,
        local_init.clone(),
        remote_init.clone(),
    )
    .expect("Should create out-of-band init association");

    // Verify the association is in ESTABLISHED state
    assert_eq!(
        assoc.state(),
        AssociationState::Established,
        "Out-of-band init association should be in ESTABLISHED state"
    );

    // Verify handshake is marked complete
    assert!(
        assoc.handshake_completed,
        "Out-of-band init association should have handshake completed"
    );

    // Verify verification tags
    assert_eq!(
        assoc.my_verification_tag, local_init.initiate_tag,
        "My verification tag should match local init"
    );
    assert_eq!(
        assoc.peer_verification_tag, remote_init.initiate_tag,
        "Peer verification tag should match remote init"
    );

    // Verify TSN setup
    assert_eq!(
        assoc.my_next_tsn, local_init.initial_tsn,
        "My next TSN should match local init"
    );
    assert_eq!(
        assoc.peer_last_tsn,
        remote_init.initial_tsn.wrapping_sub(1),
        "Peer last TSN should be remote init TSN - 1"
    );

    // Verify rwnd
    assert_eq!(
        assoc.rwnd, remote_init.advertised_receiver_window_credit,
        "rwnd should match remote advertised credit"
    );
}

#[test]
fn test_out_of_band_association_stream_negotiation() {
    let config = Arc::new(
        TransportConfig::default()
            .with_max_num_outbound_streams(100)
            .with_max_num_inbound_streams(200),
    );

    // Remote uses default config — token always advertises u16::MAX
    let remote_config = TransportConfig::default();

    let local_init_bytes = generate_snap_token(&config).unwrap();
    let remote_init_bytes = generate_snap_token(&remote_config).unwrap();

    let local_init = ChunkInit::unmarshal(&local_init_bytes).unwrap();
    let remote_init = ChunkInit::unmarshal(&remote_init_bytes).unwrap();

    // Token always advertises u16::MAX for stream counts
    assert_eq!(local_init.num_outbound_streams, u16::MAX);
    assert_eq!(local_init.num_inbound_streams, u16::MAX);
    assert_eq!(remote_init.num_outbound_streams, u16::MAX);
    assert_eq!(remote_init.num_inbound_streams, u16::MAX);

    let remote_addr: SocketAddr = "192.168.1.1:5000".parse().unwrap();

    let assoc = Association::new_with_out_of_band_init(
        config.clone(),
        1200,
        remote_addr,
        None,
        local_init,
        remote_init,
    )
    .expect("Should create out-of-band init association");

    // Stream limits should be clamped by the local config, since the
    // remote token always offers u16::MAX.
    // my_max_num_outbound_streams = min(config.max_out=100, remote_in=MAX) = 100
    assert_eq!(
        assoc.my_max_num_outbound_streams, 100,
        "Outbound streams should be clamped by local config"
    );

    // my_max_num_inbound_streams = min(config.max_in=200, remote_out=MAX) = 200
    assert_eq!(
        assoc.my_max_num_inbound_streams, 200,
        "Inbound streams should be clamped by local config"
    );
}

#[test]
fn test_out_of_band_connected_event() {
    let local_config = Arc::new(TransportConfig::default());
    let remote_config = TransportConfig::default();

    let local_init_bytes = generate_snap_token(&local_config).unwrap();
    let remote_init_bytes = generate_snap_token(&remote_config).unwrap();

    let local_init = ChunkInit::unmarshal(&local_init_bytes).unwrap();
    let remote_init = ChunkInit::unmarshal(&remote_init_bytes).unwrap();

    let remote_addr: SocketAddr = "192.168.1.1:5000".parse().unwrap();

    let mut assoc = Association::new_with_out_of_band_init(
        local_config.clone(),
        1200,
        remote_addr,
        None,
        local_init,
        remote_init,
    )
    .expect("Should create out-of-band init association");

    // Poll should return a Connected event
    let event = assoc.poll();
    assert!(
        matches!(event, Some(Event::Connected)),
        "Should emit Connected event, got {:?}",
        event
    );
}

#[test]
fn test_out_of_band_symmetric_setup() {
    // Test that both sides of an out-of-band init association work correctly
    let config_a = Arc::new(TransportConfig::default());
    let config_b = Arc::new(TransportConfig::default());

    let init_a_bytes = generate_snap_token(&config_a).unwrap();
    let init_b_bytes = generate_snap_token(&config_b).unwrap();

    let init_a = ChunkInit::unmarshal(&init_a_bytes).unwrap();
    let init_b = ChunkInit::unmarshal(&init_b_bytes).unwrap();

    let addr_a: SocketAddr = "192.168.1.1:5000".parse().unwrap();
    let addr_b: SocketAddr = "192.168.1.2:5000".parse().unwrap();

    // Create association A (local=A, remote=B)
    let assoc_a = Association::new_with_out_of_band_init(
        config_a.clone(),
        1200,
        addr_b,
        None,
        init_a.clone(),
        init_b.clone(),
    )
    .expect("Should create association A");

    // Create association B (local=B, remote=A)
    let assoc_b = Association::new_with_out_of_band_init(
        config_b.clone(),
        1200,
        addr_a,
        None,
        init_b.clone(),
        init_a.clone(),
    )
    .expect("Should create association B");

    // Verify both are in ESTABLISHED state
    assert_eq!(assoc_a.state(), AssociationState::Established);
    assert_eq!(assoc_b.state(), AssociationState::Established);

    // Verify verification tags are cross-matched
    assert_eq!(assoc_a.my_verification_tag, assoc_b.peer_verification_tag);
    assert_eq!(assoc_b.my_verification_tag, assoc_a.peer_verification_tag);
}

#[test]
fn test_out_of_band_with_forward_tsn_support() {
    let local_config = Arc::new(TransportConfig::default());
    let remote_config = TransportConfig::default();

    let local_init_bytes = generate_snap_token(&local_config).unwrap();
    let remote_init_bytes = generate_snap_token(&remote_config).unwrap();

    let local_init = ChunkInit::unmarshal(&local_init_bytes).unwrap();
    let remote_init = ChunkInit::unmarshal(&remote_init_bytes).unwrap();

    // Verify supported extensions are present
    let mut has_forward_tsn = false;
    for param in &local_init.params {
        if let Some(ext) = param
            .as_any()
            .downcast_ref::<crate::param::param_supported_extensions::ParamSupportedExtensions>(
        ) {
            for ct in &ext.chunk_types {
                if *ct == crate::chunk::chunk_type::CT_FORWARD_TSN {
                    has_forward_tsn = true;
                }
            }
        }
    }
    assert!(
        has_forward_tsn,
        "Generated INIT should include ForwardTSN support"
    );

    let remote_addr: SocketAddr = "192.168.1.1:5000".parse().unwrap();

    let assoc = Association::new_with_out_of_band_init(
        local_config.clone(),
        1200,
        remote_addr,
        None,
        local_init,
        remote_init,
    )
    .expect("Should create out-of-band init association");

    assert!(
        assoc.use_forward_tsn,
        "Out-of-band init association should have ForwardTSN enabled"
    );
}

#[test]
fn test_out_of_band_initial_tsn_zero_wrap() {
    // Test edge case where initial TSN is 0 (wraps to MAX)
    let config = Arc::new(TransportConfig::default());

    let local_init_bytes = generate_snap_token(&config).unwrap();
    let mut remote_init = ChunkInit::unmarshal(&local_init_bytes).unwrap();

    // Set initial TSN to 0 to test the edge case
    remote_init.initial_tsn = 0;
    remote_init.initiate_tag = 12345;

    // Generate a fresh local init for the association
    let actual_local_init_bytes = generate_snap_token(&config).unwrap();
    let local_init = ChunkInit::unmarshal(&actual_local_init_bytes).unwrap();
    let remote_addr: SocketAddr = "192.168.1.1:5000".parse().unwrap();

    let assoc = Association::new_with_out_of_band_init(
        config.clone(),
        1200,
        remote_addr,
        None,
        local_init,
        remote_init,
    )
    .expect("Should create out-of-band init association");

    // peer_last_tsn should be u32::MAX when initial_tsn is 0
    assert_eq!(
        assoc.peer_last_tsn,
        u32::MAX,
        "peer_last_tsn should wrap to MAX when initial_tsn is 0"
    );
}

#[test]
fn test_out_of_band_rwnd_negotiation() {
    let local_config = Arc::new(TransportConfig::default().with_max_receive_buffer_size(500_000));

    let remote_config = TransportConfig::default().with_max_receive_buffer_size(300_000);

    let local_init_bytes = generate_snap_token(&local_config).unwrap();
    let remote_init_bytes = generate_snap_token(&remote_config).unwrap();

    let local_init = ChunkInit::unmarshal(&local_init_bytes).unwrap();
    let remote_init = ChunkInit::unmarshal(&remote_init_bytes).unwrap();

    let remote_addr: SocketAddr = "192.168.1.1:5000".parse().unwrap();

    let assoc = Association::new_with_out_of_band_init(
        local_config.clone(),
        1200,
        remote_addr,
        None,
        local_init,
        remote_init,
    )
    .expect("Should create out-of-band init association");

    // rwnd should be set to remote's advertised receiver window credit
    assert_eq!(
        assoc.rwnd, 300_000,
        "rwnd should be remote's advertised window"
    );
}

#[test]
fn test_initial_cwnd_small_mtu() {
    // Regression test for #47: a small MTU made 4*MTU < 4380, which previously
    // panicked because clamp(4380, 4*MTU) was called with min > max.
    let max_payload_size = 100;
    let mtu = max_payload_size + COMMON_HEADER_SIZE + DATA_CHUNK_HEADER_SIZE;
    assert!(4 * mtu < 4380, "test must exercise the small-MTU branch");

    let assoc = Association::new(
        None,
        Arc::new(TransportConfig::default()),
        max_payload_size,
        0,
        SocketAddr::from_str("0.0.0.0:0").unwrap(),
        None,
        Instant::now(),
    );

    // RFC 4960 Sec 7.2.1: min(4*MTU, max(2*MTU, 4380)).
    assert_eq!(assoc.cwnd, (4 * mtu).min((2 * mtu).max(4380)));
    assert_eq!(assoc.cwnd, 4 * mtu);
}
