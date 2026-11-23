use super::chunk_i_data::*;
use super::chunk_payload_data::PayloadProtocolIdentifier;
use super::chunk_type::CT_I_DATA;
use super::*;

#[test]
fn i_data_beginning_fragment_round_trips() -> Result<()> {
    let original = ChunkIData {
        unordered: false,
        beginning_fragment: true,
        ending_fragment: false,
        immediate_sack: true,
        tsn: 0x0102_0304,
        stream_identifier: 7,
        message_identifier: 0x0506_0708,
        payload_type: PayloadProtocolIdentifier::Binary,
        fragment_sequence_number: 0,
        user_data: Bytes::from_static(b"first"),
    };

    let raw = original.marshal()?;
    let decoded = ChunkIData::unmarshal(&raw)?;
    assert_eq!(decoded.header().typ, CT_I_DATA);
    assert!(decoded.beginning_fragment);
    assert!(!decoded.ending_fragment);
    assert!(decoded.immediate_sack);
    assert_eq!(decoded.message_identifier, original.message_identifier);
    assert_eq!(decoded.payload_type, PayloadProtocolIdentifier::Binary);
    assert_eq!(decoded.fragment_sequence_number, 0);
    assert_eq!(decoded.user_data, original.user_data);
    Ok(())
}

#[test]
fn i_data_non_beginning_fragment_round_trips() -> Result<()> {
    let original = ChunkIData {
        unordered: true,
        beginning_fragment: false,
        ending_fragment: true,
        immediate_sack: false,
        tsn: 9,
        stream_identifier: 11,
        message_identifier: 12,
        payload_type: PayloadProtocolIdentifier::Unknown,
        fragment_sequence_number: 3,
        user_data: Bytes::from_static(b"last"),
    };

    let raw = original.marshal()?;
    let decoded = ChunkIData::unmarshal(&raw)?;
    assert!(decoded.unordered);
    assert!(!decoded.beginning_fragment);
    assert!(decoded.ending_fragment);
    assert_eq!(decoded.message_identifier, 12);
    assert_eq!(decoded.payload_type, PayloadProtocolIdentifier::Unknown);
    assert_eq!(decoded.fragment_sequence_number, 3);
    assert_eq!(decoded.user_data, original.user_data);
    Ok(())
}

#[test]
fn i_data_rejects_short_value() {
    let raw = Bytes::from_static(&[
        CT_I_DATA.0,
        0,
        0,
        19,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    ]);
    assert!(ChunkIData::unmarshal(&raw).is_err());
}
