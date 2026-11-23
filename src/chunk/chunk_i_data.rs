use super::{chunk_header::*, chunk_payload_data::*, chunk_type::*, *};

pub(crate) const I_DATA_HEADER_SIZE: usize = 16;

/// I-DATA chunk (RFC 8260).
///
/// The beginning fragment carries the Payload Protocol Identifier (PPID),
/// while all other fragments carry a Fragment Sequence Number (FSN). The MID
/// identifies one user message and permits fragments from different messages
/// to be interleaved on the wire.
#[derive(Debug, Clone)]
pub(crate) struct ChunkIData {
    pub(crate) unordered: bool,
    pub(crate) beginning_fragment: bool,
    pub(crate) ending_fragment: bool,
    pub(crate) immediate_sack: bool,

    pub(crate) tsn: u32,
    pub(crate) stream_identifier: u16,
    pub(crate) message_identifier: u32,
    pub(crate) payload_type: PayloadProtocolIdentifier,
    pub(crate) fragment_sequence_number: u32,
    pub(crate) user_data: Bytes,
}

impl Default for ChunkIData {
    fn default() -> Self {
        Self {
            unordered: false,
            beginning_fragment: false,
            ending_fragment: false,
            immediate_sack: false,
            tsn: 0,
            stream_identifier: 0,
            message_identifier: 0,
            payload_type: PayloadProtocolIdentifier::default(),
            fragment_sequence_number: 0,
            user_data: Bytes::new(),
        }
    }
}

impl fmt::Display for ChunkIData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}\n{}", self.header(), self.tsn)
    }
}

impl Chunk for ChunkIData {
    fn header(&self) -> ChunkHeader {
        let mut flags = 0;
        if self.ending_fragment {
            flags |= PAYLOAD_DATA_ENDING_FRAGMENT_BITMASK;
        }
        if self.beginning_fragment {
            flags |= PAYLOAD_DATA_BEGINING_FRAGMENT_BITMASK;
        }
        if self.unordered {
            flags |= PAYLOAD_DATA_UNORDERED_BITMASK;
        }
        if self.immediate_sack {
            flags |= PAYLOAD_DATA_IMMEDIATE_SACK;
        }

        ChunkHeader {
            typ: CT_I_DATA,
            flags,
            value_length: self.value_length() as u16,
        }
    }

    fn unmarshal(raw: &Bytes) -> Result<Self> {
        let header = ChunkHeader::unmarshal(raw)?;
        if header.typ != CT_I_DATA {
            return Err(Error::ErrChunkTypeNotIData);
        }
        if header.value_length() < I_DATA_HEADER_SIZE {
            return Err(Error::ErrChunkPayloadSmall);
        }

        let beginning_fragment = (header.flags & PAYLOAD_DATA_BEGINING_FRAGMENT_BITMASK) != 0;
        let reader = &mut raw.slice(CHUNK_HEADER_SIZE..CHUNK_HEADER_SIZE + header.value_length());
        let tsn = reader.get_u32();
        let stream_identifier = reader.get_u16();
        let _reserved = reader.get_u16();
        let message_identifier = reader.get_u32();
        let identifier = reader.get_u32();

        let payload_type = if beginning_fragment {
            identifier.into()
        } else {
            PayloadProtocolIdentifier::default()
        };
        let fragment_sequence_number = if beginning_fragment { 0 } else { identifier };
        let user_data = raw.slice(
            CHUNK_HEADER_SIZE + I_DATA_HEADER_SIZE..CHUNK_HEADER_SIZE + header.value_length(),
        );

        Ok(Self {
            unordered: (header.flags & PAYLOAD_DATA_UNORDERED_BITMASK) != 0,
            beginning_fragment,
            ending_fragment: (header.flags & PAYLOAD_DATA_ENDING_FRAGMENT_BITMASK) != 0,
            immediate_sack: (header.flags & PAYLOAD_DATA_IMMEDIATE_SACK) != 0,
            tsn,
            stream_identifier,
            message_identifier,
            payload_type,
            fragment_sequence_number,
            user_data,
        })
    }

    fn marshal_to(&self, writer: &mut BytesMut) -> Result<usize> {
        self.header().marshal_to(writer)?;
        writer.put_u32(self.tsn);
        writer.put_u16(self.stream_identifier);
        writer.put_u16(0);
        writer.put_u32(self.message_identifier);
        writer.put_u32(if self.beginning_fragment {
            self.payload_type as u32
        } else {
            self.fragment_sequence_number
        });
        writer.extend_from_slice(&self.user_data);
        Ok(writer.len())
    }

    fn check(&self) -> Result<()> {
        if self.beginning_fragment && self.fragment_sequence_number != 0 {
            return Err(Error::ErrChunkIDataBeginningFsn);
        }
        Ok(())
    }

    fn value_length(&self) -> usize {
        I_DATA_HEADER_SIZE + self.user_data.len()
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync) {
        self
    }
}
