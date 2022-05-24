use super::uid::UID;
use super::Result;
use bytes::{Buf, BytesMut};
use std::io::Cursor;
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug)]
pub struct Frame {
    pub token: UID,
    pub payload: std::vec::Vec<u8>, // TODO - elide copy.  See read_frame: &'a[u8],
}
// // The value does not include the size of `connectPacketLength` itself,
// // but only the other fields of this structure.
// uint32_t connectPacketLength = 0;
// ProtocolVersion protocolVersion; // Expect currentProtocolVersion

// uint16_t canonicalRemotePort = 0; // Port number to reconnect to the originating process
// uint64_t connectionId = 0; // Multi-version clients will use the same Id for both connections, other connections
//                            // will set this to zero. Added at protocol Version 0x0FDB00A444020001.

// // IP Address to reconnect to the originating process. Only one of these must be populated.
// uint32_t canonicalRemoteIp4 = 0;

// enum ConnectPacketFlags { FLAG_IPV6 = 1 };
// uint16_t flags = 0;å
// uint8_t canonicalRemoteIp6[16] = { 0 };

#[derive(Debug)]
pub struct ConnectPacket {
    len: u32,
    flags: u8,    // Really just 4 bits
    version: u64, // protocol version bytes.  Human readable in hex.
}

pub fn get_connect_packet(bytes: &mut BytesMut) -> Result<Option<ConnectPacket>> {
    let cur = Cursor::new(&bytes[..]);
    let start: usize = cur.position().try_into()?;
    let src = &cur.get_ref()[start..];

    let len_sz: usize = 4;
    let version_sz = 8; // note that the 4 msb of the version are flags.

    if src.len() < len_sz + version_sz {
        return Ok(None);
    }

    let len = u32::from_le_bytes(src[0..len_sz].try_into()?);
    let frame_length = len_sz + len as usize;
    let src = &src[len_sz..(len_sz + (len as usize))];

    let version = u64::from_le_bytes(src[0..version_sz].try_into()?);
    let src = &src[version_sz..];

    let flags: u8 = (version >> (60)).try_into()?;
    let version = version & !(0b1111 << 60);
    let cp = ConnectPacket {
        len,
        flags,
        version,
    };

    if src.len() > 0 {
        println!("ConnectPacket: {:x?} (trailing garbage(?): {:?}", cp, src);
    }
    bytes.advance(frame_length);
    Ok(Some(cp))
}

pub fn get_frame(bytes: &mut BytesMut) -> Result<Option<Frame>> {
    let cur = Cursor::new(&bytes[..]);
    let start: usize = cur.position().try_into()?;
    let src = &cur.get_ref()[start..];
    let len_sz = 4;
    let checksum_sz = 8;
    let uid_sz = 16;

    if src.len() < (len_sz + checksum_sz + uid_sz) {
        return Ok(None);
    }

    let len = u32::from_le_bytes(src[0..len_sz].try_into()?) as usize;
    let src = &src[len_sz..];
    let frame_length = len_sz + checksum_sz + len;

    let checksum = u64::from_le_bytes(src[0..checksum_sz].try_into()?);
    let src = &src[checksum_sz..];

    if src.len() < len {
        return Ok(None);
    }

    let xxhash = xxh3_64(&src[..len]);

    let uid = UID::new(src[0..uid_sz].try_into()?)?;
    let src = &src[uid_sz..];

    // println!("Got {} {:x} {:?} ({:?}) bytes_left={}", len, checksum, uid, uid.get_well_known_endpoint(), src.len());

    let payload = src[0..(len - uid_sz)].to_vec();
    // println!("Payload: {:?}", &src[0..len]);

    if checksum != xxhash {
        Err("checksum mismatch".into())
    } else {
        bytes.advance(frame_length);

        Ok(Some(Frame {
            token: uid,
            payload,
        }))
    }
}
