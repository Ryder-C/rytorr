pub mod bencode;

#[cfg(test)]
mod tests {
    use super::*;
    use bencode::{File, Torrent};
    use chrono::DateTime;
    use std::{path::PathBuf, str::FromStr};

    #[test]
    fn test_torrent_decoding_v1() {
        // Test the decoding of a basic torrent file
        let torrent_path = "test_data/ubuntu.torrent";
        let torrent = Torrent::new(torrent_path);
        assert!(torrent.is_ok(), "Failed to decode the torrent file");

        let torrent = torrent.unwrap();
        assert_eq!(
            torrent.info.name,
            "ubuntu-24.04.2-desktop-amd64.iso".to_owned(),
            "Failed to decode name"
        );
        assert_eq!(
            torrent.info.files,
            vec![File {
                length: 6343219200,
                md5sum: None,
                path: PathBuf::from("ubuntu-24.04.2-desktop-amd64.iso"),
            }],
            "Failed to decode files"
        );
        assert_eq!(
            torrent.info.hash,
            [97, 31, 112, 137, 157, 78, 29, 106, 156, 57, 207, 201, 37, 241, 3, 223, 239, 99, 3, 40],
            "Failed to compute correct hash"
        );
        {
            use std::collections::HashSet;
            let got: HashSet<String> = torrent.announce_list.into_iter().collect();
            let expected: HashSet<String> = [
                "https://ipv6.torrent.ubuntu.com/announce".to_owned(),
                "https://torrent.ubuntu.com/announce".to_owned(),
            ]
            .into_iter()
            .collect();
            assert_eq!(got, expected, "Failed to decode announce urls");
        }
        assert_eq!(
            torrent.creation_date,
            Some(DateTime::from_str("2025-02-20T04:24:49-08:00").unwrap()),
            "Failed to decode creation date"
        );
        assert_eq!(
            torrent.comment,
            Some("Ubuntu CD releases.ubuntu.com".to_owned()),
            "Failed to decode comment"
        );
        assert_eq!(
            torrent.created_by,
            Some("mktorrent 1.1".to_owned()),
            "Failed to decode created by"
        );
        assert!(torrent.encoding.is_none(), "Failed to decode encoding");
    }
}
