import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from collector import (
    QCEClient,
    backfill_accepted_provenance,
    command_deep_backfill,
    connect_database,
    load_deep_history_cursor,
    load_qce_cursor,
    qce_image_segments,
    qce_page_with_boundary,
    recover_stale_resolving,
    resolve_image_source,
    supersede_onebot_failures,
    update_item_provenance,
    upsert_asset,
)


class QCECollectorTests(unittest.TestCase):
    def test_full_qce_page_overrides_incorrect_cached_has_next(self):
        class FakeQCE(QCEClient):
            def __init__(self):
                pass

            def call(self, _path, params=None, method="POST"):
                limit = int((params or {})["limit"])
                return {"messages": [{"msgId": str(index)} for index in range(limit)], "hasNext": False}

        messages, has_next = FakeQCE().fetch_page("1", 0, 1, 50)
        self.assertEqual(len(messages), 50)
        self.assertTrue(has_next)

    def test_fetch_before_uses_stable_raw_message_id_endpoint(self):
        class FakeQCE(QCEClient):
            def __init__(self):
                self.request = None

            def call(self, path, params=None, method="POST"):
                self.request = (path, params, method)
                return {
                    "messages": [{"msgId": "7657847017725580001"}],
                    "hasMore": True,
                }

        client = FakeQCE()
        messages, has_more = client.fetch_before(
            "10000003",
            "7657847017725580171",
            100,
        )
        self.assertEqual(client.request[0], "/api/messages/fetch-before")
        self.assertEqual(
            client.request[1],
            {
                "peer": {"chatType": 2, "peerUid": "10000003"},
                "messageId": "7657847017725580171",
                "count": 100,
            },
        )
        self.assertEqual(messages[0]["msgId"], "7657847017725580001")
        self.assertTrue(has_more)

    def test_deep_backfill_resumes_and_marks_true_roaming_end(self):
        class FakeQCE:
            def __init__(self):
                self.anchors = []

            def fetch_before(self, _group_id, message_id, _count):
                self.anchors.append(message_id)
                if len(self.anchors) == 1:
                    return [
                        {
                            "msgId": message_id,
                            "msgSeq": "100",
                            "msgTime": "1000",
                            "elements": [],
                        },
                        {
                            "msgId": "7657847017725579999",
                            "msgSeq": "90",
                            "msgTime": "900",
                            "elements": [],
                        },
                    ], True
                return [
                    {
                        "msgId": message_id,
                        "msgSeq": "90",
                        "msgTime": "900",
                        "elements": [],
                    }
                ], False

        class NeverOneBot:
            def call(self, *_args, **_kwargs):
                raise AssertionError("No image should trigger OneBot")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = connect_database(root / "state.sqlite3")
            connection.execute(
                """
                INSERT INTO group_cursors (
                    group_id, oldest_seq, oldest_time, completed, updated_at
                ) VALUES (?, ?, ?, 1, ?)
                """,
                ("10000003", "100", 1000, int(time.time())),
            )
            connection.execute(
                """
                INSERT INTO images (
                    group_id, message_id, message_seq, image_index, sent_at,
                    status, resolver, updated_at
                ) VALUES (?, ?, ?, 0, ?, 'rejected_no_metadata', 'qce', ?)
                """,
                (
                    "10000003",
                    "7657847017725580171",
                    "100",
                    1000,
                    int(time.time()),
                ),
            )
            connection.commit()
            qce = FakeQCE()

            result = command_deep_backfill(
                qce,
                NeverOneBot(),
                connection,
                ["10000003"],
                root,
                False,
                100,
                1,
            )
            self.assertEqual(result, 0)
            cursor = load_deep_history_cursor(connection, "10000003")
            self.assertEqual(cursor["oldest_message_id"], "7657847017725579999")
            self.assertEqual(cursor["oldest_seq"], "90")
            self.assertEqual(cursor["oldest_time"], 900)
            self.assertFalse(cursor["completed"])

            result = command_deep_backfill(
                qce,
                NeverOneBot(),
                connection,
                ["10000003"],
                root,
                False,
                100,
                1,
            )
            self.assertEqual(result, 0)
            cursor = load_deep_history_cursor(connection, "10000003")
            self.assertTrue(cursor["completed"])
            self.assertEqual(
                qce.anchors,
                ["7657847017725580171", "7657847017725579999"],
            )
            connection.close()

    def test_qce_image_segments_preserve_stable_media_ids(self):
        messages = [
            {
                "msgId": "7661291436232500082",
                "msgSeq": "762657",
                "msgTime": "1783783416",
                "chatType": 2,
                "peerUid": "10000001",
                "peerUin": "10000001",
                "peerName": "绘画群",
                "senderUin": "123456789",
                "senderUid": "u_internal",
                "sendMemberName": "群名片",
                "sendNickName": "昵称",
                "isImportMsg": True,
                "elements": [
                    {
                        "elementType": 1,
                        "textElement": {"content": "作品说明"},
                    },
                    {
                        "elementType": 2,
                        "elementId": "7661291436232500081",
                        "picElement": {
                            "fileName": "image.png",
                            "fileSize": "1234",
                            "sourcePath": "C:/missing/image.png",
                            "original": False,
                        },
                    }
                ],
            }
        ]
        item = list(qce_image_segments(messages, "10000001"))[0]
        self.assertEqual(item["message_id"], "7661291436232500082")
        self.assertEqual(item["resolver"], "qce")
        self.assertEqual(item["resolver_data"]["elementId"], "7661291436232500081")
        self.assertEqual(item["declared_size"], 1234)
        self.assertEqual(item["sender_uin"], "123456789")
        self.assertEqual(item["sender_member_name"], "群名片")
        self.assertEqual(item["group_name"], "绘画群")
        self.assertEqual(item["message_text"], "作品说明")
        self.assertEqual(item["is_imported"], 1)

    def test_provenance_updates_an_already_accepted_occurrence(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = connect_database(Path(directory) / "state.sqlite3")
            connection.execute(
                """INSERT INTO images (
                       group_id, message_id, image_index, status, updated_at,
                       message_seq, sent_at, file_token, declared_size
                   ) VALUES ('1', '2', 0, 'accepted', ?, '3', 4, 'f', 5)""",
                (int(time.time()),),
            )
            connection.commit()
            changed = update_item_provenance(
                connection,
                {
                    "group_id": "1",
                    "message_id": "2",
                    "image_index": 0,
                    "group_uin": "1",
                    "group_name": "群",
                    "sender_uin": "9988",
                    "sender_uid": "uid",
                    "sender_nickname": "昵称",
                    "message_text": "说明",
                },
            )
            self.assertTrue(changed)
            row = connection.execute(
                "SELECT sender_uin, sender_uid, group_name, message_text FROM images"
            ).fetchone()
            self.assertEqual(row, ("9988", "uid", "群", "说明"))
            connection.close()

    def test_asset_canonical_source_is_not_replaced_by_duplicate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "asset.png"
            path.write_bytes(b"asset")
            connection = connect_database(root / "state.sqlite3")
            digest = "a" * 64
            upsert_asset(
                connection,
                digest,
                path,
                "novelai",
                "{}",
                {
                    "group_id": "11111",
                    "sender_uin": "22222",
                    "message_id": "first",
                    "image_index": 0,
                    "sent_at": 10,
                },
                width=10,
                height=20,
            )
            upsert_asset(
                connection,
                digest,
                path,
                "novelai",
                "{}",
                {
                    "group_id": "33333",
                    "sender_uin": "44444",
                    "message_id": "duplicate",
                    "image_index": 0,
                    "sent_at": 20,
                },
            )
            row = connection.execute(
                "SELECT canonical_group_id, canonical_sender_uin, canonical_message_id FROM assets"
            ).fetchone()
            self.assertEqual(row, ("11111", "22222", "first"))
            connection.close()

    def test_qce_provenance_backfill_matches_message_without_downloading(self):
        class FakeQCE:
            def fetch_page(self, group_id, start_time, end_time, limit, page=1):
                self.called = (group_id, start_time, end_time, limit, page)
                return [
                    {
                        "msgId": "m1",
                        "msgTime": "100",
                        "peerUid": "12345",
                        "peerUin": "12345",
                        "peerName": "测试群",
                        "senderUin": "67890",
                        "senderUid": "uid-1",
                        "sendNickName": "测试发送者",
                        "elements": [
                            {"textElement": {"content": "说明"}},
                            {"picElement": {"original": True}},
                        ],
                    }
                ], False

        with tempfile.TemporaryDirectory() as directory:
            connection = connect_database(Path(directory) / "state.sqlite3")
            connection.execute(
                """INSERT INTO images (
                       group_id, message_id, image_index, status, updated_at,
                       message_seq, sent_at, file_token, declared_size, resolver,
                       sha256
                   ) VALUES ('12345', 'm1', 0, 'accepted', ?, '1', 100, 'f', 5, 'qce', ?)""",
                (int(time.time()), "a" * 64),
            )
            connection.commit()
            stats = backfill_accepted_provenance(
                FakeQCE(), connection, ["12345"], 500
            )
            self.assertEqual(stats["messages_matched"], 1)
            self.assertEqual(stats["assets_without_sender"], 0)
            row = connection.execute(
                "SELECT sender_uin, sender_uid, group_name, message_text, original_flag FROM images"
            ).fetchone()
            self.assertEqual(row, ("67890", "uid-1", "测试群", "说明", 1))
            connection.close()

    def test_qce_cursor_restarts_from_now_for_legacy_short_id(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite3"
            connection = connect_database(database)
            connection.execute(
                "INSERT INTO group_cursors VALUES (?, ?, ?, ?, ?)",
                ("10000001", "137120235", 123, 0, int(time.time())),
            )
            connection.commit()
            cursor, completed = load_qce_cursor(connection, "10000001")
            self.assertGreater(cursor, 123)
            self.assertFalse(completed)
            connection.close()

    def test_cached_full_size_original_qce_file_avoids_network(self):
        class NeverOneBot:
            def call(self, *_args, **_kwargs):
                raise AssertionError("OneBot should not be called")

        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "image.png"
            source.write_bytes(b"x" * 100)
            item = {
                "resolver": "qce",
                "declared_size": 100,
                "resolver_data": {
                    "sourcePath": str(source),
                    "declaredSize": 100,
                    "original": True,
                },
            }
            self.assertEqual(resolve_image_source(NeverOneBot(), item), source)

    def test_boundary_second_is_fetched_before_time_cursor_moves(self):
        class FakeQCE:
            def fetch_page(self, _group_id, start_time, end_time, _limit, page=1):
                if start_time == 0 and end_time == 100:
                    return [
                        {"msgId": "a", "msgTime": "100", "msgSeq": "2"},
                        {"msgId": "b", "msgTime": "99", "msgSeq": "1"},
                    ], True
                if start_time == 99 and end_time == 99 and page == 1:
                    return [
                        {"msgId": "b", "msgTime": "99", "msgSeq": "1"},
                        {"msgId": "c", "msgTime": "99", "msgSeq": "0"},
                    ], False
                return [], False

        messages, has_next = qce_page_with_boundary(FakeQCE(), "1", 0, 100, 2)
        self.assertTrue(has_next)
        self.assertEqual({message["msgId"] for message in messages}, {"a", "b", "c"})

    def test_stale_resolving_is_recovered_for_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = connect_database(Path(directory) / "state.sqlite3")
            connection.execute(
                """INSERT INTO images (
                       group_id, message_id, image_index, status, updated_at,
                       message_seq, sent_at, file_token, declared_size
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ("1", "2", 0, "resolving", int(time.time()) - 1000, "3", 4, "f", 5),
            )
            connection.commit()
            self.assertEqual(recover_stale_resolving(connection), 1)
            row = connection.execute(
                "SELECT status, next_retry_at FROM images WHERE group_id='1'"
            ).fetchone()
            self.assertEqual(row, ("failed", 0))
            connection.close()

    def test_onebot_failures_are_superseded_by_stable_qce(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = connect_database(Path(directory) / "state.sqlite3")
            connection.execute(
                """INSERT INTO images (
                       group_id, message_id, image_index, status, updated_at,
                       message_seq, sent_at, file_token, declared_size, resolver
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ("1", "ob:1:2:3", 0, "failed", int(time.time()), "2", 1, "f", 5, "onebot"),
            )
            connection.commit()
            self.assertEqual(supersede_onebot_failures(connection), 1)
            self.assertEqual(
                connection.execute("SELECT status FROM images").fetchone()[0],
                "superseded_by_qce",
            )
            connection.close()


if __name__ == "__main__":
    unittest.main()
