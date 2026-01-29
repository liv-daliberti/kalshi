import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from _test_utils import add_src_to_path, ensure_psycopg_stub

ensure_psycopg_stub()
add_src_to_path()

from src.jobs import archive_closed  # noqa: E402


class RecordingCursor:
    def __init__(
        self,
        *,
        fetchall_results=None,
        fetchone_results=None,
        fetchmany_results=None,
        rowcount_values=None,
    ):
        self.execute_calls = []
        self.executemany_calls = []
        self._fetchall_results = list(fetchall_results or [])
        self._fetchone_results = list(fetchone_results or [])
        self._fetchmany_results = list(fetchmany_results or [])
        self._rowcount_values = list(rowcount_values or [])
        self.rowcount = 0

    def execute(self, sql, params=None) -> None:
        self.execute_calls.append((sql, params))
        if self._rowcount_values:
            self.rowcount = self._rowcount_values.pop(0)

    def executemany(self, sql, params_seq) -> None:
        self.executemany_calls.append((sql, list(params_seq)))
        if self._rowcount_values:
            self.rowcount = self._rowcount_values.pop(0)

    def fetchall(self):
        if self._fetchall_results:
            return self._fetchall_results.pop(0)
        return []

    def fetchone(self):
        if self._fetchone_results:
            return self._fetchone_results.pop(0)
        return None

    def fetchmany(self, _size):
        if self._fetchmany_results:
            return self._fetchmany_results.pop(0)
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class BatchCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.execute_calls = 0
        self.executemany_calls = []

    def execute(self, *_args, **_kwargs) -> None:
        self.execute_calls += 1

    def executemany(self, _sql, params_seq) -> None:
        self.executemany_calls.append(list(params_seq))

    def fetchmany(self, size):
        if not self.rows:
            return []
        batch = self.rows[:size]
        self.rows = self.rows[size:]
        return batch

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self, cursor_obj):
        self.cursor_obj = cursor_obj
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, *args, **kwargs):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class TestArchiveClosedConfig(unittest.TestCase):
    def test_build_archive_closed_config(self) -> None:
        settings = SimpleNamespace(
            archive_closed_hours=24,
            archive_require_terminal_lifecycle=True,
            archive_batch_size=500,
        )
        cfg = archive_closed.build_archive_closed_config(settings)
        self.assertEqual(cfg.cutoff_hours, 24)
        self.assertTrue(cfg.require_terminal_lifecycle)
        self.assertEqual(cfg.batch_size, 500)


class TestArchiveClosedHelpers(unittest.TestCase):
    def test_load_missing_tickers(self) -> None:
        settings = SimpleNamespace()
        with patch.object(archive_closed, "build_closed_cleanup_config") as build_cfg:
            with patch.object(
                archive_closed, "_load_missing_closed_markets", return_value=(["A"], ["B"])
            ) as load_missing:
                result = archive_closed._load_missing_tickers(MagicMock(), settings)
        build_cfg.assert_called_once_with(settings)
        load_missing.assert_called_once()
        self.assertEqual(result, {"A", "B"})

    def test_copy_rows_batches(self) -> None:
        src_cursor = BatchCursor([(1,), (2,), (3,), (4,), (5,)])
        dst_cursor = RecordingCursor()
        primary = FakeConn(src_cursor)
        backup = FakeConn(dst_cursor)
        copied = archive_closed._copy_rows(
            primary,
            backup,
            select_sql="SELECT 1",
            insert_sql="INSERT",
            batch_size=2,
            label="events",
        )
        self.assertEqual(copied, 5)
        self.assertEqual(backup.commits, 1)
        self.assertEqual(len(dst_cursor.executemany_calls), 3)

    def test_prepare_archive_targets_with_missing_and_terminal(self) -> None:
        cursor = RecordingCursor(
            fetchall_results=[
                [("EV1",), ("EV2",)],
                [("M1", "EV1"), ("M2", "EV2")],
            ],
        )
        conn = FakeConn(cursor)
        events, markets = archive_closed._prepare_archive_targets(
            conn,
            cutoff_hours=12,
            missing_tickers={"M2", "M1"},
            require_terminal_lifecycle=True,
        )
        sql_blob = " ".join(sql for sql, _ in cursor.execute_calls)
        self.assertIn("missing_markets", sql_blob)
        self.assertIn("missing_ws_markets", sql_blob)
        self.assertEqual(events, ["EV1", "EV2"])
        self.assertEqual(markets, [("M1", "EV1"), ("M2", "EV2")])
        self.assertTrue(cursor.executemany_calls)
        inserted = cursor.executemany_calls[0][1]
        self.assertEqual(inserted, [("M1",), ("M2",)])

    def test_create_backup_targets(self) -> None:
        cursor = RecordingCursor()
        conn = FakeConn(cursor)
        archive_closed._create_backup_targets(
            conn,
            ["EV1", "EV2"],
            [("M1", "EV1"), ("M2", "EV2")],
        )
        self.assertEqual(len(cursor.executemany_calls), 2)
        self.assertEqual(len(cursor.executemany_calls[0][1]), 2)
        self.assertEqual(len(cursor.executemany_calls[1][1]), 2)

    def test_count_tables(self) -> None:
        cursor = RecordingCursor(
            fetchone_results=[
                (1,),
                (2,),
                (3,),
                (4,),
                (5,),
                (6,),
                (7,),
                (8,),
                (9,),
                (10,),
                (11,),
            ]
        )
        conn = FakeConn(cursor)
        counts = archive_closed._count_tables(
            conn,
            event_table="events",
            market_table="markets",
        )
        self.assertEqual(counts["events"], 1)
        self.assertEqual(counts["markets"], 2)
        self.assertEqual(counts["active_markets"], 3)
        self.assertEqual(counts["market_ticks"], 4)
        self.assertEqual(counts["market_candles"], 5)
        self.assertEqual(counts["lifecycle_events"], 6)
        self.assertEqual(counts["rag_documents"], 7)
        self.assertEqual(counts["prediction_runs"], 8)
        self.assertEqual(counts["market_predictions"], 9)
        self.assertEqual(counts["work_queue"], 10)
        self.assertEqual(counts["ingest_state"], 11)

    def test_lock_targets(self) -> None:
        cursor = RecordingCursor(
            fetchmany_results=[
                [("EV1",)],
                [],
                [("M1",)],
                [],
            ]
        )
        conn = FakeConn(cursor)
        archive_closed._lock_targets(conn)
        self.assertGreaterEqual(len(cursor.execute_calls), 2)

    def test_delete_archived_rows(self) -> None:
        cursor = RecordingCursor(
            rowcount_values=list(range(1, 13)),
        )
        conn = FakeConn(cursor)
        counts = archive_closed._delete_archived_rows(conn)
        self.assertEqual(counts["work_queue"], 1)
        self.assertEqual(counts["events"], 12)


class TestArchiveClosedFlow(unittest.TestCase):
    def test_archive_selected_targets_mismatch(self) -> None:
        primary = FakeConn(RecordingCursor())
        backup = FakeConn(RecordingCursor())
        cfg = archive_closed.ArchiveClosedConfig(1, False, 10)
        with patch.object(archive_closed, "_create_backup_targets") as create_targets, \
             patch.object(archive_closed, "_lock_targets") as lock_targets, \
             patch.object(
                 archive_closed,
                 "_count_tables",
                 side_effect=[
                     {"events": 1, "markets": 1},
                     {"events": 0, "markets": 1},
                 ],
             ), \
             patch.object(archive_closed, "_copy_rows", return_value=0), \
             patch.object(archive_closed, "_delete_archived_rows", return_value={}):
            with self.assertRaises(RuntimeError):
                archive_closed._archive_selected_targets(
                    primary,
                    backup,
                    cfg,
                    ["EV1"],
                    [("M1", "EV1")],
                )
        create_targets.assert_called_once()
        lock_targets.assert_called_once()

    def test_archive_selected_targets_success(self) -> None:
        primary = FakeConn(RecordingCursor())
        backup = FakeConn(RecordingCursor())
        cfg = archive_closed.ArchiveClosedConfig(1, False, 10)
        expected_counts = {"events": 1, "markets": 1}
        with patch.object(archive_closed, "_create_backup_targets"), \
             patch.object(archive_closed, "_lock_targets"), \
             patch.object(
                 archive_closed,
                 "_count_tables",
                 side_effect=[expected_counts, expected_counts],
             ), \
             patch.object(archive_closed, "_copy_rows", return_value=1), \
             patch.object(archive_closed, "_delete_archived_rows", return_value={"events": 1}):
            archived, deleted = archive_closed._archive_selected_targets(
                primary,
                backup,
                cfg,
                ["EV1"],
                [("M1", "EV1")],
            )
        self.assertTrue(archived)
        self.assertEqual(deleted["events"], 1)
        self.assertEqual(primary.commits, 1)

    def test_archive_closed_events_no_events(self) -> None:
        primary = FakeConn(RecordingCursor())
        backup = FakeConn(RecordingCursor())
        cfg = archive_closed.ArchiveClosedConfig(1, False, 10)
        settings = SimpleNamespace()
        with patch.object(archive_closed, "ensure_schema_compatible") as ensure_schema, \
             patch.object(archive_closed, "_load_missing_tickers", return_value=set()), \
             patch.object(archive_closed, "_prepare_archive_targets", return_value=([], [])):
            result = archive_closed.archive_closed_events(
                primary,
                backup,
                settings,
                cfg,
            )
        ensure_schema.assert_called_once_with(backup)
        self.assertIsNone(result)
        self.assertEqual(primary.rollbacks, 1)

    def test_archive_closed_events_success(self) -> None:
        primary = FakeConn(RecordingCursor())
        backup = FakeConn(RecordingCursor())
        cfg = archive_closed.ArchiveClosedConfig(1, False, 10)
        settings = SimpleNamespace()
        archived = {"events": 1}
        deleted = {"events": 1}
        with patch.object(archive_closed, "ensure_schema_compatible"), \
             patch.object(archive_closed, "_load_missing_tickers", return_value=set()), \
             patch.object(archive_closed, "_prepare_archive_targets", return_value=(["EV1"], [("M1", "EV1")])), \
             patch.object(archive_closed, "_archive_selected_targets", return_value=(archived, deleted)):
            result = archive_closed.archive_closed_events(
                primary,
                backup,
                settings,
                cfg,
            )
        self.assertIsNotNone(result)
        self.assertEqual(result.events, 1)
        self.assertEqual(result.archived_rows, archived)
        self.assertEqual(result.deleted_rows, deleted)

    def test_archive_closed_events_exception_rolls_back(self) -> None:
        primary = FakeConn(RecordingCursor())
        backup = FakeConn(RecordingCursor())
        cfg = archive_closed.ArchiveClosedConfig(1, False, 10)
        settings = SimpleNamespace()
        with patch.object(archive_closed, "ensure_schema_compatible"), \
             patch.object(archive_closed, "_load_missing_tickers", return_value=set()), \
             patch.object(archive_closed, "_prepare_archive_targets", return_value=(["EV1"], [("M1", "EV1")])), \
             patch.object(archive_closed, "_archive_selected_targets", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                archive_closed.archive_closed_events(
                    primary,
                    backup,
                    settings,
                    cfg,
                )
        self.assertEqual(primary.rollbacks, 1)
        self.assertEqual(backup.rollbacks, 1)
