import unittest
from unittest import mock

import aws_background  # noqa: F401  (import before pipeline modules; see other tests)
from pipeline import summary_incremental as si


class _RecordingCursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append(" ".join(str(sql).split()))


class _Conn:
    def commit(self):
        pass


class HourWiseSalesStageTests(unittest.TestCase):
    def _run(self):
        cursor = _RecordingCursor()
        with mock.patch.object(si, "merge_staging_to_main_by_date") as merge:
            si.update_hour_wise_sales_incremental(cursor, _Conn(), "Brand", {"2026-10-22"})
        return cursor.statements, merge

    def test_stage_table_is_recreated_from_current_layout_before_insert(self):
        statements, _ = self._run()

        drop = statements.index("DROP TABLE IF EXISTS hour_wise_sales_stage")
        create = statements.index("CREATE TABLE hour_wise_sales_stage LIKE hour_wise_sales")
        insert = next(i for i, s in enumerate(statements) if s.startswith("INSERT INTO hour_wise_sales_stage"))
        self.assertLess(drop, create)
        self.assertLess(create, insert)

    def test_stale_persistent_stage_table_pattern_is_gone(self):
        statements, _ = self._run()

        self.assertFalse(any("CREATE TABLE IF NOT EXISTS hour_wise_sales_stage" in s for s in statements))
        self.assertFalse(any(s.startswith("TRUNCATE TABLE hour_wise_sales_stage") for s in statements))

    def test_still_merges_stage_into_main(self):
        _, merge = self._run()
        merge.assert_called_once()
        self.assertEqual(merge.call_args.args[2:5], ("hour_wise_sales", "hour_wise_sales_stage", "Brand"))


if __name__ == "__main__":
    unittest.main()
