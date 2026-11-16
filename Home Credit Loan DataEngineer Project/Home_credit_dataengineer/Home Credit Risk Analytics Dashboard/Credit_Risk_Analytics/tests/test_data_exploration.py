import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "notebooks"
    / "01_data_exploration.py"
)
SPEC = importlib.util.spec_from_file_location("data_exploration", SCRIPT_PATH)
data_exploration = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(data_exploration)


class ProfileCsvTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.data_dir = Path(self.temp_dir.name)

    def profile(self, path, chunk_size, max_rows=None):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        return data_exploration.profile_csv(
            path, connection, chunk_size, max_rows
        )

    def test_counts_missing_values_and_duplicate_keys_across_chunks(self):
        path = self.data_dir / "application_train.csv"
        path.write_text(
            "SK_ID_CURR,VALUE\n1,a\n2,\n1,c\n",
            encoding="utf-8",
        )

        profile = self.profile(path, chunk_size=2)

        self.assertEqual(profile["rows_scanned"], 3)
        self.assertEqual(profile["missing_values"]["VALUE"], 1)
        self.assertEqual(profile["duplicate_candidate_key_rows"], 1)
        self.assertTrue(profile["scan_complete"])

    def test_row_limit_marks_partial_scan_only_when_limit_is_reached(self):
        path = self.data_dir / "application_train.csv"
        path.write_text(
            "SK_ID_CURR,VALUE\n1,a\n2,b\n3,c\n",
            encoding="utf-8",
        )

        partial = self.profile(path, chunk_size=2, max_rows=2)
        complete = self.profile(path, chunk_size=2, max_rows=10)

        self.assertEqual(partial["rows_scanned"], 2)
        self.assertFalse(partial["scan_complete"])
        self.assertEqual(complete["rows_scanned"], 3)
        self.assertTrue(complete["scan_complete"])

    def test_description_file_uses_legacy_encoding(self):
        path = self.data_dir / "HomeCredit_columns_description.csv"
        path.write_bytes("Table,Description\nsource,café\n".encode("cp1252"))

        profile = self.profile(path, chunk_size=2)

        self.assertEqual(profile["encoding"], "cp1252")
        self.assertEqual(profile["rows_scanned"], 1)


if __name__ == "__main__":
    unittest.main()
