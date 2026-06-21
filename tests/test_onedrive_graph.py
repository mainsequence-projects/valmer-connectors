import datetime as dt
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from pydantic import SecretStr

from valmer_connectors.services.onedrive_graph import (
    OneDriveGraphFile,
    _cache_file_is_current,
    _secret_value,
    select_onedrive_vector_files_for_update,
)


class OneDriveGraphServiceTests(unittest.TestCase):
    def test_select_onedrive_vector_files_for_update_filters_by_filename_date(self):
        files = [
            OneDriveGraphFile("1", "VectorAnalitico24h_2024-12-02.xls", 10, None),
            OneDriveGraphFile("2", "VectorAnalitico24h_2024-12-03.xls", 10, None),
            OneDriveGraphFile("3", "manual-vector.xls", 10, None),
        ]

        selected = select_onedrive_vector_files_for_update(
            files,
            latest_time_index=dt.datetime(2024, 12, 2, 23, 59, 59, tzinfo=dt.UTC),
        )

        self.assertEqual([item.item_id for item in selected], ["2", "3"])

    def test_secret_value_reads_main_sequence_secret_value(self):
        secret = Mock()
        secret.value = SecretStr("secret-value")

        with patch(
            "mainsequence.client.models_foundry.Secret.get",
            return_value=secret,
        ) as get_secret:
            value = _secret_value("VALMER_ONEDRIVE_CLIENT_SECRET")

        self.assertEqual(value, "secret-value")
        get_secret.assert_called_once_with(name="VALMER_ONEDRIVE_CLIENT_SECRET")

    def test_secret_value_prefers_environment_value(self):
        with (
            patch.dict(
                "os.environ",
                {"VALMER_ONEDRIVE_CLIENT_SECRET": "env-secret-value"},
            ),
            patch("mainsequence.client.models_foundry.Secret.get") as get_secret,
        ):
            value = _secret_value("VALMER_ONEDRIVE_CLIENT_SECRET")

        self.assertEqual(value, "env-secret-value")
        get_secret.assert_not_called()

    def test_secret_value_raises_when_env_and_secret_are_missing(self):
        with (
            patch.dict("os.environ", {}, clear=True),
            patch(
                "mainsequence.client.models_foundry.Secret.get",
                side_effect=RuntimeError("not found"),
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "Set environment variable VALMER_ONEDRIVE_CLIENT_SECRET",
            ):
                _secret_value("VALMER_ONEDRIVE_CLIENT_SECRET")

    def test_cache_file_is_current_uses_graph_size(self):
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "VectorAnalitico24h_2024-12-03.xls"
            path.write_bytes(b"12345")

            self.assertTrue(
                _cache_file_is_current(
                    path,
                    OneDriveGraphFile("1", path.name, 5, None),
                )
            )
            self.assertFalse(
                _cache_file_is_current(
                    path,
                    OneDriveGraphFile("1", path.name, 6, None),
                )
            )


if __name__ == "__main__":
    unittest.main()
