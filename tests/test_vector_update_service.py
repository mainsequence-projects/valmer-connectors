import unittest
from unittest.mock import Mock, patch

from valmer_connectors.services import vector_update


class ValmerVectorUpdateServiceTests(unittest.TestCase):
    def test_vector_update_forces_current_pricing_hydration(self):
        stats_updater = Mock()
        stats_updater.get_update_statistics.return_value = object()
        updater = Mock()

        with (
            patch("valmer_connectors.services.vector_update.bootstrap_runtime") as bootstrap,
            patch("valmer_connectors.services.vector_update._debug_artifact_path") as debug_path,
            patch(
                "valmer_connectors.services.vector_update.build_import_valmer",
                side_effect=[stats_updater, updater],
            ) as build_import,
        ):
            debug_path.return_value.__enter__.return_value = None
            debug_path.return_value.__exit__.return_value = False

            vector_update.run_vector_update(bucket_name="Vector Bucket")

        bootstrap.assert_called_once_with()
        self.assertEqual(build_import.call_count, 2)
        updater.prepare_for_update.assert_called_once_with(
            force_pricing_update=True,
        )
        updater.run.assert_called_once_with(force_update=True)


if __name__ == "__main__":
    unittest.main()
