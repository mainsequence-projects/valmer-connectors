from valmer_connectors.data_nodes.nodes import ImportValmer, ImportValmerConfig
from valmer_connectors.instruments.bootstrap import bootstrap_runtime
from valmer_connectors.settings import BUCKET_NAME_HISTORICAL_VECTORS


def _build_import_valmer() -> ImportValmer:
    return ImportValmer(
        config=ImportValmerConfig(bucket_name=BUCKET_NAME_HISTORICAL_VECTORS),
    )


def main() -> None:
    bootstrap_runtime()
    first_time_update_loop = False
    ts_all_files = _build_import_valmer()
    try:
        ts_all_files.get_update_statistics()
    except AttributeError:
        first_time_update_loop = True

    if first_time_update_loop:
        for _ in range(360 // 5):
            ts_all_files = _build_import_valmer()
            ts_all_files.run(force_update=True)
        return

    ts_all_files = _build_import_valmer()
    ts_all_files.run(force_update=True)


if __name__ == "__main__":
    main()
