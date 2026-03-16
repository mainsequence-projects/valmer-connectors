from src.data_nodes.nodes import ImportValmer
from src.instruments.bootstrap import register_all
from src.settings import BUCKET_NAME_HISTORICAL_VECTORS


def main() -> None:
    register_all()
    first_time_update_loop = False
    ts_all_files = ImportValmer(
        bucket_name=BUCKET_NAME_HISTORICAL_VECTORS,
    )
    try:
        ts_all_files.get_update_statistics()
    except AttributeError:
        first_time_update_loop = True

    if first_time_update_loop:
        for _ in range(360 // 5):
            ts_all_files = ImportValmer(
                bucket_name=BUCKET_NAME_HISTORICAL_VECTORS,
            )
            ts_all_files.run(force_update=True)
        return

    ts_all_files = ImportValmer(
        bucket_name=BUCKET_NAME_HISTORICAL_VECTORS,
    )
    ts_all_files.run(force_update=True)


if __name__ == "__main__":
    main()
