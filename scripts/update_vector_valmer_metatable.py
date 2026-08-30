"""Import Valmer vectors from the repository-declared MetaTable source."""

from pathlib import Path

from valmer_connectors.services.vector_update import run_vector_update

SOURCE_CONFIG_PATH = (
    Path(__file__).resolve().parents[1] / "configs" / "valmer-metatable-sources.json"
)


def main() -> None:
    run_vector_update(
        source_kind="metatable",
        source_metatables_config_path=str(SOURCE_CONFIG_PATH),
    )


if __name__ == "__main__":
    main()
