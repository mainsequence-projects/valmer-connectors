"""Import Valmer vectors from the configured Microsoft Graph source."""

from valmer_connectors.services.vector_update import run_vector_update


def main() -> None:
    run_vector_update(source_kind="onedrive-graph")


if __name__ == "__main__":
    main()
