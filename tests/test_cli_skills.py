from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from valmer_connectors.cli.main import (
    bundled_valmer_skills_root,
    main,
    package_tree_valmer_skills_root,
    source_tree_valmer_skills_root,
)


def _bundled_skill_folders() -> list[str]:
    return sorted(
        item.name
        for item in bundled_valmer_skills_root().iterdir()
        if item.is_dir() and not item.name.startswith(".") and not item.name.startswith("__")
    )


def _bundled_skill_paths() -> list[str]:
    paths: list[str] = []

    def walk(prefix: tuple[str, ...], root) -> None:
        for item in root.iterdir():
            if item.name.startswith(".") or item.name.startswith("__"):
                continue
            if not item.is_dir():
                continue

            path = (*prefix, item.name)
            if item.joinpath("SKILL.md").is_file():
                paths.append("/".join(path))
            walk(path, item)

    walk((), bundled_valmer_skills_root())
    return sorted(paths)


class ValmerCliSkillsTests(unittest.TestCase):
    def test_packaged_skill_copy_matches_repo_skill(self):
        repo_skill = source_tree_valmer_skills_root().joinpath("registering_assets", "SKILL.md")
        packaged_skill = package_tree_valmer_skills_root().joinpath(
            "registering_assets",
            "SKILL.md",
        )

        self.assertTrue(repo_skill.is_file())
        self.assertTrue(packaged_skill.is_file())
        self.assertEqual(
            repo_skill.read_text(encoding="utf-8"),
            packaged_skill.read_text(encoding="utf-8"),
        )

    def test_copy_valmer_skills_dry_run_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    ["copy-valmer-skills", "--path", str(tmp_path), "--dry-run", "--json"]
                )

            self.assertEqual(exit_code, 0)
            self.assertFalse((tmp_path / ".agents").exists())
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["dry_run"])
            self.assertEqual(
                payload["destination_root"],
                str(tmp_path.resolve() / ".agents" / "skills" / "valmer-connectors"),
            )
            self.assertEqual(
                sorted(item["name"] for item in payload["updated"]),
                _bundled_skill_folders(),
            )

    def test_copy_valmer_skills_blocks_source_checkout(self):
        repo_root = Path(__file__).resolve().parents[1]
        before = _bundled_skill_paths()
        stderr = io.StringIO()

        with contextlib.redirect_stderr(stderr):
            exit_code = main(["copy-valmer-skills", "--path", str(repo_root)])

        self.assertEqual(exit_code, 2)
        self.assertIn("cannot run inside the valmer-connectors source checkout", stderr.getvalue())
        self.assertEqual(_bundled_skill_paths(), before)

    def test_copy_valmer_skills_blocks_source_checkout_json(self):
        repo_root = Path(__file__).resolve().parents[1]
        stdout = io.StringIO()

        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "copy-valmer-skills",
                    "--path",
                    str(repo_root),
                    "--dry-run",
                    "--json",
                ]
            )

        self.assertEqual(exit_code, 2)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["blocked"])
        self.assertTrue(payload["dry_run"])
        self.assertEqual(payload["project"], str(repo_root))
        self.assertEqual(payload["updated_count"], 0)
        self.assertEqual(payload["updated"], [])
        self.assertIn("cannot run inside the valmer-connectors source checkout", payload["reason"])

    def test_copy_valmer_skills_copies_only_valmer_namespace(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            mainsequence_skill = tmp_path / ".agents" / "skills" / "mainsequence" / "project_builder"
            mainsequence_skill.mkdir(parents=True)
            sentinel = mainsequence_skill / "SKILL.md"
            sentinel.write_text("keep me", encoding="utf-8")

            stale_skill = (
                tmp_path
                / ".agents"
                / "skills"
                / "valmer-connectors"
                / _bundled_skill_folders()[0]
            )
            stale_skill.mkdir(parents=True)
            stale_file = stale_skill / "stale.txt"
            stale_file.write_text("remove me", encoding="utf-8")

            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(["copy-valmer-skills", "--path", str(tmp_path)])

            self.assertEqual(exit_code, 0)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep me")
            self.assertFalse(stale_file.exists())
            for skill_path in _bundled_skill_paths():
                skill_file = (
                    tmp_path
                    / ".agents"
                    / "skills"
                    / "valmer-connectors"
                    / Path(*skill_path.split("/"))
                    / "SKILL.md"
                )
                self.assertTrue(skill_file.exists())


if __name__ == "__main__":
    unittest.main()
