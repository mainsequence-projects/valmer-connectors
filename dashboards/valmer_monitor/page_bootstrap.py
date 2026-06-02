from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import streamlit as st


@dataclass(frozen=True)
class PageConfig:
    title: str
    render_header: Callable[[Any], None] | None = None
    use_wide_layout: bool = True
    inject_theme_css: bool = True


def run_page(config: PageConfig) -> None:
    st.set_page_config(
        page_title=config.title,
        layout="wide" if config.use_wide_layout else "centered",
    )
    if config.render_header is not None:
        config.render_header(config)
