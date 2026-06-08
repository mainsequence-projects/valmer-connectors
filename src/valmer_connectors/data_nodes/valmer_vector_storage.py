from __future__ import annotations

import datetime as dt
from typing import Any, ClassVar

from msm.base import MarketsBase
from msm.models import AssetTable
from sqlalchemy import BigInteger, DateTime, Float, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column
from valmer_connectors.markets import ValmerMarketsTimeIndexMetaTableMixin


class ValmerVectorPricesStorage(ValmerMarketsTimeIndexMetaTableMixin, MarketsBase):
    """Valmer vector prices keyed by valuation timestamp and Asset unique identifier."""

    __metatable_identifier__: ClassVar[str] = "vector_de_precios_valmer"
    __metatable_description__ = (
        "Daily Valmer vector price storage keyed by (time_index, unique_identifier). "
        "Each row is one Valmer instrument observation with time-varying price, yield, "
        "spread, rating, liquidity, and risk fields plus derived OHLC columns. Static "
        "Valmer asset descriptors live in ValmerAssetDetailsTable."
    )
    __metatable_extra_hash_components__: ClassVar[dict[str, Any]] = {
        "storage_name": "vector_de_precios_valmer",
    }
    __time_index_name__: ClassVar[str] = "time_index"
    __index_names__: ClassVar[list[str]] = ["time_index", "unique_identifier"]

    time_index: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        info={
            "label": "Time Index",
            "description": "UTC end-of-day valuation timestamp.",
        },
    )
    unique_identifier: Mapped[str] = mapped_column(
        String(255),
        ForeignKey(
            f"{AssetTable.__table__.fullname}.unique_identifier",
            ondelete="RESTRICT",
        ),
        nullable=False,
        info={
            "label": "Unique Identifier",
            "description": "Valmer asset unique identifier from AssetTable.",
        },
    )
    open: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={
            "label": "Open",
            "description": "Synthetic OHLC open copied from dirty price.",
        },
    )
    high: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={
            "label": "High",
            "description": "Synthetic OHLC high copied from dirty price.",
        },
    )
    low: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={
            "label": "Low",
            "description": "Synthetic OHLC low copied from dirty price.",
        },
    )
    close: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={
            "label": "Close",
            "description": "Synthetic OHLC close copied from dirty price.",
        },
    )
    volume: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        info={
            "label": "Volume",
            "description": "Synthetic volume placeholder published as 0.",
        },
    )
    open_time: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        info={
            "label": "Open Time",
            "description": "Unix timestamp in seconds for the bar.",
        },
    )
    valuation_date: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        info={
            "label": "Valuation Date",
            "description": "Source FECHA normalized to UTC.",
        },
    )
    clean_price: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Clean Price", "description": "Clean price from PRECIO LIMPIO."},
    )
    dirty_price: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Dirty Price", "description": "Dirty price from PRECIO SUCIO."},
    )
    accrued_interest: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Accrued Interest", "description": "Accrued interest."},
    )
    current_coupon: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Current Coupon", "description": "Current coupon number."},
    )
    spread: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Spread", "description": "Valmer SOBRETASA spread."},
    )
    amount_outstanding: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Amount Outstanding", "description": "Outstanding amount."},
    )
    days_since_coupon: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        info={
            "label": "Days Since Coupon",
            "description": "Days since previous coupon.",
        },
    )
    coupons_remaining: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        info={"label": "Coupons Remaining", "description": "Remaining coupon count."},
    )
    market_event: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        info={"label": "Market Event", "description": "Valmer market event marker."},
    )
    uh_date: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        info={"label": "UH Date", "description": "Vendor FECHA U.H. value."},
    )
    theoretical_price: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Theoretical Price", "description": "Valmer theoretical price."},
    )
    posted_bid: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Posted Bid", "description": "Posted bid from POST COMPRA."},
    )
    posted_ask: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Posted Ask", "description": "Posted ask from POST VENTA."},
    )
    bid_yield: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Bid Yield", "description": "Bid yield from YIELD COMPRA."},
    )
    ask_yield: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Ask Yield", "description": "Ask yield from YIELD VENTA."},
    )
    bid_spread: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Bid Spread", "description": "Bid spread from SPREAD COMPRA."},
    )
    ask_spread: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Ask Spread", "description": "Ask spread from SPREAD VENTA."},
    )
    moodys_rating: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        info={"label": "Moody's Rating", "description": "Moody's rating from MDYS."},
    )
    sp_rating: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        info={"label": "S&P Rating", "description": "S&P rating."},
    )
    marketability: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        info={"label": "Marketability", "description": "Marketability label."},
    )
    liquidity: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Liquidity", "description": "Valmer liquidity value."},
    )
    daily_change_pct: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={
            "label": "Daily Change Pct",
            "description": "Daily change percentage value.",
        },
    )
    weekly_change_pct: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={
            "label": "Weekly Change Pct",
            "description": "Weekly change percentage value.",
        },
    )
    max_price_12m: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Max Price 12M", "description": "Twelve-month high price."},
    )
    min_price_12m: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Min Price 12M", "description": "Twelve-month low price."},
    )
    suspension_status: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        info={"label": "Suspension Status", "description": "Valmer suspension status."},
    )
    volatility: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Volatility", "description": "Valmer volatility."},
    )
    volatility_secondary: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Secondary Volatility", "description": "Secondary volatility."},
    )
    duration: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Duration", "description": "Valmer duration."},
    )
    monetary_duration: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Monetary Duration", "description": "Valmer monetary duration."},
    )
    convexity: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Convexity", "description": "Valmer convexity."},
    )
    value_at_risk: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Value At Risk", "description": "Valmer value at risk."},
    )
    standard_deviation: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={
            "label": "Standard Deviation",
            "description": "Valmer standard deviation.",
        },
    )
    adjusted_face_value: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Adjusted Face Value", "description": "Adjusted face value."},
    )
    fitch_rating: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        info={"label": "Fitch Rating", "description": "Fitch rating."},
    )
    max_price_date: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        info={
            "label": "Max Price Date",
            "description": "Date of twelve-month high price.",
        },
    )
    min_price_date: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        info={
            "label": "Min Price Date",
            "description": "Date of twelve-month low price.",
        },
    )
    sensitivity: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Sensitivity", "description": "Valmer sensitivity."},
    )
    macaulay_duration: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Macaulay Duration", "description": "Valmer Macaulay duration."},
    )
    yield_rate: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        info={"label": "Yield Rate", "description": "Valmer yield rate."},
    )
    hr_rating: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        info={"label": "HR Rating", "description": "HR rating."},
    )


__all__ = ["ValmerVectorPricesStorage"]
