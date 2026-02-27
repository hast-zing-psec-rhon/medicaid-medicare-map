from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class StateSummary(BaseModel):
    state_code: str
    fiscal_year: int
    medicaid_total: float
    medicare_total: float
    federal_medicaid_total: float
    state_medicaid_total: float
    total_revenue: float
    public_total: float
    public_dependency: float
    facility_count: int
    chain_count: int


class FacilityFinancial(BaseModel):
    facility_id: str
    facility_name: str
    state_code: str
    city: str
    ownership_group: str
    ownership_type: str
    facility_type: str
    chain_name: str
    chain_confidence: str
    fiscal_year: int
    total_revenue: float
    medicare_revenue: float
    medicaid_revenue: float
    federal_medicaid_revenue: float
    state_medicaid_revenue: float
    other_revenue: float
    medicare_dependency: float
    medicaid_dependency: float
    public_dependency: float
    medicare_method: str


class ChainSummary(BaseModel):
    chain_name: str
    fiscal_year: int
    medicaid_total: float
    medicare_total: float
    federal_medicaid_total: float
    state_medicaid_total: float
    total_revenue: float
    public_total: float
    public_dependency: float
    facility_count: int
    state_count: int


class ScenarioRequest(BaseModel):
    fiscal_year: int
    medicare_cut_pct: float = Field(default=0, ge=0, le=100)
    federal_medicaid_cut_pct: float = Field(default=0, ge=0, le=100)
    state_medicaid_cut_pct: float = Field(default=0, ge=0, le=100)
    state_code: Optional[str] = None
    chain_name: Optional[str] = None


class ScenarioResult(BaseModel):
    fiscal_year: int
    scope_state_code: Optional[str]
    scope_chain_name: Optional[str]
    baseline_total_revenue: float
    shocked_total_revenue: float
    revenue_at_risk_abs: float
    revenue_at_risk_pct: float
    top_impacted_facilities: list[dict]


MetricType = Literal["medicaid", "medicare", "public_total", "public_dependency"]
OwnershipFilter = Literal["all", "for_profit", "not_for_profit", "government", "unknown"]
SortField = Literal[
    "total_revenue",
    "medicare_revenue",
    "medicaid_revenue",
    "public_dependency",
    "medicare_dependency",
    "medicaid_dependency",
    "facility_name",
]
