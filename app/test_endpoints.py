from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from shared.models import (
    CompanyRecord,
    CompletedMarketRecord,
    CompletedMarketRecordWithHistory,
    OHLCVResponseRecord,
)
from app.models.geospatial import (
    NearbyCompaniesRequest,
    SpatialQueryResponse,
    CompanyLocation,
    RegionStatsRequest,
    RegionStatsResponse,
)


DEFAULT_BASE_URL = os.environ.get("VOILA_BASE_URL", "http://127.0.0.1:8000")


@dataclass
class TestResult:
    name: str
    passed: bool
    status_code: int
    note: str = ""


class EndpointTester:
    def __init__(
        self,
        base_url: str,
        tickers: List[str],
        lat: float,
        lng: float,
        radius_km: float,
        state: str,
        timeout: float = 15.0,
        verbose: bool = False,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.tickers = [t.strip().upper() for t in tickers if t.strip()]
        self.lat = lat
        self.lng = lng
        self.radius_km = radius_km
        self.state = state.upper()
        self.timeout = timeout
        self.verbose = verbose
        self.session = requests.Session()

    # --------------- helpers ---------------
    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _get(
        self, path: str, params: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        return self.session.get(
            self._url(path), params=params, timeout=self.timeout
        )

    def _post(self, path: str, payload: Dict[str, Any]) -> requests.Response:
        headers = {"Content-Type": "application/json"}
        return self.session.post(
            self._url(path),
            data=json.dumps(payload),
            headers=headers,
            timeout=self.timeout,
        )

    def _post_json(
        self, path: str, payload: Dict[str, Any]
    ) -> requests.Response:
        headers = {"Content-Type": "application/json"}
        return self.session.post(
            self._url(path),
            data=json.dumps(payload),
            headers=headers,
            timeout=self.timeout,
        )

    def _pass(self, name: str, status_code: int, note: str = "") -> TestResult:
        return TestResult(
            name=name, passed=True, status_code=status_code, note=note
        )

    def _fail(self, name: str, status_code: int, note: str) -> TestResult:
        return TestResult(
            name=name, passed=False, status_code=status_code, note=note
        )

    def _safe_model(self, model_cls, data: Any) -> Tuple[bool, str]:
        try:
            model_cls.model_validate(data)
            return True, ""
        except Exception as exc:  # pydantic validation error
            return False, f"schema validation failed: {exc}"

    # --------------- tests ---------------
    def test_root(self) -> TestResult:
        name = "GET /"
        try:
            r = self._get("/")
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            data = r.json()
            required_keys = {"status", "version", "endpoints"}
            missing = required_keys - set(data.keys())
            if missing:
                return self._fail(
                    name, r.status_code, f"missing keys: {missing}"
                )
            return self._pass(name, r.status_code)
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    def test_health(self) -> TestResult:
        name = "GET /health"
        try:
            r = self._get("/health")
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            data = r.json()
            if "status" not in data or "services" not in data:
                return self._fail(
                    name, r.status_code, "missing status/services"
                )
            return self._pass(name, r.status_code, note=data.get("status", ""))
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    # ----- core data -----
    def test_core_complete(self, ticker: str) -> TestResult:
        name = f"GET /api/v1/data/complete/{{ticker}} ({ticker})"
        try:
            r = self._get(f"/api/v1/data/complete/{ticker}")
            if r.status_code == 404:
                return self._pass(
                    name, r.status_code, note="not found (acceptable)"
                )
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            ok, note = self._safe_model(CompletedMarketRecord, r.json())
            return (
                self._pass(name, r.status_code)
                if ok
                else self._fail(name, r.status_code, note)
            )
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    def test_core_complete_with_history(self, ticker: str) -> TestResult:
        name = f"GET /api/v1/data/complete/{{ticker}}/with-history ({ticker})"
        try:
            r = self._get(
                f"/api/v1/data/complete/{ticker}/with-history",
                params={"days": 30},
            )
            if r.status_code == 404:
                return self._pass(
                    name, r.status_code, note="not found (acceptable)"
                )
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            ok, note = self._safe_model(
                CompletedMarketRecordWithHistory, r.json()
            )
            return (
                self._pass(name, r.status_code)
                if ok
                else self._fail(name, r.status_code, note)
            )
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    def test_core_bulk_complete(self, tickers: List[str]) -> TestResult:
        name = "GET /api/v1/data/complete/bulk"
        try:
            r = self._get(
                "/api/v1/data/complete/bulk",
                params={"tickers": ",".join(tickers), "limit": len(tickers)},
            )
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            data = r.json()
            if not isinstance(data, list):
                return self._fail(name, r.status_code, "expected list")
            # Validate first item if present
            if data:
                ok, note = self._safe_model(CompletedMarketRecord, data[0])
                if not ok:
                    return self._fail(name, r.status_code, note)
            return self._pass(name, r.status_code, note=f"items={len(data)}")
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    def test_core_companies(self) -> TestResult:
        name = "GET /api/v1/data/companies"
        try:
            r = self._get(
                "/api/v1/data/companies",
                params={"limit": 5, "offset": 0, "has_location": False},
            )
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            data = r.json()
            if not isinstance(data, list):
                return self._fail(name, r.status_code, "expected list")
            if data:
                ok, note = self._safe_model(CompanyRecord, data[0])
                if not ok:
                    return self._fail(name, r.status_code, note)
            return self._pass(name, r.status_code, note=f"items={len(data)}")
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    def test_core_search(self, query: str) -> TestResult:
        name = "GET /api/v1/data/search"
        try:
            r = self._get(
                "/api/v1/data/search", params={"q": query, "limit": 5}
            )
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            data = r.json()
            if not isinstance(data, list):
                return self._fail(name, r.status_code, "expected list")
            if not data:
                return self._fail(name, r.status_code, "empty result set")
            ok, note = self._safe_model(CompletedMarketRecord, data[0])
            return (
                self._pass(name, r.status_code, note=f"items={len(data)}")
                if ok
                else self._fail(name, r.status_code, note)
            )
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    def test_core_stats_summary(self) -> TestResult:
        name = "GET /api/v1/data/stats/summary"
        try:
            r = self._get("/api/v1/data/stats/summary")
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            data = r.json()
            if (
                not isinstance(data, dict)
                or "summary" not in data
                or "timestamp" not in data
            ):
                return self._fail(name, r.status_code, "shape mismatch")
            return self._pass(name, r.status_code)
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    # ----- market data -----
    def test_market_ohlcv(self, ticker: str) -> TestResult:
        name = f"GET /api/v1/market-data/ohlcv/{{ticker}} ({ticker})"
        try:
            r = self._get(
                f"/api/v1/market-data/ohlcv/{ticker}", params={"limit": 5}
            )
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            data = r.json()
            if not isinstance(data, list):
                return self._fail(name, r.status_code, "expected list")
            if not data:
                return self._fail(name, r.status_code, "empty result set")
            ok, note = self._safe_model(OHLCVResponseRecord, data[0])
            return (
                self._pass(name, r.status_code, note=f"items={len(data)}")
                if ok
                else self._fail(name, r.status_code, note)
            )
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    def test_market_latest(self, ticker: str) -> TestResult:
        name = f"GET /api/v1/market-data/latest/{{ticker}} ({ticker})"
        try:
            r = self._get(f"/api/v1/market-data/latest/{ticker}")
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            ok, note = self._safe_model(OHLCVResponseRecord, r.json())
            return (
                self._pass(name, r.status_code)
                if ok
                else self._fail(name, r.status_code, note)
            )
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    def test_market_bulk_with_location(self, tickers: List[str]) -> TestResult:
        name = "POST /api/v1/market-data/bulk-with-location"
        try:
            payload = {
                "tickers": tickers,
                "limit_per_ticker": 3,
                "include_location": True,
            }
            r = self._post_json(
                "/api/v1/market-data/bulk-with-location", payload
            )
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            data = r.json()
            if not isinstance(data, dict):
                return self._fail(
                    name, r.status_code, "expected dict keyed by ticker"
                )
            # Validate one ticker if present
            if data:
                any_ticker = next(iter(data))
                entries = data[any_ticker]
                if not isinstance(entries, list):
                    return self._fail(
                        name, r.status_code, "group value should be list"
                    )
                if entries:
                    sample = entries[0]
                    for key in (
                        "Date",
                        "Open",
                        "High",
                        "Low",
                        "Close",
                        "Volume",
                    ):
                        if key not in sample:
                            return self._fail(
                                name,
                                r.status_code,
                                f"missing key {key} in sample",
                            )
            return self._pass(name, r.status_code, note=f"groups={len(data)}")
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    def test_market_time_series_analysis(self, ticker: str) -> TestResult:
        name = f"GET /api/v1/market-data/time-series-analysis/{{ticker}} ({ticker})"
        try:
            r = self._get(
                f"/api/v1/market-data/time-series-analysis/{ticker}",
                params={"period_days": 30, "analysis_type": "volatility"},
            )
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            data = r.json()
            if (
                not isinstance(data, dict)
                or "data" not in data
                or not isinstance(data["data"], list)
            ):
                return self._fail(name, r.status_code, "shape mismatch")
            if len(data["data"]) == 0:
                return self._fail(name, r.status_code, "empty analysis data")
            return self._pass(
                name, r.status_code, note=f"rows={len(data['data'])}"
            )
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    def test_market_overview(self) -> TestResult:
        name = "GET /api/v1/market-data/market-overview"
        try:
            r = self._get("/api/v1/market-data/market-overview")
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            data = r.json()
            if (
                not isinstance(data, dict)
                or "market_summary" not in data
                or "geographical_distribution" not in data
            ):
                return self._fail(name, r.status_code, "shape mismatch")
            return self._pass(name, r.status_code)
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    # ----- geospatial -----
    def test_spatial_within_circle(self) -> TestResult:
        name = "POST /api/v1/spatial/within-circle"
        try:
            from app.models.validation import ValidatedCircleRequest

            payload = ValidatedCircleRequest(
                latitude=self.lat,
                longitude=self.lng,
                radius_km=self.radius_km,
                limit=10,
                include_market_data=False,
            ).model_dump(mode="json")
            r = self._post("/api/v1/spatial/within-circle", payload)
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            ok, note = self._safe_model(SpatialQueryResponse, r.json())
            return (
                self._pass(name, r.status_code)
                if ok
                else self._fail(name, r.status_code, note)
            )
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    def test_spatial_within_polygon(self) -> TestResult:
        name = "POST /api/v1/spatial/within-polygon"
        try:
            from app.models.validation import ValidatedPolygonRequest

            coords = [
                [self.lat + 0.2, self.lng - 0.2],
                [self.lat + 0.2, self.lng + 0.2],
                [self.lat - 0.2, self.lng + 0.2],
                [self.lat - 0.2, self.lng - 0.2],
            ]
            payload = ValidatedPolygonRequest(
                coordinates=coords,
                limit=10,
                include_market_data=False,
            ).model_dump(mode="json")
            r = self._post("/api/v1/spatial/within-polygon", payload)
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            ok, note = self._safe_model(SpatialQueryResponse, r.json())
            return (
                self._pass(name, r.status_code)
                if ok
                else self._fail(name, r.status_code, note)
            )
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    def test_spatial_by_state(self) -> TestResult:
        name = "GET /api/v1/spatial/by-state/{state}"
        try:
            r = self._get(
                f"/api/v1/spatial/by-state/{self.state}",
                params={"limit": 10, "include_market_data": False},
            )
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            ok, note = self._safe_model(SpatialQueryResponse, r.json())
            return (
                self._pass(name, r.status_code)
                if ok
                else self._fail(name, r.status_code, note)
            )
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    def test_spatial_nearby_ticker(self, ticker: str) -> TestResult:
        name = "POST /api/v1/spatial/nearby-ticker"
        try:
            payload = NearbyCompaniesRequest(
                ticker=ticker, radius_km=50.0, limit=5, include_market_data=True
            ).model_dump(mode="json")
            r = self._post("/api/v1/spatial/nearby-ticker", payload)
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            data = r.json()
            if not isinstance(data, list):
                return self._fail(name, r.status_code, "expected list")
            if data:
                ok, note = self._safe_model(CompanyLocation, data[0])
                if not ok:
                    return self._fail(name, r.status_code, note)
            return self._pass(name, r.status_code, note=f"items={len(data)}")
        except Exception as exc:
            return self._fail(name, 0, str(exc))

    def test_spatial_regional_stats(self) -> TestResult:
        name = "POST /api/v1/spatial/regional-stats"
        try:
            payload = RegionStatsRequest(
                region_type="circle",
                region_params={
                    "latitude": self.lat,
                    "longitude": self.lng,
                    "radius_km": self.radius_km,
                },
                start_date=None,
                end_date=None,
            ).model_dump(mode="json")
            r = self._post("/api/v1/spatial/regional-stats", payload)
            if r.status_code == 404:
                return self._pass(
                    name,
                    r.status_code,
                    note="no companies in region (acceptable)",
                )
            if r.status_code != 200:
                return self._fail(name, r.status_code, r.text)
            ok, note = self._safe_model(RegionStatsResponse, r.json())
            return (
                self._pass(name, r.status_code)
                if ok
                else self._fail(name, r.status_code, note)
            )
        except Exception as exc:
            return self._fail(name, 0, str(exc))


def read_default_tickers() -> List[str]:
    # Try to read from repo's data/tickers.txt; fallback to common tickers
    repo_root = Path(__file__).resolve().parent.parent
    tickers_file = repo_root / "data" / "tickers.txt"
    if tickers_file.exists():
        try:
            content = tickers_file.read_text().strip().splitlines()
            # Use first few tickers
            sample = [line.strip() for line in content if line.strip()][:5]
            return sample if sample else ["AAPL", "MSFT", "GOOGL"]
        except Exception:
            return ["AAPL", "MSFT", "GOOGL"]
    return ["AAPL", "MSFT", "GOOGL"]


def run_tests(args: argparse.Namespace) -> List[TestResult]:
    tester = EndpointTester(
        base_url=args.base_url,
        tickers=args.tickers,
        lat=args.lat,
        lng=args.lng,
        radius_km=args.radius_km,
        state=args.state,
        timeout=args.timeout,
        verbose=args.verbose,
    )

    results: List[TestResult] = []

    # Root and health
    results.append(tester.test_root())
    results.append(tester.test_health())

    # Core data
    for t in tester.tickers[:2]:
        results.append(tester.test_core_complete(t))
        results.append(tester.test_core_complete_with_history(t))
    bulk_pair = (
        tester.tickers[:2]
        if len(tester.tickers) >= 2
        else ([tester.tickers[0], tester.tickers[0]] if tester.tickers else [])
    )
    if bulk_pair:
        results.append(tester.test_core_bulk_complete(bulk_pair))
    results.append(tester.test_core_companies())
    # Use first ticker as search query fallback
    # Require non-empty search result to catch false-positives
    results.append(
        tester.test_core_search(tester.tickers[0] if tester.tickers else "AAPL")
    )
    results.append(tester.test_core_stats_summary())

    # Market data
    for t in (
        tester.tickers[:2] if len(tester.tickers) >= 2 else tester.tickers[:1]
    ):
        results.append(tester.test_market_ohlcv(t))
        results.append(tester.test_market_latest(t))
        results.append(tester.test_market_time_series_analysis(t))
    if bulk_pair:
        results.append(tester.test_market_bulk_with_location(bulk_pair))
    results.append(tester.test_market_overview())

    # Geospatial
    results.append(tester.test_spatial_within_circle())
    results.append(tester.test_spatial_within_polygon())
    results.append(tester.test_spatial_by_state())
    if tester.tickers:
        results.append(tester.test_spatial_nearby_ticker(tester.tickers[0]))
    results.append(tester.test_spatial_regional_stats())

    return results


def print_summary(results: List[TestResult]) -> int:
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed

    for r in results:
        status = "PASS" if r.passed else "FAIL"
        note = f" - {r.note}" if r.note else ""
        print(f"[{status}] {r.name} (status={r.status_code}){note}")

    print("\n---")
    print(f"Total: {total}  Passed: {passed}  Failed: {failed}")
    return 0 if failed == 0 else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate API endpoints and response shapes"
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="Base URL of the running API server",
    )
    parser.add_argument(
        "--tickers",
        type=lambda s: [t.strip() for t in s.split(",") if t.strip()],
        default=None,
        help="Comma-separated list of tickers to test",
    )
    parser.add_argument(
        "--state", default="NY", help="US state code to use in geospatial tests"
    )
    parser.add_argument(
        "--lat",
        type=float,
        default=40.7128,
        help="Latitude for geospatial tests (default NYC)",
    )
    parser.add_argument(
        "--lng",
        type=float,
        default=-74.0060,
        help="Longitude for geospatial tests (default NYC)",
    )
    parser.add_argument(
        "--radius-km",
        type=float,
        default=50.0,
        help="Radius in km for circle/regional tests",
    )
    parser.add_argument(
        "--timeout", type=float, default=15.0, help="HTTP timeout in seconds"
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    if args.tickers is None:
        args.tickers = read_default_tickers()
    return args


def main() -> None:
    args = parse_args()
    results = run_tests(args)
    exit_code = print_summary(results)
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
