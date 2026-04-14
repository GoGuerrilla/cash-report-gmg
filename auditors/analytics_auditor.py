"""
Google Analytics Data API v4 — Website Traffic Auditor
Pulls live metrics from a GA4 property and scores them for the C section
of the C.A.S.H. report.

Metrics fetched
---------------
  - Monthly website visitors (activeUsers, last 30 days)
  - Top traffic sources (sessionDefaultChannelGrouping)
  - Bounce rate (bounceRate)
  - Average session duration (averageSessionDuration)
  - Top landing pages (landingPage, sessions)
  - Traffic trend: last 30 days vs prior 30 days (% change in sessions)

Auth requirements
-----------------
  GA Data API v4 requires a Google Service Account, NOT a plain API key.
  Standard API keys (AIza...) work for public Google APIs (Maps, YouTube,
  PageSpeed) but are REJECTED by the Analytics Data API because GA data
  is private/account-gated.

  Setup:
    1. Google Cloud Console → IAM → Service Accounts → Create
    2. Enable "Google Analytics Data API" for the project
    3. In GA4: Admin → Property Access Management → add the service account
       email with "Viewer" role
    4. Download the service account JSON key file
    5. Set GOOGLE_SERVICE_ACCOUNT_JSON_PATH in .env to the full file path
    6. Set GOOGLE_ANALYTICS_PROPERTY_ID in .env to the numeric property ID
       (found in GA4 → Admin → Property Settings → Property ID)

  If either credential is missing or the API call fails the auditor returns
  score=50 (neutral) and a note explaining what is needed.

Scoring rubric
--------------
  Base = 50 (neutral when no data)
  Monthly visitors  < 500         : -10
  Monthly visitors  500–2 000     :  ±0
  Monthly visitors  2 001–10 000  : +10
  Monthly visitors  > 10 000      : +20
  Bounce rate       < 35 %        : +15
  Bounce rate       35–50 %       :  +5
  Bounce rate       50–65 %       :  ±0
  Bounce rate       > 65 %        : -15
  Traffic trend growing  > 20 %   : +10
  Traffic trend growing  0–20 %   :  +5
  Traffic trend declining 0–20 %  :  -5
  Traffic trend declining > 20 %  : -10
  Organic search in top sources   :  +5
  Capped at 100, floor 20.
"""
import os
import warnings
from datetime import date, timedelta
from typing import Dict, Any, List, Optional

# Suppress the Python 3.9 deprecation warnings from google libraries
warnings.filterwarnings("ignore", category=FutureWarning, module="google")


class AnalyticsAuditor:
    """
    Fetch GA4 traffic metrics for a given property.

    Parameters
    ----------
    property_id : str
        Numeric GA4 property ID (e.g. "123456789").
        Pass None / "" to receive a neutral-50 result with setup instructions.
    service_account_json_path : str | None
        Absolute path to the service account key JSON file.
        If None, tries Google Application Default Credentials (ADC).
    """

    def __init__(
        self,
        property_id: Optional[str] = None,
        service_account_json_path: Optional[str] = None,
    ):
        self.property_id    = (property_id or "").strip()
        self.sa_json_path   = service_account_json_path or ""

    # ── Public entry point ─────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        if not self.property_id:
            return self._no_data(
                "No GA4 property ID provided. "
                "Set GOOGLE_ANALYTICS_PROPERTY_ID in .env to unlock website traffic scoring."
            )

        # Service account file required — ADC fallback is not appropriate for client audits
        if not self.sa_json_path or not os.path.isfile(self.sa_json_path):
            return self._no_data(
                f"GA4 property {self.property_id} is connected. "
                "Waiting for service account JSON — place ga-service-account.json in the "
                "project folder and set GOOGLE_SERVICE_ACCOUNT_JSON_PATH in .env to pull "
                "live traffic data."
            )

        try:
            client = self._build_client()
        except Exception as exc:
            return self._no_data(
                f"GA4 auth failed: {exc}. "
                "Ensure GOOGLE_SERVICE_ACCOUNT_JSON_PATH points to a valid service "
                "account key file with access to this GA4 property."
            )

        try:
            return self._fetch_all(client)
        except Exception as exc:
            return self._no_data(f"GA4 API error: {exc}")

    # ── Client construction ────────────────────────────────────

    def _build_client(self):
        """Return an authenticated BetaAnalyticsDataClient via service account."""
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        return BetaAnalyticsDataClient.from_service_account_file(self.sa_json_path)

    # ── Fetch all metrics ──────────────────────────────────────

    def _fetch_all(self, client) -> Dict[str, Any]:
        from google.analytics.data_v1beta.types import (
            DateRange, Dimension, Metric, RunReportRequest,
        )

        prop = f"properties/{self.property_id}"
        today        = date.today()
        start_30     = (today - timedelta(days=30)).isoformat()
        start_60     = (today - timedelta(days=60)).isoformat()
        start_31     = (today - timedelta(days=31)).isoformat()
        end_today    = today.isoformat()

        # ── Visitors, bounce rate, session duration (last 30 days) ──
        overview_req = RunReportRequest(
            property=prop,
            date_ranges=[DateRange(start_date=start_30, end_date=end_today)],
            metrics=[
                Metric(name="activeUsers"),
                Metric(name="bounceRate"),
                Metric(name="averageSessionDuration"),
                Metric(name="sessions"),
            ],
        )
        overview = client.run_report(overview_req)
        row   = overview.rows[0] if overview.rows else None
        monthly_visitors   = int(float(row.metric_values[0].value)) if row else 0
        bounce_rate        = round(float(row.metric_values[1].value) * 100, 1) if row else None
        avg_session_sec    = round(float(row.metric_values[2].value)) if row else None
        sessions_current   = int(float(row.metric_values[3].value)) if row else 0

        avg_session_str = self._fmt_duration(avg_session_sec) if avg_session_sec else "—"

        # ── Traffic trend: prior 30 days ──────────────────────
        trend_req = RunReportRequest(
            property=prop,
            date_ranges=[DateRange(start_date=start_60, end_date=start_31)],
            metrics=[Metric(name="sessions")],
        )
        trend = client.run_report(trend_req)
        sessions_prior = int(float(trend.rows[0].metric_values[0].value)) if trend.rows else 0

        if sessions_prior > 0:
            trend_pct = round((sessions_current - sessions_prior) / sessions_prior * 100, 1)
        else:
            trend_pct = None

        trend_label = self._trend_label(trend_pct)

        # ── Top traffic sources ────────────────────────────────
        sources_req = RunReportRequest(
            property=prop,
            date_ranges=[DateRange(start_date=start_30, end_date=end_today)],
            dimensions=[Dimension(name="sessionDefaultChannelGrouping")],
            metrics=[Metric(name="sessions")],
            limit=6,
        )
        sources_resp = client.run_report(sources_req)
        top_sources  = [
            {
                "channel":  r.dimension_values[0].value,
                "sessions": int(float(r.metric_values[0].value)),
            }
            for r in sources_resp.rows
        ]

        # ── Top landing pages ──────────────────────────────────
        pages_req = RunReportRequest(
            property=prop,
            date_ranges=[DateRange(start_date=start_30, end_date=end_today)],
            dimensions=[Dimension(name="landingPage")],
            metrics=[Metric(name="sessions"), Metric(name="bounceRate")],
            limit=5,
        )
        pages_resp   = client.run_report(pages_req)
        top_pages    = [
            {
                "page":        r.dimension_values[0].value,
                "sessions":    int(float(r.metric_values[0].value)),
                "bounce_rate": round(float(r.metric_values[1].value) * 100, 1),
            }
            for r in pages_resp.rows
        ]

        # ── Score ──────────────────────────────────────────────
        score  = self._compute_score(monthly_visitors, bounce_rate, trend_pct, top_sources)
        issues, strengths = self._evaluate(monthly_visitors, bounce_rate, trend_pct, top_sources)

        return {
            "score":                score,
            "grade":                self._grade(score),
            "data_source":          "google_analytics_data_api_v4",
            "property_id":          self.property_id,
            "period":               f"Last 30 days (ending {end_today})",
            "monthly_visitors":     monthly_visitors,
            "sessions_current":     sessions_current,
            "sessions_prior_30d":   sessions_prior,
            "traffic_trend_pct":    trend_pct,
            "traffic_trend_label":  trend_label,
            "bounce_rate_pct":      bounce_rate,
            "avg_session_duration": avg_session_str,
            "top_traffic_sources":  top_sources,
            "top_landing_pages":    top_pages,
            "issues":               issues,
            "strengths":            strengths,
        }

    # ── Scoring ────────────────────────────────────────────────

    def _compute_score(
        self,
        visitors: int,
        bounce_rate: Optional[float],
        trend_pct: Optional[float],
        sources: List[Dict],
    ) -> int:
        score = 50

        # Visitor volume
        if   visitors < 500:    score -= 10
        elif visitors > 10000:  score += 20
        elif visitors > 2000:   score += 10

        # Bounce rate
        if bounce_rate is not None:
            if   bounce_rate < 35:  score += 15
            elif bounce_rate < 50:  score += 5
            elif bounce_rate > 65:  score -= 15

        # Trend
        if trend_pct is not None:
            if   trend_pct >  20: score += 10
            elif trend_pct >   0: score +=  5
            elif trend_pct > -20: score -=  5
            else:                 score -= 10

        # Organic search presence
        channels = [s["channel"].lower() for s in sources]
        if any("organic" in c or "search" in c for c in channels):
            score += 5

        return max(20, min(100, score))

    def _evaluate(self, visitors, bounce_rate, trend_pct, sources):
        issues, strengths = [], []

        if visitors == 0:
            issues.append("🔴 No visitor data returned — verify property ID and access permissions.")
        elif visitors < 500:
            issues.append(f"🔴 Only {visitors:,} visitors in the last 30 days — well below the 500+/month benchmark for a B2B service business.")
        elif visitors > 2000:
            strengths.append(f"✅ {visitors:,} monthly visitors — strong for a B2B service business.")
        else:
            strengths.append(f"✅ {visitors:,} monthly visitors.")

        if bounce_rate is not None:
            if bounce_rate < 35:
                strengths.append(f"✅ Bounce rate {bounce_rate}% — excellent (< 35%).")
            elif bounce_rate < 50:
                strengths.append(f"✅ Bounce rate {bounce_rate}% — good.")
            elif bounce_rate > 65:
                issues.append(f"🔴 Bounce rate {bounce_rate}% — high. Visitors are leaving without engaging. Review landing page relevance and page speed.")
            else:
                issues.append(f"🟡 Bounce rate {bounce_rate}% — above average. Landing page messaging may not match traffic intent.")

        if trend_pct is not None:
            if trend_pct > 20:
                strengths.append(f"✅ Traffic growing {trend_pct:+.1f}% vs prior 30 days.")
            elif trend_pct > 0:
                strengths.append(f"✅ Traffic up {trend_pct:+.1f}% vs prior 30 days.")
            elif trend_pct < -20:
                issues.append(f"🔴 Traffic declining {trend_pct:.1f}% vs prior 30 days — investigate algorithm changes, expired campaigns, or content gaps.")
            else:
                issues.append(f"🟡 Traffic down {trend_pct:.1f}% vs prior 30 days.")

        channels = [s["channel"].lower() for s in sources]
        if not any("organic" in c or "search" in c for c in channels):
            issues.append("🟡 No Organic Search traffic in top sources — SEO is not generating inbound visitors.")
        else:
            strengths.append("✅ Organic Search present in top traffic sources.")

        if not any("direct" in c for c in channels):
            issues.append("🟡 No Direct traffic — low brand recall or awareness.")

        return issues, strengths

    # ── Neutral fallback ───────────────────────────────────────

    def _no_data(self, note: str) -> Dict[str, Any]:
        return {
            "score":               50,
            "grade":               "C",
            "data_source":         "not_available",
            "note":                note,
            "monthly_visitors":    None,
            "traffic_trend_pct":   None,
            "traffic_trend_label": "—",
            "bounce_rate_pct":     None,
            "avg_session_duration": "—",
            "top_traffic_sources": [],
            "top_landing_pages":   [],
            "issues":              [],
            "strengths":           [],
        }

    # ── Helpers ────────────────────────────────────────────────

    @staticmethod
    def _fmt_duration(seconds: int) -> str:
        if seconds < 60:
            return f"{seconds}s"
        m, s = divmod(seconds, 60)
        return f"{m}m {s:02d}s"

    @staticmethod
    def _trend_label(pct: Optional[float]) -> str:
        if pct is None:
            return "—"
        if pct > 20:
            return f"Growing ▲ {pct:+.1f}%"
        if pct > 0:
            return f"Stable ▲ {pct:+.1f}%"
        if pct > -20:
            return f"Declining ▼ {pct:.1f}%"
        return f"Declining sharply ▼ {pct:.1f}%"

    @staticmethod
    def _grade(score: int) -> str:
        if score >= 80: return "A"
        if score >= 65: return "B"
        if score >= 50: return "C"
        if score >= 35: return "D"
        return "F"
