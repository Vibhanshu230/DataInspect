from __future__ import annotations

import re

from data_inspect.models import DatasetProfile, QualitySummary


class MockAgent:
    """Reads DatasetProfile JSON only. Does not open files or run Spark."""

    name = "mock"

    def summarize(self, profile: DatasetProfile) -> QualitySummary:
        worst = sorted(profile.columns, key=lambda c: c.fill_rate)[:3]
        worst_txt = ", ".join(f"{c.name}={c.fill_rate:.1%}" for c in worst)
        bools = [c for c in profile.columns if c.true_rate is not None]
        bool_txt = ", ".join(f"{c.name}={c.true_rate:.1%}" for c in bools) or "none"
        findings = [
            f"{profile.row_count} rows, {profile.column_count} cols, type={profile.source_type}, "
            f"dups={profile.duplicate_row_rate:.1%}.",
            f"Lowest fill: {worst_txt}.",
            f"True rates: {bool_txt}.",
        ]
        findings.extend(f"Flag: {f}" for f in profile.flags)
        return QualitySummary(
            headline=f"Profiled {profile.row_count} rows ({profile.source_type})",
            narrative=(
                f"Spark ({profile.backend}) computed the metrics. "
                f"Flags: {', '.join(profile.flags) or 'none'}."
            ),
            key_findings=findings,
            questions_to_ask=[
                "Which columns are below 95% fill rate?",
                "What is the true rate of default_flag?",
            ],
            recommended_checks=profile.recommended_checks,
        )

    def answer(self, profile: DatasetProfile, question: str) -> str:
        q = question.lower()
        if "fill" in q or "null" in q:
            threshold = 0.95
            m = re.search(r"(\d+)\s*%", q)
            if m:
                threshold = int(m.group(1)) / 100
            hits = [c for c in profile.columns if c.fill_rate < threshold]
            if not hits:
                return f"No columns below {threshold:.0%} fill rate."
            return "Columns below threshold: " + "; ".join(
                f"{c.name} fill_rate={c.fill_rate:.1%}" for c in hits
            )
        if "flag" in q:
            return "Flags: " + (", ".join(profile.flags) if profile.flags else "none")
        if "true rate" in q or "default" in q:
            bools = [c for c in profile.columns if c.true_rate is not None]
            if not bools:
                return "No boolean-like columns."
            return "True rates: " + ", ".join(f"{c.name}={c.true_rate:.1%}" for c in bools)
        if "row" in q and "count" in q:
            return f"row_count={profile.row_count}"
        if "what" in q and ("data" in q or "about" in q):
            cols = ", ".join(c.name for c in profile.columns)
            return f"{profile.source_type} with columns [{cols}]. {profile.row_count} rows."
        return "Ask about fill rate, true rate, flags, or row count. I only see the Spark profile."


def get_agent(backend: str | None = None) -> MockAgent:
    # Later: if backend == "openai": return OpenAIAgent() in this same file.
    return MockAgent()