import html
import logging
import os
from datetime import datetime, timezone

from airflow.exceptions import AirflowFailException

from pipeline.config import DQ_REPORTS_DIR

logger = logging.getLogger(__name__)

TEMPLATE = """<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<title>Data Quality: labeled_transactions</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 32px; color: #1f2328; }}
  h1 {{ font-size: 22px; margin-bottom: 4px; }}
  .meta {{ color: #59636e; margin-bottom: 20px; }}
  .summary {{ font-size: 16px; margin-bottom: 16px; }}
  table {{ border-collapse: collapse; width: 100%; }}
  th, td {{ border: 1px solid #d1d9e0; padding: 8px 10px; text-align: left; vertical-align: top; font-size: 14px; }}
  th {{ background: #f6f8fa; }}
  .passed {{ color: #1a7f37; font-weight: 600; }}
  .failed {{ color: #d1242f; font-weight: 600; }}
  code {{ font-size: 13px; }}
</style>
</head>
<body>
<h1>Data Quality: labeled_transactions</h1>
<div class="meta">Прогон {run_id}, {generated_at}</div>
<div class="summary">Пройдено правил: <b>{passed} из {total}</b>. Итог: <span class="{status_class}">{status}</span></div>
<table>
<tr><th>#</th><th>Правило</th><th>Описание</th><th>Статус</th><th>Факт</th><th>Ожидание</th></tr>
{rows}
</table>
</body>
</html>
"""

ROW = (
    "<tr><td>{n}</td><td><code>{rule}</code></td><td>{description}</td>"
    "<td class=\"{status_class}\">{status}</td><td>{observed}</td><td>{expected}</td></tr>"
)


def _write_atomic(path, text):
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def build_report(results: list, run_id=None) -> dict:
    """Собирает результаты всех DQ-правил в HTML-отчёт.

    Читает: результаты задач-проверок из XCom.
    Пишет: artifacts/dq_reports/dq_report_<время>.html и копию latest.html.
    Каталог отчётов лежит рядом с каталогом моделей.

    Отчёт пишется всегда, даже если часть правил не прошла: именно по нему
    видно, что сломалось. Файлы пишутся через временный файл и os.replace,
    поэтому повторный запуск просто добавит новый отчёт и перезапишет latest.html.
    """
    generated_at = datetime.now(timezone.utc)
    passed = sum(r["passed"] for r in results)
    rows = "\n".join(
        ROW.format(
            n=i,
            rule=html.escape(r["rule"]),
            description=html.escape(r["description"]),
            status_class="passed" if r["passed"] else "failed",
            status="passed" if r["passed"] else "failed",
            observed=html.escape(str(r["observed"])),
            expected=html.escape(str(r["expected"])),
        )
        for i, r in enumerate(results, start=1)
    )
    ok = passed == len(results)
    page = TEMPLATE.format(
        run_id=html.escape(str(run_id)),
        generated_at=generated_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
        passed=passed,
        total=len(results),
        status_class="passed" if ok else "failed",
        status="данные в порядке" if ok else "есть нарушения",
        rows=rows,
    )

    DQ_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report = DQ_REPORTS_DIR / f"dq_report_{generated_at:%Y%m%dT%H%M%S}.html"
    _write_atomic(report, page)
    _write_atomic(DQ_REPORTS_DIR / "latest.html", page)
    logger.info("DQ report %s: %d/%d rules passed", report, passed, len(results))
    return {"report": str(report), "passed": passed, "total": len(results)}


def assert_quality(results: list) -> None:
    """Помечает прогон DQ упавшим, если хотя бы одно правило не прошло.

    Читает: результаты проверок из XCom. Ничего не пишет.
    Отчёт к этому моменту уже опубликован build_report, так что падение
    здесь нужно только для того, чтобы нарушение было видно в UI Airflow.
    """
    failed = [r["rule"] for r in results if not r["passed"]]
    if failed:
        raise AirflowFailException(f"Data quality rules failed: {failed}")
