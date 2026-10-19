from pipeline.db import LABELED_COLUMNS, connect

MAX_AMOUNT = 50_000
MIN_MONTH_ROWS = 10_000
FRAUD_RATE_RANGE = (0.001, 0.03)
MAX_DUPLICATE_SHARE = 0.001

REQUIRED_NOT_NULL = [
    "transaction_time", "merch", "cat_id", "amount", "gender", "one_city", "us_state",
    "lat", "lon", "population_city", "jobs", "merchant_lat", "merchant_lon", "target",
]
US_STATES = [
    "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS",
    "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV",
    "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY",
]
CATEGORIES = [
    "entertainment", "food_dining", "gas_transport", "grocery_net", "grocery_pos", "health_fitness", "home",
    "kids_pets", "misc_net", "misc_pos", "personal_care", "shopping_net", "shopping_pos", "travel",
]


def _result(rule, description, passed, observed, expected):
    return {
        "rule": rule,
        "description": description,
        "passed": bool(passed),
        "observed": observed,
        "expected": expected,
    }


def _query(sql, params=None):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def check_schema_and_volume() -> dict:
    """DQ-1. В витрине есть все колонки контракта и она не пустая.

    Читает: information_schema.columns и count(*) по labeled_transactions.
    Ничего не пишет, поэтому перезапуск безопасен.
    """
    columns = {r[0] for r in _query(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'labeled_transactions'"
    )}
    missing = [c for c in LABELED_COLUMNS if c not in columns]
    rows = _query("SELECT count(*) FROM labeled_transactions")[0][0]
    return _result(
        "schema_and_volume",
        "Все 18 колонок на месте, в витрине есть строки",
        not missing and rows > 0,
        f"rows={rows}, missing columns={missing or 'none'}",
        "rows > 0, missing columns = none",
    )


def check_completeness() -> dict:
    """DQ-2. В обязательных колонках нет пропусков.

    Читает: число NULL по каждой колонке из REQUIRED_NOT_NULL.
    Ничего не пишет, поэтому перезапуск безопасен.
    """
    exprs = ", ".join(f"count(*) FILTER (WHERE {c} IS NULL)" for c in REQUIRED_NOT_NULL)
    counts = dict(zip(REQUIRED_NOT_NULL, _query(f"SELECT {exprs} FROM labeled_transactions")[0]))
    bad = {c: n for c, n in counts.items() if n}
    return _result(
        "completeness",
        "Нет NULL в колонках, которые нужны модели и витрине",
        not bad,
        f"columns with NULL: {bad or 'none'}",
        "0 NULL in " + ", ".join(REQUIRED_NOT_NULL),
    )


def check_value_ranges() -> dict:
    """DQ-3. Числовые значения в допустимых диапазонах.

    Читает: число строк, где amount вне (0, MAX_AMOUNT], координаты вне
    допустимых широты и долготы, население города не положительное.
    Ничего не пишет, поэтому перезапуск безопасен.
    """
    amount, geo, population = _query(
        f"""
        SELECT
            count(*) FILTER (WHERE amount <= 0 OR amount > {MAX_AMOUNT}),
            count(*) FILTER (WHERE lat NOT BETWEEN -90 AND 90 OR merchant_lat NOT BETWEEN -90 AND 90
                              OR lon NOT BETWEEN -180 AND 180 OR merchant_lon NOT BETWEEN -180 AND 180),
            count(*) FILTER (WHERE population_city <= 0)
        FROM labeled_transactions
        """
    )[0]
    return _result(
        "value_ranges",
        f"amount в (0, {MAX_AMOUNT}], координаты в пределах широты и долготы, population_city > 0",
        amount == 0 and geo == 0 and population == 0,
        f"bad amount={amount}, bad coordinates={geo}, bad population={population}",
        "0 / 0 / 0",
    )


def check_categories() -> dict:
    """DQ-4. Категориальные поля принимают только известные значения.

    Читает: число строк, где target не 0/1, gender не F/M, us_state не из
    списка 51 штата, cat_id не из 14 известных категорий.
    Ничего не пишет, поэтому перезапуск безопасен.
    """
    target, gender, state, category = _query(
        """
        SELECT
            count(*) FILTER (WHERE target NOT IN (0, 1)),
            count(*) FILTER (WHERE gender NOT IN ('F', 'M')),
            count(*) FILTER (WHERE NOT us_state = ANY(%s)),
            count(*) FILTER (WHERE NOT cat_id = ANY(%s))
        FROM labeled_transactions
        """,
        (US_STATES, CATEGORIES),
    )[0]
    return _result(
        "categories",
        "target в {0, 1}, gender в {F, M}, us_state из 51 штата, cat_id из 14 категорий",
        target == 0 and gender == 0 and state == 0 and category == 0,
        f"bad target={target}, gender={gender}, us_state={state}, cat_id={category}",
        "0 / 0 / 0 / 0",
    )


def check_month_continuity() -> dict:
    """DQ-5. Месяцы идут без пропусков и в каждом достаточно строк.

    Читает: число строк по каждому календарному месяцу от самого раннего до
    самого позднего. Пустой месяц внутри истории сломал бы скользящее окно
    переобучения. Ничего не пишет, поэтому перезапуск безопасен.
    """
    rows = _query(
        """
        WITH bounds AS (
            SELECT date_trunc('month', min(transaction_time)) AS first_month,
                   date_trunc('month', max(transaction_time)) AS last_month
            FROM labeled_transactions
        ),
        months AS (
            SELECT generate_series(first_month, last_month, interval '1 month') AS month FROM bounds
        )
        SELECT to_char(m.month, 'YYYY-MM'), count(t.transaction_time)
        FROM months m
        LEFT JOIN labeled_transactions t ON date_trunc('month', t.transaction_time) = m.month
        GROUP BY m.month
        ORDER BY m.month
        """
    )
    small = {month: n for month, n in rows if n < MIN_MONTH_ROWS}
    return _result(
        "month_continuity",
        f"Нет пропущенных месяцев, в каждом месяце не меньше {MIN_MONTH_ROWS} строк",
        bool(rows) and not small,
        f"{len(rows)} months from {rows[0][0] if rows else '-'} to {rows[-1][0] if rows else '-'}, "
        f"too small: {small or 'none'}",
        f">= {MIN_MONTH_ROWS} rows in every month",
    )


def check_fraud_rate() -> dict:
    """DQ-6. Доля фрода в каждом месяце в правдоподобном коридоре.

    Читает: среднее target по месяцам. Слишком низкая или высокая доля
    обычно значит сломанную разметку или кривую поставку батча.
    Ничего не пишет, поэтому перезапуск безопасен.
    """
    low, high = FRAUD_RATE_RANGE
    rows = _query(
        """
        SELECT to_char(date_trunc('month', transaction_time), 'YYYY-MM'), avg(target)::float
        FROM labeled_transactions
        GROUP BY 1
        ORDER BY 1
        """
    )
    bad = {month: round(rate, 5) for month, rate in rows if not low <= rate <= high}
    rates = [rate for _, rate in rows]
    return _result(
        "fraud_rate",
        f"Доля фрода в каждом месяце от {low:.1%} до {high:.0%}",
        bool(rows) and not bad,
        f"min={min(rates):.4%}, max={max(rates):.4%}, out of range: {bad or 'none'}" if rates else "no data",
        f"{low:.1%} <= rate <= {high:.0%}",
    )


def check_duplicates() -> dict:
    """DQ-7. Полных дублей строк почти нет.

    Читает: число лишних копий строк, совпадающих по всем колонкам.
    Ничего не пишет, поэтому перезапуск безопасен.
    """
    columns = ", ".join(LABELED_COLUMNS)
    duplicates, total = _query(
        f"""
        SELECT coalesce(sum(n - 1), 0), (SELECT count(*) FROM labeled_transactions)
        FROM (SELECT count(*) AS n FROM labeled_transactions GROUP BY {columns} HAVING count(*) > 1) d
        """
    )[0]
    share = duplicates / total if total else 0
    return _result(
        "duplicates",
        f"Доля полных дублей не больше {MAX_DUPLICATE_SHARE:.1%}",
        share <= MAX_DUPLICATE_SHARE,
        f"duplicates={duplicates} of {total} ({share:.4%})",
        f"<= {MAX_DUPLICATE_SHARE:.1%}",
    )
