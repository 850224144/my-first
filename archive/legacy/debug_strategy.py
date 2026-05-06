# debug_strategy.py
from __future__ import annotations

import polars as pl

from core.data import get_db_connection
import core.strategy as strategy


def read_universe(limit: int = 50) -> pl.DataFrame:
    """
    读取当前股票池。
    默认只诊断前 50 只，避免输出太多。
    """
    try:
        df = pl.read_parquet("data/universe.parquet")
    except Exception as e:
        print(f"❌ 读取 data/universe.parquet 失败：{e}")
        return pl.DataFrame()

    if limit:
        return df.head(limit)

    return df


def load_daily_from_cache(code: str, bars: int = 520) -> pl.DataFrame:
    """
    只从 stock_daily 本地缓存读取日线。
    不走 get_data_with_status，避免拿到盘中实时 1 行数据。
    """
    con = get_db_connection()

    try:
        rows = con.execute(
            """
            SELECT
                date,
                open,
                high,
                low,
                close,
                volume,
                amount,
                adj_type,
                source
            FROM stock_daily
            WHERE code = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            [code, bars],
        ).fetchall()

        cols = [x[0] for x in con.description]

        if not rows:
            return pl.DataFrame()

        df = pl.DataFrame(rows, schema=cols, orient="row")

        df = df.with_columns(
            [
                pl.col("date").cast(pl.Date, strict=False),
                pl.col("open").cast(pl.Float64, strict=False),
                pl.col("high").cast(pl.Float64, strict=False),
                pl.col("low").cast(pl.Float64, strict=False),
                pl.col("close").cast(pl.Float64, strict=False),
                pl.col("volume").cast(pl.Float64, strict=False),
                pl.col("amount").cast(pl.Float64, strict=False),
            ]
        )

        return df.sort("date")

    finally:
        con.close()


def get_strategy_func():
    """
    兼容不同版本 strategy.py：
    优先使用 score_second_buy；
    如果没有，则使用 is_second_buy。
    """
    fn = getattr(strategy, "score_second_buy", None)

    if fn is None:
        fn = getattr(strategy, "is_second_buy", None)

    return fn


def analyze_one(code: str, fn) -> dict:
    """
    诊断单只股票策略返回结果。
    """
    df = load_daily_from_cache(code, bars=520)

    if df is None or df.is_empty():
        return {
            "code": code,
            "bars": 0,
            "last_date": None,
            "last_close": None,
            "result_type": "no_data",
            "result": "no_data",
            "score": None,
            "keys": "",
        }

    last_date = None
    last_close = None

    try:
        last = df.tail(1)
        last_date = str(last["date"][0])
        last_close = float(last["close"][0])
    except Exception:
        pass

    try:
        try:
            res = fn(df, mode="observe")
        except TypeError:
            res = fn(df)

        result_type = type(res).__name__

        if isinstance(res, dict):
            score = (
                res.get("total_score")
                or res.get("score")
                or res.get("total")
                or res.get("final_score")
            )

            return {
                "code": code,
                "bars": len(df),
                "last_date": last_date,
                "last_close": last_close,
                "result_type": result_type,
                "result": str(res)[:300],
                "score": score,
                "keys": ",".join([str(k) for k in res.keys()]),
            }

        return {
            "code": code,
            "bars": len(df),
            "last_date": last_date,
            "last_close": last_close,
            "result_type": result_type,
            "result": str(res),
            "score": None,
            "keys": "",
        }

    except Exception as e:
        return {
            "code": code,
            "bars": len(df),
            "last_date": last_date,
            "last_close": last_close,
            "result_type": "exception",
            "result": str(e),
            "score": None,
            "keys": "",
        }


def print_data_quality(universe: pl.DataFrame):
    """
    打印股票池数据质量。
    """
    if universe.is_empty():
        print("❌ 股票池为空")
        return

    codes = universe["code"].cast(pl.Utf8).to_list()

    rows = []

    for code in codes:
        df = load_daily_from_cache(code, bars=520)

        if df is None or df.is_empty():
            rows.append(
                {
                    "code": code,
                    "bars": 0,
                    "last_date": None,
                    "last_close": None,
                }
            )
            continue

        try:
            last = df.tail(1)
            rows.append(
                {
                    "code": code,
                    "bars": len(df),
                    "last_date": str(last["date"][0]),
                    "last_close": float(last["close"][0]),
                }
            )
        except Exception:
            rows.append(
                {
                    "code": code,
                    "bars": len(df),
                    "last_date": None,
                    "last_close": None,
                }
            )

    out = pl.DataFrame(rows)

    print("\n数据质量统计：")
    print(
        out.select(
            [
                pl.len().alias("total"),
                (pl.col("bars") >= 250).sum().alias("bars_ge_250"),
                (pl.col("bars") < 250).sum().alias("bars_lt_250"),
                pl.col("bars").min().alias("min_bars"),
                pl.col("bars").mean().alias("mean_bars"),
                pl.col("bars").max().alias("max_bars"),
            ]
        )
    )


def main():
    universe = read_universe(limit=50)

    if universe.is_empty():
        print("❌ 股票池为空，请先执行：")
        print("python run_scan.py --build-universe --workers 1")
        return

    fn = get_strategy_func()

    if fn is None:
        print("❌ strategy.py 中没有 score_second_buy 或 is_second_buy")
        return

    print("使用策略函数:", fn.__name__)
    print("测试股票数量:", len(universe))

    print_data_quality(universe)

    rows = []

    for code in universe["code"].cast(pl.Utf8).to_list():
        rows.append(analyze_one(code, fn))

    out = pl.DataFrame(rows)

    print("\n策略诊断明细：")
    print(out)

    print("\n结果类型统计：")
    print(out.group_by("result_type").len())

    if "score" in out.columns:
        score_df = out.with_columns(
            pl.col("score").cast(pl.Float64, strict=False).alias("score_num")
        )

        print("\n分数分布：")
        print(
            score_df.select(
                [
                    pl.col("score_num").min().alias("min"),
                    pl.col("score_num").mean().alias("mean"),
                    pl.col("score_num").max().alias("max"),
                ]
            )
        )

        print("\nTop 20：")
        print(
            score_df.sort("score_num", descending=True, nulls_last=True)
            .select(
                [
                    "code",
                    "bars",
                    "last_date",
                    "last_close",
                    "result_type",
                    "score_num",
                    "result",
                ]
            )
            .head(20)
        )


if __name__ == "__main__":
    main()