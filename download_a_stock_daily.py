"""
批量下载A股日K线行情数据（前复权），一个代码一个CSV文件。
数据源：baostock，股票列表：akshare。
手动进程池 + 真超时kill，分批重启。

模式:
  python download_a_stock_daily.py            # 普通模式：从akshare取股票列表，新下载+增量更新
  python download_a_stock_daily.py --sync     # 同步模式：扫描所有CSV文件最新日期，补齐缺失
  python download_a_stock_daily.py --retry-failed  # 重试模式：重试failed_codes.txt中的失败项
"""

import akshare as ak
import multiprocessing
import sys
import time
from time import perf_counter
from pathlib import Path
import json
import pandas as pd

# ============================================================
# 配置
# ============================================================
OUTPUT_DIR = Path("/Users/nayiahlu/Desktop/stocks")
START_DATE = "2021-01-01"
END_DATE = time.strftime("%Y-%m-%d")
MANIFEST_PATH = OUTPUT_DIR / "manifest.json"
MAX_WORKERS = 6         # 并行进程数
BATCH_SIZE = 200        # 每批数量，完成后重启连接池
STOCK_TIMEOUT = 30      # 单只超时秒数，超时直接 SIGKILL
RESTART_SLEEP = 3       # 批次间暂停秒数

FIELDS = [
    "date", "code", "open", "high", "low", "close", "preclose",
    "volume", "amount", "adjustflag", "turn", "tradestatus", "pctChg",
    "peTTM", "pbMRQ", "psTTM", "pcfNcfTTM", "isST",
]


def load_manifest():
    """加载说明文件，不存在则返回初始结构。"""
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "description": "A股日K线行情数据说明",
        "adjust_type": "前复权",
        "adjustflag": "2",
        "data_source": "baostock",
        "stock_list_source": "akshare",
        "start_date": START_DATE,
        "download_history": [],
        "stocks": {},
    }


def save_manifest(manifest):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)


def get_csv_info(filepath):
    """返回CSV中的 (最后交易日, 记录条数)。"""
    try:
        df = pd.read_csv(filepath, encoding="gbk", usecols=["date"])
        return df["date"].max(), len(df)
    except Exception:
        return None, 0


def _read_existing_stats(filepath):
    """读取已有CSV的最后交易日和记录数。"""
    try:
        old_df = pd.read_csv(filepath, encoding="gbk", dtype=str)
        return old_df["date"].max(), len(old_df)
    except Exception:
        return None, 0


def _try_query(bs_code, fields, start_date, end_date):
    """尝试查询baostock，返回data_list。空列表=无数据。"""
    import baostock as bs
    rs = bs.query_history_k_data_plus(
        bs_code, ",".join(fields),
        start_date=start_date, end_date=end_date,
        frequency="d", adjustflag="2",
    )
    data_list = []
    while (rs.error_code == "0") and rs.next():
        data_list.append(rs.get_row_data())
    return data_list


def download_one(code, output_dir, start_date, end_date, fields, result_queue):
    """子进程入口。下载数据写CSV，通过Queue返回 (code6, last_date, records, error)。"""
    import baostock as bs
    import random

    code6 = str(code).strip().zfill(6)
    bs_code = ("sh." if code6.startswith("6") else "sz.") + code6
    filepath = Path(output_dir) / f"{code6}.csv"
    is_update = filepath.exists()

    time.sleep(random.uniform(0, 0.5))

    # 登录（重试1次）
    for attempt in range(2):
        lg = bs.login()
        if lg.error_code == "0":
            break
        if attempt == 0:
            time.sleep(random.uniform(1, 3))
    else:
        result_queue.put((code6, None, 0, "登录失败"))
        return

    try:
        data_list = _try_query(bs_code, fields, start_date, end_date)

        # 增量更新返回空：可能是限流，退避重试1次
        if not data_list and is_update:
            time.sleep(random.uniform(2, 5))
            data_list = _try_query(bs_code, fields, start_date, end_date)

        if not data_list:
            if is_update:
                # 已有CSV数据完好，无新数据不算失败
                last_date, records = _read_existing_stats(filepath)
                result_queue.put((code6, last_date, records, ""))
            else:
                # 新下载确实无数据
                result_queue.put((code6, None, 0, "无数据"))
            return

        new_df = pd.DataFrame(data_list, columns=fields)

        if is_update:
            try:
                old_df = pd.read_csv(filepath, encoding="gbk", dtype=str)
            except Exception:
                new_df.to_csv(filepath, encoding="gbk", index=False)
                result_queue.put((code6, new_df["date"].max(), len(new_df), ""))
                return

            merged = pd.concat([old_df, new_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["date"], keep="last")
            merged = merged.sort_values("date")
            merged.to_csv(filepath, encoding="gbk", index=False)
            result_queue.put((code6, merged["date"].max(), len(merged), ""))
        else:
            new_df.to_csv(filepath, encoding="gbk", index=False)
            result_queue.put((code6, new_df["date"].max(), len(new_df), ""))
    except Exception as e:
        result_queue.put((code6, None, 0, str(e)))
    finally:
        bs.logout()


def process_batch(batch, output_dir, fields, max_workers=MAX_WORKERS, stock_timeout=STOCK_TIMEOUT):
    """处理一批股票。batch: [(code, start_date), ...]。返回 (results, failed_list)。"""
    ctx = multiprocessing.get_context("spawn")
    result_queue = ctx.Queue()
    running = {}
    results = []
    failed = []
    code_iter = iter(batch)
    done_count = 0
    total = len(batch)

    def start_worker(code, start_date):
        p = ctx.Process(
            target=download_one,
            args=(code, output_dir, start_date, END_DATE, fields, result_queue),
        )
        p.start()
        running[p.pid] = (p, code, perf_counter())

    def drain_queue():
        while True:
            try:
                code6, last_date, records, error = result_queue.get_nowait()
                if not error and last_date:
                    results.append((code6, last_date, records))
                elif error:
                    failed.append((code6, error))
            except Exception:
                break

    batch_start = perf_counter()

    for _ in range(min(max_workers, total)):
        try:
            code, start_date = next(code_iter)
            start_worker(code, start_date)
        except StopIteration:
            break

    while running:
        drain_queue()

        to_reap = []
        now = perf_counter()

        for pid, (p, code, st) in running.items():
            if not p.is_alive():
                to_reap.append((pid, False))
            elif now - st > stock_timeout:
                to_reap.append((pid, True))

        for pid, timed_out in to_reap:
            p, code, _start = running.pop(pid)
            code6 = str(code).strip().zfill(6)

            if timed_out:
                p.kill()
                failed.append((code6, f"超时{stock_timeout}s"))

            p.join(timeout=5)
            if p.is_alive():
                p.kill()
                p.join()

            done_count += 1
            elapsed = perf_counter() - batch_start
            rate = done_count / elapsed if elapsed > 0 else 0
            eta = (total - done_count) / rate if rate > 0 else 0
            print(f"\r  [{done_count}/{total}] "
                  f"成功:{len(results)} 失败:{len(failed)}  "
                  f"{rate:.2f}只/s  ETA:{eta:.0f}s",
                  end="", flush=True)

            try:
                code, start_date = next(code_iter)
                start_worker(code, start_date)
            except StopIteration:
                pass

        if not to_reap:
            time.sleep(0.3)

    drain_queue()
    return results, failed


def main():
    start_time = perf_counter()

    # ---- 加载说明文件 ----
    manifest = load_manifest()

    # ---- 检查运行模式 ----
    retry_failed = "--retry-failed" in sys.argv
    sync_mode = "--sync" in sys.argv

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if sync_mode:
        # ---- 同步模式：扫描所有CSV文件，补齐缺失日期的行情数据 ----
        csv_files = sorted(OUTPUT_DIR.glob("*.csv"))
        incremental = []
        already_current = []
        corrupt_files = []
        code_name_map = {}
        skipped_prefix = 0

        for filepath in csv_files:
            code6 = filepath.stem
            if not code6.isdigit() or len(code6) != 6:
                continue

            file_last_date, record_count = get_csv_info(filepath)
            if file_last_date is None:
                corrupt_files.append(code6)
                continue

            stock_info = manifest.get("stocks", {}).get(code6, {})
            code_name_map[code6] = stock_info.get("name", "")

            if file_last_date >= END_DATE:
                already_current.append(code6)
            else:
                incremental.append((code6, file_last_date))

        workers = MAX_WORKERS
        batch_size = BATCH_SIZE
        timeout = STOCK_TIMEOUT
        new_downloads = []

        total = len(csv_files)
        print(f"[同步模式] 扫描到 {total} 个CSV文件")
        print(f"  需补齐: {len(incremental)}  已最新: {len(already_current)}"
              f"  损坏: {len(corrupt_files)}")
        print(f"  截止日期: {END_DATE}")
        print(f"并行: {workers}进程  每批: {batch_size}只  超时: {timeout}s")
        print(f"批次暂停: {RESTART_SLEEP}s\n")

        if corrupt_files:
            print(f"  损坏文件(将跳过): {', '.join(corrupt_files[:20])}"
                  f"{'...' if len(corrupt_files) > 20 else ''}\n")

    elif retry_failed:
        failed_log = OUTPUT_DIR / "failed_codes.txt"
        if not failed_log.exists():
            print("没有 failed_codes.txt，无法重试。请先运行一次完整下载。")
            return

        code_name_map = {}
        incremental = []
        already_current = []
        new_downloads = []
        skipped_prefix = 0
        workers = 3          # 重试模式降低并发，避免再次被限流
        batch_size = 100
        timeout = 45         # 放宽超时

        with open(failed_log, "r", encoding="utf-8") as f:
            next(f)  # 跳过 header
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(",", 1)
                code6 = parts[0].strip().zfill(6)
                reason = parts[1] if len(parts) > 1 else ""

                filepath = OUTPUT_DIR / f"{code6}.csv"
                if not filepath.exists():
                    new_downloads.append((code6, START_DATE))
                    continue

                stock_info = manifest.get("stocks", {}).get(code6)
                if stock_info and stock_info.get("last_trade_date"):
                    last_date = stock_info["last_trade_date"]
                    if last_date >= END_DATE:
                        already_current.append(code6)
                    else:
                        incremental.append((code6, last_date))
                else:
                    file_last_date, _ = get_csv_info(filepath)
                    if file_last_date and file_last_date >= END_DATE:
                        already_current.append(code6)
                    elif file_last_date:
                        incremental.append((code6, file_last_date))
                    else:
                        new_downloads.append((code6, START_DATE))

        total = len(new_downloads) + len(incremental) + len(already_current)
        print(f"[重试失败模式] 并发:{workers}  超时:{timeout}s  每批:{batch_size}")
        print(f"  新下载:{len(new_downloads)}  增量更新:{len(incremental)}  已最新:{len(already_current)}")
        print(f"  截止日期:{END_DATE}\n")
    else:
        # ---- 获取股票列表 ----
        ak.stock_info_sh_name_code.cache_clear()
        stocks = ak.stock_info_a_code_name()
        stocks = stocks.rename(columns={
            stocks.columns[0]: "证券代码",
            stocks.columns[1]: "证券名称",
        })

        # ---- 过滤：只保留 3、0、6 开头的证券代码 ----
        valid_prefixes = {"3", "0", "6"}
        filtered_codes = []
        code_name_map = {}
        skipped_prefix = 0
        for _, row in stocks.iterrows():
            code = row["证券代码"]
            code6 = str(code).strip().zfill(6)
            if code6[0] not in valid_prefixes:
                skipped_prefix += 1
            else:
                filtered_codes.append(code)
                code_name_map[code6] = row["证券名称"]

        # ---- 分类：新下载 / 增量更新 / 已是最新 ----
        new_downloads = []      # [(code, START_DATE), ...]
        incremental = []         # [(code, last_date_from_manifest), ...]
        already_current = []

        for code in filtered_codes:
            code6 = str(code).strip().zfill(6)
            filepath = OUTPUT_DIR / f"{code6}.csv"

            if not filepath.exists():
                new_downloads.append((code, START_DATE))
                continue

            stock_info = manifest.get("stocks", {}).get(code6)
            if stock_info and stock_info.get("last_trade_date"):
                last_date = stock_info["last_trade_date"]
                if last_date >= END_DATE:
                    already_current.append(code6)
                else:
                    incremental.append((code, last_date))
            else:
                # 文件存在但manifest无记录（首次运行无manifest），从文件读截止日
                file_last_date, _ = get_csv_info(filepath)
                if file_last_date and file_last_date >= END_DATE:
                    already_current.append(code6)
                elif file_last_date:
                    incremental.append((code, file_last_date))
                else:
                    # 文件损坏或为空，重新下载
                    new_downloads.append((code, START_DATE))

        total = len(stocks)
        workers = MAX_WORKERS
        batch_size = BATCH_SIZE
        timeout = STOCK_TIMEOUT
        print(f"总数:{total}  非3/0/6开头跳过:{skipped_prefix}")
        print(f"  新下载:{len(new_downloads)}  增量更新:{len(incremental)}  已最新:{len(already_current)}")
        print(f"  截止日期:{END_DATE}  复权类型:{manifest['adjust_type']}")
        print(f"并行:{MAX_WORKERS}进程  每批:{BATCH_SIZE}只  超时:{STOCK_TIMEOUT}s")
        print(f"批次暂停:{RESTART_SLEEP}s\n")

    todo = new_downloads + incremental
    if not todo:
        print("所有股票已是最新，无需下载。")
        manifest["download_history"].append({
            "time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_date": END_DATE,
            "new_downloaded": 0,
            "updated": 0,
            "already_current": len(already_current),
            "failed": 0,
            "duration_seconds": round(perf_counter() - start_time, 1),
        })
        save_manifest(manifest)
        return

    # ---- 分批处理 ----
    all_failed = []
    new_success = 0
    update_success = 0

    for i in range(0, len(todo), batch_size):
        batch = todo[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(todo) + batch_size - 1) // batch_size

        print(f"--- 第{batch_num}/{total_batches}批, {len(batch)}只 ---")
        results, batch_failed = process_batch(batch, str(OUTPUT_DIR), FIELDS,
                                              max_workers=workers,
                                              stock_timeout=timeout)
        all_failed.extend(batch_failed)

        batch_failed_codes = {f[0] for f in batch_failed}
        for code6, last_date, records in results:
            manifest["stocks"][code6] = {
                "name": code_name_map.get(code6, ""),
                "last_trade_date": last_date,
                "records": records,
                "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
            }

        for code, start_date in batch:
            code6 = str(code).strip().zfill(6)
            if code6 in batch_failed_codes:
                continue
            if start_date == START_DATE:
                new_success += 1
            else:
                update_success += 1

        save_manifest(manifest)

        remaining = len(todo) - i - len(batch)
        if remaining > 0:
            print(f"\n--- 批次完成，暂停{RESTART_SLEEP}s ---")
            time.sleep(RESTART_SLEEP)

    # ---- 记录本次下载历史 ----
    manifest["download_history"].append({
        "time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "end_date": END_DATE,
        "new_downloaded": new_success,
        "updated": update_success,
        "already_current": len(already_current),
        "failed": len(all_failed),
        "duration_seconds": round(perf_counter() - start_time, 1),
    })
    save_manifest(manifest)

    # ---- 写失败日志 ----
    if all_failed:
        failed_log = OUTPUT_DIR / "failed_codes.txt"
        with open(failed_log, "w", encoding="utf-8") as f:
            f.write("code,reason\n")
            for code, reason in all_failed:
                f.write(f"{code},{reason}\n")

    # ---- 汇总 ----
    duration = perf_counter() - start_time
    print("\n" + "=" * 50)
    print(f"完成！耗时 {duration:.1f}s ({duration/60:.1f}min)")
    if sync_mode:
        print(f"扫描文件数: {total}")
        if corrupt_files:
            print(f"损坏文件: {len(corrupt_files)}")
    elif not retry_failed:
        print(f"总数: {total}")
        print(f"非3/0/6开头跳过: {skipped_prefix}")
    else:
        print(f"重试失败数: {total}")
    print(f"新下载: {new_success}")
    print(f"增量更新: {update_success}")
    print(f"已是最新: {len(already_current)}")
    print(f"失败/超时: {len(all_failed)}")
    if all_failed:
        print(f"失败详情: {OUTPUT_DIR / 'failed_codes.txt'}")
    print(f"输出目录: {OUTPUT_DIR}")
    print(f"说明文件: {MANIFEST_PATH}")
    print(f"复权类型: {manifest['adjust_type']} (adjustflag={manifest['adjustflag']})")
    total_success = new_success + update_success
    if total_success > 0:
        print(f"平均速度: {duration/total_success:.1f}s/只")
    print("=" * 50)


if __name__ == "__main__":
    main()
