"""
批量下载A股日K线行情数据（前复权），一个代码一个CSV文件。
数据源：baostock，股票列表：akshare。
手动进程池 + 真超时kill，分批重启。
支持增量更新：根据manifest.json中的行情截止日，只下载截止日之后的数据。
"""

import akshare as ak
import multiprocessing
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
MAX_WORKERS = 3         # 并行进程数
BATCH_SIZE = 200        # 每批数量，完成后重启连接池
STOCK_TIMEOUT = 30      # 单只超时秒数，超时直接 SIGKILL
RESTART_SLEEP = 10      # 批次间暂停秒数

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


def download_one(code, output_dir, start_date, end_date, fields):
    """子进程入口。文件不存在→全量下载；存在→增量合并。"""
    import baostock as bs
    import random

    code6 = str(code).strip().zfill(6)
    bs_code = ("sh." if code6.startswith("6") else "sz.") + code6
    filepath = Path(output_dir) / f"{code6}.csv"
    is_update = filepath.exists()

    time.sleep(random.uniform(0, 1.5))

    lg = bs.login()
    if lg.error_code != "0":
        return

    try:
        rs = bs.query_history_k_data_plus(
            bs_code, ",".join(fields),
            start_date=start_date, end_date=end_date,
            frequency="d", adjustflag="2",
        )

        data_list = []
        while (rs.error_code == "0") and rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            return

        new_df = pd.DataFrame(data_list, columns=fields)

        if is_update:
            try:
                old_df = pd.read_csv(filepath, encoding="gbk", dtype=str)
            except Exception:
                # 文件损坏，按全量处理
                new_df.to_csv(filepath, encoding="gbk", index=False)
                return

            merged = pd.concat([old_df, new_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["date"], keep="last")
            merged = merged.sort_values("date")
            merged.to_csv(filepath, encoding="gbk", index=False)
        else:
            new_df.to_csv(filepath, encoding="gbk", index=False)
    finally:
        bs.logout()


def process_batch(batch, output_dir, fields):
    """处理一批股票。batch: [(code, start_date), ...]。返回 (success, failed_list)。"""
    ctx = multiprocessing.get_context("spawn")
    running = {}   # pid -> (process, code, start_time)
    success = 0
    failed = []
    code_iter = iter(batch)
    done_count = 0
    total = len(batch)

    def start_worker(code, start_date):
        p = ctx.Process(
            target=download_one,
            args=(code, output_dir, start_date, END_DATE, fields),
        )
        p.start()
        running[p.pid] = (p, code, perf_counter())

    def reap_worker(pid, timed_out=False):
        nonlocal success, done_count
        p, code, _start = running.pop(pid)
        code6 = str(code).strip().zfill(6)

        if timed_out:
            p.kill()
            reason = f"超时{STOCK_TIMEOUT}s"
        else:
            reason = "异常退出" if p.exitcode != 0 else ""

        p.join(timeout=5)
        if p.is_alive():
            p.kill()
            p.join()

        if not timed_out and not reason:
            if (Path(output_dir) / f"{code6}.csv").exists():
                success += 1
            else:
                reason = "无数据"

        if reason:
            failed.append((code6, reason))

        done_count += 1
        elapsed = perf_counter() - batch_start
        rate = done_count / elapsed if elapsed > 0 else 0
        eta = (total - done_count) / rate if rate > 0 else 0
        print(f"\r  [{done_count}/{total}] "
              f"成功:{success} 失败:{len(failed)}  "
              f"{rate:.2f}只/s  ETA:{eta:.0f}s",
              end="", flush=True)

    batch_start = perf_counter()

    # 初始填充
    for _ in range(min(MAX_WORKERS, total)):
        try:
            code, start_date = next(code_iter)
            start_worker(code, start_date)
        except StopIteration:
            break

    # 主循环
    while running:
        to_reap = []
        now = perf_counter()

        for pid, (p, code, st) in running.items():
            if not p.is_alive():
                to_reap.append((pid, False))
            elif now - st > STOCK_TIMEOUT:
                to_reap.append((pid, True))

        for pid, timed_out in to_reap:
            reap_worker(pid, timed_out)
            try:
                code, start_date = next(code_iter)
                start_worker(code, start_date)
            except StopIteration:
                pass

        if not to_reap:
            time.sleep(0.3)

    return success, failed


def main():
    start_time = perf_counter()

    # ---- 加载说明文件 ----
    manifest = load_manifest()

    # ---- 获取股票列表 ----
    ak.stock_info_sh_name_code.cache_clear()
    stocks = ak.stock_info_a_code_name()
    stocks = stocks.rename(columns={
        stocks.columns[0]: "证券代码",
        stocks.columns[1]: "证券名称",
    })

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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
    success = 0
    all_failed = []

    for i in range(0, len(todo), BATCH_SIZE):
        batch = todo[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(todo) + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"--- 第{batch_num}/{total_batches}批, {len(batch)}只 ---")
        s, f = process_batch(batch, str(OUTPUT_DIR), FIELDS)
        success += s
        all_failed.extend(f)

        remaining = len(todo) - i - len(batch)
        if remaining > 0:
            print(f"\n--- 批次完成，暂停{RESTART_SLEEP}s ---")
            time.sleep(RESTART_SLEEP)

    # ---- 更新说明文件中各股票的行情截止日 ----
    failed_set = {code for code, _ in all_failed}
    new_success = 0
    update_success = 0

    for code, start_date in todo:
        code6 = str(code).strip().zfill(6)
        if code6 in failed_set:
            continue
        filepath = OUTPUT_DIR / f"{code6}.csv"
        if not filepath.exists():
            continue
        last_date, records = get_csv_info(filepath)
        if last_date is None:
            continue
        manifest["stocks"][code6] = {
            "name": code_name_map.get(code6, ""),
            "last_trade_date": last_date,
            "records": records,
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        if start_date == START_DATE:
            new_success += 1
        else:
            update_success += 1

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
    print(f"总数: {total}")
    print(f"非3/0/6开头跳过: {skipped_prefix}")
    print(f"新下载: {new_success}")
    print(f"增量更新: {update_success}")
    print(f"已是最新: {len(already_current)}")
    print(f"失败/超时: {len(all_failed)}")
    if all_failed:
        print(f"失败详情: {OUTPUT_DIR / 'failed_codes.txt'}")
    print(f"输出目录: {OUTPUT_DIR}")
    print(f"说明文件: {MANIFEST_PATH}")
    print(f"复权类型: {manifest['adjust_type']} (adjustflag={manifest['adjustflag']})")
    if success > 0:
        print(f"平均速度: {duration/success:.1f}s/只")
    print("=" * 50)


if __name__ == "__main__":
    main()
