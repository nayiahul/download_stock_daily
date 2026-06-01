"""
批量下载A股日K线行情数据（前复权），一个代码一个CSV文件。
数据源：baostock，股票列表：akshare。
手动进程池 + 真超时kill，分批重启。
"""

import akshare as ak
import multiprocessing
import time
from time import perf_counter
from pathlib import Path

# ============================================================
# 配置
# ============================================================
OUTPUT_DIR = Path("/Users/nayiahlu/Desktop/stocks")
START_DATE = "2021-01-01"
END_DATE = "2026-05-31"
MAX_WORKERS = 3         # 并行进程数
BATCH_SIZE = 200        # 每批数量，完成后重启连接池
STOCK_TIMEOUT = 30      # 单只超时秒数，超时直接 SIGKILL
RESTART_SLEEP = 10      # 批次间暂停秒数
SKIP_EXISTING = True

FIELDS = [
    "date", "code", "open", "high", "low", "close", "preclose",
    "volume", "amount", "adjustflag", "turn", "tradestatus", "pctChg",
    "peTTM", "pbMRQ", "psTTM", "pcfNcfTTM", "isST",
]


def download_one(code, output_dir, start_date, end_date, fields):
    """子进程入口：下载单只股票CSV。成功=文件存在。"""
    import baostock as bs
    import pandas as pd
    import random

    code6 = str(code).strip().zfill(6)
    bs_code = ("sh." if code6.startswith("6") else "sz.") + code6
    filepath = Path(output_dir) / f"{code6}.csv"

    time.sleep(random.uniform(0, 1.5))  # 错开登录

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

        if data_list:
            df = pd.DataFrame(data_list, columns=fields)
            df.to_csv(filepath, encoding="gbk", index=False)
    finally:
        bs.logout()


def process_batch(batch_codes, output_dir, start_date, end_date, fields):
    """处理一批股票，返回 (success_count, failed_list)。"""
    ctx = multiprocessing.get_context("spawn")
    running = {}   # pid -> (process, code, start_time)
    success = 0
    failed = []
    code_iter = iter(batch_codes)
    done_count = 0
    total = len(batch_codes)

    def start_worker(code):
        p = ctx.Process(
            target=download_one,
            args=(code, output_dir, start_date, end_date, fields),
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
            start_worker(next(code_iter))
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
            # 补充新任务
            try:
                start_worker(next(code_iter))
            except StopIteration:
                pass

        if not to_reap:
            time.sleep(0.3)

    return success, failed


def main():
    start_time = perf_counter()

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
    skipped_prefix = 0
    for code in stocks.证券代码:
        code6 = str(code).strip().zfill(6)
        if code6[0] not in valid_prefixes:
            skipped_prefix += 1
        else:
            filtered_codes.append(code)

    # ---- 过滤已存在的文件 ----
    todo = []
    skipped_exist = 0
    for code in filtered_codes:
        code6 = str(code).strip().zfill(6)
        if SKIP_EXISTING and (OUTPUT_DIR / f"{code6}.csv").exists():
            skipped_exist += 1
        else:
            todo.append(code)

    total = len(stocks)
    print(f"总数:{total}  非3/0/6开头跳过:{skipped_prefix}  已存在跳过:{skipped_exist}  待下载:{len(todo)}")
    print(f"并行:{MAX_WORKERS}进程  每批:{BATCH_SIZE}只  超时:{STOCK_TIMEOUT}s")
    print(f"批次暂停:{RESTART_SLEEP}s\n")

    if not todo:
        print("没有需要下载的股票。")
        return

    # ---- 分批处理 ----
    success = 0
    all_failed = []

    for i in range(0, len(todo), BATCH_SIZE):
        batch = todo[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(todo) + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"--- 第{batch_num}/{total_batches}批, {len(batch)}只 ---")
        s, f = process_batch(
            batch, str(OUTPUT_DIR), START_DATE, END_DATE, FIELDS,
        )
        success += s
        all_failed.extend(f)

        remaining = len(todo) - i - len(batch)
        if remaining > 0:
            print(f"\n--- 批次完成，暂停{RESTART_SLEEP}s ---")
            time.sleep(RESTART_SLEEP)

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
    print(f"成功下载: {success}")
    print(f"已存在跳过: {skipped_exist}")
    print(f"失败/超时: {len(all_failed)}")
    if all_failed:
        print(f"失败详情: {OUTPUT_DIR / 'failed_codes.txt'}")
    print(f"输出目录: {OUTPUT_DIR}")
    if success > 0:
        print(f"平均速度: {duration/success:.1f}s/只")
    print("=" * 50)


if __name__ == "__main__":
    main()
