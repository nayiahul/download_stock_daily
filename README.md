# A股日K线行情数据批量下载

数据源：baostock（前复权），股票列表：akshare。
一个代码一个 CSV 文件，输出到 `/Users/nayiahlu/Desktop/stocks/`。

## 三种运行模式

| 命令 | 含义 |
|------|------|
| `python download_a_stock_daily.py` | **普通模式**：从 akshare 取全量股票列表，新下载 + 增量更新 |
| `python download_a_stock_daily.py --sync` | **同步模式**：扫描已有 CSV 文件最新日期，补齐缺失 |
| `python download_a_stock_daily.py --retry-failed` | **重试模式**：重试 `failed_codes.txt` 中的失败项 |

## 可选参数

| 参数 | 适用模式 | 含义 |
|------|----------|------|
| `--end-date YYYY-MM-DD` | 全部 | 指定行情截止日，默认今天。非交易时段应手动指定最新交易日 |

## 关键配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `MAX_WORKERS` | 6 | 并发进程数（重试模式降为 3） |
| `BATCH_SIZE` | 200 | 每批股票数（重试模式降为 100） |
| `STOCK_TIMEOUT` | 30s | 单只超时秒数（重试模式放宽到 45s） |
| `RESTART_SLEEP` | 3s | 批次间暂停秒数 |
| `START_DATE` | 2021-01-01 | 新下载的起始日期 |

## 使用场景

```bash
# 首次下载：全量拉取所有A股数据
python download_a_stock_daily.py

# 日常更新：扫描补齐到最新交易日（非交易时段需指定日期）
python download_a_stock_daily.py --sync --end-date 2026-06-02

# 重试之前失败的股票（自动降并发、放宽超时）
python download_a_stock_daily.py --retry-failed
```

## 输出文件

| 文件 | 说明 |
|------|------|
| `XXXXXX.csv` | 单只股票日K线数据（GBK 编码） |
| `manifest.json` | 说明文件，记录每只股票的最新交易日、记录数、更新时间 |
| `failed_codes.txt` | 失败股票列表，供 `--retry-failed` 使用 |

## 数据字段

date, code, open, high, low, close, preclose, volume, amount, adjustflag, turn, tradestatus, pctChg, peTTM, pbMRQ, psTTM, pcfNcfTTM, isST

## 注意事项

- **增量更新空结果**：增量查询返回空时，重试 3 次（递增延迟 3s~24s），仍空则静默接受（停牌股属正常情况）
- **登录失败**：自动重试 1 次
- **超时处理**：子进程超时直接 SIGKILL，记入失败列表
- **`--end-date` 务必指定最新交易日**：非交易时段 `END_DATE` 默认取当天，会导致全部文件被误判为需更新
