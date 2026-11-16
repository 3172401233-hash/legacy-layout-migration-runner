# legacy-layout-migration-runner

Python 旧数据布局迁移库，把按年月分桶的旧目录迁移到按业务日期、区域分区的数据湖。
迁移期间旧写入器仍在产生数据，执行器按水位逐批迁移、双读校验，并能持续追赶新到达的旧数据。

## 设计要点

- **批次与水位**：按 `batch_size` 切批，业务时间晚于 `WriteWatermark` 的记录留在旧端，等下一轮追赶。
- **确定性批次 ID**：由源文件、字节偏移和内容哈希生成；同一份数据重跑得到相同批次，已验证批次直接跳过，绝不覆盖新湖中已验证的数据。
- **分区就绪语义**：一个批次可能写入多个分区，一个分区也可能来自多个批次。只有该分区下所有源端批次都通过双读校验，分区才标记 `ready=true`。
- **双读校验**：批次级校验比较旧端转换后记录与新湖落盘记录；`dual_read_check()` 再做全量主键、内容比对。
- **分区冲突保护**：目标分区出现非本迁移所有的文件时抛 `PartitionConflictError`，拒绝覆盖。
- **断点恢复**：状态原子写入 `migration-state.json`。进程崩溃后，新执行器清理临时文件、回滚 `written` 状态的在途批次，再从水位偏移继续；状态文件缺失但存在批次文件时报 `CheckpointError`，防止孤儿数据被静默重放。
- **回滚**：`rollback_migration(force=True)` 只删除迁移器拥有的 `batch-*.jsonl`，外来文件保留并在返回值中列出。

## 快速开始

```python
from datetime import datetime, timezone
from migration import MigrationExecutor, WriteWatermark

def partition_fn(record):
    return {
        "biz_date": record["biz_date"][:10],
        "region": record["region"],
    }

field_mapping = {
    "id": "id",
    "biz_date": "biz_date",
    "region": "region",
    "amount": "amount",
}

executor = MigrationExecutor(
    old_path_mapping={"/data/legacy": "orders"},
    target_root="/data/lake",
    partition_fn=partition_fn,
    field_mapping=field_mapping,
    write_watermark=WriteWatermark(datetime(2024, 1, 5, tzinfo=timezone.utc)),
    event_time_field="biz_date",
    primary_key="id",
    batch_size=1000,
)

while True:
    executor.run()                       # 每轮追赶旧写入器的新数据
    report = executor.dual_read_check()  # 全量双读
    if not executor.pending_records():
        break
```

`old_path_mapping` 支持三种形式：单个旧根目录、根目录列表，或 `{旧根: 源名称}` 映射。
`field_mapping` 既可以是 `{目标字段: 源字段}` 字典，也可以是返回新记录的可调用对象；
任何转换异常都会统一包装为 `FieldMappingError`。

## 状态文件

状态默认落在目标根目录的 `migration-state.json`，包含：

- `offsets`：每个旧文件已迁移的字节偏移；
- `batches`：批次 ID、源偏移、目标文件、状态（`pending`/`written`/`validated`/`failed`/`rolled_back`）；
- `partitions`：分区相对路径、`ready` 标记、已验证与在途批次列表；
- `watermark`：本轮写入水位。

所有写入先写 `.tmp` 再 `os.replace`，保证状态文件和数据文件都是原子落盘。

## 切换前双读步骤

1. 冻结或确认旧写入器水位，构造不晚于切换点的 `WriteWatermark`。
2. 反复执行 `executor.run()` 追赶，直到 `dual_read_check()` 不再报缺失主键，且 `ready_partitions()` 覆盖切换所需分区。
3. 检查 `DualReadReport`：`source_records == target_records`，且 `missing_keys`、`extra_keys`、`mismatched_keys` 均为空。
4. 保持旧布局只读，让下游同时从旧目录和新湖读取同一时间窗口，按主键比对计数与抽样内容。
5. 双读观察期内若旧端仍有新数据，继续运行 `run()` 追赶；已验证批次会被跳过，不会重写。
6. 观察期无差异后，先把读流量按分区灰度切到新湖，再全量切换。

```python
report = executor.dual_read_check()
assert report.ok
assert report.source_records == report.target_records
assert executor.ready_partitions()
```

## 切回旧布局步骤

1. 冻结新湖写入，把读流量切回旧目录；旧布局在迁移期间一直保留，可直接承接读请求。
2. 确认回退窗口内旧写入器仍在写旧路径，必要时让旧写入器补写切换期间只落在新湖的数据。
3. 如需清空迁移产物，执行 `rollback_migration(force=True)`；该方法只删除 `batch-*.jsonl`，返回保留下来的外来文件列表，不会删除非迁移器写入的数据。
4. 重新迁移前，用空的或备份的目标根启动新的 `MigrationExecutor`，从旧端偏移重新构建 `migration-state.json`。
5. 再次执行切换前双读步骤，全部通过后才允许重新切流。

```python
kept_foreign = executor.rollback_migration(force=True)
```

## 测试

```bash
python -m unittest
```

覆盖场景：增量到达与重跑不覆盖、分区冲突、字段映射错误、双读校验失败、中途回滚与断点恢复、全量回滚保留外来文件、状态丢失后的孤儿文件检测。
