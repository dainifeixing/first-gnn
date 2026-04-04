# txflow-risk 开发手册

## 1. 文档目标

这份文档面向需要维护或扩展本仓库的开发者，重点说明：

- 代码结构怎么分
- 命令是怎么接起来的
- 数据对象长什么样
- 哪些地方适合扩展
- 改代码时应优先验证什么

如果你只是日常跑数据，请优先看：

- [user_guide_zh.md](/home/doudougou/codex/txflow-risk/docs/user_guide_zh.md)

如果你要看完整项目定位和全部命令清单，请看：

- [project_manual_zh.md](/home/doudougou/codex/txflow-risk/docs/project_manual_zh.md)

## 2. 开发原则

当前仓库的工程方向很明确：

- 以 CLI 为中心
- 以离线批处理为中心
- 以人工复核闭环为中心
- 以轻量、可解释、可测试为中心

因此扩展时建议遵守：

- 不要跳过人工复核闭环
- 不要把模型输出改成敏感属性或违法性质定性
- 优先补数据稳定性、标签稳定性、阈值运营能力
- 在没有必要前，不要引入重型图框架

## 3. 代码总览

核心代码位于：

```text
src/txflow/
├── analysis.py
├── cli.py
├── excel.py
├── gnn_pipeline.py
├── graph_risk.py
├── ingest.py
├── labels.py
├── model.py
├── models.py
├── reporting.py
└── training.py
```

## 4. 模块职责

### 4.1 `analysis.py`

面向 CSV 规则分析。

主要职责：

- 从 CSV 记录构建简单网络
- 计算节点画像和边画像
- 基于规则生成 `RiskFinding`

典型入口：

- `analyze`

适合扩展的内容：

- 新规则
- 新画像统计
- 规则分数说明

### 4.2 `training.py`

面向 Excel 数据集和监督训练样本。

关键数据类：

- `TrainingSample`
- `TrainingExample`
- `DatasetSplit`

主要职责：

- 从工作簿行中提取交易 ID、时间、金额、对手方、备注等字段
- 用标签 manifest 给交易行打标签
- 构建训练样本与完整训练特征
- 切分训练集和验证集

适合扩展的内容：

- 新字段映射
- 新特征列
- 新的数据切分策略

### 4.3 `model.py`

轻量文本基线模型。

核心对象：

- `BaselineTextClassifier`

主要职责：

- 将 `TrainingExample` 转成离散 token
- 基于朴素贝叶斯思路训练与预测
- 输出基本评估指标

适合扩展的内容：

- 新 token 规则
- 新 bucket 规则
- 更丰富的基线对照实验

### 4.4 `graph_risk.py`

图传播与图模型实现主模块。

这里是后续模型演进的核心位置。

主要职责：

- 图特征与图结构处理
- 图传播基线
- 图模型训练
- 模型保存、加载和评分

适合扩展的内容：

- 更好的图节点/边特征
- 更稳的训练逻辑
- 更丰富的模型元信息
- 更可解释的图级输出

### 4.5 `gnn_pipeline.py`

当前高层运营编排模块。

关键数据类包括：

- `NormalizedTransaction`
- `GraphDatasetSummary`
- `GNNScoreRow`
- `GNNScoreReport`
- `RoundComparisonRow`
- `RoundComparisonReport`
- `RoundReport`
- `RoundBootstrap`

这类对象统一提供 `to_dict()`，便于：

- 写 JSON
- 生成 Markdown
- 做后续聚合分析

这里主要负责：

- 从工作簿构建标准化流水输出
- 汇总训练和评分结果
- 构建多轮比较
- 构建阈值扫描与负载预测
- 生成决策摘要
- 生成便携 demo 包

这是最适合继续扩展“运营层 CLI”的地方。

### 4.6 `labels.py`

标签清单与标签回流模块。

关键对象：

- `LabelManifest`

主要职责：

- manifest 读写
- 构建标签索引
- 人工复核 CSV 转 manifest
- 多份 manifest 合并

适合扩展的内容：

- 更丰富的 manifest 元数据
- 更严格的 manifest 校验
- 标签冲突检测

### 4.7 `cli.py`

所有命令的统一入口。

当前做法是：

1. `build_parser()` 中声明命令与参数
2. 每个命令有独立 `run_*` 函数
3. 在 `main()` 中按 `args.command` 分发

如果你新增命令，最好保持这种模式，不要把复杂业务逻辑直接堆在参数解析里。

## 5. 数据对象设计

### 5.1 `TrainingExample`

这是监督训练和基线训练的重要中间对象。典型字段包括：

- `transaction_id`
- `label`
- `label_status`
- `subject`
- `source_file`
- `amount`
- `timestamp`
- `hour`
- `weekday`
- `is_night`
- `counterparty`
- `direction`
- `channel`
- `remark`
- `raw`

扩展原则：

- 尽量保持字段语义稳定
- 新增字段优先追加，不要破坏现有字段
- 改字段名会连带影响训练、导出、测试

### 5.2 `LabelManifest`

manifest 是整个迭代闭环的核心约束对象。

字段包括：

- `dataset_name`
- `label`
- `subject`
- `status`
- `source_file`
- `transaction_ids`
- `polarity`
- `verified_by`
- `verified_on`
- `notes`

扩展原则：

- 保持 JSON 可读、可人工检查
- 不要写入强依赖本机路径的数据
- 如果加新字段，优先保持向后兼容

### 5.3 `NormalizedTransaction`

这是标准化导出的核心对象。

字段包括：

- `workbook_path`
- `row_index`
- `transaction_id`
- `amount`
- `timestamp`
- `counterparty`
- `direction`
- `channel`
- `remark`
- `label_status`
- `label`
- `subject`
- `review_flags`

这类对象既服务后续训练，也服务人工复核。

## 6. CLI 扩展约定

新增命令时建议遵循：

1. 在 `cli.py` 的 `build_parser()` 里新增 parser
2. 参数命名优先延续现有风格
3. 单独实现 `run_*` 函数
4. 在 `main()` 分发中接入
5. 为 JSON 与 Markdown 输出同时考虑
6. 增加最小 CLI 测试

参数风格建议：

- 路径参数明确命名为 `--json`、`--md`、`--csv`、`--model`
- 多输入文件用 `nargs="+"` 或 `action="append"`
- 约束参数用显式名字，例如 `--max-team-days`

## 7. 新能力优先放哪

### 7.1 如果是数据清洗类能力

优先考虑放在：

- `training.py`
- `gnn_pipeline.py`

### 7.2 如果是模型特征或图结构类能力

优先考虑放在：

- `graph_risk.py`
- `training.py`

### 7.3 如果是阈值、报告、轮次运营类能力

优先考虑放在：

- `gnn_pipeline.py`

### 7.4 如果是标签读写与回流类能力

优先考虑放在：

- `labels.py`

## 8. 当前最值得继续优化的技术点

从现有仓库出发，优先级更高的不是更复杂模型，而是下面这些：

### 8.1 manifest 校验

可以增加：

- 字段完整性检查
- polarity 与 label 的一致性检查
- 重复交易 ID 检查
- manifest 冲突提示

### 8.2 更稳定的字段解析

可以增强：

- 时间解析格式
- 金额字符串容错
- 渠道字段识别
- 对手方字段映射

### 8.3 更明确的评估输出

可以增加：

- `precision@k`
- 候选量分布
- 误报样本摘要
- 标签净增量对比

### 8.4 更清晰的模型元信息

`metadata.json` 可以逐步补充：

- 训练参数
- 标签摘要
- 特征版本
- 数据摘要
- 训练时间

## 9. 测试策略

当前测试在：

- [test_analysis.py](/home/doudougou/codex/txflow-risk/tests/test_analysis.py)
- [test_cli.py](/home/doudougou/codex/txflow-risk/tests/test_cli.py)
- [test_graph_risk.py](/home/doudougou/codex/txflow-risk/tests/test_graph_risk.py)
- [test_ingest.py](/home/doudougou/codex/txflow-risk/tests/test_ingest.py)
- [test_labels.py](/home/doudougou/codex/txflow-risk/tests/test_labels.py)
- [test_model.py](/home/doudougou/codex/txflow-risk/tests/test_model.py)
- [test_training.py](/home/doudougou/codex/txflow-risk/tests/test_training.py)

推荐测试习惯：

- 改 CLI 就补 `test_cli.py`
- 改 manifest 逻辑就补 `test_labels.py`
- 改样本构建就补 `test_training.py`
- 改图模型就补 `test_graph_risk.py`
- 改规则分析就补 `test_analysis.py`

统一运行：

```bash
python3 -m unittest discover -s tests -q
```

## 10. 兼容性和迁移注意点

### 10.1 绝对路径问题

manifest 中的 `source_file` 最好不要依赖本机绝对路径，否则跨机运行会不稳定。

### 10.2 向后兼容旧标签

仓库当前仍兼容部分旧标签语义，但新能力应优先使用通用风险标签：

- `high_risk_transaction`
- `low_risk_transaction`
- `needs_review`
- `high_risk_account`

### 10.3 不要破坏现有输出 schema

很多 CLI 命令会写 JSON/Markdown/CSV。若你改字段名，文档、脚本和测试都要同步调整。

## 11. 推荐的开发节奏

更稳的节奏是：

1. 先做最小可用改动
2. 先补测试
3. 再补 README 或 docs
4. 再考虑是否需要扩展 demo 包

不推荐：

- 一次同时重构 CLI、模型、标签和报告
- 在没有测试的情况下改 manifest 结构
- 直接引入大而重的依赖链

## 12. 新增命令时的最小清单

如果你要新增一个命令，建议至少完成：

- `cli.py` 参数定义
- `run_*` 实现
- JSON 输出
- Markdown 输出
- README 示例
- 一条 CLI 测试

## 13. 如果要做更强的图模型

当前仓库适合渐进式演进，不适合一次性跳到重型异构图平台。

建议顺序：

1. 先稳住现有行级与工作簿级图训练
2. 再加更好的节点特征
3. 再加更好的评估输出
4. 最后才考虑更复杂的图结构和多关系机制

这样更符合当前项目的工程形态。

## 14. 如果要做跨机交付

技术上建议优先补：

1. `requirements.txt`
2. `Dockerfile`
3. `scripts/quickstart.sh`

这三项比继续堆新命令更能提升可交付性。

## 15. 开发者最该配套看的文档

- [README.md](/home/doudougou/codex/txflow-risk/README.md)
- [project_manual_zh.md](/home/doudougou/codex/txflow-risk/docs/project_manual_zh.md)
- [iterative_training.md](/home/doudougou/codex/txflow-risk/docs/iterative_training.md)
- [user_guide_zh.md](/home/doudougou/codex/txflow-risk/docs/user_guide_zh.md)
