# txflow-risk 项目完整中文文档

## 1. 文档目的

本文档面向以下读者：

- 需要快速理解项目能力边界的使用者
- 需要在本机或其他机器上部署项目的工程人员
- 需要基于现有 CLI 做迭代训练与人工复核的操作人员
- 需要继续扩展模型、数据流和报告能力的开发人员

本文档覆盖项目定位、目录结构、数据与标签规范、命令说明、迭代闭环、跨机运行、演示包、测试验证与后续扩展建议。

## 2. 项目定位

`txflow-risk` 是一个面向资金流水的异常交易模式发现工具包，当前定位是：

- 发现高风险交易模式
- 发现高风险账户与关系簇
- 输出适合人工复核的候选结果
- 不自动判定个人敏感属性或违法事实

这意味着本项目的输出属于：

- 风险候选
- 异常模式
- 复核线索
- 决策辅助材料

而不属于：

- 对个人身份属性的自动推断
- 对具体违法行为的自动定性
- 无人工介入的自动执法结论

## 3. 当前项目能力

当前仓库已经具备以下能力：

- CSV 规则分析与 Markdown/JSON 报告输出
- 从 Excel 工作簿导出训练样本和完整数据集
- 标签清单汇总与训练集切分
- 轻量基线模型训练
- 图传播筛查和轻量 GNN 训练、评分
- 评分结果导出为人工复核清单
- 将人工复核结果回流为下一轮训练标签
- 合并多轮标签清单
- 比较多轮训练与复核效果
- 生成单轮报告、阈值扫描、复核负载预测、工作阈值推荐和决策摘要
- 生成不依赖本机路径的便携演示包
- 支持轻量 `annotations.csv/jsonl` 标注输入，减少 manifest 维护成本

当前仓库尚未内建：

- 在线服务化 API
- 分布式训练或分布式推理
- 重型异构图框架
- 复杂实体解析和统一身份图谱
- `requirements.txt`
- `Dockerfile`
- 一键跨机快速启动脚本

## 4. 适用场景与不适用场景

适用场景：

- 对 Excel 或 CSV 导出的交易流水做统一梳理
- 基于人工确认的风险样本训练轻量模型
- 对未标注流水做批量评分
- 组织人工复核队列
- 进行多轮“训练 -> 评分 -> 复核 -> 标签回流”闭环
- 在离线环境下比较不同轮次和不同阈值的运行效果

不适用场景：

- 没有人工标签也没有人工复核流程时，直接让模型自动扩散
- 希望模型直接输出对个人敏感属性的判断
- 希望模型直接给出违法性质结论
- 需要在线实时 API 或毫秒级实时图推理的生产系统

## 5. 总体架构

项目建议按三层理解：

### 5.1 数据清洗与标准化层

作用：

- 读取 Excel 或 CSV 交易导出
- 统一字段命名
- 标准化金额、时间、方向和备注
- 识别缺失、重复、异常等复核标记

主要命令：

- `analyze`
- `export-training`
- `export-dataset`
- `normalize-ledgers`
- `export-ledger-review`
- `export-rule-audit`
- `build-rule-summary`

主要产物：

- 内部全量标准化流水表
- 人工复核最小字段表
- 规则命中审计表
- 训练样本 CSV/JSONL
- 图数据摘要

### 5.2 模型训练与批量跑数层

作用：

- 用人工确认的标签训练轻量基线模型或轻量图模型
- 对目录中的工作簿批量评分
- 输出最值得人工复核的交易和账户线索

主要命令：

- `train-baseline`
- `triage-workbooks`
- `graph-triage`
- `build-graph-dataset`
- `train-gnn`
- `score-gnn`

主要产物：

- `model.pt`
- `metrics.json`
- `metadata.json`
- `scores.json`
- `scores.md`

### 5.3 人工复核与轮次决策层

作用：

- 从评分结果导出复核候选
- 导入人工复核结论
- 合并多轮标签
- 对比轮次指标
- 选择阈值并形成决策摘要

主要命令：

- `export-review-candidates`
- `import-review-labels`
- `merge-label-manifests`
- `compare-round-metrics`
- `make-round-report`
- `bootstrap-round`
- `score-threshold-sweep`
- `review-workload-forecast`
- `select-operating-threshold`
- `round-decision-sheet`

## 6. 目录结构

仓库根目录的关键结构如下：

```text
txflow-risk/
├── README.md
├── pyproject.toml
├── docs/
│   ├── iterative_training.md
│   └── project_manual_zh.md
├── data/
│   └── labels/
├── reports/
├── scripts/
│   ├── export_training_samples.py
│   └── run_iteration.sh
├── src/
│   └── txflow/
│       ├── analysis.py
│       ├── annotations.py
│       ├── cli.py
│       ├── gnn_pipeline.py
│       ├── graph_risk.py
│       ├── ingest.py
│       ├── ledger_ops.py
│       ├── labels.py
│       ├── model.py
│       ├── report.py
│       ├── report_io.py
│       ├── round_ops.py
│       ├── thresholds.py
│       └── training.py
└── tests/
```

各目录职责：

- `src/txflow/`
  核心代码目录，CLI、数据处理、模型训练、报告生成都在这里
- `data/labels/`
  标签清单存放位置
- `docs/`
  项目文档
- `scripts/`
  批处理脚本和辅助脚本
- `reports/`
  示例输出报告
- `tests/`
  单元测试与 CLI 测试

## 7. 关键模块说明

### 7.1 `src/txflow/cli.py`

项目统一命令行入口。所有公开能力都通过该模块暴露。

主要职责：

- 定义子命令
- 解析命令参数
- 调用分析、训练、评分、导入导出和报告逻辑

### 7.2 `src/txflow/annotations.py`

轻量标注输入模块。

主要负责：

- 加载 `annotations.csv` 和 `annotations.jsonl`
- 将 `positive / negative / skip` 转换为训练可消费的内部标签结构
- 将人工复核 CSV 直接导出为轻量标注文件
### 7.3 `src/txflow/training.py`

主要负责：

- 从工作簿构建训练样本
- 导出训练集
- 导出完整特征表
- 构建训练/验证切分

### 7.4 `src/txflow/model.py`

轻量文本基线分类器所在模块。

适合：

- 先跑通监督训练基线
- 做 GNN 之前的低成本对照实验

### 7.5 `src/txflow/graph_risk.py`

图传播与图模型核心模块。

主要负责：

- 图特征处理
- 图风险评分
- 图模型训练与保存
- 图模型加载与推理

### 7.6 `src/txflow/ledger_ops.py`

标准化账本与图数据摘要模块。

主要负责：

- 生成内部使用的全量标准化流水字段
- 生成人工复核使用的最小字段表
- 生成规则命中审计表
- 识别镜像流水与近似镜像候选
- 汇总主体级统计

### 7.7 内部字段与人工复核字段分离

项目现在明确区分两类输出：

- 内部全量字段
  用于建模、规则计算、图特征和溯源分析
- 人工复核字段
  只保留复核必须项，避免把低价值技术字段推给人工

内部全量字段示例：

- `subject_account`
- `tx_id_secondary`
- `counterparty_account`
- `counterparty_name`
- `payer_account`
- `payee_account`
- `merchant_name`
- `flow_family`
- `trade_pattern`
- `is_qr_transfer`
- `is_trade_like`
- `buyer_account`
- `seller_account`

人工复核字段示例：

- `record_id`
- `tx_time`
- `amount`
- `direction`
- `trade_pattern`
- `is_qr_transfer`
- `buyer_account`
- `seller_account`
- `counterparty_name`
- `merchant_name`
- `rule_reason`
- `remark_excerpt`
- `review_label`
- `review_note`

命令对应关系：

- `normalize-ledgers`
  导出内部全量字段
- `export-ledger-review`
  从标准化结果导出人工复核表
- `export-rule-audit`
  从标准化结果导出规则命中审计表，单独查看规则层是否命中过密、过宽或过窄
- `build-rule-summary`
  从标准化结果导出规则命中分布摘要，直接看渠道、交易模式和规则原因分布

### 7.8 规则与 GNN 的关系

项目里预设规则不会直接替代监督标签，而是进入模型特征层。

人工确认信息属于监督或强人工知识：

- 红黄标注
- 主体归并
- 角色标注
- 人工确认镜像关系

规则派生信息属于模型特征：

- `trade_pattern`
- `is_qr_transfer`
- `is_red_packet`
- `is_platform_settlement`
- `is_withdrawal_like`
- `is_failed_or_invalid`
- `is_trade_like`

设计原则：

- 规则用于增强 baseline 和 GNN 的判别能力
- 规则不直接等价于正负标签
- 人工结论仍然是训练真值的核心来源

建议把三层职责明确分开：

- 规则层
  负责字段派生、模式识别、二维码/红包/平台结算等提示，以及 `rule_reason` 解释字段
- GNN 层
  负责学习多特征组合、主体与账户关系传播、规则覆盖不到的边界情况
- 人工标签层
  负责最终监督真值，包括正负标注、主体归并、主体角色和人工复核结论

- 工作簿行级标准化
- 重复交易流水号检测
- 复核标记生成
- 图训练前的数据集摘要输出
- 主体级资金行为汇总输出

### 7.7 `src/txflow/thresholds.py`

阈值扫描与复核负载评估模块。

主要负责：

- 评分阈值扫描
- 已复核样本命中率统计
- 人工复核负载预测
- 工作阈值推荐

### 7.7 `src/txflow/round_ops.py`

轮次报告与演示包模块。

主要负责：

- 单轮报告生成
- 轮次工作区 bootstrap
- 便携演示包生成
- 人工复核统计聚合

### 7.8 `src/txflow/report_io.py`

通用报告写盘辅助模块。

主要负责：

- 统一 JSON 输出写盘
- 统一 Markdown 输出写盘
- 降低多模块重复导出逻辑

### 7.9 `src/txflow/gnn_pipeline.py`

当前项目的 GNN 编排与决策摘要模块。

主要负责：

- GNN 训练与评分结果汇总
- 多轮指标对比
- 决策摘要

### 7.10 `src/txflow/labels.py`

标签清单读写与回流模块。

主要负责：

- 加载标签 manifest
- 构建标签索引
- 从人工复核 CSV 生成正负标签 manifest
- 合并多轮标签 manifest

## 8. 环境要求

最低要求：

- Python `>=3.10`
- 可用的 `pip`
- 能安装 `torch`

当前 `pyproject.toml` 中显式依赖较少，便于在新机器迁移：

```toml
[project]
requires-python = ">=3.10"
dependencies = ["torch"]
```

## 9. 安装与基础验证

### 9.1 本地开发安装

```bash
cd /path/to/txflow-risk
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

### 9.2 基础验证

```bash
python3 -m unittest discover -s tests -q
txflow --help
python3 -m txflow.cli --help
```

当前仓库内置了 CLI 测试和模块级测试；如果以上命令都通过，说明基础安装、核心模块和 CLI 入口都正常。

## 10. 数据输入规范

### 10.1 输入类型

当前项目主要处理两类输入：

- `.csv`
- `.xlsx`

其中：

- `analyze` 主要面向 CSV
- 大部分训练、评分和轮次命令主要面向工作簿目录中的 `.xlsx`

### 10.2 常见字段别名

项目会识别一些常见中文字段别名，例如：

- 时间：`时间`、`交易时间`、`发生时间`
- 金额：`金额`、`交易金额`
- 对手方：`付款方`、`收款方`、`交易对方`
- 方向：`方向`、`收/支`、`交易类型`
- 备注：`备注`、`摘要`、`附言`

若部分字段缺失，解析器不会直接丢弃整条记录，而是尽量保留并标记异常信息。

### 10.3 标准化后推荐保留字段

标准化后建议重点关注以下字段：

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

## 11. 标签设计与管理规范

### 11.1 推荐标签语义

新工作建议统一使用以下通用风险标签：

- `high_risk_transaction`
- `low_risk_transaction`
- `needs_review`
- `high_risk_account`

### 11.2 标签使用原则

- 只有人工确认的结果才应进入训练正负样本
- `needs_review` 只表示待人工处理，不应进入监督训练
- 不建议把模型高分直接当成正样本
- 不建议让伪标签无限扩散
- 交易流水号应原样保留，避免跨轮失配

### 11.3 标签 manifest 结构

标签文件为 JSON manifest，核心字段包括：

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

### 11.4 manifest 示例

```json
{
  "dataset_name": "round_01_reviewed_positive",
  "label": "high_risk_transaction",
  "subject": "reviewed_batch",
  "status": "verified",
  "source_file": "",
  "transaction_ids": ["TX001", "TX002"],
  "polarity": "positive",
  "verified_by": "analyst",
  "verified_on": "2026-04-02",
  "notes": "Generated from manual review results."
}
```

## 12. CLI 命令总览

### 12.1 分析与导出类

#### `analyze`

用途：

- 分析一个 CSV 文件或一个 CSV 目录
- 输出 JSON 或 Markdown 报告

参数：

- `input`：CSV 文件或目录
- `--format`：`markdown` 或 `json`
- `--output` / `-o`：输出文件路径

示例：

```bash
python3 -m txflow.cli analyze data/sample.csv --format markdown
```

仓库已自带 `data/sample.csv`，可直接用这份最小样例验证 CSV 分析流程。

#### `export-training`

用途：

- 从一个 Excel 工作簿中导出已标注训练样本

参数：

- `--xlsx`
- `--labels`
- `--csv`
- `--jsonl`

#### `export-dataset`

用途：

- 导出包含全部行的特征表
- 保留与标签匹配的标注信息

参数：

- `--xlsx`
- `--labels`
- `--csv`
- `--jsonl`

#### `label-catalog`

用途：

- 汇总多个标签清单，输出目录化摘要

参数：

- `--labels`
- `--json`
- `--md`

#### `split-dataset`

用途：

- 将已标注数据切成训练集和验证集

参数：

- `--xlsx`
- `--labels`
- `--train-csv`
- `--train-jsonl`
- `--validation-csv`
- `--validation-jsonl`
- `--ratio`
- `--seed`

#### `train-baseline`

用途：

- 训练轻量基线模型

参数：

- `--xlsx`
- `--labels`
- `--model`
- `--metrics`
- `--split-ratio`
- `--seed`

### 12.2 工作簿筛查与图模型类

#### `triage-workbooks`

用途：

- 对工作簿目录做规则筛查

参数：

- `--root`
- `--labels`
- `--json`
- `--md`

#### `graph-triage`

用途：

- 使用图传播方式对工作簿目录评分

参数：

- `--root`
- `--labels`
- `--json`
- `--md`
- `--top-k`
- `--threshold`
- `--synthetic-warmup`
- `--self-train-rounds`
- `--pseudo-positive-threshold`
- `--pseudo-negative-threshold`
- `--pseudo-max-rows`

说明：

- 这是图模型链路之前的轻量图传播筛查工具
- 适合作为 GNN 训练前的对照或预筛步骤

#### `normalize-ledgers`

用途：

- 将目录中的工作簿统一导出为标准化流水表

参数：

- `--root`
- `--labels`
- `--roles`
- `--owners`
- `--csv`
- `--jsonl`

输出重点：

- 标准化字段
- 标签状态
- 复核标记

#### `build-graph-dataset`

用途：

- 为训练前生成图数据摘要

参数：

- `--root`
- `--labels`
- `--roles`
- `--owners`
- `--json`

摘要内容通常包括：

- 工作簿总数
- 总行数
- 已标注行数
- 正负样本数
- 未标注行数
- 被标记异常的行数
- 角色分布统计

#### `build-owner-summary`

用途：

- 按 `owner_id` 聚合导出主体级资金行为汇总
- 辅助人工识别一对多、多对一、归集后分发等模式

参数：

- `--root`
- `--labels`
- `--roles`
- `--owners`
- `--csv`
- `--json`

摘要内容通常包括：

- 主体命中的流水总数
- 不同交易对手数量
- 收入/支出笔数与占比
- 是否同时存在收款和付款
- 渠道数量
- 覆盖工作簿数量
- 主导角色与角色分布
- 已标注、正样本、负样本、异常标记数量

#### `export-owner-review`

用途：

- 将 `build-owner-summary` 的结果导出成主体人工复核模板
- 支持 `csv/xlsx`
- `xlsx` 默认整行黄色填充，便于直接人工回填

参数：

- `--summary`
- `--csv`
- `--xlsx`

模板内容通常包括：

- `owner_id`
- `owner_name`
- `priority_rank`
- `priority_score`
- `dominant_role`
- `pattern_tags`
- `top_counterparties`
- `review_role`
- `review_confidence`
- `review_note`

#### `import-owner-review`

用途：

- 将主体人工复核结果转换成 `role_annotations.csv/jsonl`
- 回流后可直接被 `normalize-ledgers`、`train-baseline`、`train-gnn`、`score-gnn` 使用

参数：

- `--reviews`
- `--roles-csv`
- `--roles-jsonl`
- `--scene`
- `--evidence`

#### `train-gnn`

用途：

- 在工作簿目录上训练图模型

参数：

- `--root`
- `--labels`
- `--annotations`
- `--roles`
- `--owners`
- `--model`
- `--metrics`
- `--metadata`
- `--hidden-dim`
- `--dropout`
- `--epochs`
- `--seed`
- `--split-ratio`
- `--synthetic-warmup`
- `--self-train-rounds`

建议：

- 第一轮先保持 `self-train-rounds=0`
- 先积累人工确认标签，再考虑伪标签
- 优先使用轻量 `annotations.csv/jsonl`，manifest 更适合归档和兼容旧流程
- 每轮保留单独的 `model.pt` 和 `metrics.json`

#### `score-gnn`

用途：

- 用已训练模型对目录评分

参数：

- `--root`
- `--model`
- `--labels`
- `--annotations`
- `--roles`
- `--owners`
- `--json`
- `--md`
- `--top-k`
- `--include-labeled`

输出关注点：

- 高分交易行
- 高风险工作簿
- 带复核标记的高分记录

#### `export-review-candidates`

用途：

- 从评分结果中导出人工复核清单
- 导出的 CSV 可直接人工填写，不需要先删除默认值

参数：

- `--scores`
- `--csv`
- `--md`
- `--threshold`
- `--limit`

推荐复核状态：

- `confirmed_positive`
- `confirmed_negative`
- `uncertain`

补充说明：

- 导出的 `review_label` 默认留空
- 导出的 `review_options` 会提示可填写值

### 12.3 角色旁路标注

如果你已经能人工判断某些交易对手或资金主体更像 `buyer`、`seller`、`broker`，建议不要改原账单字段，而是单独维护 `role_annotations.csv`。

推荐格式：

```csv
target_type,target_id,scene,role_label,confidence,evidence,note
counterparty,夜间私单,vice,seller,high,manual_review,长期夜间多收款
counterparty,中转账户A,vice,broker,high,manual_review,先收后分账
transaction,420000...,vice,buyer,medium,pattern,单向付款到多个卖家
```

字段建议：

- `target_type`：`transaction` 或 `counterparty`
- `target_id`：交易流水号或交易对手名称/账号
- `scene`：如 `vice`、`laughing_gas`
- `role_label`：`buyer`、`seller`、`broker`、`mixed`、`unknown`
- `confidence`：`high`、`medium`、`low`
- `evidence`：证据来源，如 `manual_review`、`pattern`
- `note`：补充说明

规则：

- 交易级角色会覆盖交易对手级角色
- 角色标签进入标准化输出、图摘要和训练特征
- 推荐优先标 `high` 置信度角色，避免把模糊判断直接喂给模型

### 12.4 主体归并旁路标注

如果同一个卖家、中间商或买家使用多个微信、支付宝、银行卡，建议不要改原账单字段，而是单独维护 `owner_annotations.csv`，把多个号统一映射到同一个 `owner_id`。

推荐格式：

```csv
target_type,target_id,owner_id,owner_name,confidence,evidence,note
counterparty,夜间私单,owner_001,张三,high,manual_review,同一卖家的微信收款号
counterparty,支付宝账号A,owner_001,张三,high,manual_review,同一卖家的支付宝收款号
transaction,420000...,owner_002,李四,medium,case_note,该笔明确属于中间商代收
```

字段建议：

- `target_type`：`transaction` 或 `counterparty`
- `target_id`：交易流水号或交易对手名称/账号
- `owner_id`：统一资金主体 ID
- `owner_name`：人工识别后的主体名称，可为空
- `confidence`：`high`、`medium`、`low`
- `evidence`：证据来源，如 `manual_review`、`case_note`
- `note`：补充说明

规则：

- 交易级主体归并会覆盖交易对手级主体归并
- 你确认属于同一卖家的多个微信/支付宝/银行卡，应使用同一个 `owner_id`
- `owner_id` 会进入标准化输出、图摘要和训练特征
- 推荐优先维护 `high` 置信度归并关系，避免把不稳的归并直接喂给模型

推荐再执行一次：

```bash
python3 -m txflow.cli build-owner-summary \
  --root /path/to/funds \
  --roles out/role_annotations.csv \
  --owners out/owner_annotations.csv \
  --csv out/owner_summary.csv \
  --json out/owner_summary.json
```

这样可以直接按主体复核：

- 哪个 `owner_id` 更像卖家
- 哪个 `owner_id` 更像中间商
- 是否存在明显的“一对多收款”或“归集后分发”

推荐闭环：

```bash
python3 -m txflow.cli build-owner-summary \
  --root /path/to/funds \
  --owners out/owner_annotations.csv \
  --json out/owner_summary.json

python3 -m txflow.cli export-owner-review \
  --summary out/owner_summary.json \
  --xlsx out/owner_review.xlsx

python3 -m txflow.cli import-owner-review \
  --reviews out/owner_review.xlsx \
  --roles-csv out/role_annotations.csv \
  --scene vice
```
### 12.5 标签回流与轮次管理类

#### `import-review-labels`

用途：

- 将人工复核 CSV 转换为下一轮训练标签
- 可直接导出轻量 `annotations.csv/jsonl`
- 也可继续导出正负 manifest 兼容旧流程

参数：

- `--reviews`
- `--dataset-prefix`
- `--positive-json`
- `--negative-json`
- `--annotations-csv`
- `--annotations-jsonl`
- `--subject`
- `--verified-by`
- `--source-file`

结果：

- 生成一份正样本 manifest
- 生成一份负样本 manifest

#### `merge-label-manifests`

用途：

- 合并多轮标签文件
- 分别生成稳定的正例与负例 manifest

参数：

- `--labels`
- `--dataset-prefix`
- `--positive-json`
- `--negative-json`
- `--subject`
- `--verified-by`

#### `compare-round-metrics`

用途：

- 比较多轮训练与复核结果

参数：

- `--round round_name:metrics_json[:review_csv]`
- `--json`
- `--md`

重点指标：

- `best_val_f1`
- `best_val_loss`
- `positive_rate`
- `review_resolution_rate`
- `review_positive_rate`

#### `make-round-report`

用途：

- 汇总一轮的训练、评分、复核和标签信息

参数：

- `--round-name`
- `--metrics`
- `--scores`
- `--reviews`
- `--labels`
- `--json`
- `--md`

#### `bootstrap-round`

用途：

- 为新轮次创建标准目录和命令模板

参数：

- `--round-name`
- `--base-dir`
- `--train-root`
- `--score-root`
- `--label-glob`
- `--json`
- `--md`

#### `score-threshold-sweep`

用途：

- 比较多个评分阈值下的候选量和复核结果

参数：

- `--scores`
- `--reviews`
- `--threshold`
- `--json`
- `--md`

#### `review-workload-forecast`

用途：

- 估算不同阈值下的人工复核负载

参数：

- `--scores`
- `--reviews`
- `--threshold`
- `--reviewers`
- `--daily-capacity`
- `--json`
- `--md`

#### `select-operating-threshold`

用途：

- 在工作负载约束下推荐更合适的工作阈值

参数：

- `--scores`
- `--reviews`
- `--threshold`
- `--reviewers`
- `--daily-capacity`
- `--max-team-days`
- `--min-confirmed-positive-rate`
- `--min-candidates`
- `--json`
- `--md`

#### `round-decision-sheet`

用途：

- 汇总一轮的关键决策信息，形成一页摘要

参数：

- `--round-name`
- `--metrics`
- `--scores`
- `--reviews`
- `--labels`
- `--threshold`
- `--reviewers`
- `--daily-capacity`
- `--max-team-days`
- `--min-confirmed-positive-rate`
- `--min-candidates`
- `--json`
- `--md`

## 13. 常用命令示例

### 13.1 标准化流水

```bash
python3 -m txflow.cli normalize-ledgers \
  --root /path/to/workbooks \
  --labels data/labels/*.json \
  --csv out/normalized.csv \
  --jsonl out/normalized.jsonl
```

### 13.2 构建图数据摘要

```bash
python3 -m txflow.cli build-graph-dataset \
  --root /path/to/workbooks \
  --labels data/labels/*.json \
  --json out/graph_dataset.json
```

### 13.3 训练 GNN

```bash
python3 -m txflow.cli train-gnn \
  --root /path/to/workbooks \
  --labels data/labels/*.json \
  --model out/model.pt \
  --metrics out/metrics.json \
  --metadata out/metadata.json \
  --epochs 120 \
  --split-ratio 0.8 \
  --seed 42
```

### 13.4 评分

```bash
python3 -m txflow.cli score-gnn \
  --root /path/to/unlabeled_workbooks \
  --model out/model.pt \
  --annotations out/annotations.csv \
  --json out/scores.json \
  --md out/scores.md \
  --top-k 200
```

### 13.5 导出复核候选

```bash
python3 -m txflow.cli export-review-candidates \
  --scores out/scores.json \
  --csv out/review.csv \
  --md out/review.md \
  --threshold 0.75 \
  --limit 100
```

### 13.6 导入人工复核标签

```bash
python3 -m txflow.cli import-review-labels \
  --reviews out/review.csv \
  --dataset-prefix round_01_reviewed \
  --annotations-csv out/annotations_round_01.csv
```

如需兼容旧流程，也可以同时导出 manifest：

```bash
python3 -m txflow.cli import-review-labels \
  --reviews out/review.csv \
  --dataset-prefix round_01_reviewed \
  --positive-json data/labels/round_01_reviewed_positive.json \
  --negative-json data/labels/round_01_reviewed_negative.json \
  --annotations-csv out/annotations_round_01.csv \
  --subject reviewed_batch \
  --verified-by analyst
```

### 13.7 合并多轮标签

```bash
python3 -m txflow.cli merge-label-manifests \
  --labels data/labels/*.json \
  --dataset-prefix merged_round \
  --positive-json data/labels/merged_round_positive.json \
  --negative-json data/labels/merged_round_negative.json \
  --subject merged_batch \
  --verified-by analyst
```

### 13.8 比较两轮结果

```bash
python3 -m txflow.cli compare-round-metrics \
  --round round_01:out/round_01/metrics.json:out/round_01/review.csv \
  --round round_02:out/round_02/metrics.json:out/round_02/review.csv \
  --md out/round_comparison.md \
  --json out/round_comparison.json
```

### 13.9 生成单轮报告

```bash
python3 -m txflow.cli make-round-report \
  --round-name round_02 \
  --metrics out/round_02/metrics.json \
  --scores out/round_02/scores.json \
  --reviews out/round_02/review.csv \
  --labels data/labels/round_02_reviewed_positive.json data/labels/round_02_reviewed_negative.json \
  --md out/round_02/report.md \
  --json out/round_02/report.json
```

### 13.10 生成轮次决策摘要

```bash
python3 -m txflow.cli round-decision-sheet \
  --round-name round_03 \
  --metrics out/round_03/metrics.json \
  --scores out/round_03/scores.json \
  --reviews out/round_03/review.csv \
  --labels data/labels/round_03_reviewed_positive.json data/labels/round_03_reviewed_negative.json \
  --threshold 0.60 \
  --threshold 0.70 \
  --threshold 0.80 \
  --reviewers 2 \
  --daily-capacity 40 \
  --max-team-days 1.0 \
  --min-confirmed-positive-rate 0.40 \
  --md out/round_03/decision_sheet.md \
  --json out/round_03/decision_sheet.json
```

## 14. 一轮迭代训练的推荐操作

推荐顺序如下：

1. 使用 `bootstrap-round` 建立标准轮次目录
2. 使用 `normalize-ledgers` 导出标准化流水
3. 使用 `build-graph-dataset` 先看数据规模与标记情况
4. 使用 `train-gnn --annotations` 或 `train-gnn --labels` 训练当前轮模型
5. 使用 `score-gnn` 对待评分工作簿目录跑分
6. 使用 `export-review-candidates` 生成复核清单
7. 人工填写 `review_label`
8. 使用 `import-review-labels --annotations-csv` 直接生成下一轮轻量标签
9. 如需归档或兼容旧流程，再额外导出 manifest 或执行 `merge-label-manifests`
10. 使用 `score-threshold-sweep` 比较不同阈值
11. 使用 `review-workload-forecast` 评估人力成本
12. 使用 `select-operating-threshold` 推荐工作阈值
13. 使用 `round-decision-sheet` 输出最终决策摘要

如果只想跑最短闭环，也可以直接使用脚本：

```bash
bash scripts/run_annotation_loop.sh \
  round_02 \
  /path/to/train_workbooks \
  /path/to/score_workbooks \
  out/annotations.csv \
  0.7
```

这个脚本会依次执行：

- `train-gnn --annotations`
- `score-gnn --annotations`
- `export-review-candidates`

## 15. 轮次输出文件说明

常见输出文件：

- `normalized.csv` / `normalized.jsonl`
  标准化流水导出
- `graph_dataset.json`
  图数据摘要
- `model.pt`
  当前轮模型权重
- `metrics.json`
  当前轮训练指标
- `metadata.json`
  模型元信息
- `scores.json` / `scores.md`
  当前轮评分结果
- `review.csv` / `review.md`
  人工复核清单
- `report.json` / `report.md`
  单轮综合报告
- `threshold_sweep.json` / `threshold_sweep.md`
  阈值扫描报告
- `workload_forecast.json` / `workload_forecast.md`
  复核负载预测
- `operating_threshold.json` / `operating_threshold.md`
  工作阈值推荐
- `decision_sheet.json` / `decision_sheet.md`
  决策摘要

## 16. 评估与运营建议

### 16.1 不要只看一个指标

不建议只盯 `F1`。更适合组合关注：

- `best_val_f1`
- `best_val_loss`
- `positive_rate`
- 候选量
- 人工复核解决率
- 确认正例率
- 每轮新增正负标签数量

### 16.2 推荐阈值选择原则

建议按以下顺序选择阈值：

1. 先满足人工复核负载
2. 再保证最低确认正例率
3. 再保证候选量不至于过小
4. 最后才考虑进一步收紧阈值

### 16.3 迭代训练原则

- 第一轮先以人工确认标签为主
- 先把正负样本质量做稳，再扩大样本量
- 不建议在标签很少时启用强伪标签扩散
- 每轮都保留模型和指标，不覆盖上一轮产物

## 17. `scripts/run_iteration.sh` 的作用

项目内置了一个单轮批处理脚本：

[`scripts/run_iteration.sh`](/home/doudougou/codex/txflow-risk/scripts/run_iteration.sh)

作用：

- 用一组固定参数串起一轮核心流程
- 适合快速跑通“标准化 -> 图摘要 -> 训练 -> 评分 -> 导出复核”

用法：

```bash
scripts/run_iteration.sh <round_name> <workbook_root> <score_root> <label_glob> [threshold]
```

示例：

```bash
scripts/run_iteration.sh round_01 /path/to/train_workbooks /path/to/score_workbooks "data/labels/*.json" 0.75
```

## 18. 跨机器运行建议

如果项目要脱离当前电脑，在其他人的机器上运行，建议至少做到：

- 不依赖本机绝对路径
- 标签 manifest 中 `source_file` 尽量为空或使用相对路径
- 使用 `pip install -e .` 完成安装

更稳妥的交付方式包括：

- 源码仓库 + 虚拟环境
- 后续补 `requirements.txt`
- 后续补 `Dockerfile`
- 后续补 `scripts/quickstart.sh`

## 19. 测试与验证

项目当前使用标准库 `unittest`。

统一测试命令：

```bash
python3 -m unittest discover -s tests -q
```

建议在以下变更后至少跑一次：

- 修改 CLI 参数或命令分发
- 修改标签回流逻辑
- 修改图训练与评分逻辑
- 修改报告生成逻辑
- 修改 demo 包生成逻辑

## 21. 已知限制

当前版本仍有这些限制：

- 图模型仍偏轻量，适合离线批处理，不是重型图平台
- 标签质量对模型影响很大，错误标签会快速污染结果
- 当前更适合交易行级和工作簿级候选发现，不是完整账户图谱系统
- 没有在线服务治理、权限控制和审计能力
- 没有容器化与锁定依赖文件，跨机运行便利性仍可继续加强

## 22. 后续建议

如果继续扩展，推荐优先级如下：

1. 补 `requirements.txt`
2. 补 `Dockerfile`
3. 补 `scripts/quickstart.sh`
4. 增加更多便携样例数据
5. 增加更明确的评估指标看板
6. 在保持轻量前提下，再逐步演进图结构和特征工程

## 23. 文档索引

建议配套阅读：

- [README.md](/home/doudougou/codex/txflow-risk/README.md)
- [iterative_training.md](/home/doudougou/codex/txflow-risk/docs/iterative_training.md)
- [project_manual_zh.md](/home/doudougou/codex/txflow-risk/docs/project_manual_zh.md)
- [user_guide_zh.md](/home/doudougou/codex/txflow-risk/docs/user_guide_zh.md)
- [developer_guide_zh.md](/home/doudougou/codex/txflow-risk/docs/developer_guide_zh.md)

三份文档的分工：

- `README.md`
  快速入门、核心命令、项目定位
- `docs/iterative_training.md`
  一轮一轮的操作流程
- `docs/project_manual_zh.md`
  完整中文项目手册
- `docs/user_guide_zh.md`
  面向日常使用者的操作手册
- `docs/developer_guide_zh.md`
  面向开发者的代码与扩展手册
