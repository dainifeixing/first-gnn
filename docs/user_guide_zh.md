# txflow-risk 使用手册

## 1. 这份文档给谁看

这份文档面向日常使用项目的人，重点回答四类问题：

- 项目能做什么
- 一轮操作怎么跑
- 每个输出文件怎么看
- 下一轮该怎么继续

如果你要看系统全貌，请读：

- [project_manual_zh.md](/home/doudougou/codex/txflow-risk/docs/project_manual_zh.md)

如果你要改代码或扩展命令，请读：

- [developer_guide_zh.md](/home/doudougou/codex/txflow-risk/docs/developer_guide_zh.md)

## 2. 使用前先记住三条

- 这个项目输出的是高风险候选，不是最终结论
- 只有人工确认的结果才应该回流训练
- 每一轮都要保留独立目录，不覆盖上一轮结果

新增一条现在必须记住：

- `holdout_eval` 是冻结评估集，不能回流训练

## 3. 一轮工作流概览

一轮标准流程如下：

1. 准备工作簿目录和人工标注
2. 拆分 `seed_train / holdout_eval / feedback_pool`
3. 训练 GNN
4. 对待评分目录打分
5. 导出账号级扩线候选榜
6. 人工填写复核结果
7. 导入复核结果为新标签
8. 合并标签，准备下一轮
9. 查看冻结评估和阈值/负载分析
10. 打开最小可用可视化页面
11. 形成决策摘要

## 4. 目录建议

建议每轮使用固定目录，例如：

```text
out/
└── round_01/
    ├── seed_train.csv
    ├── holdout_eval.csv
    ├── feedback_pool.csv
    ├── train_annotations.csv
    ├── normalized.csv
    ├── normalized.jsonl
    ├── graph_dataset.json
    ├── model.pt
    ├── metrics.json
    ├── metadata.json
    ├── scores.json
    ├── scores.md
    ├── seller_review.csv
    ├── seller_review.xlsx
    ├── seller_review.md
    ├── frozen_eval.json
    ├── frozen_eval.md
    ├── round_viz.html
    ├── showcase_bundle/
    │   ├── index.html
    │   ├── app.js
    │   ├── styles.css
    │   ├── manifest.js
    │   └── showcase_manifest.json
    ├── review.csv
    ├── review.md
    ├── report.json
    ├── report.md
    ├── threshold_sweep.json
    ├── threshold_sweep.md
    ├── workload_forecast.json
    ├── workload_forecast.md
    ├── operating_threshold.json
    ├── operating_threshold.md
    ├── decision_sheet.json
    └── decision_sheet.md
```

## 5. 开始一轮前要准备什么

你至少需要：

- 一个训练目录，里面放 `.xlsx` 工作簿
- 一个评分目录，里面放待评分 `.xlsx`
- 一份或多份人工标注文件

建议把人工确认后的标签分成：

- `seed_train`
- `holdout_eval`
- `feedback_pool`

## 6. 第一步：创建轮次目录

推荐先运行：

```bash
python3 -m txflow.cli bootstrap-round \
  --round-name round_01 \
  --base-dir out \
  --train-root /path/to/train_workbooks \
  --score-root /path/to/score_workbooks \
  --label-glob "data/labels/*.json" \
  --md out/round_01/bootstrap.md \
  --json out/round_01/bootstrap.json
```

这一步会给你：

- 标准目录
- 标准文件名
- 一轮建议命令模板

如果你已有统一人工标注，建议先拆分：

```bash
python3 -m txflow.cli split-feedback-loop \
  --annotations data/annotations/reviewed.csv \
  --seed-train-csv out/round_01/seed_train.csv \
  --holdout-eval-csv out/round_01/holdout_eval.csv \
  --feedback-pool-csv out/round_01/feedback_pool.csv
```

拆分后：

- `seed_train` 用于首轮训练
- `holdout_eval` 只用于评估
- `feedback_pool` 用于逐轮并入

## 7. 第二步：标准化流水

命令：

```bash
python3 -m txflow.cli normalize-ledgers \
  --root /path/to/train_workbooks \
  --labels data/labels/*.json \
  --csv out/round_01/normalized.csv \
  --jsonl out/round_01/normalized.jsonl
```

你应该重点看这些问题：

- 有没有缺失交易流水号
- 有没有缺失时间
- 有没有缺失对手方
- 有没有明显重复流水
- 夜间活动是否集中
- 标记出来的 `review_flags` 是否合理

## 8. 第三步：看训练前摘要

命令：

```bash
python3 -m txflow.cli build-graph-dataset \
  --root /path/to/train_workbooks \
  --labels data/labels/*.json \
  --json out/round_01/graph_dataset.json
```

你应该先确认：

- 总工作簿数是否符合预期
- 总行数是否正常
- 已标注行数够不够
- 正负样本是否明显失衡
- 标记异常的行是否过多

如果这一步就发现数据量或标签质量有问题，不建议直接训练。

## 9. 第四步：训练 GNN

命令：

```bash
python3 -m txflow.cli run-extension-round \
  --round-name round_01 \
  --train-root /path/to/train_workbooks \
  --score-root /path/to/score_workbooks \
  --seed-annotations out/round_01/seed_train.csv \
  --holdout-annotations out/round_01/holdout_eval.csv \
  --feedback-annotations out/round_01/feedback_pool.csv \
  --output-dir out/round_01
```

训练后优先看：

- `best_val_f1`
- `best_val_loss`
- `positive_rate`
- `train_nodes`
- `val_nodes`
- `frozen_eval.f1`
- `seller_candidate_recovery.recovery_rate`

现在建议额外关注：

- `strong_bridge_candidates`
- `weak_bridge_candidates`
- `avg_bridge_uplift`

使用建议：

- 第一轮先别急着开伪标签
- 正负样本数量太少时，先补标签再训
- 每轮模型单独保存，别覆盖旧模型
- 不要把 `holdout_eval` 再喂回训练

如果你要快速看“账号候选 + buyer bridge + 冻结评估”，直接生成一个最小可用页面：

```bash
python3 -m txflow.cli visualize-round \
  --scores out/round_01/scores.json \
  --report out/round_01/report.json \
  --frozen-eval out/round_01/frozen_eval.json \
  --reviews out/round_01/seller_review.csv \
  --html out/round_01/round_viz.html \
  --title "round_01 visual review"
```

如果你还想把多轮指标直接塞进同一页，不用先手工做 `comparison.json`，可以直接追加：

```bash
python3 -m txflow.cli visualize-round \
  --scores out/round_10/scores.json \
  --report out/round_10/report.json \
  --frozen-eval out/round_10/frozen_eval.json \
  --compare-round round_08:out/round_08/metrics.json \
  --compare-round round_09:out/round_09/metrics.json \
  --compare-round round_10:out/round_10/metrics.json \
  --html out/round_10/round_viz.html
```

这页最适合三件事：

- 先看 `seller candidates` 谁最值得查
- 再看 `buyer -> seller` 桥接关系是否成立
- 最后看 `frozen eval` 是否真的变好

现在 `round_viz.html` 里也补了一个轻量版协作面板，会显示：

- agent 分工
- 共识与冲突
- 最终采纳改动
- 协作时间线

现在推荐的看法顺序是：

- 先点 `Strong Bridge`
- 优先复核 `strong_bridge_unknown_seller`
- 第一优先层看完，再切到 `Weak Bridge`

如果你要把一轮结果给其他人展示，不要直接把内部复核页丢出去，更建议额外导出静态展示包：

```bash
python3 -m txflow.cli build-showcase-bundle \
  --scores out/round_01/scores.json \
  --report out/round_01/report.json \
  --frozen-eval out/round_01/frozen_eval.json \
  --reviews out/round_01/seller_review.csv \
  --output-dir out/round_01/showcase_bundle \
  --title "round_01 showcase"
```

这套展示包的用途是：

- 给非开发人员或非建模人员看结果
- 一页展示 `overview / seller candidates / candidate detail / bridge graph / frozen eval`
- 单独保留 `showcase_manifest.json`，后面可以继续接 React 或正式 Web 前端

这套展示包现在有两种查看模式：

- `Presentation`
  - 默认模式
  - 自动对 seller/buyer/对手方/复核备注做脱敏
  - 适合给外部汇报或非研判人员看
- `Internal`
  - 展示完整字段
  - 适合内部研判或复核会商

切换方式有两种：

- 直接点页面右上角 `View Mode`
- 或在地址后面加：
  - `?view=public`
  - `?view=internal`

如果你要在一次汇报里切多轮结果，不要来回换目录，可以把多轮 manifest 合成一个 bundle：

```bash
python3 -m txflow.cli build-showcase-bundle \
  --scores out/round_10/scores.json \
  --report out/round_10/report.json \
  --frozen-eval out/round_10/frozen_eval.json \
  --output-dir out/showcase_multi \
  --extra-showcase round_08:out/round_08/showcase_manifest.json \
  --extra-showcase round_09:out/round_09/showcase_manifest.json \
  --title "multi-round showcase"
```

生成后页面顶部会出现 round 切换按钮，可以直接在一个展示页里切不同轮次。

现在首页还会额外给你两块更适合汇报的内容：

- `Executive Summary`
  - 概括这一轮的扩线价值
  - 如果有上一轮，还会直接给出相对变化
- `Agent Collaboration`
  - 把多条工作线如何分工、形成共识、解决冲突、最终采纳哪些改动放到一个面板里
  - 适合向他人解释“这不是单个模型自己打分”，而是多条分析线协同收敛出的结果
- `Collaboration Timeline`
  - 把规则、结构、实验、评估几条线按步骤串成一条时间线
  - 适合汇报时说明“这轮方案是怎么一步步定下来的”
  - 现在 timeline 里的步骤也可以直接点开对应证据面板，不只是静态说明
- `Presentation Queue`
  - 给出这次汇报最值得先讲的 seller 候选
- `Presenter Notes`
  - 针对当前候选自动生成一段可直接照着讲的说明
  - 支持 `Export Briefing .md` 一键导出当前汇报稿
  - 也支持 `Export Briefing .html` 导出单页 HTML 汇报材料，适合打印或单独转发

如果你要直接投屏汇报，可以打开：

- `Stage Mode`
  - 会切到更干净的展示布局
  - 浏览器允许的话会直接进入全屏

快捷键：

- `Left / Right`
  - 切上一条 / 下一条候选
- `Space`
  - 开始或停止自动播放
- `F`
  - 进入或退出 `Stage Mode`

如果你要把当前候选单独导出成一页汇报稿，推荐这样用：

- 先在展示页里选中当前 seller 候选
- 再点 `Export Briefing .md` 或 `Export Briefing .html`
- `md` 适合二次编辑
- `html` 适合直接发送、打印或归档
- 单候选 `html` 现在还会带“为什么这个候选被多线共同推到前面”的协作摘要
- 单候选 `md` 现在也会带同样的协作摘要，和 `html` 口径保持一致

如果你导出的是 `Export Executive .html`，现在里面还会带上：

- 多条 agent 工作线
- 共识与冲突
- 最终采纳改动
- 协作时间线

如果你暂时还要手工使用旧命令，至少这样写：

```bash
python3 -m txflow.cli train-gnn \
  --root /path/to/train_workbooks \
  --annotations out/round_01/seed_train.csv \
  --exclude-annotations out/round_01/holdout_eval.csv \
  --model out/round_01/model.pt \
  --metrics out/round_01/metrics.json
```

## 10. 第五步：评分

命令：

```bash
python3 -m txflow.cli score-gnn \
  --root /path/to/score_workbooks \
  --model out/round_01/model.pt \
  --labels data/labels/*.json \
  --json out/round_01/scores.json \
  --md out/round_01/scores.md \
  --top-k 200
```

你需要重点看：

- Top-k 高分记录
- `seller_candidates`
- 哪些工作簿的最高分明显更高
- 带 `review_flags` 的记录是否同时高分
- 已标注数据是否被高分重复打出来
- 哪些 seller 账号被多个 buyer 共同指向

## 11. 第六步：导出人工复核清单

命令：

```bash
python3 -m txflow.cli export-review-candidates \
  --scores out/round_01/scores.json \
  --csv out/round_01/seller_review.csv \
  --xlsx out/round_01/seller_review.xlsx \
  --md out/round_01/seller_review.md \
  --threshold 0.75 \
  --limit 100 \
  --entity-type seller_account \
  --tier strong_bridge_unknown_seller
```

这也是当前推荐的默认人工复核入口。`run-extension-round` 默认 seller review 导出就会按这个 tier 过滤。

推荐复核状态只用三种：

- `confirmed_positive`
- `confirmed_negative`
- `uncertain`

注意：

- 只有前两种才能进下一轮训练
- `uncertain` 不要直接回流训练
- 优先复核账号级候选，而不是先盯着单条高分交易
- 优先复核 `strong_bridge_unknown_seller`
- `weak_bridge_high_score` 放在第二批次

现在 seller 候选里最重要的字段是：

- `candidate_tier`
- `bridge_uplift`
- `bridge_buyers`
- `bridge_support_ratio`
- `known_buyer_support`

## 12. 第七步：人工复核怎么填

你需要打开：

- `out/round_01/seller_review.csv`
- 或 `out/round_01/seller_review.xlsx`

然后给每个候选账号填写 `review_label`。

建议你同时补充：

- 简短复核备注
- 是否需要二次复查
- 是否需要保留样本做误报分析
- 如已知，补上 `extension_role`
- 如已知，补上 `anchor_subject`

## 13. 第八步：导入复核结果为新标签

命令：

```bash
python3 -m txflow.cli import-review-labels \
  --reviews out/round_01/review.csv \
  --dataset-prefix round_01_reviewed \
  --positive-json data/labels/round_01_reviewed_positive.json \
  --negative-json data/labels/round_01_reviewed_negative.json \
  --subject reviewed_batch \
  --verified-by analyst
```

这一步会生成：

- 一份新的正例 manifest
- 一份新的负例 manifest

如果你同时导出了 `annotations.csv/jsonl`，建议优先保留这些字段：

- `transaction_id`
- `label`
- `extension_role`
- `anchor_subject`
- `note`

## 14. 第九步：合并标签

命令：

```bash
python3 -m txflow.cli merge-label-manifests \
  --labels data/labels/*.json \
  --dataset-prefix round_02_merged \
  --positive-json data/labels/round_02_merged_positive.json \
  --negative-json data/labels/round_02_merged_negative.json \
  --subject merged_batch \
  --verified-by analyst
```

这一步的意义是：

- 去重
- 合并多轮标签
- 为下一轮训练生成稳定输入

## 15. 第十步：做阈值分析

### 15.1 阈值扫描

```bash
python3 -m txflow.cli score-threshold-sweep \
  --scores out/round_01/scores.json \
  --reviews out/round_01/review.csv \
  --threshold 0.60 \
  --threshold 0.70 \
  --threshold 0.80 \
  --md out/round_01/threshold_sweep.md \
  --json out/round_01/threshold_sweep.json
```

看什么：

- 候选量
- 已复核量
- 确认正例率
- 阈值变化是否过于敏感

### 15.2 负载预测

```bash
python3 -m txflow.cli review-workload-forecast \
  --scores out/round_01/scores.json \
  --reviews out/round_01/review.csv \
  --threshold 0.60 \
  --threshold 0.70 \
  --threshold 0.80 \
  --reviewers 2 \
  --daily-capacity 40 \
  --md out/round_01/workload_forecast.md \
  --json out/round_01/workload_forecast.json
```

看什么：

- 每个阈值下候选量
- 团队需要多少天看完
- 是否超过可接受的人力负担

### 15.3 工作阈值推荐

```bash
python3 -m txflow.cli select-operating-threshold \
  --scores out/round_01/scores.json \
  --reviews out/round_01/review.csv \
  --threshold 0.60 \
  --threshold 0.70 \
  --threshold 0.80 \
  --reviewers 2 \
  --daily-capacity 40 \
  --max-team-days 1.0 \
  --min-confirmed-positive-rate 0.40 \
  --min-candidates 2 \
  --md out/round_01/operating_threshold.md \
  --json out/round_01/operating_threshold.json
```

## 16. 第十一步：生成轮次决策摘要

命令：

```bash
python3 -m txflow.cli round-decision-sheet \
  --round-name round_01 \
  --metrics out/round_01/metrics.json \
  --scores out/round_01/scores.json \
  --reviews out/round_01/review.csv \
  --labels data/labels/round_01_reviewed_positive.json data/labels/round_01_reviewed_negative.json \
  --threshold 0.60 \
  --threshold 0.70 \
  --threshold 0.80 \
  --reviewers 2 \
  --daily-capacity 40 \
  --max-team-days 1.0 \
  --min-confirmed-positive-rate 0.40 \
  --md out/round_01/decision_sheet.md \
  --json out/round_01/decision_sheet.json
```

这份摘要适合用来回答：

- 这一轮有没有比上一轮更好
- 当前阈值该选多少
- 团队是否看得过来
- 下一轮是补标签还是继续扩量

## 17. 最常见的判断错误

### 17.1 把高分直接当正例

这是最常见错误。高分只是候选，不是确认结果。

### 17.2 只积累正样本，不积累负样本

这样会让模型越来越偏，误报难以下降。

### 17.3 每轮都覆盖老模型

这样你会丢掉对照基线，无法判断迭代是否真的变好。

### 17.4 只看 F1，不看人工复核通过率

项目最终是给人审用的，所以候选质量和复核负担更重要。

## 18. 快速命令清单

如果你只想记最核心的几条，记下面这些：

```bash
python3 -m txflow.cli normalize-ledgers ...
python3 -m txflow.cli train-gnn ...
python3 -m txflow.cli score-gnn ...
python3 -m txflow.cli export-review-candidates ...
python3 -m txflow.cli import-review-labels ...
python3 -m txflow.cli merge-label-manifests ...
python3 -m txflow.cli round-decision-sheet ...
```

## 19. 一条命令跑一轮

项目内置脚本：

[`run_iteration.sh`](/home/doudougou/codex/txflow-risk/scripts/run_iteration.sh)

用法：

```bash
scripts/run_iteration.sh round_01 /path/to/train_workbooks /path/to/score_workbooks "data/labels/*.json" 0.75
```

这个脚本会串起：

- 标准化
- 图摘要
- 训练
- 评分
- 复核清单导出

## 20. 使用者最该配套看的文档

- [README.md](/home/doudougou/codex/txflow-risk/README.md)
- [iterative_training.md](/home/doudougou/codex/txflow-risk/docs/iterative_training.md)
- [project_manual_zh.md](/home/doudougou/codex/txflow-risk/docs/project_manual_zh.md)
