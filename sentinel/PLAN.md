# Smart Money Scanner - 开发计划

## 项目概览

基于币安永续合约的资金行为分析系统，识别资金异动，辅助日内交易和1~4周波段埋伏。

---

## 模块划分

### 模块1: 数据采集层 (data/)
- `collector.py` ← **已完成**
  - Binance REST API 封装
  - 获取全部USDT合约、K线、OI、Funding、Trades、Depth
- `stream.py` (阶段2)
  - 实时WebSocket流
  - 增量更新而非全量轮询

### 模块2: 指标计算引擎 (engine/)
- `indicators.py`
  - OI变化率、CVD、成交量增长、Funding趋势
  - ATR、VWAP、VPVR
- `market.py`
  - BTC/ETH/TOTAL3/USDT.D市场环境
  - 板块轮动分析

### 模块3: 行为模式引擎 (patterns/)
- `detector.py`
  - 10种行为模式识别
  - Accumulation Score (0-100) 评分
  - 吸筹效率指标
- `filters.py`
  - 解锁风险、新币风险、Funding过热、多空比极端

### 模块4: AI分析层 (ai/)
- `analyzer.py`
  - DeepSeek V4 Pro 集成
  - 解释/总结/推理/风险提示
  - 不参与计算

### 模块5: 数据库 (db/)
- `store.py`
  - 市场快照、告警记录、后验结果
  - SQLite

### 模块6: 告警推送 (alerts/)
- `formatter.py`
  - 告警格式模板
  - 多级告警分级
- `pusher.py`
  - iLink微信推送
  - 面板API推送

### 模块7: 回测系统 (backtest/)
- `backtest.py`
  - 历史数据回放
  - 评分有效性验证

### 模块8: 主调度器 (root)
- `main.py`
  - 定时扫描循环
  - 模块编排
- `top10.py`
  - Top10榜单生成
  - 板块轮动榜

---

## 开发阶段

### 阶段0: 基础架构 ✅
- [x] 项目目录结构
- [x] 配置文件 (settings.py / constants.py)
- [x] 数据采集器 (collector.py)
- [x] PROJECT.md 设计文档

### 阶段1: 核心指标引擎
- [ ] indicators.py - OI变化率
- [ ] indicators.py - CVD计算
- [ ] indicators.py - 成交量增长
- [ ] indicators.py - Funding趋势
- [ ] indicators.py - ATR/ATR变化
- [ ] indicators.py - VWAP/VWAP偏离
- [ ] indicators.py - VPVR成交量分布

### 阶段2: 行为模式引擎
- [ ] detector.py - 10种模式识别
- [ ] detector.py - Accumulation Score
- [ ] detector.py - 吸筹效率
- [ ] filters.py - 风险过滤器

### 阶段3: 市场环境 + 板块轮动
- [ ] market.py - BTC/ETH/TOTAL3/USDT.D
- [ ] market.py - 板块资金流入榜

### 阶段4: AI分析层
- [ ] analyzer.py - DeepSeek集成
- [ ] analyzer.py - 告警解释生成

### 阶段5: 数据库 + 告警推送
- [ ] store.py - SQLite存储
- [ ] formatter.py - 告警模板
- [ ] pusher.py - 推送集成

### 阶段6: 主调度器 + 榜单
- [ ] main.py - 完整扫描循环
- [ ] top10.py - 榜单生成
- [ ] 定时任务配置

### 阶段7: 回测系统
- [ ] backtest.py - 数据回放
- [ ] backtest.py - 有效性验证

### 阶段8: 第二阶段升级
- [ ] 样本收集（10000+告警）
- [ ] XGBoost/LightGBM/CatBoost训练
- [ ] 历史相似案例统计

---

## 当前进度

**已完成：** 阶段0 ✅（目录结构 + 配置 + 数据采集器）
**进行中：** 阶段1（核心指标引擎）
**下一步：** 完成 indicators.py
