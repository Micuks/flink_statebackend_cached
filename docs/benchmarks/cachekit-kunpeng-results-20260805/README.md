# CacheKit 鲲鹏 Nexmark 结果索引（2026-08-05）

本目录固化四份统一格式的审计报告：每个 query 的原始 K/s/core（两位小数）、
提升百分比、分组算术平均、有效配置和来源审计。

- [Bloom-only：SST、memtable 及 combined](BLOOM_ONLY.md)
- [FullOpt + SST/memtable Bloom](FULLOPT_BLOOM.md)
- [Access-guided MultiGet](MULTIGET.md)
- [鲲鹏 TSV110/LSE 编译器对照](TSV110_LSE.md)
- [CDC 正确性证据索引](CDC_EVIDENCE.md)
- [公开 paste 地址和 listed 核验](PASTE_URLS.md)

## 统计口径

- 前八：q4、q5、q8、q9、q11、q18、q19、q20。
- 后七：q3、q7、q12、q13、q15、q16、q17。
- ValueState-only：q4、q5、q7、q8、q9、q11、q12、q15、q16、q17、q18。
- 任意 state：ValueState-only 加 q3、q19、q20。
- 分组提升是组内各 query 提升百分比的算术平均。
- 原始 K/s/core 固定显示两位小数；有未舍入原值时使用未舍入值计算。

公开 paste 的最长可选保存期为一周；Git 中的报告为长期副本。
