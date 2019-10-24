## 自增ID表结构
```sql
CREATE TABLE `t_autoinc` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `business` varchar(128) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '业务ID',
  `max` bigint(20) unsigned DEFAULT NULL COMMENT '最大ID',
  `step` int(10) unsigned DEFAULT NULL COMMENT '步长',
  `description` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '描述',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_business` (`business`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='分布式自增主键';
```