binlog2sql
========================

Parse MySQL binlog to SQL you want.

从MySQL binlog生成各类SQL。根据不同设置，你可以得到原始SQL、回滚SQL、去除主键的INSERT SQL等。

用途
===========

* 数据回滚
* 主从切换后数据不一致的修复
* 审计
* 从binlog生成的标准SQL，带来的衍生功能


安装
==============

```
git clone https://github.com/danfengcao/binlog2sql.git
pip install -r requirements.txt
```

使用
=========

* 解析出标准SQL

```bash
python binlog2sql.py --host='127.0.0.1' -P3306 -uadmin -p'admin' --start-file='mysql-bin.000002'
```

* 解析出回滚SQL

```bash
python binlog2sql.py --flashback --host='127.0.0.1' -P3306 -uadmin -p'admin' --start-file='mysql-bin.000002' --start-pos=1240 --end-file='mysql-bin.000004' --end-pos=9620
```
* 主从切换后数据不一致的修复

```bash
1. 取出老master额外的事务；
    python binlog2sql.py --popPk --host='10.1.1.1' -P3306 -uadmin -p'admin' --start-file='mysql-bin.000002' --start-pos=1240 > oldMaster.sql
2. 回滚老master额外的事务；
    python binlog2sql.py --flashback --host='10.1.1.1' -P3306 -uadmin -p'admin' --start-file='mysql-bin.000002' --start-pos=1240 | mysql -h10.1.1.1 -P3306 -uadmin -p'admin'
3. 在新master重新执行事务；
    mysql -h10.1.1.2 -P3306 -uadmin -p'admin' < oldMaster.sql
```


注意事项
=========================

#### MySQL server必须设置以下参数:

    [mysqld]
    server-id		 = 1
    log_bin		 = /var/log/mysql/mysql-bin.log
    expire_logs_days = 10
    max_binlog_size  = 1000M
    binlog-format    = row

#### 如果使用flashback模式，一次性处理的binlog不宜过大，不能超过内存大小。恢复操作有风险，请在备库操作或是在经验丰富的同学指导下进行。

#### flashback模式只用于恢复DML语句引起的误操作，不支持DDL语句。






