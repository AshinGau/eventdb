# eventdb
面向高能物理的大数据管理系统

## 环境依赖
1. 分析dst文件需要Root环境依赖
2. 解决python依赖：pip install happybase ConfigParser optparse pydoop
3. java依赖通过maven解决，工程根目录运行：mvn clean compile && mvn clean package
4. hadoop&hbase环境依赖：确保jps中有ResourceManager和Thrift服务

## 配置文件
配置文件./config.ini说明hdfs和hbase的运行环境， 本地环境可以使用默认配置。

## rowkey编码
hbase只有字符类型，但是数字的大小顺序和字符顺序不匹配，导致针对属性值的范围搜索失效。为了使数字的大小顺序和字符顺序匹配，对数字进行编码。c代码./c/TyperSer.c可以对整数和浮点数进行编码。./python/lib/TyperSer.so是编译生成的动态链接。

## eventdb存储结构
为了加快生成hbase数据表的速度，将生成的倒排索引存储在hdfs文件中，hbase表结构为rowkey, data:run, data:offset, data:length, data:count。一个run生成一个hdfs数据文件，data:run指向这个文件名，data:offset指向当前rowkey的倒排索引字符串在文件中的偏移量，data:length说明字符串的长度，data:count说明有多少个entryID。所以查询的时候先在hbase中查询，再通过offset等文件信息在hdfs文件中读取倒排索引。![eventdb存储结构](http://img.blog.csdn.net/20171024114336587?watermark/2/text/aHR0cDovL2Jsb2cuY3Nkbi5uZXQvaGVsbG94aXl1ZQ==/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70/gravity/SouthEast)

## hbase数据表
脚本./python/create_table新建hbase数据表。命令格式：./create_talbe 'table-name'

## root分析
脚本./python/run2data分析root文件，同一个run下的root文件建立事例倒排索引。脚本支持多线程并行，命令格式：./run2data 'root-dir-path' 'save-dir-path' 'multiprocessing-num'。确保'root-dir-path'目录下的root文件根据run号分发在对应的子文件下，'save-dir-path'存在，并且有csv和data两个子文件卡。运行完成以后，将csv和data两个子文件卡拷贝到hdfs:///eventdb/'talbe-name'/目录下。

## hbase-bulkload导入
每一个数据表有大量数据，使用hbase-bulkload批量导入。运行命令mvn clean compile && mvn clean package生成jar包，然后运行java -jar ./target/eventdb.jar org.osv.eventdb.CsvInsert 'hdfs-input-path' 'hdfs-output-path' 'hbase-table-name'进行批量导入。

## eventdb查询
查询支持多个run查询，每个run号用逗号分隔。条件支持范围查询和交并运算。命令格式：./evtQuery -v 'hbase-table-name' -r 'runID' -q 'range(property1, min, max) && range(property2, min, max) || range(property3, min, max) ... ' -f 'output-file-name'