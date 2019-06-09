## 导入eventdb
from eventdb import events, db

## db接口说明
class db.runInfo
```python
#新建对象，连接hbase, tableName - 数据表名称
info = db.runInfo(tableName)

#分页操作, pageIndex - 返回第pageIndex页，从1开始，rows - 每页的行数
#row_start - 由于hbase的性能问题，指定row_start可以加速分页，访问第一页以后，后面的分页信息可以通过前一页最后一行的rowkey赋值给row_start，达到快速分页
rowcount, pages, result = info.page(pageIndex = 1, rows =15, row_start = None)
#rowcount - 总行数
#pages - 总页数
#result - 当前页信息，json字符串

#查看某个run号的具体信息, runID - run号， property - 属性名
result = info.runDetail(runID, property)
#result - runID#property的具体信息，json字符串

#查询， command - 查询字符串
#查询字符串格式例子： runID1, runID2, ... : 1<Npip<=2 && Npim=2 && Ntrack > 3 || BeamVx < 0.56
result, count, time_cost = info.query(command)
#result, 结果字符串
#count, 返回的事例数
#time_cost, 查询时间
```
