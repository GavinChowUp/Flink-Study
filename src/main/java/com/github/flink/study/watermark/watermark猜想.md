📆 [2021/9/1 4:25 PM]

# watermark猜想

## 【单并行度】猜想1

- s:source 源数据有时间字段，1即聚合保留时间字段，2级聚合保留时间字段
- r:source 插入watermark,结构变化，多级聚合
- a:WatermarkDemo1
- r:各级窗口正常触发

## 【单并行度】猜想2

- s:source 源数据有时间字段，1即聚合保留时间字段，2级聚合丢失时间字段
- t:source 插入watermark,结构变化，多级聚合
- a:WatermarkDemo2
- r:各级窗口正常触发

## 【单并行度】猜想3

- s:source 源数据有时间字段，1即聚合丢失时间字段，2级聚合丢失时间字段
- t:source 插入watermark,结构变化，多级聚合
- a:WatermarkDemo2
- r:各级窗口正常触发

## 【多并行度】猜想3 source 插入watermark,无结构map,多级聚合，窗口正常触发

## 【多并行度】猜想4 source 插入watermark,结构map,map后再插入一次watermark,多级聚合，窗口正常触发

