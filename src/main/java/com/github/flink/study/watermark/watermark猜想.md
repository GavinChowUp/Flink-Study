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

## 为什么各级窗口还被正常触发？

查看源码：

- Watermarks:
    - are the progress indicators in the data streams. A watermark signifies that no events with a timestamp smaller or equal to the watermark's time will occur after the water. A
      watermark with timestamp T indicates that the stream's event time has progressed to time T.
- Trigger:
    - determines when a pane of a window should be evaluated to emit the results for that part of the window.
    - 所以trigger才是触发窗口的组件,

根据EventTimeTrigger.[onElement源码](https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/windowing/triggers/EventTimeTrigger.java#L37-L47)
, EventTimeTrigger触发只和window， watermark有关，和element没有任何关系

## 【多并行度】猜想3 source 插入watermark,无结构map,多级聚合，窗口正常触发

## 【多并行度】猜想4 source 插入watermark,结构map,map后再插入一次watermark,多级聚合，窗口正常触发

