# GoBeansDB [![Build Status](https://travis-ci.org/douban/gobeansdb.svg?branch=master)](https://travis-ci.org/douban/gobeansdb) 

Yet anonther distributed key-value storage system from Douban Inc.

Any memcached client cache interactive with GobeansDB without any modification.

## Prepare

GoBeansDB use `vgo` manage dependencies, please install [vgo](https://godoc.org/golang.org/x/vgo) first.


## Install

```shell
$ cd ${GOPATH}
$ git clone http://github.com/douban/gobeansdb.git src/github.com/douban/gobeansdb
$ cd src/github.com/douban/gobeansdb
$ make
```

## test

```shell
$ make test  # unit test
$ make pytest  # Integrated test
```

## run

```shell
$ ${GOPATH}/bin/gobeansdb -h
```

## Python Example

```
import libmc


mc = libmc.Client(['localhost:7900'])
mc.set("foo", "bar")
mc.get("foo")

```

## Features

- 协议： memcached。推荐 libmc 客户端（c++ 实现，目前支持 go 和 python，基于 poll 的并发 get_multi/set_multi）
- sharding： 静态 hash 路由，分桶数 16 整数倍
- 索引： 内存索引全部 key，开销约为每个 key 20 字节，主要内存用 c 分配。 get hit 直接定位到文件 record，get miss 不落盘。
- 最终一致性：同步脚本不断比较一个桶三副本 htree（每个桶一个 16 叉的内存 merkle tree）做同步，比较时间戳。
- 文件格式：data 文件可以看成 log（顺序写入）； 每个 record 256 bytes 对齐，有 crc 校验。

## 在 douban 使用方法

```
mc_client --- cache
          |
          --- any beansdb proxy（即将开源） -- beansdb servers 
```

- 用于单个 key 并发写很少的数据
- [gobeansproxy](https://github.com/douban/gobeansproxy) 负责路由， 固定 3 副本
- 两个集群： 分别存储 图片类 （cache 为 CDN）  和 长文本 （cache 为 mc 集群）。
- 支持离线  [dpark](https://github.com/douban/dpark) 读写，读支持本地化。
- 可以视情况保留一段时间完整的写记录。


## 入坑指南

优点

1. 数据文件即 log， 结构简单，数据安全
2. htree 设计 方便数据同步；
3. 全内存索引，访问磁盘次数少，尤其可以准确过滤掉不存在的 key。

缺点/注意

1. 一致性支持较弱，时间戳目前是秒级（受限于数据文件格式）。
2. 全内存索引，有一定内存开销，在启动载入索引略慢（约十几秒到半分钟， 决定于key 数量）。
3. 数据文件格式的 padding 对小 value 有一定浪费。

配置重点

- 分桶数
- htree 高度（决定 merkle tree 部分内存大小）


## 与 [beansdb](https://github.com/douban/beansdb) 关系

- 兼容
  - 数据文件格式不变
  - 仍使用 mc 协议（"@" "?" 开头的特殊 key 用法略不同）
  - htree 的核心设计不变
- 改进
  - go 实现，更稳定，方便配置和维护（http api等）
  - 内存开销变小
  - hint 文件格式和后缀变了
  - 同一个节点不同数据文件不在混部署，以避免坏一个损失整个点数据
