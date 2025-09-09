mkdir -p data/node1 data/node2 data/node3  
  
# 启动节点（按顺序）  
CONFIG=node1.conf ./target/myredis-linux &  
CONFIG=node2.conf ./target/myredis-linux &  
CONFIG=node3.conf ./target/myredis-linux &