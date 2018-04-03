PATH := ./redis-git/src:${PATH}

# CLUSTER REDIS NODES
define NODE1_CONF
daemonize yes
port 7000
cluster-node-timeout 5000
pidfile /tmp/redis_cluster_node1.pid
logfile /tmp/redis_cluster_node1.log
save ""
appendonly no
cluster-enabled yes
cluster-config-file /tmp/redis_cluster_node1.conf
endef

define NODE2_CONF
daemonize yes
port 7001
cluster-node-timeout 5000
pidfile /tmp/redis_cluster_node2.pid
logfile /tmp/redis_cluster_node2.log
save ""
appendonly no
cluster-enabled yes
cluster-config-file /tmp/redis_cluster_node2.conf
endef

define NODE3_CONF
daemonize yes
port 7002
cluster-node-timeout 5000
pidfile /tmp/redis_cluster_node3.pid
logfile /tmp/redis_cluster_node3.log
save ""
appendonly no
cluster-enabled yes
cluster-config-file /tmp/redis_cluster_node3.conf
endef

define NODE4_CONF
daemonize yes
port 7003
cluster-node-timeout 5000
pidfile /tmp/redis_cluster_node4.pid
logfile /tmp/redis_cluster_node4.log
save ""
appendonly no
cluster-enabled yes
cluster-config-file /tmp/redis_cluster_node4.conf
endef

define NODE5_CONF
daemonize yes
port 7004
cluster-node-timeout 5000
pidfile /tmp/redis_cluster_node5.pid
logfile /tmp/redis_cluster_node5.log
save ""
appendonly no
cluster-enabled yes
cluster-config-file /tmp/redis_cluster_node5.conf
endef

define NODE6_CONF
daemonize yes
port 7005
cluster-node-timeout 5000
pidfile /tmp/redis_cluster_node6.pid
logfile /tmp/redis_cluster_node6.log
save ""
appendonly no
cluster-enabled yes
cluster-config-file /tmp/redis_cluster_node6.conf
endef

export NODE1_CONF
export NODE2_CONF
export NODE3_CONF
export NODE4_CONF
export NODE5_CONF
export NODE6_CONF

start-cluster: install-cluster
	echo "$$NODE1_CONF" | redis-server -
	echo "$$NODE2_CONF" | redis-server -
	echo "$$NODE3_CONF" | redis-server -
	echo "$$NODE4_CONF" | redis-server -
	echo "$$NODE5_CONF" | redis-server -
	echo "$$NODE6_CONF" | redis-server -
	echo 'start six redis node'
	sleep 5
	echo "yes" | ruby redis-git/src/redis-trib.rb create --replicas 1 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005
	sleep 5
	echo 'join node together'

install-cluster:
	[ ! -e redis-git ] && git clone --depth=2 --branch=3.2 https://github.com/antirez/redis.git redis-git || true
	gem install redis

clean-cluster:
	rm -vf /tmp/redis_cluster_node*.conf 2>/dev/null
	rm -f /tmp/redis_cluster_node1.conf
	rm dump.rdb appendonly.aof - 2>/dev/null

stop-cluster:
	kill `cat /tmp/redis_cluster_node1.pid` || true
	kill `cat /tmp/redis_cluster_node2.pid` || true
	kill `cat /tmp/redis_cluster_node3.pid` || true
	kill `cat /tmp/redis_cluster_node4.pid` || true
	kill `cat /tmp/redis_cluster_node5.pid` || true
	kill `cat /tmp/redis_cluster_node6.pid` || true
	make clean-cluster

all:
	@grep -Ee '^[a-z].*:' Makefile | cut -d: -f1 | grep -vF all

clean: clean-cluster
	rm -rf build/ dist/

release: clean
	# Check if latest tag is the current head we're releasing
	echo "Latest tag = $$(git tag | sort -nr | head -n1)"
	echo "HEAD SHA       = $$(git sha head)"
	echo "Latest tag SHA = $$(git tag | sort -nr | head -n1 | xargs git sha)"
	@test "$$(git sha head)" = "$$(git tag | sort -nr | head -n1 | xargs git sha)"
	make force_release

force_release: clean
	git push --tags
	python setup.py sdist bdist_wheel
	twine upload dist/*

test:
	RUN_SLOW_TESTS_TOO=1 py.test --cov rq --durations=5