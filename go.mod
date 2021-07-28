module github.com/cloudwego/kitex

go 1.13

require (
	github.com/apache/thrift v0.13.0
	github.com/bytedance/gopkg v0.0.0-20210709064845-3c00f9323f09
	github.com/cespare/xxhash v1.1.0
	github.com/cloudwego/netpoll v0.0.2
	github.com/cloudwego/netpoll-http2 v0.0.4
	github.com/cloudwego/thriftgo v0.0.2-0.20210726073420-0145861fcd04
	github.com/json-iterator/go v1.1.11
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/tools v0.1.0
	google.golang.org/genproto v0.0.0-20210513213006-bf773b8c8384
	google.golang.org/protobuf v1.26.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
)

replace github.com/bytedance/gopkg => github.com/joway/gopkg v0.0.0-20210728100110-f384061d02c7

replace github.com/cloudwego/netpoll => github.com/joway/netpoll v0.0.4-0.20210728085139-3243e5d51949
