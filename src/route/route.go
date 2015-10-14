package route

// for backend
type RouteConfig struct {
	NumBucket int
	Buckets   []int
}

type RouteTable struct {
	NumBucket int // 1, 16, 256, 4096
	Nodes     []string
}
