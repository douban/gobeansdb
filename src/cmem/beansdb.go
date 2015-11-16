package cmem

var (
	DBRL      BeansdbRL
	MemConfig = Config{
		16,
		1024 * 4,
		100000000, // about 100M
		400000,    // about 4M
	}
)

func init() {
	DBRL.ResetAll()
}

func (dbrl *BeansdbRL) ResetAll() {
	dbrl.FlushData.reset()
	dbrl.SetData.reset()
	dbrl.GetData.reset()
	dbrl.AllocRL = &AllocRL
	AllocRL.reset()
}

type BeansdbRL struct {
	AllocRL *ResourceLimiter

	GetData   ResourceLimiter
	SetData   ResourceLimiter
	FlushData ResourceLimiter

	//SetBigData   ResourceLimiter
}

type Config struct {
	NumReqToken int
	AllocLimit  int

	// set with body larger then VictimSize will fail if (flush buffer > FlushBufferHWM)
	FlushBufferHWM int64
	VictimSize     int
}
