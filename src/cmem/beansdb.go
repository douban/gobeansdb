package cmem

var (
	DBRL      BeansdbRL
	MemConfig = Config{
		16,
		1024 * 4,
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
}
