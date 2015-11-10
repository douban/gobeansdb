package cmem

var (
	DBRL BeansdbRL
)

func init() {
	DBRL.ResetAll()
}

func (dbrl *BeansdbRL) ResetAll() {
	dbrl.FlushData.reset()
	dbrl.SetData.reset()
	dbrl.GetData.reset()
}

type BeansdbRL struct {
	GetData   ResourceLimiter
	SetData   ResourceLimiter
	FlushData ResourceLimiter
	//SetBigData   ResourceLimiter
}

type Config struct {
	NumReqToken int
	AllocLimit  int
}

var GConfig = Config{
	16,
	1024 * 4,
}
