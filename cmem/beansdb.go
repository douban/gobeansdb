package cmem

var (
	DBRL BeansdbRL
)

func init() {
	DBRL.ResetAll()
}

type BeansdbRL struct {
	AllocRL *ResourceLimiter

	GetData   ResourceLimiter
	SetData   ResourceLimiter
	FlushData ResourceLimiter
}

func (dbrl *BeansdbRL) ResetAll() {
	dbrl.FlushData.reset()
	dbrl.SetData.reset()
	dbrl.GetData.reset()
	dbrl.AllocRL = &AllocRL
	AllocRL.reset()
}

func (dbrl *BeansdbRL) IsZero() bool {
	return dbrl.FlushData.IsZero() && dbrl.SetData.IsZero() && dbrl.GetData.IsZero() && dbrl.AllocRL.IsZero()
}
