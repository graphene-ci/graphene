package def

// Option is something optional a definition carries.
//
// There is one so far and there is a type anyway, because the alternative
// is a constructor that grows a positional argument every time a kind
// learns to say something new — and every call site changing with it.
type Option func(*Definition)

// Reference declares references instances of this kind carry.
func Reference(refs ...Ref) Option {
	return func(d *Definition) { d.refs = append(d.refs, refs...) }
}
