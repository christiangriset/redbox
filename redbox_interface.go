package redbox

// API establishes the Redbox interface
type API interface {
	Pack(data []byte) error
	Ship() ([]string, error)
}
