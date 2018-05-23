package storage

// IStorage define the common storage interface
type IStorage interface {
	Set(string, interface{}) error
	Get(string) (interface{}, error)
	Delete(string) error
	Save() error
}
