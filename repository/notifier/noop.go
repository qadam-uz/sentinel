package notifier

import (
	"context"

	"github.com/qadam-uz/sentinel/entity"
)

type noopNotifier struct {
}

func NewNoopNotifier() *noopNotifier {
	return &noopNotifier{}
}

func (nn *noopNotifier) Notify(ctx context.Context, e entity.ErrorInfo) error {
	// No-op implementation
	return nil
}
