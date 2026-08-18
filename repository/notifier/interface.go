package notifier

import (
	"context"

	"github.com/qadam-uz/sentinel/entity"
)

type Notifier interface {
	Notify(ctx context.Context, e entity.ErrorInfo) error
}
