package usecase

import (
	"context"

	"github.com/qadam-uz/sentinel/entity"
)

type UseCase interface {
	SendError(ctx context.Context, e entity.ErrorInfo) error
}
