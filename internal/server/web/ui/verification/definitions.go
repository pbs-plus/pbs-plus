package verification

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

// Definitions renders the data-verification job panel and its edit windows.
func Definitions() []js.Value {
	return []js.Value{
		verificationPanel,
		verificationHelpers,
		verificationFilterEditWindow,
		verificationOptionsPanel,
		verificationSpotCheckPanel,
		verificationJobEdit,
	}
}
