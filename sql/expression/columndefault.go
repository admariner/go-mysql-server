package expression

import (
	"fmt"
	"github.com/liquidata-inc/go-mysql-server/sql"
)

// This function is to prevent import cycles. Since the sql package cannot reference the expression package, we have to
// push the required function to a global that will be referenced from within the sql package. Otherwise, we'd need to
// duplicate a lot of code or split up sql into smaller packages. If a better way is found, then feel free to replace this.
func init() {
	sql.ColumnDefaultManager.ExpressionTransformUp = func(inputExpr sql.Expression) (sql.Expression, error) {
		return TransformUp(inputExpr, func(e sql.Expression) (sql.Expression, error) {
			switch expr := e.(type) {
			case *UnresolvedFunction:
				funcName := expr.Name()
				builtInFunc, err := sql.ColumnDefaultManager.AllowedDefaultFunctions.Function(funcName)
				if err != nil {
					return nil, err
				}
				resolvedFunc, err := builtInFunc.Call(expr.Arguments...)
				if err != nil {
					return nil, err
				}
				return resolvedFunc, nil
			case *UnresolvedColumn:
				//TODO: handle this
				return nil, fmt.Errorf("columns in default are not yet supported")
			default:
				//TODO: explicitly handle all accepted expressions and reject in the default
				return e, nil
			}
		})
	}
}