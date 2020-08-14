package sql_test

import (
	. "github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/expression/function"
	_ "github.com/liquidata-inc/go-mysql-server/sql/parse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestStringToExpression(t *testing.T) {
	tests := []struct{
		exprStr string
		expectedExpr Expression
	}{
		{ //TODO: expand tests, more types
			"2",
			NewColumnDefaultValue(
				expression.NewLiteral(int8(2), Int8),
				true,
			),
		},
		{
			"(2)",
			NewColumnDefaultValue(
				expression.NewLiteral(int8(2), Int8),
				false,
			),
		},
		{
			"(RAND() + 5)",
			NewColumnDefaultValue(
				expression.NewArithmetic(
					must(function.NewRand),
					expression.NewLiteral(int8(5), Int8),
					"+",
				),
				false,
			),
		},
		{
			"(GREATEST(RAND(), RAND()))",
			NewColumnDefaultValue(
				must(function.NewGreatest,
					must(function.NewRand),
					must(function.NewRand),
				),
				false,
			),
		},
	}

	for _, test := range tests {
		t.Run(test.exprStr, func(t *testing.T) {
			res, err := StringToColumnDefaultValue(NewEmptyContext(), test.exprStr)
			if test.expectedExpr == nil {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedExpr, res)
			}
		})
	}
}

// must executes functions of the form "func(args...) (sql.Expression, error)" and panics on errors
func must(f interface{}, args ...interface{}) Expression {
	fType := reflect.TypeOf(f)
	if fType.Kind() != reflect.Func ||
		fType.NumOut() != 2 ||
		!fType.Out(0).AssignableTo(reflect.TypeOf((*Expression)(nil)).Elem()) ||
		!fType.Out(1).AssignableTo(reflect.TypeOf((*error)(nil)).Elem()) {
		panic("invalid function given")
	}
	// we let reflection ensure that the arguments match
	argVals := make([]reflect.Value, len(args))
	for i, arg := range args {
		argVals[i] = reflect.ValueOf(arg)
	}
	fVal := reflect.ValueOf(f)
	out := fVal.Call(argVals)
	err, _ := out[1].Interface().(error)
	if err != nil {
		panic("must err is nil")
	}
	return out[0].Interface().(Expression)
}

