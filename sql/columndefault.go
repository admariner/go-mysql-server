package sql

import (
	"fmt"
	"strings"
)

type columnDefaultManager struct {
	AllowedDefaultFunctions FunctionRegistry
	ExpressionTransformUp func(Expression) (Expression, error)
	ParseDefaultString func(*Context, string) (Expression, error)
}
var ColumnDefaultManager = &columnDefaultManager{
	AllowedDefaultFunctions: NewFunctionRegistry(),
}

// StringToColumnDefaultValue takes in a string representing a default value and returns the equivalent Expression.
func StringToColumnDefaultValue(ctx *Context, exprStr string) (*ColumnDefaultValue, error) {
	parsedExpr, err := ColumnDefaultManager.ParseDefaultString(ctx, exprStr)
	if err != nil {
		return nil, err
	}
	// The literal and expression distinction seems to be decided by the presence of parentheses, even for defaults like NOW() vs (NOW())
	// 2+2 would evaluate to a literal under the parentheses check, but will have children due to being an Arithmetic expression, thus we check for children.
	return ExpressionToColumnDefaultValue(ctx, parsedExpr, len(parsedExpr.Children()) == 0 && !strings.HasPrefix(exprStr, "("))
}

// ExpressionToColumnDefaultValue takes in an Expression and returns the equivalent ColumnDefaultValue if the expression
// is valid for a default value. If the expression represents a literal (and not an expression that returns a literal, so "5"
// rather than "(5)"), then the parameter "isLiteral" should be true.
func ExpressionToColumnDefaultValue(ctx *Context, expr Expression, isLiteral bool) (*ColumnDefaultValue, error) {
	expr, err := ColumnDefaultManager.ExpressionTransformUp(expr)
	if err != nil {
		return nil, err
	}
	//TODO: currently (2+2)/2 will, when output as a string, give (2 + 2 / 2), which is clearly wrong
	return NewColumnDefaultValue(expr, isLiteral), nil
}

// MustStringToColumnDefaultValue is StringToColumnDefaultValue except that it panics on errors.
func MustStringToColumnDefaultValue(ctx *Context, exprStr string) *ColumnDefaultValue {
	expr, err := StringToColumnDefaultValue(ctx, exprStr)
	if err != nil {
		panic(err)
	}
	return expr
}

// ColumnDefaultValue is an expression representing the default value of a column. May represent both a default literal
// and a default expression. A nil pointer is a valid instance of this struct, and all method calls will return without error.
type ColumnDefaultValue struct {
	Expression
	literal bool
}

var _ Expression = (*ColumnDefaultValue)(nil)

// NewColumnDefaultValue returns a new ColumnDefaultValue expression.
func NewColumnDefaultValue(expr Expression, representsLiteral bool) *ColumnDefaultValue {
	return &ColumnDefaultValue{
		Expression: expr,
		literal:    representsLiteral,
	}
}

// Children implements sql.Expression
func (e *ColumnDefaultValue) Children() []Expression {
	return []Expression{e.Expression}
}

// Eval implements sql.Expression
func (e *ColumnDefaultValue) Eval(ctx *Context, r Row) (interface{}, error) {
	if e == nil {
		return nil, nil
	}
	return e.Expression.Eval(ctx, r)
}

// IsLiteral returns whether this expression represents a literal default value (otherwise it's an expression default value).
func (e *ColumnDefaultValue) IsLiteral() bool {
	return e.literal
}

// IsNullable implements sql.Expression
func (e *ColumnDefaultValue) IsNullable() bool {
	if e == nil {
		return true
	}
	return e.Expression.IsNullable()
}

// MustEval evaluates the expression and returns the result. The same value is returned when using Eval. All valid
// ColumnDefaultValue expressions will properly evaluate, thus the returned error is unnecessary.
//TODO: verify the above claim
func (e *ColumnDefaultValue) MustEval(ctx *Context) interface{} {
	if e == nil {
		return nil
	}
	result, err := e.Eval(ctx, nil)
	if err != nil {
		panic(err) // should never happen
	}
	return result
}

// Resolved implements sql.Expression
func (e *ColumnDefaultValue) Resolved() bool {
	if e == nil {
		return true
	}
	return e.Expression.Resolved()
}

// String implements sql.Expression
func (e *ColumnDefaultValue) String() string {
	if e == nil {
		return ""
	}
	if e.literal {
		return e.Expression.String()
	} else {
		return fmt.Sprintf("(%s)", e.Expression.String())
	}
}

// Type implements sql.Expression
func (e *ColumnDefaultValue) Type() Type {
	if e == nil {
		return Null
	}
	return e.Expression.Type()
}

// WithChildren implements sql.Expression
func (e *ColumnDefaultValue) WithChildren(children ...Expression) (Expression, error) {
	if len(children) != 1 {
		return nil, ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	return NewColumnDefaultValue(children[0], e.literal), nil
}

