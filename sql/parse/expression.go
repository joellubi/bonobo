package parse

import (
	"fmt"
	"strings"
)

type SqlExpr interface {
	fmt.Stringer
	SqlNode
}

type SqlIdentifier struct {
	Names []string
	Alias string
}

func (*SqlIdentifier) Children() []SqlNode {
	return nil
}

func (s *SqlIdentifier) String() string {
	return strings.Join(s.Names, ".")
}

type SqlStringLiteral struct {
	Value string
}

// Children implements SqlExpr.
func (*SqlStringLiteral) Children() []SqlNode {
	return nil
}

func (s *SqlStringLiteral) String() string {
	return s.Value
}

type SqlIntLiteral struct {
	Value int
}

// Children implements SqlExpr.
func (*SqlIntLiteral) Children() []SqlNode {
	return nil
}

func (s *SqlIntLiteral) String() string {
	return fmt.Sprint(s.Value)
}

type SqlBinaryExpr struct {
	Left, Right SqlExpr
	Op          string
}

// Children implements SqlExpr.
func (e *SqlBinaryExpr) Children() []SqlNode {
	return []SqlNode{e.Left, e.Right}
}

func (s *SqlBinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", s.Left, s.Op, s.Right)
}

type SqlFunctionExpr struct {
	Name string
	Args []SqlExpr
}

// Children implements SqlExpr.
func (e *SqlFunctionExpr) Children() []SqlNode {
	children := make([]SqlNode, len(e.Args))
	for i, expr := range e.Args {
		children[i] = expr
	}
	return children
}

func (e *SqlFunctionExpr) String() string {
	args := make([]string, len(e.Args))
	for i, arg := range e.Args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("%s(%s)", e.Name, strings.Join(args, ", "))
}

type SqlAlias struct {
	Name  string
	Input SqlExpr
}

func (e *SqlAlias) Children() []SqlNode {
	return []SqlNode{e.Input}
}

func (e *SqlAlias) String() string {
	return fmt.Sprintf("%s AS %s", e.Input.String(), e.Name)
}

var _ SqlExpr = (*SqlIdentifier)(nil)
var _ SqlExpr = (*SqlStringLiteral)(nil)
var _ SqlExpr = (*SqlIntLiteral)(nil)
var _ SqlExpr = (*SqlBinaryExpr)(nil)
var _ SqlExpr = (*SqlFunctionExpr)(nil)
var _ SqlExpr = (*SqlAlias)(nil)
