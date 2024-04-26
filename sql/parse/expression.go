package parse

import "fmt"

type SqlExpr interface {
	fmt.Stringer
}

type SqlIdentifier struct {
	ID string
}

func (s *SqlIdentifier) String() string {
	return s.ID
}

type SqlStringLiteral struct {
	Value string
}

func (s *SqlStringLiteral) String() string {
	return s.Value
}

type SqlIntLiteral struct {
	Value int
}

func (s *SqlIntLiteral) String() string {
	return fmt.Sprint(s.Value)
}

type SqlBinaryExpr struct {
	Left, Right SqlExpr
	Op          string
}

func (s *SqlBinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", s.Left, s.Op, s.Right)
}

var _ SqlExpr = (*SqlIdentifier)(nil)
var _ SqlExpr = (*SqlStringLiteral)(nil)
var _ SqlExpr = (*SqlBinaryExpr)(nil)
