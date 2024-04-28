package parse

import "fmt"

type SqlExpr interface {
	fmt.Stringer
	SqlNode
}

type SqlIdentifier struct {
	ID string
}

func (*SqlIdentifier) Children() []SqlNode {
	return nil
}

func (s *SqlIdentifier) String() string {
	return s.ID
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

var _ SqlExpr = (*SqlIdentifier)(nil)
var _ SqlExpr = (*SqlStringLiteral)(nil)
var _ SqlExpr = (*SqlIntLiteral)(nil)
var _ SqlExpr = (*SqlBinaryExpr)(nil)
