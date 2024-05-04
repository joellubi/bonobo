package token

type TokenStream interface {
	Next() (Token, bool)
	Peek() (Token, bool)
}

func NewTokenStream(lex *Lexer) *tokenStream {
	first := lex.NextToken()
	return &tokenStream{lex: lex, next: first}
}

type tokenStream struct {
	lex  *Lexer
	next Token
}

func (ts *tokenStream) Next() (Token, bool) {
	tok, more := ts.Peek()
	if more {
		ts.next = ts.lex.NextToken()
	}
	return tok, more
}

func (ts *tokenStream) Peek() (Token, bool) {
	more := ts.next.Name != EOF
	return ts.next, more
}

func NewListTokenStream(tokens []Token) *listTokenStream {
	return &listTokenStream{tokens: tokens}
}

type listTokenStream struct {
	tokens []Token
	cur    int
}

func (ts *listTokenStream) Next() (Token, bool) {
	tok, more := ts.Peek()
	if more {
		ts.cur++
	}

	return tok, more
}

func (ts *listTokenStream) Peek() (Token, bool) {
	if ts.cur >= len(ts.tokens) {
		return Token{Name: ERROR, Val: "unexpected: reached end of input without encountering EOF token"}, false
	}

	tok := ts.tokens[ts.cur]
	more := tok.Name != EOF
	return tok, more
}

var _ TokenStream = (*tokenStream)(nil)
var _ TokenStream = (*listTokenStream)(nil)
