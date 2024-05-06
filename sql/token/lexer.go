package token

import (
	"fmt"
	"unicode"
	"unicode/utf8"
)

const eof rune = -1

type Lexer struct {
	input  string
	start  int
	pos    int
	width  int
	tokens chan Token
}

type stateFn func(*Lexer) stateFn

func Lex(input string) *Lexer {
	l := &Lexer{
		input: input,
		// Rough heuristic for buffer size: avg lexeme is 3 chars
		tokens: make(chan Token, len(input)/3),
	}
	go l.run()
	return l
}

func (l *Lexer) NextToken() Token { return <-l.tokens }

func (l *Lexer) run() {
	for state := lexInitial; state != nil; {
		state = state(l)
	}
	close(l.tokens)
}

func (l *Lexer) cur() string {
	return l.input[l.start:l.pos]
}

func (l *Lexer) emit(name TokenName) {
	l.tokens <- Token{
		Name: name,
		Val:  l.cur(),
		Pos:  l.start,
	}
	l.start = l.pos
}

func (l *Lexer) next() (r rune) {
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}
	r, l.width = utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += l.width
	return r
}

func (l *Lexer) ignore() {
	l.start = l.pos
}

func (l *Lexer) backup() {
	l.pos -= l.width
}

func (l *Lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

func (l *Lexer) errorf(format string, args ...any) stateFn {
	l.tokens <- Token{
		Name: ERROR,
		Val:  fmt.Sprintf(format, args...),
		Pos:  l.pos,
	}
	return nil
}

func lexInitial(l *Lexer) stateFn {
	for {
		switch r := l.next(); {
		case r == eof:
			l.emit(EOF)
			return nil
		case unicode.IsSpace(r):
			l.ignore()
		case isAlpha(r):
			return lexWord
		case isDigit(r):
			return lexNumber
		case isPeriod(r):
			if isDigit(l.peek()) {
				return lexFloat
			}
			l.emit(PERIOD)
		case isQuote(r):
			l.ignore()
			return lexQuote
		case isOperatorStart(r):
			possibleOps := operatorsStartingWith(r)
			if len(possibleOps) == 1 {
				l.emit(possibleOps[0])
				continue
			}

			if isAlphaNumeric(l.peek()) || unicode.IsSpace(l.peek()) {
				possibleOps = operatorsStartingWith(r, 0)
			} else {
				possibleOps = operatorsStartingWith(r, l.next())
			}
			if len(possibleOps) == 1 {
				l.emit(possibleOps[0])
				continue
			}

			return l.errorf("no known operator starting with: %s", l.cur())
		}
	}
}

func lexWord(l *Lexer) stateFn {
	for isAlphaNumeric(l.next()) {
		// continue to end of run
	}
	l.backup()

	tok, isKeyword := LookupKeyword(l.cur())
	if isKeyword {
		l.emit(tok)
	} else {
		l.emit(IDENT)
	}

	return lexInitial
}

func lexQuote(l *Lexer) stateFn {
	for {
		switch r := l.next(); {
		case r == eof:
			return l.errorf("unterminated quoted string: %s", l.cur())
		case isQuote(r):
			l.backup()
			l.emit(STRING)
			l.next()
			l.ignore()
			return lexInitial
		}
	}
}

func lexNumber(l *Lexer) stateFn {
	r := l.next()
	for isDigit(r) {
		r = l.next()
		// continue to end of run
	}

	if r == '.' {
		return lexFloat
	}

	l.backup()
	l.emit(INT)
	return lexInitial
}

func lexFloat(l *Lexer) stateFn {
	for isDigit(l.next()) {
		// continue to end of run
	}
	l.backup()
	l.emit(FLOAT)
	return lexInitial
}

func isAlpha(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

func isAlphaNumeric(r rune) bool {
	return isAlpha(r) || unicode.IsDigit(r)
}

func isOperatorStart(r rune) bool {
	return len(operatorsStartingWith(r)) > 0
}

func isQuote(r rune) bool {
	return r == '\''
}

func isDigit(r rune) bool {
	return unicode.IsDigit(r)
}

func isPeriod(r rune) bool {
	return r == '.'
}
