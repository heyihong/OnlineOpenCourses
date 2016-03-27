header {package decaf;}

options 
{
  mangleLiteralPrefix = "TK_";
  language="Java";
}

class DecafScanner extends Lexer;
options 
{
  k=2;
}

tokens 
{
    "boolean";
    "break";
    "callout";
    "class";
    "continue";
    "else";
    "for";
    "if";
    "int";
    "return";
    "void";
    "true";
    "false";
}

LCURLY : "{";

RCURLY : "}";

LBRACKET : "[";

RBRACKET : "]";

LPARENT : "(";

RPARENT : ")";

COMMA : ",";

SEMICOLON : ";";

PLUS: "+";

MINUS: "-";

MULTIPLY: "*";

DIVIDE: "/";

MODULO: "%";

EQUAL_ASSIGN: "=";

PLUS_ASSIGN: "+=";

MINUS_ASSIGN: "-=";

LT: "<";

GT: ">";

LTEQ: "<=";

GTEQ: ">=";

EQ: "==";

NEQ: "!=";

NOT: "!";

AND: "&&";

OR: "||";

WS : (' ' | '\t' | '\n' {newline();}) {_ttype = Token.SKIP; };

SL_COMMENT : "//" (~'\n')* '\n' {_ttype = Token.SKIP; newline (); };

protected
CHAR
    : ('\u0020'..'\u0021')
    | ('\u0023'..'\u0026')
    | ('\u0028'..'\u005b')
    | ('\u005d'..'\u007e')
    | '\\' ('"' | '\'' | '\\' | 't' | 'n');

ID : ALPHA (ALPHA_NUM)*;

protected
ALPHA_NUM
    : ALPHA | DIGIT;

protected
ALPHA
    : '_'
    | 'a'..'z'
    | 'A'..'Z';

protected
DIGIT
    : '0'..'9';

protected
HEX_DIGIT
    : DIGIT | 'a'..'f' | 'A'..'F';

INT_LITERAL : DECIMAL_LITERAL | HEX_LITERAL;

protected
DECIMAL_LITERAL : ('0'..'9')+;

protected
HEX_LITERAL : "0x" (HEX_DIGIT)+;

CHAR_LITERAL : '\'' CHAR '\'';

STRING_LITERAL : '"' (CHAR)* '"';
