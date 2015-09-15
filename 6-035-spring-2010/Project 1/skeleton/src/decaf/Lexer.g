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
    "false";
    "for";
    "if";
    "int";
    "return";
    "true";
    "void";
}

LCURLY : "{";

RCURLY : "}";

LBRACKET : "[";

RBRACKET : "]";

LPARENT : "(";

RPARENT : ")";

DOT : ",";

SEMICOLON : ";";

ASSIGN : "=";

PLUS : "+";

MINUS : "-";

MULTIPLY : "*";

LT : "<";

LTE : "<=";

GT : ">";

GTE : ">=";

EQ : "==";

NEQ : "!=";

AND : "&&";

OR : "||";

ID :
    ('_' | 'a'..'z' | 'A'..'Z')('_' | 'a' .. 'z' | 'A'..'Z' | '0'..'9')*;

WS : (' ' | '\t' | '\n' {newline();}) {_ttype = Token.SKIP; };

SL_COMMENT : "//" (~'\n')* '\n' {_ttype = Token.SKIP; newline (); };


CHAR : '\'' (ASCII_CHAR | ESC_CHAR) '\'';

STRING : '"' (ASCII_CHAR | ESC_CHAR)* '"';

protected
ASCII_CHAR
    : ('\u0020'..'\u0021')
    | ('\u0023'..'\u0026')
    | ('\u0028'..'\u005b')
    | ('\u005d'..'\u007e');

protected
ESC_CHAR :  '\\' ('"' | '\'' | '\\' | 't' | 'n');

INT : DECIMAL | "0x" HEXDECIMAL;

protected
DECIMAL : ('0'..'9')+;

protected
HEXDECIMAL : ('0'..'9' | 'a'..'f' | 'A'..'F')+;
